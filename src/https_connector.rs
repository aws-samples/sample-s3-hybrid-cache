//! Custom HTTPS Connector for Hyper Connection Pooling
//!
//! This module provides a custom connector that integrates Hyper's connection pooling
//! with the existing ConnectionPoolManager for IP selection and load balancing.
//! Applies TCP socket options (keepalive, receive buffer) via socket2 before TLS handshake.
//!
//! Also provides [`build_tls_config_for_host`], the single source of truth for
//! outbound TLS version selection: override-matched hosts get TLS 1.2 only;
//! all others get the default (TLS 1.2 or 1.3).

use crate::config::ConnectionPoolConfig;
use crate::connection_pool::{ConnectionPoolManager, IpHealthTracker};
use crate::upstream_overrides::{TransportMode, UpstreamOverrides};
use crate::{ProxyError, Result};
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper::Uri;
use hyper_util::client::legacy::connect::{CaptureConnection, Connected, Connection};
use rustls::pki_types::ServerName;
use socket2::{Socket, TcpKeepalive};
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::{client::TlsStream, TlsConnector};
use tower::Service;
use tracing::{debug, warn};

/// Build the appropriate `rustls::ClientConfig` for an outbound connection to `host`.
///
/// Override-matched hosts (those present in `pool_manager.resolve_override`) receive
/// a TLS 1.2-only configuration (VPC interface endpoints / PrivateLink don't support
/// TLS 1.3). All other hosts receive the default configuration (TLS 1.2 or 1.3).
///
/// This is the single source of truth for outbound TLS version selection — all
/// callsites that need a per-host `ClientConfig` should use this function.
pub(crate) fn build_tls_config_for_host(
    host: &str,
    root_store: Arc<rustls::RootCertStore>,
    pool_manager: &ConnectionPoolManager,
) -> rustls::ClientConfig {
    if pool_manager.resolve_override(host).is_some() {
        debug!(
            host = %host,
            "Using TLS 1.2-only config for PrivateLink/override-matched destination"
        );
        build_tls12_only_config(root_store)
    } else {
        build_tls_default_config(root_store)
    }
}

/// Build the default TLS configuration (TLS 1.2 or 1.3).
pub(crate) fn build_tls_default_config(
    root_store: Arc<rustls::RootCertStore>,
) -> rustls::ClientConfig {
    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// Build a TLS 1.2-only configuration for PrivateLink / VPC interface endpoints.
pub(crate) fn build_tls12_only_config(
    root_store: Arc<rustls::RootCertStore>,
) -> rustls::ClientConfig {
    rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// A rustls certificate verifier that accepts **any** server certificate without
/// performing chain validation, hostname/SNI verification, or signature checks.
///
/// **Security warning:** this disables MITM protection on the proxy→origin leg. It is
/// used **only** for the `TlsUnvalidated` upstream-override transport mode, where an
/// operator has explicitly opted out of certificate validation for a destination in a
/// trusted network (e.g. a store with a self-signed cert). It MUST NOT be used for the
/// default verified-TLS-on-443 egress or the `TlsValidated` mode, which verify against
/// the system trust store via [`build_tls_default_config`] / [`build_tls12_only_config`].
#[derive(Debug)]
struct AcceptAnyServerCert;

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // Accept unconditionally: no chain, no hostname, no expiry check.
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        // Advertise the schemes the active (ring) crypto provider can verify, so the
        // handshake offers a sane set even though we accept any signature.
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Build a TLS configuration that accepts **any** server certificate (no verification).
///
/// **Security warning:** this disables MITM protection on the proxy→origin leg and is
/// intended **only** for the `TlsUnvalidated` upstream-override transport mode. Unlike
/// [`build_tls_default_config`] / [`build_tls12_only_config`], it does not consult the
/// system trust store — it installs [`AcceptAnyServerCert`] as a custom verifier via the
/// `dangerous()` builder. Use it solely when an operator has explicitly opted out of
/// certificate validation for a specific destination.
pub fn build_tls_accept_any_config() -> rustls::ClientConfig {
    rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert))
        .with_no_client_auth()
}

/// Outbound caching-egress stream. Either a TLS stream (verified-TLS-on-443 default
/// path, plus the `TlsValidated`/`TlsUnvalidated` override modes) or a plaintext TCP
/// stream (the `Plaintext` override mode). Both `TlsStream<TcpStream>` and `TcpStream`
/// implement tokio's `AsyncRead`/`AsyncWrite`, so each arm delegates to its inner stream.
///
/// The `Tls` payload is boxed because `TlsStream` is far larger than a bare
/// `TcpStream`; boxing keeps the enum small (clippy `large_enum_variant`) at the cost
/// of one allocation per (long-lived, pooled) connection.
pub enum HttpsStreamKind {
    Tls(Box<TlsStream<TcpStream>>),
    Plain(TcpStream),
}

/// An outbound stream together with the instant it was established.
///
/// The establishment instant is a property of the stream rather than of the
/// moment [`Connection::connected`] happens to be called, so a connection's age
/// is well defined for the whole life of the connection.
pub struct HttpsStream {
    kind: HttpsStreamKind,
    established_at: Instant,
}

/// The instant a pooled upstream connection was established, carried as a
/// `Connected` extra.
///
/// hyper computes `Connected` once, when the connection is made, and hands the
/// same value back on every later checkout of that connection — so a stamp
/// placed here is how a request that reuses a connection can learn how old the
/// connection is. This is what makes `connection_pool.max_lifetime` enforceable:
/// hyper's pool builder has no maximum-age option of its own.
#[derive(Clone, Copy, Debug)]
pub struct ConnectionEstablished(pub Instant);

impl HttpsStream {
    /// Wrap a completed TLS stream, stamped as established now.
    pub fn tls(stream: TlsStream<TcpStream>) -> Self {
        Self {
            kind: HttpsStreamKind::Tls(Box::new(stream)),
            established_at: Instant::now(),
        }
    }

    /// Wrap a plaintext stream, stamped as established now.
    pub fn plain(stream: TcpStream) -> Self {
        Self {
            kind: HttpsStreamKind::Plain(stream),
            established_at: Instant::now(),
        }
    }

    /// Wrap a plaintext stream with an explicit establishment instant.
    ///
    /// Public so the max-lifetime cap can be exercised against a genuinely
    /// over-age connection without a test having to wait out a lifetime. No
    /// production path calls this — production stamps the real connect instant
    /// via [`HttpsStream::plain`] and [`HttpsStream::tls`].
    pub fn plain_with_established_at(stream: TcpStream, established_at: Instant) -> Self {
        Self {
            kind: HttpsStreamKind::Plain(stream),
            established_at,
        }
    }

    /// The instant this connection was established.
    pub fn established_at(&self) -> Instant {
        self.established_at
    }
}

impl Read for HttpsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let mut tokio_buf = tokio::io::ReadBuf::uninit(unsafe { buf.as_mut() });
        let poll = match &mut self.kind {
            HttpsStreamKind::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, &mut tokio_buf),
            HttpsStreamKind::Plain(s) => Pin::new(s).poll_read(cx, &mut tokio_buf),
        };
        match poll {
            Poll::Ready(Ok(())) => {
                let filled = tokio_buf.filled().len();
                unsafe {
                    buf.advance(filled);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Write for HttpsStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.kind {
            HttpsStreamKind::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
            HttpsStreamKind::Plain(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.kind {
            HttpsStreamKind::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
            HttpsStreamKind::Plain(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.kind {
            HttpsStreamKind::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            HttpsStreamKind::Plain(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl Connection for HttpsStream {
    fn connected(&self) -> Connected {
        Connected::new().extra(ConnectionEstablished(self.established_at))
    }
}

/// How long ago the connection described by `connected` was established, or
/// `None` when it carries no [`ConnectionEstablished`] stamp (which is the case
/// for any connection made by a connector other than [`CustomHttpsConnector`]).
pub fn connection_age(connected: &Connected, now: Instant) -> Option<Duration> {
    let mut extras = hyper::http::Extensions::new();
    connected.get_extras(&mut extras);
    extras
        .get::<ConnectionEstablished>()
        .map(|established| now.saturating_duration_since(established.0))
}

/// Retire the connection a request was checked out onto if it has reached
/// `max_lifetime`, and report whether it was retired.
///
/// Retirement is `Connected::poison`, which is a **pool-reuse** decision and
/// nothing else: hyper consults it when popping an idle connection, when the
/// idle reaper runs, and when a finished request's connection is about to go
/// back into the pool. It is never consulted while a request is in flight, so a
/// transfer already running on the connection reads and writes to completion
/// exactly as it would have. That is the property that makes this safe on a
/// proxy whose whole workload is long transfers — the cap takes a connection out
/// of service, it does not cut one off.
///
/// hyper's pool has no hook that runs on the reuse of an existing connection, so
/// the earliest point a cap can act is immediately after checkout. An over-age
/// connection therefore serves the request that discovered it and is never
/// handed out again.
pub fn retire_if_over_max_lifetime(
    capture: &CaptureConnection,
    max_lifetime: Duration,
    now: Instant,
) -> bool {
    let metadata = capture.connection_metadata();
    let Some(connected) = metadata.as_ref() else {
        // No connection was established for this request (a connect failure), or
        // the capture handle was never filled in.
        return false;
    };

    match connection_age(connected, now) {
        Some(age) if age >= max_lifetime => {
            connected.poison();
            debug!(
                age_secs = age.as_secs(),
                max_lifetime_secs = max_lifetime.as_secs(),
                "[HTTPS_CONNECTOR] Connection reached max_lifetime — retiring it from the pool; \
                 the in-flight request on it is unaffected"
            );
            true
        }
        _ => false,
    }
}

/// Custom HTTPS connector that integrates with ConnectionPoolManager.
///
/// Applies TCP keepalive and receive buffer options via socket2 on new connections.
/// Records connection failures in IpHealthTracker for automatic IP exclusion.
/// Uses [`build_tls_config_for_host`] to select the correct TLS version per host.
pub struct CustomHttpsConnector {
    pool_manager: Arc<tokio::sync::RwLock<ConnectionPoolManager>>,
    /// Root certificate store for building per-host TLS configurations.
    root_store: Arc<rustls::RootCertStore>,
    config: ConnectionPoolConfig,
    health_tracker: Arc<IpHealthTracker>,
    metrics_manager:
        Arc<tokio::sync::RwLock<Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>>>,
    /// Per-destination upstream transport overrides. Shared with `S3Client`
    /// (which uses it to skip IP-distribution rewriting for matched targets).
    /// Empty by default — every destination then takes the verified-TLS-on-443
    /// path verbatim (Secure_Default_Behaviour).
    upstream_overrides: Arc<UpstreamOverrides>,
}

impl CustomHttpsConnector {
    pub fn new(
        pool_manager: Arc<tokio::sync::RwLock<ConnectionPoolManager>>,
        root_store: Arc<rustls::RootCertStore>,
        config: ConnectionPoolConfig,
        health_tracker: Arc<IpHealthTracker>,
        upstream_overrides: Arc<UpstreamOverrides>,
    ) -> Self {
        Self {
            pool_manager,
            root_store,
            config,
            health_tracker,
            metrics_manager: Arc::new(tokio::sync::RwLock::new(None)),
            upstream_overrides,
        }
    }

    pub(crate) fn set_metrics_manager_ref(
        &mut self,
        metrics_manager: Arc<
            tokio::sync::RwLock<Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>>,
        >,
    ) {
        self.metrics_manager = metrics_manager;
    }

    /// Probe a single excluded IP for reachability, then close the connection.
    ///
    /// Performs the same TCP connect **and** TLS handshake the live egress path
    /// performs, honouring the resolved `upstream_overrides` transport mode and
    /// port, and using `endpoint` for SNI. Covering the handshake matters because
    /// `record_failure` fires on TLS-handshake failure as well as TCP-connect
    /// failure, so a TCP-only probe would re-admit a host that accepts TCP and
    /// then fails to negotiate TLS, causing it to flap straight back out.
    ///
    /// Deliberately does **not** apply keepalive or `SO_RCVBUF` socket options,
    /// record connection metrics, or feed `record_failure`: the socket is dropped
    /// immediately and the caller owns the probe-result bookkeeping, so a probe
    /// must not perturb the live pool's counters or advance the failure count that
    /// excluded the IP in the first place.
    pub(crate) async fn probe_ip(
        &self,
        ip: IpAddr,
        endpoint: &str,
        timeout: Duration,
    ) -> Result<()> {
        // Resolve the transport exactly as the egress path does. The caching
        // egress is HTTP-origin, so a portless endpoint defaults to 80, not 443.
        let target_port = 80;
        let transport_mode = self.upstream_overrides.resolve(endpoint, target_port);
        let port = connect_port(transport_mode, target_port);

        tokio::time::timeout(timeout, async {
            let tcp = TcpStream::connect((ip, port)).await.map_err(|e| {
                ProxyError::ConnectionError(format!(
                    "Probe TCP connect failed to {}:{}: {}",
                    ip, port, e
                ))
            })?;

            let tls_config = match transport_mode {
                // Plaintext upstreams have no handshake to verify; a completed
                // TCP connect is the whole reachability signal available.
                Some(TransportMode::Plaintext) => return Ok(()),
                Some(TransportMode::TlsUnvalidated) => build_tls_accept_any_config(),
                Some(TransportMode::TlsValidated) | None => {
                    let pm = self.pool_manager.read().await;
                    build_tls_config_for_host(endpoint, Arc::clone(&self.root_store), &pm)
                }
            };

            let server_name = ServerName::try_from(upstream_certificate_name(endpoint).to_string())
                .map_err(|e| {
                    ProxyError::TlsError(format!("Probe invalid server name '{}': {}", endpoint, e))
                })?;

            TlsConnector::from(Arc::new(tls_config))
                .connect(server_name, tcp)
                .await
                .map_err(|e| {
                    ProxyError::TlsError(format!(
                        "Probe TLS handshake failed to {} ({}): {}",
                        endpoint, ip, e
                    ))
                })?;

            Ok(())
        })
        .await
        .map_err(|_| {
            ProxyError::TimeoutError(format!(
                "Probe timed out after {:?} connecting to {}:{}",
                timeout, ip, port
            ))
        })?
    }
}

/// Select the TCP connect port for the caching egress.
///
/// A matched override (`Some(_)`) connects to the request authority's port — the
/// port the client targeted, which is also the override lookup key. An unmatched
/// target (`None`) takes the unchanged Secure_Default_Behaviour port, the literal
/// 443, regardless of any non-443 port carried by the authority. The transport
/// mode itself does not affect the port — all three override modes connect to the
/// authority port; only the override-vs-default distinction matters here.
fn connect_port(mode: Option<TransportMode>, target_port: u16) -> u16 {
    match mode {
        Some(_) => target_port,
        None => 443,
    }
}

/// Record a connection creation against the endpoint key the request side also
/// uses, or record NOTHING and say so at WARN.
///
/// `connection_pool.connections_reused` is derived per key as
/// `total_requests - connections_created`, and the request-side producer
/// (`MetricsManager::record_request_to_endpoint`, called from `S3Client` with
/// `S3RequestContext::host`) keys on the port-stripped signed Host. This
/// connector only ever sees a `Uri`, which IP distribution may have rewritten to
/// an IP literal, so it must map back to that same string before it can file a
/// creation that the subtraction can match.
///
/// A `None` key means the mapping failed. Recording under the dotted quad — the
/// behaviour before this function existed — guarantees a wrong published figure
/// in **both** directions: the real endpoint's `connections_created` reads low so
/// its `connections_reused` reads high (up to a spurious 100% reuse), and a junk
/// IP-literal key appears in `connections_created` that no request ever produces.
/// Skipping loses one creation for one endpoint, which biases that endpoint's
/// reuse high by one, and leaves the two maps' key sets coherent. The WARN is the
/// fleet-visible signal that it happened.
async fn record_connection_created_if_keyable(
    metrics_manager: &Arc<
        tokio::sync::RwLock<Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>>,
    >,
    metrics_endpoint_key: Option<&str>,
    connect_ip: &IpAddr,
) {
    let Some(endpoint) = metrics_endpoint_key else {
        warn!(
            "[HTTPS_CONNECTOR] No metrics endpoint key for IP {}, not recording a \
             connection creation (connection_pool.connections_reused for the \
             owning endpoint will read high by one)",
            connect_ip
        );
        return;
    };
    let mm = metrics_manager.read().await;
    if let Some(ref metrics) = *mm {
        metrics
            .read()
            .await
            .record_connection_created(endpoint)
            .await;
    }
}

/// Apply TCP socket options (keepalive + receive buffer) via socket2.
/// Converts TcpStream → socket2::Socket → apply options → convert back.
/// Errors are logged as warnings and do not fail the connection.
fn apply_socket_options(
    tcp: TcpStream,
    config: &ConnectionPoolConfig,
) -> std::result::Result<TcpStream, ProxyError> {
    let std_stream = tcp.into_std().map_err(|e| {
        ProxyError::ConnectionError(format!("Failed to convert TcpStream to std: {}", e))
    })?;

    let socket = Socket::from(std_stream);

    // TCP keepalive
    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(config.keepalive_idle_secs))
        .with_interval(Duration::from_secs(config.keepalive_interval_secs))
        .with_retries(config.keepalive_retries);
    if let Err(e) = socket.set_tcp_keepalive(&keepalive) {
        warn!("Failed to set TCP keepalive: {}", e);
    }

    // Receive buffer
    if let Some(size) = config.tcp_recv_buffer_size {
        if let Err(e) = socket.set_recv_buffer_size(size) {
            warn!("Failed to set SO_RCVBUF to {}: {}", size, e);
        }
    }

    let std_stream: std::net::TcpStream = socket.into();
    let tcp = TcpStream::from_std(std_stream).map_err(|e| {
        ProxyError::ConnectionError(format!(
            "Failed to convert std TcpStream back to tokio: {}",
            e
        ))
    })?;

    tcp.set_nodelay(true).ok();

    Ok(tcp)
}

/// The name an upstream certificate is checked against, and sent as TLS SNI, for
/// `host`.
///
/// S3's certificate for an endpoint covers the endpoint and `*.<endpoint>`, and a
/// wildcard matches exactly one label, so the virtual-hosted host of a bucket whose
/// name contains a dot (`my.bucket.s3.us-west-2.amazonaws.com`) is never covered.
/// For such a host this returns the S3 endpoint with the bucket removed
/// (`s3.us-west-2.amazonaws.com`), which S3's certificate does cover; S3 routes the
/// request by its `Host` header, which stays as the client sent it. Every other
/// host, including a non-dotted bucket host, a path-style host and any host outside
/// S3's own domains, is returned unchanged.
///
/// The endpoint is matched from the right against S3's endpoint shapes
/// (`s3.<region>`, `s3.dualstack.<region>`, `s3-fips.<region>`,
/// `s3-fips.dualstack.<region>`, the legacy `s3-<region>`, and the global `s3`)
/// under `amazonaws.com`, or under `amazonaws.com.cn` for the regional forms. A
/// bucket name holding an `s3` label (`a.s3.b.s3.us-west-2.amazonaws.com`, bucket
/// `a.s3.b`) therefore still resolves to the real endpoint. The result is always a
/// right-hand suffix of `host` ending in one of those S3 endpoint names, so it can
/// never name a host outside S3's domains.
pub(crate) fn upstream_certificate_name(host: &str) -> &str {
    s3_dotted_bucket_endpoint(host).unwrap_or(host)
}

/// The S3 endpoint suffix of a virtual-hosted host whose bucket part contains a
/// dot, or `None` for any other host.
fn s3_dotted_bucket_endpoint(host: &str) -> Option<&str> {
    let lower = host.to_ascii_lowercase();
    let (rest, china) = if let Some(rest) = lower.strip_suffix(".amazonaws.com") {
        (rest, false)
    } else {
        (lower.strip_suffix(".amazonaws.com.cn")?, true)
    };
    let labels: Vec<&str> = rest.split('.').collect();
    let n = labels.len();
    let last = labels[n - 1];
    let from_right = |k: usize| n.checked_sub(k + 1).map(|i| labels[i]);
    // How many trailing labels form the S3 endpoint, in front of the zone.
    let endpoint_labels = if is_aws_region(last) {
        match (from_right(1), from_right(2)) {
            (Some("dualstack"), Some("s3" | "s3-fips")) => 3,
            (Some("s3" | "s3-fips"), _) => 2,
            _ => return None,
        }
    } else if (last == "s3" && !china) || last.strip_prefix("s3-").is_some_and(is_aws_region) {
        1
    } else {
        return None;
    };
    // The bucket is every label left of the endpoint. Fewer than two labels means
    // no dot in the bucket (or no bucket at all: path-style), which S3's wildcard
    // already covers, so the name stays as it is.
    let bucket_labels = n.checked_sub(endpoint_labels)?;
    if bucket_labels < 2 || labels[..bucket_labels].iter().any(|l| l.is_empty()) {
        return None;
    }
    // Byte offset of the endpoint in `host`: the bucket labels plus one dot after
    // each. ASCII lowercasing preserves byte lengths, so the offset holds in `host`.
    let offset = labels[..bucket_labels]
        .iter()
        .map(|l| l.len() + 1)
        .sum::<usize>();
    host.get(offset..)
}

/// Whether `label` has the shape of an AWS region name (`us-west-2`,
/// `us-gov-west-1`, `cn-north-1`): a two-letter prefix, one or more alphabetic
/// parts, and a numeric suffix, joined by hyphens.
fn is_aws_region(label: &str) -> bool {
    let parts: Vec<&str> = label.split('-').collect();
    parts.len() >= 3
        && parts[0].len() == 2
        && parts[0].bytes().all(|b| b.is_ascii_lowercase())
        && parts[1..parts.len() - 1]
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_lowercase()))
        && {
            let suffix = parts[parts.len() - 1];
            !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
        }
}

/// Choose the TLS server name for a request whose URI authority is an IP literal.
///
/// Prefers the host the request itself targeted, which
/// [`S3Client`](crate::s3_client::S3Client)'s IP-distribution rewrite carries in the
/// rewritten authority's userinfo. That name is the one the client signed and the one
/// the upstream certificate has to cover, so it is the only correct SNI and
/// certificate-validation name for this request.
///
/// Falls back to [`ConnectionPoolManager::get_hostname_for_ip`] when the authority
/// carries no usable name, which is every URI shape with no request host behind it:
/// the recovery probe and the health-exclusion path, an `upstream_overrides` target
/// configured as an IP literal, and a client whose own `Host` is an address. That
/// lookup answers with the lexicographically smallest endpoint holding the address,
/// so it can name a host this request never asked for; it is consulted only where
/// there is no request name to prefer.
///
/// `None` means no name at all, the caller's existing "no mapping found" branch.
pub(crate) fn recover_tls_name(uri: &Uri, pool_manager: &ConnectionPoolManager) -> Option<String> {
    if let Some(carried) = carried_tls_name(uri) {
        return Some(carried);
    }
    uri.host()
        .and_then(|host| host.parse::<IpAddr>().ok())
        .and_then(|ip| pool_manager.get_hostname_for_ip(&ip))
}

/// Read the host carried in a rewritten authority's userinfo, if it is usable as a
/// TLS server name.
///
/// The carrier holds the client's `Host` header, which the proxy has checked for RFC
/// 3986 authority syntax and nothing further. That is a weaker rule than a DNS name
/// follows, so the value is validated here with the same `ServerName::try_from` the
/// handshake applies: a name rustls would reject yields `None` and the caller falls
/// back, which can never panic and never widens what the handshake will accept.
fn carried_tls_name(uri: &Uri) -> Option<String> {
    let (userinfo, _host_and_port) = uri.authority()?.as_str().rsplit_once('@')?;
    if userinfo.is_empty() {
        return None;
    }
    ServerName::try_from(userinfo.to_string()).ok()?;
    Some(userinfo.to_string())
}

impl Service<Uri> for CustomHttpsConnector {
    type Response = HttpsStream;
    type Error = ProxyError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let pool_manager = Arc::clone(&self.pool_manager);
        let root_store = Arc::clone(&self.root_store);
        let metrics_manager = self.metrics_manager.clone();
        let config = self.config.clone();
        let health_tracker = Arc::clone(&self.health_tracker);
        let upstream_overrides = Arc::clone(&self.upstream_overrides);

        Box::pin(async move {
            let uri_host = uri
                .host()
                .ok_or_else(|| ProxyError::ConfigError("No host in URI".to_string()))?;

            // Resolve the upstream transport mode from the request authority. The
            // caching egress is HTTP-origin, so a portless authority defaults to 80
            // (NOT 443). The lookup is keyed on the URI authority host — for a
            // matched override the IP-distribution rewrite is skipped upstream
            // (task 4.2), so this is the un-rewritten hostname or IP literal.
            let target_port = uri.port_u16().unwrap_or(80);
            let transport_mode = upstream_overrides.resolve(uri_host, target_port);

            // Connect port: the request authority's port for a matched override;
            // the literal 443 for the unchanged Secure_Default_Behaviour path.
            let connect_port = connect_port(transport_mode, target_port);

            // Determine connect IP and TLS hostname.
            //
            // `metrics_endpoint_key` is resolved SEPARATELY from `tls_hostname` and
            // is NOT the same string. `tls_hostname` is an SNI name; the metric key
            // has to be the endpoint hostname the request side signs, because
            // `connection_pool.connections_reused` is derived per key as
            // `total_requests - connections_created` and the request side keys on
            // `S3RequestContext::host`. A `None` here means "file nothing", never
            // "file it under the dotted quad" — see
            // `ConnectionPoolManager::get_metrics_endpoint_key`.
            let (connect_ip, tls_hostname, metrics_endpoint_key) = if let Ok(ip) =
                uri_host.parse::<IpAddr>()
            {
                // URI host is an IP — IpDistributor already selected it. The TLS
                // name is the host this request targeted, carried in the authority's
                // userinfo by `rewrite_uri_authority`; `recover_tls_name` falls back
                // to the pool manager's reverse lookup only for a URI that carries
                // none. Both failure paths below key their distributor on the same
                // string, so an exclusion lands on the endpoint whose request failed.
                let (hostname, metrics_key) = {
                    let pm = pool_manager.read().await;
                    (
                        recover_tls_name(&uri, &pm),
                        pm.get_metrics_endpoint_key(&ip),
                    )
                };

                match hostname {
                    Some(h) => {
                        debug!(
                            "[HTTPS_CONNECTOR] URI host is IP {}, using hostname '{}' for TLS SNI",
                            ip, h
                        );
                        (ip, h, metrics_key)
                    }
                    None => {
                        warn!(
                            "[HTTPS_CONNECTOR] No hostname mapping found for IP {}, TLS SNI may fail",
                            ip
                        );
                        (ip, uri_host.to_string(), metrics_key)
                    }
                }
            } else {
                // URI host is a regular hostname — resolve via the pool manager's
                // configured DNS resolver (Google/Cloudflare/custom), which bypasses
                // /etc/hosts. This matches the HTTP proxy's resolution behaviour.
                debug!(
                    "[HTTPS_CONNECTOR] Resolving hostname via configured DNS: {}",
                    uri_host
                );
                let ips = {
                    let pm = pool_manager.read().await;
                    pm.resolve_endpoint(uri_host).await?
                };
                let ip = *ips.first().ok_or_else(|| {
                    ProxyError::ConnectionError(format!("No addresses found for {}", uri_host))
                })?;
                // The authority was never rewritten, so this IS the host the
                // request side signs and keys its counter on.
                (ip, uri_host.to_string(), Some(uri_host.to_string()))
            };

            // Establish TCP connection to the resolved port (authority port for a
            // matched override, else the literal 443 for Secure_Default_Behaviour).
            let tcp = TcpStream::connect((connect_ip, connect_port))
                .await
                .map_err(|e| {
                    warn!(
                        "[HTTPS_CONNECTOR] TCP connection failed to {}:{}: {}",
                        connect_ip, connect_port, e
                    );
                    // Record failure for health tracking
                    if health_tracker.record_failure(&connect_ip) {
                        warn!(ip = %connect_ip, "IP failure threshold reached on TCP connect, excluding");
                        let pm = pool_manager.clone();
                        let host = tls_hostname.clone();
                        tokio::spawn(async move {
                            let mut pm = pm.write().await;
                            if let Some(dist) = pm.get_distributor_mut(&host) {
                                dist.remove_ip(connect_ip, "TCP connect failure");
                            }
                        });
                    }
                    ProxyError::ConnectionError(format!(
                        "Failed to connect to {}:{}: {}",
                        connect_ip, connect_port, e
                    ))
                })?;

            // Apply TCP socket options (keepalive + receive buffer) via socket2.
            // Applied for every transport mode (plaintext and TLS alike).
            let tcp = apply_socket_options(tcp, &config)?;

            debug!(
                "[HTTPS_CONNECTOR] TCP connection established to {}:{}",
                connect_ip, connect_port
            );

            // Select the TLS configuration per resolved transport mode:
            //  - Plaintext        → no TLS; return the bare TCP stream.
            //  - TlsUnvalidated   → accept-any verifier (no chain/hostname check).
            //  - TlsValidated     → system-roots verification (same as the 443 path).
            //  - None (default)   → existing verified-TLS-on-443 path, verbatim.
            let tls_config = match transport_mode {
                Some(TransportMode::Plaintext) => {
                    // No TLS handshake. Record the connection and return plaintext.
                    debug!(
                        "[HTTPS_CONNECTOR] Plaintext connection established to {} ({}:{})",
                        uri_host, connect_ip, connect_port
                    );
                    record_connection_created_if_keyable(
                        &metrics_manager,
                        metrics_endpoint_key.as_deref(),
                        &connect_ip,
                    )
                    .await;
                    return Ok(HttpsStream::plain(tcp));
                }
                Some(TransportMode::TlsUnvalidated) => build_tls_accept_any_config(),
                Some(TransportMode::TlsValidated) | None => {
                    let pm = pool_manager.read().await;
                    build_tls_config_for_host(&tls_hostname, root_store, &pm)
                }
            };

            let tls_connector = TlsConnector::from(Arc::new(tls_config));

            // TLS handshake. A dotted bucket's virtual-hosted name is sent and
            // checked as its S3 endpoint (`upstream_certificate_name`); the
            // distributor and health keys below stay on `tls_hostname`.
            let server_name =
                ServerName::try_from(upstream_certificate_name(&tls_hostname).to_string())
                    .map_err(|e| {
                        ProxyError::TlsError(format!(
                            "Invalid server name '{}': {}",
                            tls_hostname, e
                        ))
                    })?;

            // Only the explicit TlsValidated override surfaces a distinct,
            // non-retryable validation-failure error. The default-443 path and the
            // TlsUnvalidated path keep the existing generic TLS error behaviour.
            let is_validated_override = matches!(transport_mode, Some(TransportMode::TlsValidated));

            let tls = tls_connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| {
                    // Record failure for health tracking
                    if health_tracker.record_failure(&connect_ip) {
                        warn!(ip = %connect_ip, "IP failure threshold reached on TLS handshake, excluding");
                        let pm = pool_manager.clone();
                        let host = tls_hostname.clone();
                        tokio::spawn(async move {
                            let mut pm = pm.write().await;
                            if let Some(dist) = pm.get_distributor_mut(&host) {
                                dist.remove_ip(connect_ip, "TLS handshake failure");
                            }
                        });
                    }

                    if is_validated_override {
                        let endpoint = format!("{}:{}", uri_host, target_port);
                        warn!(
                            endpoint = %endpoint,
                            error = %e,
                            "[HTTPS_CONNECTOR] Upstream TLS validation failed"
                        );
                        ProxyError::UpstreamTlsValidationFailed {
                            endpoint,
                            source_err: e.to_string(),
                        }
                    } else {
                        warn!(
                            "[HTTPS_CONNECTOR] TLS handshake failed to {} ({}): {}",
                            tls_hostname, connect_ip, e
                        );
                        ProxyError::TlsError(format!(
                            "TLS handshake failed to {} ({}): {}",
                            tls_hostname, connect_ip, e
                        ))
                    }
                })?;

            debug!(
                "[HTTPS_CONNECTOR] TLS connection established to {} ({}:{})",
                tls_hostname, connect_ip, connect_port
            );

            // Record connection creation in metrics
            record_connection_created_if_keyable(
                &metrics_manager,
                metrics_endpoint_key.as_deref(),
                &connect_ip,
            )
            .await;

            Ok(HttpsStream::tls(tls))
        })
    }
}

impl Clone for CustomHttpsConnector {
    fn clone(&self) -> Self {
        Self {
            pool_manager: Arc::clone(&self.pool_manager),
            root_store: Arc::clone(&self.root_store),
            config: self.config.clone(),
            health_tracker: Arc::clone(&self.health_tracker),
            metrics_manager: self.metrics_manager.clone(),
            upstream_overrides: Arc::clone(&self.upstream_overrides),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConnectionPoolConfig, UpstreamOverrideConfig, UpstreamScheme};

    /// Build a connector whose overrides map `host:port` to the given transport.
    fn probe_connector_with_override(
        entries: Vec<(&str, UpstreamOverrideConfig)>,
    ) -> CustomHttpsConnector {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = ConnectionPoolConfig::default();
        let pool_manager = Arc::new(tokio::sync::RwLock::new(
            ConnectionPoolManager::new_with_config(config.clone()).unwrap(),
        ));
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let overrides: std::collections::HashMap<String, UpstreamOverrideConfig> = entries
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        CustomHttpsConnector::new(
            pool_manager,
            Arc::new(root_store),
            config,
            Arc::new(IpHealthTracker::new(3)),
            Arc::new(UpstreamOverrides::from_config(&overrides)),
        )
    }

    #[tokio::test]
    async fn test_probe_ip_targets_443_with_no_override() {
        // Secure_Default_Behaviour: an unmatched endpoint is probed on 443, the
        // same port live egress uses, so the probe tests the real path.
        let connector = probe_connector_with_override(vec![]);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let err = connector
            .probe_ip(ip, "probe-test.invalid", Duration::from_secs(3))
            .await
            .expect_err("nothing is listening on loopback:443 in CI");
        let msg = err.to_string();
        assert!(
            msg.contains("127.0.0.1:443"),
            "probe should target the default 443, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_probe_ip_plaintext_override_targets_override_port_and_skips_tls() {
        // Mirrors a plaintext upstream override on an `s3.<region>:80` key: the
        // probe must follow the override to port 80 and must not attempt a
        // handshake, since a plaintext upstream has none to verify.
        let connector = probe_connector_with_override(vec![(
            "probe-plain.invalid:80",
            UpstreamOverrideConfig {
                scheme: UpstreamScheme::Http,
                validate_tls: true,
            },
        )]);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let err = connector
            .probe_ip(ip, "probe-plain.invalid", Duration::from_secs(3))
            .await
            .expect_err("nothing is listening on loopback:80 in CI");
        let msg = err.to_string();
        assert!(
            msg.contains("127.0.0.1:80"),
            "plaintext override should target port 80, got: {msg}"
        );
        assert!(
            matches!(err, ProxyError::ConnectionError(_)),
            "a plaintext probe must fail at connect, never at TLS: {err:?}"
        );
    }

    // NOTE: the plaintext success arm (TCP connect Ok -> probe Ok, no handshake)
    // is not unit-tested. `probe_ip` derives its target port from override
    // resolution at port 80, matching live egress, so reaching it would require
    // binding privileged port 80 or 443. Distorting the production port logic to
    // make it reachable would test the distortion, not the behaviour, so this arm
    // is left to end-to-end coverage against a real plaintext upstream.

    #[tokio::test]
    async fn test_connector_creation() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let config = ConnectionPoolConfig::default();

        let pool_manager = Arc::new(tokio::sync::RwLock::new(
            ConnectionPoolManager::new_with_config(config.clone()).unwrap(),
        ));

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let health_tracker = Arc::new(IpHealthTracker::new(3));

        let upstream_overrides = Arc::new(UpstreamOverrides::from_config(
            &std::collections::HashMap::new(),
        ));

        let connector = CustomHttpsConnector::new(
            pool_manager,
            Arc::new(root_store),
            config,
            health_tracker,
            upstream_overrides,
        );

        let _connector2 = connector.clone();
    }

    #[test]
    fn test_build_tls_config_default() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = ConnectionPoolConfig::default();
        let pm = ConnectionPoolManager::new_with_config(config).unwrap();

        // Non-override host should get default TLS config (1.2 + 1.3)
        let cfg =
            build_tls_config_for_host("s3.us-east-1.amazonaws.com", Arc::new(root_store), &pm);
        // Default config supports TLS 1.2 and 1.3
        assert!(cfg.alpn_protocols.is_empty()); // no ALPN set by default
    }

    #[test]
    fn test_build_tls_config_override_matched() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        // Create a config with endpoint overrides
        let mut config = ConnectionPoolConfig::default();
        config.endpoint_overrides.insert(
            "vpce-bucket.s3.us-east-1.vpce.amazonaws.com".to_string(),
            vec!["10.0.1.100".parse().unwrap()],
        );
        let pm = ConnectionPoolManager::new_with_config(config).unwrap();

        // Override-matched host should get TLS 1.2-only config
        let _cfg = build_tls_config_for_host(
            "vpce-bucket.s3.us-east-1.vpce.amazonaws.com",
            Arc::new(root_store),
            &pm,
        );
        // The config is TLS 1.2 only — verified by the builder_with_protocol_versions call
    }

    #[test]
    fn test_build_tls_default_config_fn() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let _cfg = build_tls_default_config(Arc::new(root_store));
    }

    #[test]
    fn test_build_tls12_only_config_fn() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let _cfg = build_tls12_only_config(Arc::new(root_store));
    }

    #[test]
    fn test_build_tls_accept_any_config_fn() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        // The accept-any config builds without a root store and installs the
        // custom verifier. It is the TLS config used for the TlsUnvalidated mode.
        let _cfg = build_tls_accept_any_config();
    }

    #[test]
    fn test_accept_any_verifier_accepts_unknown_cert() {
        use rustls::client::danger::ServerCertVerifier;

        let _ = rustls::crypto::ring::default_provider().install_default();

        let verifier = AcceptAnyServerCert;

        // A bogus, untrusted certificate must still be accepted by the accept-any
        // verifier (no chain/hostname/signature checks).
        let bogus_cert = rustls::pki_types::CertificateDer::from(vec![0x30, 0x00]);
        let server_name = ServerName::try_from("self-signed.local").unwrap();
        let now = rustls::pki_types::UnixTime::now();

        let result = verifier.verify_server_cert(&bogus_cert, &[], &server_name, &[], now);
        assert!(
            result.is_ok(),
            "accept-any verifier must accept any certificate"
        );

        // It must advertise at least one supported signature scheme.
        assert!(
            !verifier.supported_verify_schemes().is_empty(),
            "accept-any verifier must advertise supported signature schemes"
        );
    }

    // ---- Port selection: authority port for a matched override, 443 for the
    //      unchanged default path (Property 7, Property 1; Req 3.4, 3.5) ----

    #[test]
    fn test_connect_port_matched_override_uses_authority_port() {
        // Every matched override mode connects to the request authority's port,
        // never the literal 443.
        assert_eq!(connect_port(Some(TransportMode::Plaintext), 9000), 9000);
        assert_eq!(connect_port(Some(TransportMode::TlsValidated), 9000), 9000);
        assert_eq!(
            connect_port(Some(TransportMode::TlsUnvalidated), 9000),
            9000
        );
        // The default-HTTP port 80 is honoured when it is the authority port.
        assert_eq!(connect_port(Some(TransportMode::Plaintext), 80), 80);
        // A matched override whose authority port happens to be 443 still uses 443.
        assert_eq!(connect_port(Some(TransportMode::TlsValidated), 443), 443);
    }

    #[test]
    fn test_connect_port_no_override_uses_443() {
        // No match → Secure_Default_Behaviour: connect on the literal 443 and
        // ignore whatever port the authority carried (regression guard).
        assert_eq!(connect_port(None, 80), 443);
        assert_eq!(connect_port(None, 9000), 443);
        assert_eq!(connect_port(None, 443), 443);
    }

    // ---- Mode resolution + port selection together, driven by the real matcher
    //      via `UpstreamOverrides::from_config` (Property 2, Property 7) ----

    fn overrides(entries: &[(&str, crate::config::UpstreamScheme, bool)]) -> UpstreamOverrides {
        let map: std::collections::HashMap<String, crate::config::UpstreamOverrideConfig> = entries
            .iter()
            .map(|(k, scheme, validate_tls)| {
                (
                    (*k).to_string(),
                    crate::config::UpstreamOverrideConfig {
                        scheme: scheme.clone(),
                        validate_tls: *validate_tls,
                    },
                )
            })
            .collect();
        UpstreamOverrides::from_config(&map)
    }

    #[test]
    fn test_plaintext_override_resolves_and_connects_on_authority_port() {
        use crate::config::UpstreamScheme;

        let o = overrides(&[("127.0.0.1:9000", UpstreamScheme::Http, true)]);

        // The matcher resolves the target to Plaintext...
        let mode = o.resolve("127.0.0.1", 9000);
        assert_eq!(mode, Some(TransportMode::Plaintext));
        // ...and the connector dials the authority port, not 443.
        assert_eq!(connect_port(mode, 9000), 9000);
    }

    #[test]
    fn test_validated_override_resolves_and_connects_on_authority_port() {
        use crate::config::UpstreamScheme;

        // https with validate_tls defaulting on → TlsValidated.
        let o = overrides(&[("store.local:9000", UpstreamScheme::Https, true)]);

        let mode = o.resolve("store.local", 9000);
        assert_eq!(mode, Some(TransportMode::TlsValidated));
        assert_eq!(connect_port(mode, 9000), 9000);

        // Virtual-hosted subdomain resolves the same and connects on the same port.
        let sub = o.resolve("bucket.store.local", 9000);
        assert_eq!(sub, Some(TransportMode::TlsValidated));
        assert_eq!(connect_port(sub, 9000), 9000);
    }

    #[test]
    fn test_unvalidated_override_resolves_and_connects_on_authority_port() {
        use crate::config::UpstreamScheme;

        let o = overrides(&[("store.local:8443", UpstreamScheme::Https, false)]);

        let mode = o.resolve("store.local", 8443);
        assert_eq!(mode, Some(TransportMode::TlsUnvalidated));
        assert_eq!(connect_port(mode, 8443), 8443);
    }

    #[test]
    fn test_off_override_authority_resolves_to_default_443_path() {
        use crate::config::UpstreamScheme;

        // A configured override on one host:port must not leak to a different
        // authority. The unmatched target resolves to None and takes the unchanged
        // verified-TLS-on-443 path even though the authority carried a non-443 port
        // (Property 1 / Property 7 regression guard; Req 3.5).
        let o = overrides(&[("store.local:9000", UpstreamScheme::Http, true)]);

        let mode = o.resolve("other.example.com", 9000);
        assert_eq!(mode, None, "off-override authority must not match");
        assert_eq!(
            connect_port(mode, 9000),
            443,
            "unmatched target takes the literal-443 Secure_Default_Behaviour path"
        );
    }

    #[test]
    fn test_empty_overrides_always_take_default_443_path() {
        // With no overrides configured (the default), every authority — including
        // one carrying a non-443 port — resolves to None and connects on 443,
        // byte-for-byte the pre-feature egress (Property 1; Req 1.3, 3.5).
        let o = UpstreamOverrides::from_config(&std::collections::HashMap::new());
        assert!(o.is_empty());

        assert_eq!(o.resolve("127.0.0.1", 9000), None);
        assert_eq!(connect_port(o.resolve("127.0.0.1", 9000), 9000), 443);

        assert_eq!(o.resolve("s3.us-east-1.amazonaws.com", 80), None);
        assert_eq!(
            connect_port(o.resolve("s3.us-east-1.amazonaws.com", 80), 80),
            443
        );
    }

    // ---- the TLS name follows the request, not a reverse lookup ----------
    //
    // Fixture shape, mirroring the real one: two registered endpoints share one
    // address, because every virtual-hosted bucket hostname in a region resolves to
    // one shared address set. `SMALLER_HOST` sorts first, so it is what
    // `get_hostname_for_ip` answers with; `TARGET_HOST` is the host a request
    // targets. The two vacuity traps these tests avoid, named:
    //
    // * An assertion on a value adjacent to the one the code branches on. Tests 2
    //   and 3 drive `CustomHttpsConnector::call` itself, so the name asserted is the
    //   name `ServerName::try_from` and the handshake consumed, not a helper's
    //   return that the branch might never read.
    // * A fixture whose smallest name is also covered by the certificate, which
    //   never reaches the guard. The certificate in test 2 covers `TARGET_HOST`
    //   alone and each test asserts that precondition before the verdict.
    const SUFFIX_KEY: &str = "*.case60.invalid";
    const SMALLER_HOST: &str = "aa.uncovered.case60.invalid";
    const TARGET_HOST: &str = "zz.covered.case60.invalid";

    fn shared_ip() -> IpAddr {
        "127.0.0.1".parse().unwrap()
    }

    /// A pool manager holding two endpoints that share the loopback address.
    async fn two_endpoints_one_address() -> ConnectionPoolManager {
        let mut endpoint_overrides = std::collections::HashMap::new();
        endpoint_overrides.insert(SUFFIX_KEY.to_string(), vec![shared_ip().to_string()]);
        let config = ConnectionPoolConfig {
            endpoint_overrides,
            ..ConnectionPoolConfig::default()
        };
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();
        manager.register_endpoint(SMALLER_HOST).await;
        manager.register_endpoint(TARGET_HOST).await;
        manager
    }

    /// A connector over that pool manager, with `127.0.0.1:port` overridden to the
    /// given upstream scheme. The override is what lets a test reach a loopback
    /// port at all: the default path dials the literal 443, which no test may bind.
    async fn connector_on_loopback(
        port: u16,
        scheme: UpstreamScheme,
        root_store: rustls::RootCertStore,
        failure_threshold: u32,
    ) -> (
        CustomHttpsConnector,
        Arc<tokio::sync::RwLock<ConnectionPoolManager>>,
    ) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let manager = two_endpoints_one_address().await;
        let pool_manager = Arc::new(tokio::sync::RwLock::new(manager));
        let mut overrides_map = std::collections::HashMap::new();
        overrides_map.insert(
            format!("127.0.0.1:{port}"),
            UpstreamOverrideConfig {
                scheme,
                validate_tls: true,
            },
        );
        let connector = CustomHttpsConnector::new(
            Arc::clone(&pool_manager),
            Arc::new(root_store),
            ConnectionPoolConfig::default(),
            Arc::new(IpHealthTracker::new(failure_threshold)),
            Arc::new(UpstreamOverrides::from_config(&overrides_map)),
        );
        (connector, pool_manager)
    }

    /// Test 1. The pure choice: the carried host wins over the reverse lookup, and
    /// with no carrier the deterministic tie-break is returned unchanged.
    #[tokio::test]
    async fn test_recover_tls_name_prefers_the_host_the_request_carried() {
        let manager = two_endpoints_one_address().await;
        // Fixture precondition: the reverse lookup must answer with the host the
        // request did NOT target, or the assertion below could pass by coincidence.
        assert_eq!(
            manager.get_hostname_for_ip(&shared_ip()).as_deref(),
            Some(SMALLER_HOST),
            "fixture: two endpoints must share the address and the smaller must win"
        );
        let carried: Uri = format!("https://{}@{}:443/bucket/key", TARGET_HOST, shared_ip())
            .parse()
            .unwrap();
        assert_eq!(
            carried.host(),
            Some("127.0.0.1"),
            "the connector must still dial the address"
        );
        assert_eq!(
            recover_tls_name(&carried, &manager).as_deref(),
            Some(TARGET_HOST),
            "the TLS name must be the host the request targeted"
        );
        // Positive control: the reverse lookup is untouched for a URI with no
        // carrier, which is the probe's and the exclusion path's shape.
        let bare: Uri = format!("https://{}:443/bucket/key", shared_ip())
            .parse()
            .unwrap();
        assert_eq!(
            recover_tls_name(&bare, &manager).as_deref(),
            Some(SMALLER_HOST),
            "with nothing carried the deterministic tie-break must be unchanged"
        );
        // A carrier that is valid authority syntax but not a name rustls will
        // accept falls back rather than failing the request or panicking.
        let unusable: Uri = format!("https://under_score!@{}:443/k", shared_ip())
            .parse()
            .expect("sub-delims and underscores are valid userinfo");
        assert_eq!(
            recover_tls_name(&unusable, &manager).as_deref(),
            Some(SMALLER_HOST),
            "a carrier rustls would reject must fall back to the reverse lookup"
        );
    }

    /// Test 2. The wiring: `call`'s IP-authority branch consumes the carried name.
    /// The certificate covers `TARGET_HOST` only, so a handshake presenting the
    /// smaller registered name cannot verify.
    #[tokio::test]
    async fn test_connector_handshake_verifies_against_the_carried_name() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert = rcgen::generate_simple_self_signed(vec![TARGET_HOST.to_string()]).unwrap();
        let cert_der = rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
        let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let _ = acceptor.accept(sock).await;
                });
            }
        });
        let mut root_store = rustls::RootCertStore::empty();
        root_store.add(cert_der).unwrap();
        let (mut connector, pool_manager) =
            connector_on_loopback(port, UpstreamScheme::Https, root_store, 3).await;
        {
            let pm = pool_manager.read().await;
            assert_eq!(
                pm.get_hostname_for_ip(&shared_ip()).as_deref(),
                Some(SMALLER_HOST),
                "fixture: the reverse lookup must name the host the certificate omits"
            );
        }
        let uri: Uri = format!("https://{}@127.0.0.1:{}/bucket/key", TARGET_HOST, port)
            .parse()
            .unwrap();
        let outcome = connector.call(uri).await;
        assert!(
            outcome.is_ok(),
            "the handshake must present and verify the carried name, got: {:?}",
            outcome.err().map(|e| e.to_string())
        );
    }

    /// Test 3. The failure path excludes the address from the request's OWN
    /// endpoint, not from the endpoint the reverse lookup would have named.
    #[tokio::test]
    async fn test_handshake_failure_excludes_the_requests_own_endpoint() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        // Accept the TCP connection, then close: the failure is at the handshake,
        // which is the path that reads `tls_hostname` for the distributor key.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                drop(sock);
            }
        });
        let (mut connector, pool_manager) = connector_on_loopback(
            port,
            UpstreamScheme::Https,
            rustls::RootCertStore::empty(),
            1,
        )
        .await;
        let uri: Uri = format!("https://{}@127.0.0.1:{}/bucket/key", TARGET_HOST, port)
            .parse()
            .unwrap();
        assert!(
            connector.call(uri).await.is_err(),
            "fixture: the handshake must fail for the exclusion to run at all"
        );
        // The exclusion runs in a spawned task, so wait on the state it changes.
        let mut excluded = false;
        for _ in 0..60 {
            {
                let mut pm = pool_manager.write().await;
                if let Some(dist) = pm.get_distributor_mut(TARGET_HOST) {
                    excluded = !dist.get_ips().contains(&shared_ip());
                }
            }
            if excluded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            excluded,
            "the request's own endpoint must lose the address (a timeout here is an \
             apparatus result, not a verdict on the other endpoint)"
        );
        let mut pm = pool_manager.write().await;
        assert!(
            pm.get_distributor_mut(SMALLER_HOST)
                .expect("fixture: the smaller endpoint must have a distributor")
                .get_ips()
                .contains(&shared_ip()),
            "the endpoint the reverse lookup would have named must keep its address"
        );
    }

    /// Test 4. Nothing of the carrier reaches the wire, and the pool keys on it.
    ///
    /// Three sequential requests through a real hyper client over this connector to
    /// a loopback origin: two carrying one host, then one carrying another. The
    /// origin records the exact request head it read and counts its accepts.
    #[tokio::test]
    async fn test_carried_host_stays_off_the_wire_and_keys_the_pool() {
        use http_body_util::BodyExt;
        use http_body_util::Full;
        use hyper::body::Bytes;
        use hyper_util::client::legacy::Client;
        use hyper_util::rt::TokioExecutor;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        fn headers_end(buf: &[u8]) -> Option<usize> {
            buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepts = Arc::new(AtomicUsize::new(0));
        let heads: Arc<tokio::sync::Mutex<Vec<String>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let accepts_srv = Arc::clone(&accepts);
        let heads_srv = Arc::clone(&heads);
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                accepts_srv.fetch_add(1, Ordering::SeqCst);
                let heads_conn = Arc::clone(&heads_srv);
                tokio::spawn(async move {
                    let mut buf: Vec<u8> = Vec::new();
                    let mut chunk = [0u8; 1024];
                    loop {
                        let n = match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        while let Some(end) = headers_end(&buf) {
                            heads_conn
                                .lock()
                                .await
                                .push(String::from_utf8_lossy(&buf[..end]).to_string());
                            buf.drain(..end);
                            if sock
                                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                });
            }
        });

        let (connector, _pool_manager) = connector_on_loopback(
            port,
            UpstreamScheme::Http,
            rustls::RootCertStore::empty(),
            3,
        )
        .await;
        let client: Client<CustomHttpsConnector, Full<Bytes>> =
            Client::builder(TokioExecutor::new()).build(connector);

        for (round, carried) in [TARGET_HOST, TARGET_HOST, SMALLER_HOST].iter().enumerate() {
            let request = hyper::Request::builder()
                .method("GET")
                .uri(format!("http://{}@127.0.0.1:{}/bucket/key", carried, port))
                .header("host", *carried)
                .body(Full::new(Bytes::new()))
                .unwrap();
            let response = client
                .request(request)
                .await
                .expect("the loopback origin must answer");
            assert_eq!(response.status(), 200);
            // Drain the body so hyper returns the connection to the pool.
            response.into_body().collect().await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            if round == 1 {
                assert_eq!(
                    accepts.load(Ordering::SeqCst),
                    1,
                    "two requests carrying the same host must share one pooled connection"
                );
            }
        }
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            2,
            "a request carrying a different host must open its own connection: the pool \
             key is the whole authority, so the pool holds one entry per host-and-address \
             pair rather than one per address"
        );
        let heads = heads.lock().await;
        assert_eq!(heads.len(), 3, "the origin must have read three requests");
        for head in heads.iter() {
            assert!(
                head.starts_with("GET /bucket/key HTTP/1.1\r\n"),
                "the request target must be origin-form with no authority: {head:?}"
            );
            assert!(
                !head.contains('@'),
                "nothing of the carrier may reach the wire: {head:?}"
            );
        }
        assert!(
            heads[0]
                .to_lowercase()
                .contains(&format!("host: {}\r\n", TARGET_HOST)),
            "the Host header must be the one the request set: {:?}",
            heads[0]
        );
    }
    // ---- a dotted bucket name is checked as its S3 endpoint ---------------
    #[test]
    fn test_upstream_certificate_name_strips_a_dotted_bucket_for_each_s3_shape() {
        for (host, expected) in [
            (
                "my.bucket.s3.us-west-2.amazonaws.com",
                "s3.us-west-2.amazonaws.com",
            ),
            (
                "my.bucket.s3.dualstack.us-west-2.amazonaws.com",
                "s3.dualstack.us-west-2.amazonaws.com",
            ),
            (
                "my.bucket.s3-fips.us-east-1.amazonaws.com",
                "s3-fips.us-east-1.amazonaws.com",
            ),
            (
                "my.bucket.s3-fips.dualstack.us-gov-west-1.amazonaws.com",
                "s3-fips.dualstack.us-gov-west-1.amazonaws.com",
            ),
            ("my.bucket.s3.amazonaws.com", "s3.amazonaws.com"),
            (
                "my.bucket.s3-us-west-2.amazonaws.com",
                "s3-us-west-2.amazonaws.com",
            ),
            (
                "my.bucket.s3.cn-north-1.amazonaws.com.cn",
                "s3.cn-north-1.amazonaws.com.cn",
            ),
            (
                "my.bucket.s3.dualstack.cn-northwest-1.amazonaws.com.cn",
                "s3.dualstack.cn-northwest-1.amazonaws.com.cn",
            ),
            // Parsed from the right: the bucket `a.s3.b` holds an `s3` label.
            (
                "a.s3.b.s3.us-west-2.amazonaws.com",
                "s3.us-west-2.amazonaws.com",
            ),
            // Case is preserved in the returned suffix; matching ignores it.
            (
                "My.Bucket.S3.US-West-2.AmazonAWS.com",
                "S3.US-West-2.AmazonAWS.com",
            ),
        ] {
            assert_eq!(upstream_certificate_name(host), expected, "host {host}");
        }
    }

    #[test]
    fn test_upstream_certificate_name_leaves_every_other_host_unchanged() {
        for host in [
            // A bucket name without a dot: S3's wildcard covers it today.
            "mybucket.s3.us-west-2.amazonaws.com",
            "mybucket.s3.amazonaws.com",
            // Path-style: no bucket in the host.
            "s3.us-west-2.amazonaws.com",
            "s3.dualstack.us-west-2.amazonaws.com",
            "s3.amazonaws.com",
            // Not AWS.
            "my.bucket.minio.example",
            "my.bucket.s3.us-west-2.example.com",
            "127.0.0.1",
            // Look-alikes that only contain an S3 name.
            "my.bucket.s3.us-west-2.amazonaws.com.evil.example",
            "my.bucket.s3.us-west-2.notamazonaws.com",
            "my.bucket.s3.us-west-2-amazonaws.com",
            // S3 shapes the rule leaves out.
            "my.bucket.s3.amazonaws.com.cn",
            "my.bucket.s3-external-1.amazonaws.com",
            "my.bucket.s3-accelerate.amazonaws.com",
            "my.bucket.s3-website-us-west-2.amazonaws.com",
            "my.bucket.s3-website.us-west-2.amazonaws.com",
            "my.bucket.ec2.us-west-2.amazonaws.com",
            // Malformed bucket part.
            "my..s3.us-west-2.amazonaws.com",
        ] {
            assert_eq!(upstream_certificate_name(host), host, "host {host}");
        }
    }

    /// Test 5. The certificate covers exactly what S3's regional certificate covers,
    /// `s3.us-west-2.amazonaws.com` and `*.s3.us-west-2.amazonaws.com`, so it does
    /// not cover the dotted bucket's virtual-hosted host. The handshake must send and
    /// verify the endpoint name with the bucket removed.
    #[tokio::test]
    async fn test_connector_handshake_uses_the_s3_endpoint_name_for_a_dotted_bucket() {
        const DOTTED_HOST: &str = "my.bucket.s3.us-west-2.amazonaws.com";
        const REGIONAL: &str = "s3.us-west-2.amazonaws.com";
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert =
            rcgen::generate_simple_self_signed(vec![REGIONAL.to_string(), format!("*.{REGIONAL}")])
                .unwrap();
        let cert_der = rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
        let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen_sni: Arc<std::sync::Mutex<Vec<Option<String>>>> = Arc::default();
        let seen_srv = Arc::clone(&seen_sni);
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                let seen = Arc::clone(&seen_srv);
                tokio::spawn(async move {
                    if let Ok(stream) = acceptor.accept(sock).await {
                        let sni = stream.get_ref().1.server_name().map(str::to_string);
                        seen.lock().unwrap().push(sni);
                    }
                });
            }
        });
        let mut root_store = rustls::RootCertStore::empty();
        root_store.add(cert_der).unwrap();
        let (mut connector, _pool_manager) =
            connector_on_loopback(port, UpstreamScheme::Https, root_store, 1).await;
        let uri: Uri = format!("https://{}@127.0.0.1:{}/key", DOTTED_HOST, port)
            .parse()
            .unwrap();
        let outcome = connector.call(uri).await;
        assert!(
            outcome.is_ok(),
            "a dotted bucket's handshake must verify against S3's endpoint name, got: {:?}",
            outcome.err().map(|e| e.to_string())
        );
        for _ in 0..40 {
            let recorded = !seen_sni.lock().unwrap().is_empty();
            if recorded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            seen_sni.lock().unwrap().as_slice(),
            &[Some(REGIONAL.to_string())],
            "the SNI sent must be the endpoint name with the bucket removed"
        );
    }
}
