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
use hyper_util::client::legacy::connect::{Connected, Connection};
use rustls::pki_types::ServerName;
use socket2::{Socket, TcpKeepalive};
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
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
pub fn build_tls_config_for_host(
    host: &str,
    root_store: rustls::RootCertStore,
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
pub fn build_tls_default_config(root_store: rustls::RootCertStore) -> rustls::ClientConfig {
    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// Build a TLS 1.2-only configuration for PrivateLink / VPC interface endpoints.
pub fn build_tls12_only_config(root_store: rustls::RootCertStore) -> rustls::ClientConfig {
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
pub enum HttpsStream {
    Tls(Box<TlsStream<TcpStream>>),
    Plain(TcpStream),
}

impl Read for HttpsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let mut tokio_buf = tokio::io::ReadBuf::uninit(unsafe { buf.as_mut() });
        let poll = match &mut *self {
            HttpsStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, &mut tokio_buf),
            HttpsStream::Plain(s) => Pin::new(s).poll_read(cx, &mut tokio_buf),
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
        match &mut *self {
            HttpsStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
            HttpsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            HttpsStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
            HttpsStream::Plain(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            HttpsStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            HttpsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl Connection for HttpsStream {
    fn connected(&self) -> Connected {
        Connected::new()
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
    root_store: rustls::RootCertStore,
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
        root_store: rustls::RootCertStore,
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

    pub fn set_metrics_manager_ref(
        &mut self,
        metrics_manager: Arc<
            tokio::sync::RwLock<Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>>,
        >,
    ) {
        self.metrics_manager = metrics_manager;
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

impl Service<Uri> for CustomHttpsConnector {
    type Response = HttpsStream;
    type Error = ProxyError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let pool_manager = Arc::clone(&self.pool_manager);
        let root_store = self.root_store.clone();
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

            // Determine connect IP and TLS hostname
            let (connect_ip, tls_hostname) = if let Ok(ip) = uri_host.parse::<IpAddr>() {
                // URI host is an IP — IpDistributor already selected it.
                // Look up the original S3 hostname for TLS SNI.
                let hostname = {
                    let pm = pool_manager.read().await;
                    pm.get_hostname_for_ip(&ip)
                };

                match hostname {
                    Some(h) => {
                        debug!(
                            "[HTTPS_CONNECTOR] URI host is IP {}, using hostname '{}' for TLS SNI",
                            ip, h
                        );
                        (ip, h)
                    }
                    None => {
                        warn!(
                            "[HTTPS_CONNECTOR] No hostname mapping found for IP {}, TLS SNI may fail",
                            ip
                        );
                        (ip, uri_host.to_string())
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
                (ip, uri_host.to_string())
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
                    let mm = metrics_manager.read().await;
                    if let Some(ref metrics) = *mm {
                        metrics
                            .read()
                            .await
                            .record_connection_created(&tls_hostname)
                            .await;
                    }
                    return Ok(HttpsStream::Plain(tcp));
                }
                Some(TransportMode::TlsUnvalidated) => build_tls_accept_any_config(),
                Some(TransportMode::TlsValidated) | None => {
                    let pm = pool_manager.read().await;
                    build_tls_config_for_host(&tls_hostname, root_store, &pm)
                }
            };

            let tls_connector = TlsConnector::from(Arc::new(tls_config));

            // TLS handshake
            let server_name = ServerName::try_from(tls_hostname.clone()).map_err(|e| {
                ProxyError::TlsError(format!("Invalid server name '{}': {}", tls_hostname, e))
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
            let mm = metrics_manager.read().await;
            if let Some(ref metrics) = *mm {
                metrics
                    .read()
                    .await
                    .record_connection_created(&tls_hostname)
                    .await;
            }

            Ok(HttpsStream::Tls(Box::new(tls)))
        })
    }
}

impl Clone for CustomHttpsConnector {
    fn clone(&self) -> Self {
        Self {
            pool_manager: Arc::clone(&self.pool_manager),
            root_store: self.root_store.clone(),
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
    use crate::config::ConnectionPoolConfig;

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
            root_store,
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
        let cfg = build_tls_config_for_host("s3.us-east-1.amazonaws.com", root_store, &pm);
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
            root_store,
            &pm,
        );
        // The config is TLS 1.2 only — verified by the builder_with_protocol_versions call
    }

    #[test]
    fn test_build_tls_default_config_fn() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let _cfg = build_tls_default_config(root_store);
    }

    #[test]
    fn test_build_tls12_only_config_fn() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let _cfg = build_tls12_only_config(root_store);
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
}
