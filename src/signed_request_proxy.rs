//! Signed Request Proxy Module
//!
//! Handles AWS Signature Version 4 signed requests by preserving the original
//! HTTP request exactly as received, only changing the destination IP address.
//! This prevents signature validation failures that occur when headers are modified.

use crate::{ProxyError, Result};
use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::body::Body;
use hyper::{Request, Response, StatusCode};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::TlsConnector;
use tracing::{debug, error, info, warn};

/// Process-wide cache-tee backpressure counters for the signed-PUT/UploadPart
/// streaming path — the PUT-side analogue of `tee_stream::BACKPRESSURE_EVENTS_TOTAL`.
/// Incremented directly at the point backpressure occurs, mirroring
/// `crate::metrics::LAST_OVER_BOUND_WARN_SECS`'s process-wide-atomic pattern.
///
/// A non-zero value means the cache writer, not the upstream, was the
/// bottleneck for at least one streamed PUT/UploadPart request.
///
/// Spec: cache-eviction-at-scale Requirement 18.4
static TEE_BACKPRESSURE_EVENTS_TOTAL_PUT: AtomicU64 = AtomicU64::new(0);
static TEE_BACKPRESSURE_WAIT_MS_TOTAL_PUT: AtomicU64 = AtomicU64::new(0);

/// Cumulative count of cache-tee backpressure events across all streamed
/// PUT/UploadPart requests since process start.
pub fn tee_backpressure_events_total_put() -> u64 {
    TEE_BACKPRESSURE_EVENTS_TOTAL_PUT.load(Ordering::Relaxed)
}

/// Cumulative milliseconds spent applying cache-tee backpressure across all
/// streamed PUT/UploadPart requests since process start.
pub fn tee_backpressure_wait_ms_total_put() -> u64 {
    TEE_BACKPRESSURE_WAIT_MS_TOTAL_PUT.load(Ordering::Relaxed)
}

/// Maximum body accepted by S3 for a single-part PUT or UploadPart.
///
/// This streamed-path cap prevents the proxy from relaying a body S3 will reject,
/// avoiding unnecessary proxy-to-S3 egress. It is intentionally separate from the
/// smaller buffered-body bound used only by the remaining buffering sites.
pub(crate) const STREAMED_BODY_CAP: u64 = 5 * 1024 * 1024 * 1024;

/// Maximum request body retained whole in memory on the remaining buffered paths.
///
/// A DeleteObjects request for its maximum 1,000 keys is only a few hundred KiB.
/// One MiB leaves headroom for that API without making an object-sized upload a
/// buffered allocation. Do not use this for streamed PUT or UploadPart paths;
/// [`STREAMED_BODY_CAP`] is their separate S3-derived limit.
pub(crate) const BUFFERED_BODY_BOUND: u64 = 1024 * 1024;

/// How the signed-write path (single-part PUT and the multipart operations)
/// reaches the upstream for one request.
///
/// Built once per request from `connection_pool.upstream_overrides`, mirroring
/// the GET-path connector (`CustomHttpsConnector`):
/// - **no override** → `tls: Some(verified-443 connector)`, `port: 443`
///   (Secure_Default_Behaviour — byte-for-byte the prior egress).
/// - **`scheme: https`** → `tls: Some(connector)` (system-roots-validated, or the
///   accept-any verifier when `validate_tls: false`), `port`: the authority port.
/// - **`scheme: http`** → `tls: None` (plaintext HTTP), `port`: the authority port.
///
/// The signed `Host`/SNI hostname is passed separately (it is forwarded verbatim
/// to preserve SigV4); this descriptor only carries the connect target and the
/// transport decision.
#[derive(Clone)]
pub struct UpstreamTransport {
    /// Resolved IP address to connect to.
    pub ip: IpAddr,
    /// Port to connect to: 443 for the default path, else the request authority port.
    pub port: u16,
    /// `Some` → perform a TLS handshake with this connector; `None` → plaintext HTTP.
    pub tls: Option<Arc<TlsConnector>>,
    /// For a `TlsValidated` override only: `Some("host:port")`. When set, a TLS
    /// *handshake* failure is surfaced as a non-retryable
    /// [`ProxyError::UpstreamTlsValidationFailed`] naming this endpoint (Requirement
    /// 4), mirroring the GET-path connector. `None` for the default-443 path and the
    /// `TlsUnvalidated`/`Plaintext` modes, which keep the generic-error behaviour.
    pub validated_endpoint: Option<String>,
    /// Bounds on this attempt's waits on the upstream.
    pub timeouts: UpstreamTimeouts,
    /// The in-flight ledger the buffered response is reserved against. `None`
    /// reserves nothing; the response bound applies either way.
    pub response_ledger: Option<Arc<crate::inflight_ledger::InflightLedger>>,
    /// Where the outcome of connecting to `ip` is recorded, so an address that keeps
    /// failing is excluded from the host's distributor as it is for GETs. `None`
    /// records nothing: an override target, which no distributor serves, or a test.
    pub health: Option<UpstreamHealth>,
}

/// The per-address health record a signed write's connect outcome goes into: the
/// tracker the GET path uses, and the distributor an address is excluded from.
#[derive(Clone)]
pub struct UpstreamHealth {
    pub tracker: Arc<crate::connection_pool::IpHealthTracker>,
    pub pool: Arc<tokio::sync::RwLock<crate::connection_pool::ConnectionPoolManager>>,
    /// The request host, whose distributor holds `ip`.
    pub host: String,
}

impl UpstreamHealth {
    /// Record one connect outcome for `ip`. A failure that reaches the tracker's
    /// threshold removes `ip` from the host's distributor, as the GET path does; the
    /// recovery probe re-admits it.
    fn record(&self, ip: IpAddr, connected: bool) {
        if connected {
            self.tracker.record_success(&ip);
            return;
        }
        if self.tracker.record_failure(&ip) {
            warn!(ip = %ip, host = %self.host, "IP failure threshold reached on a signed-write connect, excluding from distributor");
            let pool = Arc::clone(&self.pool);
            let host = self.host.clone();
            tokio::spawn(async move {
                let mut pm = pool.write().await;
                if let Some(dist) = pm.get_distributor_mut(&host) {
                    dist.remove_ip(ip, "signed-write connect failure");
                }
            });
        }
    }
}
/// Bounds on one signed-write attempt's waits on the upstream.
///
/// Every deadline is per attempt and inside the forward: nothing here times the
/// client's request body.
#[derive(Clone)]
pub struct UpstreamTimeouts {
    /// One deadline around the whole connect: every TCP attempt, every TLS
    /// handshake and the backoff sleeps between them
    /// (`connection_pool.connection_timeout`).
    pub connect: Duration,
    /// How long the upstream may move no byte while the proxy is writing the
    /// request to it or waiting on its response
    /// (`connection_pool.upstream_write_idle_timeout`).
    pub idle: Duration,
    /// The pool settings applied to the write socket after the TCP connect and
    /// before TLS: `keepalive_idle_secs`, `keepalive_interval_secs`,
    /// `keepalive_retries`, `tcp_recv_buffer_size` and `TCP_NODELAY`, as on the
    /// pooled path ([`crate::https_connector::apply_socket_options`]).
    pub socket: Arc<crate::config::ConnectionPoolConfig>,
}

/// The default pool settings, shared so a default [`UpstreamTimeouts`] allocates
/// nothing.
static DEFAULT_SOCKET_SETTINGS: LazyLock<Arc<crate::config::ConnectionPoolConfig>> =
    LazyLock::new(|| Arc::new(crate::config::ConnectionPoolConfig::default()));

impl Default for UpstreamTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            idle: Duration::from_secs(120),
            socket: Arc::clone(&DEFAULT_SOCKET_SETTINGS),
        }
    }
}

/// What [`send_request`] wrote: when the last request byte was flushed, and how
/// many body bytes went upstream.
pub(crate) struct RequestSent {
    pub(crate) at: Instant,
    pub(crate) body_bytes: u64,
}

/// The upstream response of one signed write, read into memory under a bound and
/// reserved against the in-flight ledger as it grows.
///
/// The ledger never refuses this growth. The request has already been sent, so S3 may
/// have executed the write, and refusing its answer would tell the client the write was
/// shed when it was not. Every byte read is recorded on the ledger at once, past its
/// ceiling if need be, so the figure and its peak show what is held; `bound` still caps
/// the buffer.
struct ResponseBuffer {
    bytes: Vec<u8>,
    bound: usize,
    reservation: crate::inflight_ledger::Reservation,
}

impl ResponseBuffer {
    fn new(bound: usize, ledger: Option<&Arc<crate::inflight_ledger::InflightLedger>>) -> Self {
        let reservation = ledger.map_or_else(crate::inflight_ledger::Reservation::none, |ledger| {
            ledger.reserve_unrefused()
        });
        Self {
            bytes: Vec::new(),
            bound,
            reservation,
        }
    }

    /// Read once from `stream` and append what arrived. Growth past `bound` is refused
    /// before the append; what is appended is accounted on the ledger even past its
    /// ceiling. Returns the number of bytes read, 0 at end of stream.
    async fn read_from<S: AsyncRead + Unpin>(
        &mut self,
        stream: &mut S,
        scratch: &mut [u8],
        phase: &str,
    ) -> Result<usize> {
        let n = stream
            .read(scratch)
            .await
            .map_err(|e| upstream_io_error(e, phase, "Failed to read response"))?;
        if n == 0 {
            return Ok(0);
        }
        let total = self.bytes.len() + n;
        if total > self.bound {
            warn!(
                bound_bytes = self.bound,
                "Upstream response to a signed write exceeds the buffered-response bound; refusing it"
            );
            return Err(ProxyError::HttpError(format!(
                "upstream response exceeds the {}-byte bound for a buffered write response",
                self.bound
            )));
        }
        self.reservation.force_grow(n as u64);
        self.bytes.extend_from_slice(&scratch[..n]);
        Ok(n)
    }
}

/// A signed write's upstream response, held in memory: its head, its body, and the
/// in-flight ledger reservation that accounts for it until the response is dropped.
pub(crate) struct BufferedResponse {
    pub(crate) parts: hyper::http::response::Parts,
    pub(crate) body: Bytes,
    reservation: crate::inflight_ledger::Reservation,
}

impl BufferedResponse {
    pub(crate) fn status(&self) -> StatusCode {
        self.parts.status
    }

    /// The head, the body, and the reservation, for a caller that reads the body
    /// and then answers with [`Self::respond`].
    pub(crate) fn into_parts(
        self,
    ) -> (
        hyper::http::response::Parts,
        Bytes,
        crate::inflight_ledger::Reservation,
    ) {
        (self.parts, self.body, self.reservation)
    }

    /// The client response. It holds the reservation until its body has been sent or
    /// dropped: the body is served in several frames, so hyper does not drop it at
    /// the first poll.
    pub(crate) fn into_response(self) -> Response<BoxBody<Bytes, hyper::Error>> {
        Self::respond(self.parts, self.body, self.reservation)
    }

    pub(crate) fn respond(
        parts: hyper::http::response::Parts,
        body: Bytes,
        reservation: crate::inflight_ledger::Reservation,
    ) -> Response<BoxBody<Bytes, hyper::Error>> {
        let body =
            crate::permit_body::PermitBody::new(crate::permit_body::ChunkedBytes::new(body), None)
                .with_reservation(reservation);
        Response::from_parts(parts, body.map_err(|never| match never {}).boxed())
    }
}

impl UpstreamTransport {
    /// The verified-TLS-on-443 default transport (Secure_Default_Behaviour). A
    /// handshake failure here stays a generic (retryable) error, exactly as before.
    pub(crate) fn verified_tls_443(ip: IpAddr, tls_connector: Arc<TlsConnector>) -> Self {
        Self {
            ip,
            port: 443,
            tls: Some(tls_connector),
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        }
    }
}

/// Outbound stream for the signed-write path — either a TLS stream (the
/// verified-TLS-on-443 default path plus the `TlsValidated`/`TlsUnvalidated`
/// override modes) or a plaintext TCP stream (the `Plaintext` override mode).
///
/// Mirrors the GET-path `HttpsStream` enum. Both `TlsStream<TcpStream>` and
/// `TcpStream` implement tokio's `AsyncRead`/`AsyncWrite`, so each arm delegates
/// to its inner stream. The TLS variant is boxed to keep the enum small
/// (clippy `large_enum_variant`).
pub(crate) enum UpstreamStream {
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    Plain(TcpStream),
}

impl AsyncRead for UpstreamStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            UpstreamStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
            UpstreamStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for UpstreamStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            UpstreamStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
            UpstreamStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            UpstreamStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
            UpstreamStream::Plain(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            UpstreamStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            UpstreamStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// An upstream stream whose reads, writes and flushes fail once one of them has
/// waited `idle` without moving a byte.
///
/// The timer is armed when an upstream poll returns `Pending` with no timer armed,
/// and disarmed by every poll that returns `Ready`, which is every poll that moves
/// a byte. So it measures how long the current upstream operation has waited since
/// the last byte moved, and it is re-armed by any progress: a large `write_all` to
/// a slow but steady upstream is never cut off. It runs only inside these polls,
/// and an awaited upstream operation always ends in a `Ready` before the next
/// client body frame is read, so the time spent waiting on the client's body is
/// never counted.
///
/// Over TLS this wraps the TLS stream, so a byte means a plaintext byte. A read
/// poll that receives part of a TLS record, or a write poll that drains part of
/// the TLS send buffer, returns `Pending` and does not re-arm the timer, so an
/// upstream that takes longer than `idle` to deliver one whole record is cut off
/// although TCP bytes moved. An upstream sending small records is timed per record.
pub(crate) struct IdleBoundStream<S> {
    inner: S,
    idle: Duration,
    deadline: Pin<Box<tokio::time::Sleep>>,
    armed: bool,
}

/// The `io::Error` payload of an [`IdleBoundStream`] timeout.
#[derive(Debug)]
struct IdleElapsed(Duration);

impl std::fmt::Display for IdleElapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "no byte moved to or from the upstream for {:?}", self.0)
    }
}

impl std::error::Error for IdleElapsed {}

impl<S> IdleBoundStream<S> {
    pub(crate) fn new(inner: S, idle: Duration) -> Self {
        Self {
            inner,
            idle,
            deadline: Box::pin(tokio::time::sleep(idle)),
            armed: false,
        }
    }

    /// Account for one inner poll's result: disarm on `Ready`; on `Pending`, arm
    /// the timer if it is not armed and fail once it has expired.
    fn bound<T>(
        &mut self,
        cx: &mut Context<'_>,
        polled: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        if polled.is_ready() {
            self.armed = false;
            return polled;
        }
        if !self.armed {
            self.armed = true;
            let at = tokio::time::Instant::now() + self.idle;
            self.deadline.as_mut().reset(at);
        }
        if self.deadline.as_mut().poll(cx).is_ready() {
            self.armed = false;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                IdleElapsed(self.idle),
            )));
        }
        Poll::Pending
    }
}

/// The idle bound an `io::Error` reports, when it is an [`IdleBoundStream`] timeout.
fn idle_elapsed(e: &io::Error) -> Option<Duration> {
    e.get_ref()?.downcast_ref::<IdleElapsed>().map(|i| i.0)
}

/// Prefix on the message of every error a signed write raises after its whole
/// request was sent upstream: waiting for or reading the response, refusing it for
/// size, or parsing it. S3 may have executed the write, so its outcome is unknown.
const AFTER_SEND: &str = "request sent, outcome unknown: ";

/// Mark `e`, raised after the request was sent, with [`AFTER_SEND`].
fn after_send(e: ProxyError) -> ProxyError {
    match e {
        ProxyError::TimeoutError(m) => ProxyError::TimeoutError(format!("{AFTER_SEND}{m}")),
        ProxyError::HttpError(m) => ProxyError::HttpError(format!("{AFTER_SEND}{m}")),
        other => other,
    }
}

/// Whether `e` ended a signed write after its whole request was sent upstream, so S3
/// may have executed the write. A failure before the last request byte was sent
/// leaves S3 an incomplete request, which it does not commit, and is not one.
pub(crate) fn write_outcome_unknown(e: &ProxyError) -> bool {
    matches!(e, ProxyError::TimeoutError(m) | ProxyError::HttpError(m) if m.starts_with(AFTER_SEND))
}

/// Response extension on a client error response answering a signed write whose
/// outcome is unknown (see [`write_outcome_unknown`]), for a caller that receives the
/// response rather than the error.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WriteOutcomeUnknown;

/// Map an upstream I/O error in `phase` to a `TimeoutError` naming the phase when
/// the idle bound expired, and otherwise to the `HttpError` it always was.
fn upstream_io_error(e: io::Error, phase: &str, failure: &str) -> ProxyError {
    match idle_elapsed(&e) {
        Some(idle) => {
            warn!("Upstream {} stalled: no byte moved for {:?}", phase, idle);
            ProxyError::TimeoutError(format!("{}: no byte moved for {:?}", phase, idle))
        }
        None => ProxyError::HttpError(format!("{}: {}", failure, e)),
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for IdleBoundStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        self.bound(cx, polled)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for IdleBoundStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.bound(cx, polled)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let polled = Pin::new(&mut self.inner).poll_flush(cx);
        self.bound(cx, polled)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let polled = Pin::new(&mut self.inner).poll_shutdown(cx);
        self.bound(cx, polled)
    }
}

/// Connect to the upstream per the resolved [`UpstreamTransport`], with retry.
///
/// TLS modes go through [`establish_tls_with_retry`] (TCP + TLS handshake);
/// the plaintext mode performs a plain TCP connect with the same retry/backoff
/// policy. `hostname` is the SNI/Host name (used only for the TLS handshake and
/// log lines); the connect target is `transport.ip:transport.port`.
pub(crate) async fn connect_upstream_with_retry(
    transport: &UpstreamTransport,
    hostname: &str,
    max_retries: usize,
) -> Result<UpstreamStream> {
    match &transport.tls {
        Some(connector) => {
            if let Some(endpoint) = &transport.validated_endpoint {
                // TlsValidated override: retry only the transient TCP connect, then
                // perform a single TLS handshake. A handshake failure is a
                // configuration error that cannot succeed on retry, so it is surfaced
                // immediately as a non-retryable `UpstreamTlsValidationFailed` naming
                // the endpoint (Requirement 4) — never retried, never downgraded.
                let tcp = connect_tcp_with_retry(
                    transport.ip,
                    transport.port,
                    hostname,
                    max_retries,
                    &transport.timeouts.socket,
                )
                .await?;
                let tls = establish_tls(tcp, hostname, connector.as_ref())
                    .await
                    .map_err(|e| ProxyError::UpstreamTlsValidationFailed {
                        endpoint: endpoint.clone(),
                        source_err: e.to_string(),
                    })?;
                Ok(UpstreamStream::Tls(Box::new(tls)))
            } else {
                // Default-443 path and TlsUnvalidated override: existing TCP+TLS retry
                // behaviour, generic error on failure (transient, retryable).
                let tls = establish_tls_with_retry(
                    transport.ip,
                    transport.port,
                    hostname,
                    connector.as_ref(),
                    max_retries,
                    &transport.timeouts.socket,
                )
                .await?;
                Ok(UpstreamStream::Tls(Box::new(tls)))
            }
        }
        None => {
            let tcp = connect_tcp_with_retry(
                transport.ip,
                transport.port,
                hostname,
                max_retries,
                &transport.timeouts.socket,
            )
            .await?;
            Ok(UpstreamStream::Plain(tcp))
        }
    }
}

/// Plain TCP connect with exponential-backoff retry. Used for the `Plaintext`
/// override mode and for the TCP leg of a `TlsValidated` override (where the TLS
/// handshake itself is then attempted once, non-retryably). Mirrors the
/// TCP-connect retry policy of [`establish_tls_with_retry`].
async fn connect_tcp_with_retry(
    target_ip: IpAddr,
    port: u16,
    hostname: &str,
    max_retries: usize,
    socket: &crate::config::ConnectionPoolConfig,
) -> Result<TcpStream> {
    let mut last_error = None;
    let mut had_failures = false;

    for attempt in 0..max_retries {
        match connect_to_target(target_ip, port, socket).await {
            Ok(stream) => {
                if had_failures {
                    info!(
                        "TCP connection succeeded on attempt {} for {} after previous failures",
                        attempt + 1,
                        hostname
                    );
                }
                return Ok(stream);
            }
            Err(e) => {
                had_failures = true;
                warn!(
                    "TCP connection attempt {} failed for {} ({}:{}): {}",
                    attempt + 1,
                    hostname,
                    target_ip,
                    port,
                    e
                );
                last_error = Some(e);
                if attempt < max_retries - 1 {
                    let clamped_attempt = attempt.min(20) as u32;
                    let delay_ms = 100u64
                        .saturating_mul(1u64.checked_shl(clamped_attempt).unwrap_or(u64::MAX))
                        .min(60_000);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
                error!(
                    "TCP connection failed after {} attempts for {}: {}",
                    max_retries,
                    hostname,
                    last_error.as_ref().unwrap()
                );
                return Err(last_error.unwrap());
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| ProxyError::ConnectionError("All retry attempts failed".to_string())))
}

/// Check whether an Authorization header value uses an AWS SigV4-family algorithm
/// with proper structural validation.
///
/// Recognizes both classic SigV4 (`AWS4-HMAC-SHA256`) and SigV4A
/// (`AWS4-ECDSA-P256-SHA256`), which the AWS CLI uses by default for
/// Multi-Region Access Point (MRAP) requests. Both share the same
/// `Credential=.../SignedHeaders=.../Signature=...` wire format, so every
/// downstream parser that treats them identically is correct.
///
/// Validates that the header:
/// 1. Starts with the algorithm identifier followed by a space
/// 2. Contains `Credential=`, `SignedHeaders=`, and `Signature=` components
///
/// This prevents a random string containing the substring "AWS4-HMAC-SHA256"
/// from being misclassified as a SigV4-signed request.
pub(crate) fn is_sigv4_algorithm(auth: &str) -> bool {
    let has_algorithm_prefix =
        auth.starts_with("AWS4-HMAC-SHA256 ") || auth.starts_with("AWS4-ECDSA-P256-SHA256 ");

    has_algorithm_prefix
        && auth.contains("Credential=")
        && auth.contains("SignedHeaders=")
        && auth.contains("Signature=")
}

/// Per-process `Via` token, random at process start so instances differ and a restart rotates it.
static VIA_TOKEN: OnceLock<String> = OnceLock::new();

/// The `Via` received-by pseudonym this process appends to forwarded requests
/// (`s3-proxy-` plus 16 random hex digits). A request that comes back carrying it has
/// passed through this process already.
pub fn via_pseudonym() -> &'static str {
    static PSEUDONYM: OnceLock<String> = OnceLock::new();
    PSEUDONYM.get_or_init(|| {
        let token =
            VIA_TOKEN.get_or_init(|| uuid::Uuid::new_v4().simple().to_string()[..16].to_string());
        format!("s3-proxy-{}", token)
    })
}

/// True when any entry of a `Via` header value names `pseudonym` as its received-by.
///
/// Compares the whole received-by token, never a prefix, so `s3-proxy-ab` does not
/// match `s3-proxy-abc`. Entries are `<protocol> <received-by> [comment]`, comma separated.
pub(crate) fn via_names_pseudonym(value: &str, pseudonym: &str) -> bool {
    value.split(',').any(|entry| {
        let mut parts = entry.split_whitespace();
        let _protocol = parts.next();
        parts.next() == Some(pseudonym)
    })
}

/// True when any inbound `Via` header line names this process. Checked at ingress.
pub(crate) fn via_loop_detected(headers: &hyper::HeaderMap) -> bool {
    let pseudonym = via_pseudonym();
    headers
        .get_all(hyper::header::VIA)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| via_names_pseudonym(v, pseudonym))
}

/// True when `name` is listed in the SigV4 `SignedHeaders` of an Authorization value.
pub(crate) fn auth_signs_header(auth: &str, name: &str) -> bool {
    if !is_sigv4_algorithm(auth) {
        return false;
    }
    match auth.find("SignedHeaders=") {
        Some(pos) => {
            let after_param = &auth[pos + 14..];
            let end = after_param
                .find(',')
                .or_else(|| after_param.find(' '))
                .unwrap_or(after_param.len());
            after_param[..end].split(';').any(|h| h == name)
        }
        None => false,
    }
}

/// The `Via` value to send: any inbound value kept intact, this process appended last
/// (RFC 9110 section 7.6.3).
pub(crate) fn append_via(existing: Option<&str>) -> String {
    let ours = format!("1.1 {}", via_pseudonym());
    match existing {
        Some(prev) if !prev.trim().is_empty() => format!("{}, {}", prev.trim(), ours),
        _ => ours,
    }
}

/// Append this process to the request's `Via` header, unless `via` is in the request's
/// SigV4 `SignedHeaders` (a signed header is never modified).
///
/// Idempotent: a value that already names this process is left as it is, so a header set
/// that passes through more than one egress helper gets one entry.
pub(crate) fn maybe_add_via(headers: &mut HashMap<String, String>, auth: Option<&str>) {
    if auth.is_some_and(|a| auth_signs_header(a, "via")) {
        return;
    }
    let existing_key = headers
        .keys()
        .find(|k| k.eq_ignore_ascii_case("via"))
        .cloned();
    if let Some(v) = existing_key.as_ref().and_then(|k| headers.get(k)) {
        if via_names_pseudonym(v, via_pseudonym()) {
            return;
        }
    }
    let existing = existing_key.as_ref().and_then(|k| headers.remove(k));
    headers.insert("Via".to_string(), append_via(existing.as_deref()));
}

/// Add this process's `Via` to the headers of an upstream request, for every egress
/// that builds an `S3RequestContext`. `uri` is the client request target, so a `via`
/// listed in a presigned URL's `X-Amz-SignedHeaders` is left alone as well as one
/// listed in the `Authorization` header.
pub(crate) fn add_via_for_egress(headers: &mut HashMap<String, String>, uri: &hyper::Uri) {
    if is_header_signed_in_query(request_target(uri), "via") {
        return;
    }
    let auth = headers
        .get("authorization")
        .or_else(|| headers.get("Authorization"))
        .cloned();
    maybe_add_via(headers, auth.as_deref());
}

/// Check if a request is signed with AWS Signature Version 4 (or SigV4A)
pub(crate) fn is_aws_sigv4_signed(headers: &HashMap<String, String>) -> bool {
    headers
        .get("authorization")
        .or_else(|| headers.get("Authorization"))
        .map(|auth| is_sigv4_algorithm(auth))
        .unwrap_or(false)
}

/// Check if the Range header is included in the request's SigV4 signed headers
///
/// Reads both places a client records what it signed: the `Authorization`
/// header's `SignedHeaders=` and a presigned URL's `X-Amz-SignedHeaders` query
/// parameter. If either lists `range`, modifying the Range header would
/// invalidate the signature.
///
/// Accepts both SigV4 (`AWS4-HMAC-SHA256`) and SigV4A
/// (`AWS4-ECDSA-P256-SHA256`); the `SignedHeaders=...` portion has identical
/// syntax in both.
///
/// # Arguments
/// * `headers` - Request headers including Authorization
/// * `uri` - The client's request target (path and query), as the client sent
///   it; see [`request_target`]
///
/// # Returns
/// * `true` if Range is in SignedHeaders, `false` otherwise
///
/// # Example Authorization Header
/// ```text
/// AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, #gitleaks:allow
/// SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,
/// Signature=...
/// ```
///
/// # Requirements
/// - Requirement 1.2: Check if request contains Authorization header with AWS SigV4 signature
/// - Requirement 1.3: Mark request as signed range request if SignedHeaders includes "range"
/// - Requirement 1.4: Return false if SignedHeaders does not include "range"
/// - Requirement 1.5: Return false for malformed or absent Authorization header
pub fn is_range_signed(headers: &HashMap<String, String>, uri: &str) -> bool {
    is_header_signed(headers, "range") || is_header_signed_in_query(uri, "range")
}

/// The request target (path and query) of `uri`, the form
/// [`is_header_signed_in_query`] and [`is_range_signed`] read.
pub(crate) fn request_target(uri: &hyper::Uri) -> &str {
    uri.path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| uri.path())
}

/// Check whether a named header is included in AWS SigV4 `SignedHeaders`.
///
/// Generalization of [`is_range_signed`] for any header the proxy may want to
/// rewrite or strip: doing so to a SIGNED header invalidates the client's
/// signature and earns a `SignatureDoesNotMatch` from S3, so every such
/// rewrite must be gated on this returning `false`.
///
/// `header_name` must be given lowercase, matching the canonical form used in
/// the `SignedHeaders` list.
///
/// Returns `false` for an absent or malformed `Authorization` header, and for
/// a non-SigV4-family signature — in those cases there is no signature for a
/// rewrite to invalidate. Matching is exact per semicolon-separated entry, so
/// `x-range` and `content-range` never match `range`.
pub(crate) fn is_header_signed(headers: &HashMap<String, String>, header_name: &str) -> bool {
    // Get Authorization header (case-insensitive)
    let auth = headers
        .get("authorization")
        .or_else(|| headers.get("Authorization"));

    let auth_value = match auth {
        Some(v) => v,
        None => return false, // No Authorization header (Requirement 1.5)
    };

    // Check if this is a SigV4-family signature (SigV4 or SigV4A)
    if !is_sigv4_algorithm(auth_value) {
        return false; // Not SigV4 signature (Requirement 1.5)
    }

    // Find SignedHeaders parameter
    let signed_headers_start = match auth_value.find("SignedHeaders=") {
        Some(pos) => pos,
        None => return false, // Malformed header - no SignedHeaders (Requirement 1.5)
    };

    let after_param = &auth_value[signed_headers_start + 14..]; // Skip "SignedHeaders="

    // Extract until comma, space, or end of string
    let signed_headers_end = after_param
        .find(',')
        .or_else(|| after_param.find(' '))
        .unwrap_or(after_param.len());

    let signed_headers = &after_param[..signed_headers_end];

    // Check if the named header is in the semicolon-separated list.
    // Must be exact match to avoid matching "x-range" or similar (Requirement 1.3, 1.4)
    signed_headers.split(';').any(|h| h == header_name)
}

/// Whether a presigned SigV4 URL's `X-Amz-SignedHeaders` query parameter lists
/// `header_name`.
///
/// The query-string counterpart of [`is_header_signed`]: a presigned URL carries
/// its signed-header list in the query, not in an `Authorization` header, so a
/// header-only check reports "unsigned" for every presigned request. The value is
/// matched after decoding `%3B`/`%3b` to `;`, the encoding SDKs emit.
///
/// `header_name` must be lowercase. Returns `false` when the URI has no such
/// parameter.
pub(crate) fn is_header_signed_in_query(uri: &str, header_name: &str) -> bool {
    let query = match uri.split_once('?') {
        Some((_, q)) => q,
        None => return false,
    };
    query.split('&').any(|pair| {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        name.eq_ignore_ascii_case("x-amz-signedheaders")
            && value
                .replace("%3B", ";")
                .replace("%3b", ";")
                .split(';')
                .any(|h| h.eq_ignore_ascii_case(header_name))
    })
}

/// Whether the proxy may ADD `header_name` to a forwarded request without
/// invalidating its signature.
///
/// Adding a header SigV4 signs breaks the signature exactly as removing one does,
/// because the server recomputes the canonical request from what arrives. A header
/// that appears in neither the `Authorization` header's `SignedHeaders` nor a
/// presigned URL's `X-Amz-SignedHeaders` is outside the signature, so adding it is
/// safe; SigV2 never signs conditional headers.
pub(crate) fn may_add_unsigned_header(
    headers: &HashMap<String, String>,
    uri: &str,
    header_name: &str,
) -> bool {
    !is_header_signed(headers, header_name) && !is_header_signed_in_query(uri, header_name)
}

/// Buffer and forward a request whose caller must inspect or retain its full body.
///
/// This is the buffered counterpart to [`forward_signed_request_streaming_verbatim`].
/// It reserves against the in-flight buffered-byte ledger before allocation and is
/// correct only when the caller needs a complete `Bytes` body. Callers that only
/// forward must use [`forward_signed_request_streaming_verbatim`] so they do not
/// materialize the request body or participate in the buffered-byte ledger.
pub(crate) async fn forward_signed_request_bounded_with_ledger(
    req: Request<hyper::body::Incoming>,
    target_host: &str,
    transport: &UpstreamTransport,
    proxy_referer: Option<&str>,
    max_body_bytes: u64,
    inflight_ledger: &Arc<crate::inflight_ledger::InflightLedger>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
    debug!(
        "Forwarding signed request to {} ({}:{})",
        target_host, transport.ip, transport.port
    );

    // Extract request components
    let method = req.method().clone();
    let uri = req.uri().clone();
    let headers = req.headers().clone();
    let version = req.version();

    // Read request body with bounded size (Requirement 11.4), reserving
    // against the ledger before allocating (Requirement IMA 1.2). This
    // enforces the cap up front and returns the fully-buffered body, exactly
    // as before.
    let body_bytes =
        read_request_body_bounded_with_ledger(req, max_body_bytes, inflight_ledger).await?;

    // Delegate to the single streaming forward implementation, wrapping the
    // pre-buffered body in a single-frame `Full<Bytes>`. Header serialization and
    // Referer-injection live in `forward_signed_request_streaming` and are identical
    // to the prior inline logic, so this is byte-for-byte unchanged on the wire.
    // `tee = None` keeps caching off; passing `max_body_bytes` preserves the same cap
    // value this function already applied.
    forward_signed_request_streaming(
        &method,
        &uri,
        &headers,
        version,
        Full::new(body_bytes),
        target_host,
        transport,
        proxy_referer,
        max_body_bytes,
        None,
    )
    .await
}

/// Stream a request that only needs verbatim forwarding to the upstream.
///
/// This is the non-buffering counterpart to
/// [`forward_signed_request_bounded_with_ledger`]. It decomposes the inbound
/// request and forwards its original body frame-by-frame with `tee = None`, so it
/// neither materializes the body nor accepts a ledger parameter. Use it for
/// forward-only callers; callers that need to inspect or retain the complete body
/// must use the bounded forward instead.
pub(crate) async fn forward_signed_request_streaming_verbatim(
    req: Request<hyper::body::Incoming>,
    target_host: &str,
    transport: &UpstreamTransport,
    proxy_referer: Option<&str>,
    cap: u64,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
    debug!(
        "Streaming verbatim request to {} ({}:{}) without caching",
        target_host, transport.ip, transport.port
    );

    let (parts, body) = req.into_parts();
    forward_signed_request_streaming(
        &parts.method,
        &parts.uri,
        &parts.headers,
        parts.version,
        body,
        target_host,
        transport,
        proxy_referer,
        cap,
        None,
    )
    .await
}

/// [`forward_signed_request_streaming_verbatim`], returning the buffered response
/// for a caller that reads its body.
pub(crate) async fn forward_signed_request_streaming_verbatim_buffered(
    req: Request<hyper::body::Incoming>,
    target_host: &str,
    transport: &UpstreamTransport,
    proxy_referer: Option<&str>,
    cap: u64,
) -> Result<BufferedResponse> {
    let (parts, body) = req.into_parts();
    forward_signed_request_streaming_buffered(
        &parts.method,
        &parts.uri,
        &parts.headers,
        parts.version,
        body,
        target_host,
        transport,
        proxy_referer,
        cap,
        None,
    )
    .await
}

/// [`forward_signed_request_with_body`], returning the buffered response for a
/// caller that reads its body.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn forward_signed_request_with_body_buffered(
    method: hyper::Method,
    uri: hyper::Uri,
    headers: hyper::HeaderMap,
    version: hyper::Version,
    body_bytes: Bytes,
    target_host: &str,
    transport: &UpstreamTransport,
    proxy_referer: Option<&str>,
) -> Result<BufferedResponse> {
    forward_signed_request_streaming_buffered(
        &method,
        &uri,
        &headers,
        version,
        Full::new(body_bytes),
        target_host,
        transport,
        proxy_referer,
        u64::MAX,
        None,
    )
    .await
}

/// Forward a signed request with a pre-buffered body
///
/// This function is similar to `forward_signed_request` but accepts a pre-buffered body
/// instead of reading from the request. This is useful when the body needs to be parsed
/// before forwarding (e.g., CompleteMultipartUpload).
///
/// # Arguments
///
/// * `method` - HTTP method
/// * `uri` - Request URI
/// * `headers` - Request headers
/// * `version` - HTTP version
/// * `body_bytes` - Pre-buffered request body
/// * `target_host` - Target hostname for TLS SNI / `Host`
/// * `transport` - Resolved upstream transport (connect IP/port + TLS-or-plaintext)
///
/// # Returns
///
/// Returns the S3 response on success
#[allow(clippy::too_many_arguments)]
pub(crate) async fn forward_signed_request_with_body(
    method: hyper::Method,
    uri: hyper::Uri,
    headers: hyper::HeaderMap,
    version: hyper::Version,
    body_bytes: Bytes,
    target_host: &str,
    transport: &UpstreamTransport,
    proxy_referer: Option<&str>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
    debug!(
        "Forwarding signed request with pre-buffered body to {} ({}:{})",
        target_host, transport.ip, transport.port
    );

    // Delegate to the single streaming forward implementation, wrapping the
    // pre-buffered body in a single-frame `Full<Bytes>`. Header serialization and
    // Referer-injection live in `forward_signed_request_streaming` and are identical
    // to the prior inline logic here, so this is byte-for-byte unchanged on the wire.
    //
    // This caller (CompleteMultipartUpload, the only buffered-body caller) reads the
    // small completion XML and never applied a body cap, so `cap = u64::MAX`
    // preserves that no-cap behaviour exactly. `tee = None` keeps caching off — this
    // path forwards only; cache finalization is handled by the caller.
    forward_signed_request_streaming(
        &method,
        &uri,
        &headers,
        version,
        Full::new(body_bytes),
        target_host,
        transport,
        proxy_referer,
        u64::MAX,
        None,
    )
    .await
}

/// Does the serialized outbound header block advertise `Transfer-Encoding:
/// chunked`?
///
/// chunked-request-framing (GitHub issue #19) R1.3: the re-framing decision is
/// evaluated against the header block actually being sent to the upstream —
/// not the inbound request's header map and not a config field or heuristic —
/// so that a future header rewrite between "decide the headers" and "send the
/// headers" cannot desynchronize the framing decision from what really goes on
/// the wire. `header_block` is the exact bytes `forward_signed_request_streaming`
/// writes to the socket, so scanning it directly is the ground truth.
///
/// This is a plain case-insensitive substring scan of the raw serialized
/// header block, matching the header name followed by its value, on its own
/// line. HTTP header names and the `chunked` token are case-insensitive; the
/// header block itself is built with `\r\n`-terminated lines by this same
/// function, so scanning for `\r\ntransfer-encoding:` (with a leading `\r\n`
/// this function's own request line ends with) is a real line-boundary match,
/// not a substring that could appear inside a header value.
fn header_block_advertises_chunked_transfer_encoding(header_block: &[u8]) -> bool {
    let lower = header_block.to_ascii_lowercase();
    // Each header line in `header_block` is written as `\r\n{name}: {value}\r\n`,
    // so prefixing the needle with `\r\n` anchors the match to a line start
    // rather than to a byte sequence that happens to appear inside a header
    // value (e.g. a `Referer` URL path segment).
    let needle = b"\r\ntransfer-encoding:";
    let Some(pos) = lower
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| p + needle.len())
    else {
        return false;
    };
    // From the end of the header name to the end of that line is the value.
    let rest = &lower[pos..];
    let line_end = rest
        .windows(2)
        .position(|w| w == b"\r\n")
        .unwrap_or(rest.len());
    let value = &rest[..line_end];
    value.windows(b"chunked".len()).any(|w| w == b"chunked")
}

/// Forward a signed request by **streaming** the body to the upstream
/// frame-by-frame (the Streaming_Write_Path), instead of buffering the whole body
/// in RAM first.
///
/// This is the single streaming forward implementation the signed-write path
/// converges on. Header serialization is **byte-for-byte identical** to
/// [`forward_signed_request_with_body`]: the same request line, the same
/// `headers.iter()` order and casing, and the same Referer-injection rules. This
/// is load-bearing for SigV4 — any drift in header order, casing, or values
/// invalidates the signature (Requirements 4.1, 4.2, 4.3, 4.4). The signed bytes
/// and the header block are always forwarded verbatim; the body, however, is
/// re-transmitted rather than merely relayed (chunked-request-framing, GitHub
/// issue #19): each client body frame is written to the upstream with an
/// awaited `write_all`, so the proxy never holds the whole body in memory
/// (Requirements 1.1, 1.2, 1.3) and the awaited write provides the primary
/// backpressure to the client (Requirement 2.1). WHEN the outbound request
/// advertises `Transfer-Encoding: chunked`, the frame's bytes are wrapped in a
/// fresh HTTP chunk (`{len:x}\r\n{data}\r\n`), because hyper has already
/// stripped that outer framing on the way in and this function is what
/// restores it on the way out — the bytes forwarded are the client's, but the
/// transport framing around them is the proxy's, not a passthrough of what it
/// received. For every other request shape (no `Transfer-Encoding: chunked`
/// header on the outbound side) the frame is still written raw, unchanged from
/// before this fix. See [`header_block_advertises_chunked_transfer_encoding`] for
/// the exact condition and R1-R2 in the chunked request framing feature for
/// why this is required rather than optional: a raw write of a chunked body's
/// already-unwrapped frames produces bytes S3 cannot parse as either valid
/// HTTP chunking or a valid Content-Length body.
///
/// The upstream connection, transport selection, and the
/// `UpstreamTlsValidationFailed` mapping are unchanged — they live entirely in
/// [`connect_upstream_with_retry`] (Requirements 5.3, 5.4), which this function
/// calls exactly as the buffered path does.
///
/// # Parameters
/// * `cap` — an internal body-size cap enforced without buffering the whole
///   body (Requirements 8.1, 8.2, 8.3, 8.4): a declared `Content-Length`
///   exceeding `cap` is rejected with HTTP 413 `EntityTooLarge` before any
///   body byte is read, and a body whose streamed bytes exceed `cap` mid-stream
///   stops the upload and fails with the same 413. `cap == u64::MAX` means
///   "no cap" for the already-buffered CompleteMultipartUpload caller and can
///   never trip either check.
/// * `tee` — optional cache tee (`None` means no caching). When `Some`, each
///   frame forwarded to the upstream is also sent to this bounded channel using
///   the `TeeStream` discipline (`try_send`; on `Full` an awaited `send`; on
///   `Closed` the tee is dropped and forwarding continues verbatim). The bounded
///   channel plus one in-flight frame is the whole per-request streaming memory
///   budget (Req 2.2, 2.3, 7.1, 7.3). The background cache task that *consumes*
///   this channel (incremental decode + `WriteCacheRangeSink`) is wired in a
///   later task; this function only owns the send side.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn forward_signed_request_streaming_buffered<B>(
    method: &hyper::Method,
    uri: &hyper::Uri,
    headers: &hyper::HeaderMap,
    version: hyper::Version,
    body: B,
    target_host: &str,
    transport: &UpstreamTransport,
    proxy_referer: Option<&str>,
    cap: u64,
    tee: Option<mpsc::Sender<Bytes>>,
) -> Result<BufferedResponse>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
{
    // `cap` is enforced below without buffering the whole body (Req 8). When `tee`
    // is `Some`, each forwarded frame is also tee'd to the bounded cache channel in
    // the body loop below (the send side of the TeeStream discipline); the
    // background task that consumes the channel is wired in a later task.
    debug!(
        "Streaming signed request to {} ({}:{}), body_cap={}, caching={}",
        target_host,
        transport.ip,
        transport.port,
        cap,
        tee.is_some()
    );

    // Body_Size_Cap fast-reject (Req 8.1): if the client declares a `Content-Length`
    // exceeding the cap, reject with HTTP 413 `EntityTooLarge` *before* reading any
    // body bytes — and before connecting to the upstream, so an oversized upload
    // never opens an upstream connection or streams a single byte. This mirrors the
    // up-front Content-Length check of the buffered body reader. The same
    // `RequestBodyTooLarge` variant is mapped to HTTP 413 by the handler error
    // mapping. `cap == u64::MAX` (no-cap callers) can never trip this.
    let content_length_hint = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if let Some(cl) = content_length_hint {
        if cl > cap {
            warn!(
                content_length = cl,
                max_bytes = cap,
                "Streamed request body exceeds S3 upload size limit (Content-Length)"
            );
            return Err(ProxyError::RequestBodyTooLarge {
                content_length: Some(cl),
                max_bytes: cap,
            });
        }
    }

    // Determine if we should inject the Referer header. These rules are identical to
    // the buffered forward functions and MUST stay so: they decide which headers are
    // sent, and SigV4 covers the signed header set.
    let should_inject_referer = if proxy_referer.is_some() {
        let has_referer = headers.contains_key("referer");
        if has_referer {
            false
        } else {
            let auth_value = headers.get("authorization").and_then(|v| v.to_str().ok());
            if let Some(auth) = auth_value {
                if is_sigv4_algorithm(auth) {
                    if let Some(pos) = auth.find("SignedHeaders=") {
                        let after_param = &auth[pos + 14..];
                        let end = after_param
                            .find(',')
                            .or_else(|| after_param.find(' '))
                            .unwrap_or(after_param.len());
                        let signed_headers = &after_param[..end];
                        !signed_headers.split(';').any(|h| h == "referer")
                    } else {
                        true
                    }
                } else {
                    true
                }
            } else {
                true
            }
        }
    } else {
        false
    };

    // Serialize the request line + headers + terminating CRLF exactly as the
    // buffered path does (byte-for-byte — SigV4 depends on it). This header block is
    // small and bounded, so buffering it is fine; only the body is streamed.
    let mut header_block = Vec::new();

    // Request line: METHOD /path?query HTTP/1.1
    let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    header_block
        .extend_from_slice(format!("{} {} {:?}\r\n", method, path_and_query, version).as_bytes());

    // Loop detection: append this process to `Via` unless the client signed `via`,
    // in the Authorization header or in a presigned URL's `X-Amz-SignedHeaders`.
    let via_signed = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| auth_signs_header(a, "via"))
        || is_header_signed_in_query(path_and_query, "via");

    // Headers - preserve exactly as received (a `Via` is merged below, unless signed)
    let mut inbound_via: Vec<String> = Vec::new();
    for (name, value) in headers.iter() {
        if let Ok(value_str) = value.to_str() {
            if !via_signed && name == hyper::header::VIA {
                inbound_via.push(value_str.to_string());
                continue;
            }
            header_block
                .extend_from_slice(format!("{}: {}\r\n", name.as_str(), value_str).as_bytes());
        }
    }
    if !via_signed {
        let existing = (!inbound_via.is_empty()).then(|| inbound_via.join(", "));
        header_block
            .extend_from_slice(format!("via: {}\r\n", append_via(existing.as_deref())).as_bytes());
    }

    // Inject Referer header if conditions are met
    if should_inject_referer {
        if let Some(referer_value) = proxy_referer {
            debug!(
                "Adding proxy identification Referer header to signed request: {}",
                referer_value
            );
            header_block.extend_from_slice(format!("Referer: {}\r\n", referer_value).as_bytes());
        }
    }

    // End of headers
    header_block.extend_from_slice(b"\r\n");

    // chunked-request-framing (GitHub issue #19, R1.1-R1.3): decide whether the
    // body must be re-framed as HTTP chunked transfer-encoding, or written raw
    // as before. Evaluated against the serialized `header_block` actually being
    // sent — not the inbound `headers` map and not a config field or client
    // heuristic — so that a future header rewrite cannot desynchronize the
    // condition from what really goes on the wire. Today `header_block` is a
    // byte-for-byte copy of `headers` (plus an optional Referer line that never
    // sets this header), so this is equivalent to testing `headers` directly,
    // but testing the sent bytes is what R1.3 requires.
    let chunked_outbound = header_block_advertises_chunked_transfer_encoding(&header_block);

    // Establish the upstream connection per the resolved transport (unchanged: TLS
    // on the default/validated/unvalidated paths, plaintext for the http override;
    // the UpstreamTlsValidationFailed mapping is preserved inside this call).
    let mut stream = IdleBoundStream::new(
        open_upstream(transport, target_host).await?,
        transport.timeouts.idle,
    );

    let sent = send_request(
        &mut stream,
        &header_block,
        chunked_outbound,
        body,
        cap,
        content_length_hint,
        tee,
    )
    .await?;
    let received: Result<BufferedResponse> = async {
        let mut response = ResponseBuffer::new(
            crate::http_proxy::BUFFERED_OPERATION_BOUND as usize,
            transport.response_ledger.as_ref(),
        );
        let start = await_final_head(&mut stream, &sent, &mut response).await?;
        read_response_rest(&mut stream, start, &mut response).await?;
        let ResponseBuffer {
            bytes, reservation, ..
        } = response;
        debug!("Received raw HTTP response: {} bytes", bytes.len());

        let (parts, body) = parse_http_response(bytes, method, path_and_query)?;
        Ok(BufferedResponse {
            parts,
            body,
            reservation,
        })
    }
    .await;
    received.map_err(after_send)
}

/// Connect to the upstream for one signed-write attempt.
///
/// `timeouts.connect` is one deadline around the whole of
/// [`connect_upstream_with_retry`]: every TCP attempt, every TLS handshake and the
/// backoff between them, so the retries cannot multiply it. No request byte has
/// been written when it expires. The outcome, connected or not, is recorded against
/// `transport.ip` when the transport carries a health record.
async fn open_upstream(transport: &UpstreamTransport, target_host: &str) -> Result<UpstreamStream> {
    let connect = transport.timeouts.connect;
    let opened = match tokio::time::timeout(
        connect,
        connect_upstream_with_retry(transport, target_host, 5),
    )
    .await
    {
        Ok(stream) => stream,
        Err(_) => {
            warn!(
                "Upstream connect to {} ({}:{}) did not complete within {:?}",
                target_host, transport.ip, transport.port, connect
            );
            Err(ProxyError::TimeoutError(format!(
                "upstream connect: not connected within {:?}",
                connect
            )))
        }
    };
    if let Some(health) = &transport.health {
        health.record(transport.ip, opened.is_ok());
    }
    opened
}
/// Write the request head, then the body frame by frame, then the chunked
/// terminator or trailers, and flush.
///
/// The awaited `write_all` is the primary backpressure source: the next client
/// frame is not pulled until the upstream socket accepts the current one
/// (Requirement 2.1), and no whole-body buffer is ever materialized.
async fn send_request<S, B>(
    stream: &mut S,
    header_block: &[u8],
    chunked_outbound: bool,
    body: B,
    cap: u64,
    content_length_hint: Option<u64>,
    tee: Option<mpsc::Sender<Bytes>>,
) -> Result<RequestSent>
where
    S: AsyncWrite + Unpin,
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
{
    stream.write_all(header_block).await.map_err(|e| {
        upstream_io_error(
            e,
            "upstream request write",
            "Failed to write request headers",
        )
    })?;

    // Stream the body frame-by-frame, verbatim. The awaited `write_all` is the
    // primary backpressure source: the next client frame is not pulled until the
    // upstream socket accepts the current one (Requirement 2.1), and no whole-body
    // buffer is ever materialized (Requirements 1.1, 1.2, 1.3, 4.1, 4.3).
    //
    // `sent` tracks the running count of body bytes forwarded so the Body_Size_Cap
    // is enforced mid-stream without buffering the whole body (Req 8.2, 8.3): on
    // exceeding `cap` we stop reading the client body and fail with the same 413
    // `EntityTooLarge` behaviour the buffered body reader produces, before
    // writing the offending frame.
    let mut body = body;
    // `tee` is rebound mutable so the channel-closed branch below can drop it
    // (set it to `None`) and stop teeing while continuing to forward verbatim.
    let mut tee = tee;
    let mut sent: u64 = 0;
    // Observability for the streaming-write tuning question (write_cache_tee_channel_depth):
    // record the body-frame size distribution the tee actually sees and how often the
    // bounded tee channel backpressured (`Full` → awaited send). `tee_full_waits > 0`
    // means the cache writer, not the upstream, was the bottleneck for this request.
    let mut frame_count: u64 = 0;
    let mut max_frame: usize = 0;
    let mut tee_full_waits: u64 = 0;
    // R2.1/R2.3: set when a trailers frame has already written the zero-length
    // chunk plus trailer section, so the fallback terminator after the loop is
    // not written a second time.
    let mut wrote_terminator = false;
    while let Some(frame) = body.frame().await {
        let frame = frame
            .map_err(|e| ProxyError::HttpError(format!("Failed to read request body: {}", e)))?;
        // A trailers frame carries no entity bytes to forward as body data, but
        // when the outbound request is chunked it must still be re-emitted as an
        // HTTP trailer section after the terminating zero-length chunk (R2.1,
        // R2.2): S3 was told via `x-amz-trailer` to expect it, and dropping it
        // means a promised checksum trailer never arrives. `into_trailers()`
        // consumes `frame`, so try it first and fall through to `into_data()`
        // only on its `Err` (which returns the original frame unchanged).
        let frame = match frame.into_trailers() {
            Ok(trailer_map) => {
                if chunked_outbound {
                    // The zero-length chunk terminator comes first, then the
                    // trailer section (`name: value\r\n` lines), then the
                    // terminating blank line (R2.1, R2.3).
                    let mut trailer_bytes = Vec::from(&b"0\r\n"[..]);
                    for (name, value) in trailer_map.iter() {
                        if let Ok(value_str) = value.to_str() {
                            trailer_bytes.extend_from_slice(
                                format!("{}: {}\r\n", name.as_str(), value_str).as_bytes(),
                            );
                        }
                    }
                    trailer_bytes.extend_from_slice(b"\r\n");
                    stream.write_all(&trailer_bytes).await.map_err(|e| {
                        upstream_io_error(
                            e,
                            "upstream request write",
                            "Failed to write request trailers",
                        )
                    })?;
                    wrote_terminator = true;
                }
                // A trailers frame is always the last frame in a body (RFC 9112
                // §7.1.2); nothing else to do with it here.
                continue;
            }
            Err(original_frame) => original_frame,
        };
        // Only data frames carry body bytes; any other non-data frame kind (not
        // used by hyper's request bodies today) carries no entity bytes to
        // forward either.
        if let Ok(data) = frame.into_data() {
            // Under raw (non-chunked) framing this guard exists only to skip a
            // no-op write. Under chunked framing (R1.4) it is load-bearing: an
            // empty data frame emitted as `0\r\n\r\n` would be indistinguishable
            // from the real terminating zero-length chunk and would end the
            // body early, corrupting every subsequent frame and any trailer
            // section that follows.
            if data.is_empty() {
                continue;
            }
            frame_count += 1;
            if data.len() > max_frame {
                max_frame = data.len();
            }
            // Running cap enforcement (Req 8.2), counting payload bytes only —
            // never the chunk-framing overhead this fix adds (R5.1), so the
            // effective limit does not shift for a chunked request.
            // `saturating_add` makes the `cap == u64::MAX` (no-cap) callers
            // safe: the sum caps at `u64::MAX` and `u64::MAX > u64::MAX` is
            // false, so they never trip this check.
            if sent.saturating_add(data.len() as u64) > cap {
                warn!(
                    sent_bytes = sent,
                    chunk_bytes = data.len(),
                    max_bytes = cap,
                    content_length = ?content_length_hint,
                    "Streamed request body exceeds S3 upload size limit"
                );
                return Err(ProxyError::RequestBodyTooLarge {
                    content_length: content_length_hint,
                    max_bytes: cap,
                });
            }
            // Forward the frame to the upstream. When the outbound request
            // advertises `Transfer-Encoding: chunked` (R1.1), re-establish HTTP
            // chunked framing around this frame's bytes — a hex length, CRLF,
            // the frame bytes, CRLF — since hyper already stripped that outer
            // layer on the way in and never restores it on a raw write.
            // Otherwise (R1.2) write the frame raw, exactly as before: no
            // existing working request shape changes on the wire. Either way
            // this happens at the point of the socket write only — `data`
            // itself is never mutated, so the tee below still receives the
            // original unframed bytes (R4.1, R4.2).
            if chunked_outbound {
                let mut framed = Vec::with_capacity(data.len() + 16);
                framed.extend_from_slice(format!("{:x}\r\n", data.len()).as_bytes());
                framed.extend_from_slice(&data);
                framed.extend_from_slice(b"\r\n");
                stream.write_all(&framed).await.map_err(|e| {
                    upstream_io_error(e, "upstream request write", "Failed to write request body")
                })?;
            } else {
                // The awaited `write_all` is the primary backpressure source and
                // must happen for every forwarded frame regardless of the tee
                // (Req 4.1, 4.3, 2.1).
                stream.write_all(&data).await.map_err(|e| {
                    upstream_io_error(e, "upstream request write", "Failed to write request body")
                })?;
            }
            sent = sent.saturating_add(data.len() as u64);

            // Tee the forwarded frame to the cache, applying the same bounded-channel
            // discipline as `TeeStream` on the GET path. Only frames actually
            // forwarded to the upstream are tee'd (a frame rejected by the cap check
            // above returns before reaching here, so it is never tee'd). The tee
            // receives a cheap `Bytes` clone of the ORIGINAL unframed `data` (a
            // refcount bump), never the HTTP-chunk-framed bytes written to the
            // socket above — re-framing happens only at the write call, so the
            // tee can never see anything but the frame's original bytes (Req
            // 4.1, 4.2, 4.3, 7.3).
            //
            // Channel discipline (mirrors `tee_stream.rs`):
            //   - `try_send` fast path: on `Ok`, the frame is queued, continue.
            //   - `Full`: the bounded channel is at capacity, so switch to an awaited
            //     `send` — the one place the cache can briefly backpressure the upload
            //     (a bounded wait gated by the channel capacity, Req 2.2). The channel
            //     bounds the buffered bytes to its capacity; there is no unbounded
            //     internal queue, so per-request streaming memory is one in-flight
            //     frame plus the channel capacity (Req 2.3, 1.4).
            //   - `Closed`: the background cache task is gone. Drop the tee (set it to
            //     `None`) and keep forwarding to the upstream verbatim — a cache
            //     failure must never fail an upload the upstream would accept
            //     (Req 7.1, 7.3).
            if let Some(sender) = tee.as_ref() {
                let mut tee_gone = false;
                match sender.try_send(data.clone()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(frame)) => {
                        tee_full_waits += 1;
                        TEE_BACKPRESSURE_EVENTS_TOTAL_PUT.fetch_add(1, Ordering::Relaxed);
                        let backpressure_start = Instant::now();
                        let send_result = sender.send(frame).await;
                        TEE_BACKPRESSURE_WAIT_MS_TOTAL_PUT.fetch_add(
                            backpressure_start.elapsed().as_millis() as u64,
                            Ordering::Relaxed,
                        );
                        if send_result.is_err() {
                            debug!(
                                "Cache tee channel closed during backpressure, \
                                 continuing to forward without caching"
                            );
                            tee_gone = true;
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        debug!("Cache tee channel closed, continuing to forward without caching");
                        tee_gone = true;
                    }
                }
                if tee_gone {
                    // Stop teeing for the remainder of this request; keep forwarding.
                    tee = None;
                }
            }
        }
    }

    // R2.3: when the outbound request is chunked and no trailers frame arrived
    // (so `wrote_terminator` is still `false`), the body must end with the
    // zero-length chunk terminator `0\r\n\r\n`. When a trailers frame DID arrive,
    // the trailer-section write above already emitted the zero chunk followed
    // by the trailer lines and the final blank line, which is the correct
    // terminator for that case — do not write a second one.
    if chunked_outbound && !wrote_terminator {
        stream.write_all(b"0\r\n\r\n").await.map_err(|e| {
            upstream_io_error(
                e,
                "upstream request write",
                "Failed to write chunk terminator",
            )
        })?;
    }

    stream
        .flush()
        .await
        .map_err(|e| upstream_io_error(e, "upstream request write", "Failed to flush request"))?;

    debug!("Streamed signed request to upstream");

    // Frame-size / backpressure summary for streaming-write tuning. `mean_frame` and
    // `max_frame` reveal whether the tee channel holds KB-scale socket reads or
    // multi-MB client chunks (the write_cache_tee_channel_depth memory question), and
    // `tee_full_waits` reveals whether the bounded tee ever throttled the upload.
    if frame_count > 0 {
        debug!(
            frame_count,
            max_frame_bytes = max_frame,
            mean_frame_bytes = sent.checked_div(frame_count).unwrap_or(0),
            total_bytes = sent,
            tee_full_waits,
            "Streamed-write body frame summary"
        );
    }

    Ok(RequestSent {
        at: Instant::now(),
        body_bytes: sent,
    })
}

/// Read until the final (non-1xx) response head is complete, skipping any 1xx
/// heads before it. Returns where that head starts in `response`, or `None` when the
/// upstream closed first (`response` then holds everything it sent).
async fn await_final_head<S: AsyncRead + Unpin>(
    stream: &mut S,
    sent: &RequestSent,
    response: &mut ResponseBuffer,
) -> Result<Option<usize>> {
    let mut scratch = vec![0u8; 8192];
    loop {
        if let Some(start) = final_head_start(&response.bytes)? {
            debug!(
                post_body_wait_ms = sent.at.elapsed().as_millis() as u64,
                body_bytes = sent.body_bytes,
                "Upstream final response head received"
            );
            return Ok(Some(start));
        }
        if response
            .read_from(stream, &mut scratch, "upstream response head")
            .await?
            == 0
        {
            return Ok(None);
        }
    }
}

/// Read the rest of the final response under the completeness rules of
/// [`has_complete_response_from_pos`], leaving the final response alone in
/// `response`. When the upstream closes first, `response` keeps everything it sent.
async fn read_response_rest<S: AsyncRead + Unpin>(
    stream: &mut S,
    start: Option<usize>,
    response: &mut ResponseBuffer,
) -> Result<()> {
    let Some(start) = start else {
        return Ok(());
    };
    let mut scratch = vec![0u8; 8192];
    loop {
        if has_complete_response_from_pos(&response.bytes, start) {
            response.bytes.drain(..start);
            return Ok(());
        }
        if response
            .read_from(stream, &mut scratch, "upstream response body")
            .await?
            == 0
        {
            return Ok(());
        }
    }
}

/// [`forward_signed_request_streaming_buffered`], answered as a client response.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn forward_signed_request_streaming<B>(
    method: &hyper::Method,
    uri: &hyper::Uri,
    headers: &hyper::HeaderMap,
    version: hyper::Version,
    body: B,
    target_host: &str,
    transport: &UpstreamTransport,
    proxy_referer: Option<&str>,
    cap: u64,
    tee: Option<mpsc::Sender<Bytes>>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
{
    forward_signed_request_streaming_buffered(
        method,
        uri,
        headers,
        version,
        body,
        target_host,
        transport,
        proxy_referer,
        cap,
        tee,
    )
    .await
    .map(BufferedResponse::into_response)
}

/// Compute the allocation capacity for a body the buffered path must retain.
///
/// A declared length has already passed the request-body cap before this is called.
/// Reserving exactly that bounded length avoids `Vec` growth reallocations, whose
/// simultaneous old and new allocations previously raised peak memory above the body
/// size. Unknown-length requests retain the small initial allocation and grow only as
/// frames arrive.
fn buffered_body_initial_capacity(content_length_hint: Option<u64>, max_bytes: u64) -> usize {
    content_length_hint.unwrap_or(8192).min(max_bytes) as usize
}

/// Read the request body up to `max_bytes`, reserving against the in-flight
/// buffered-byte ledger before allocating.
///
/// A declared `Content-Length` is reserved up front, before the
/// `Vec::with_capacity` below — the Admission_Check happens before allocation
/// (Requirement IMA 1.2). Absent a declared length, this is an Unknown_Size_Site:
/// the reservation grows per chunk received via [`Reservation::try_grow`], and an
/// over-ceiling growth aborts the accumulation (committing nothing — there is no
/// cache write on this path to commit, so "abort" here means returning the
/// rejection error) rather than continuing to allocate (Requirements 3.2, 3.3,
/// 3.5). A disabled ledger (`Ledger_Disabled`) reserves nothing and this function
/// is byte-for-byte the pre-feature behaviour.
///
/// # Requirements
/// - Requirement 11.1, 11.2, 11.4 (Body_Size_Cap, unchanged from the original)
/// - Requirement IMA 1.2, 1.3, 2.1, 2.5, 3.2, 3.3, 3.5, 4.1
pub(crate) async fn read_request_body_bounded_with_ledger(
    req: Request<hyper::body::Incoming>,
    max_bytes: u64,
    ledger: &Arc<crate::inflight_ledger::InflightLedger>,
) -> Result<Bytes> {
    use http_body_util::BodyExt;

    let content_length_hint = req
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    // Fast reject if Content-Length already exceeds cap
    if let Some(cl) = content_length_hint {
        if cl > max_bytes {
            warn!(
                content_length = cl,
                max_bytes = max_bytes,
                "Request body exceeds max_buffered_request_body_bytes (Content-Length)"
            );
            return Err(ProxyError::RequestBodyTooLarge {
                content_length: Some(cl),
                max_bytes,
            });
        }
    }

    // Admission_Check before allocation. A declared Content-Length is reserved
    // up front in full; an absent one starts at zero and grows per chunk below
    // (Unknown_Size_Site treatment).
    let mut reservation = match ledger.try_reserve(content_length_hint.unwrap_or(0)) {
        Some(r) => r,
        None => {
            return Err(ProxyError::InflightCeilingExceeded {
                ceiling_bytes: ledger.ceiling_bytes(),
                requested_bytes: content_length_hint.unwrap_or(0),
            });
        }
    };

    let mut body = req.into_body();
    // The up-front cap check above constrains a declared length before this
    // allocation. Matching the capacity to that bounded length prevents
    // `Vec` from doubling through progressively larger allocations while it
    // accumulates a known-size body.
    let initial_capacity = buffered_body_initial_capacity(content_length_hint, max_bytes);
    let mut accumulated = Vec::with_capacity(initial_capacity);

    while let Some(frame) = body.frame().await {
        let frame = frame
            .map_err(|e| ProxyError::HttpError(format!("Failed to read request body: {}", e)))?;
        if let Ok(data) = frame.into_data() {
            if accumulated.len() as u64 + data.len() as u64 > max_bytes {
                warn!(
                    accumulated_bytes = accumulated.len(),
                    chunk_bytes = data.len(),
                    max_bytes = max_bytes,
                    content_length = ?content_length_hint,
                    "Request body exceeds max_buffered_request_body_bytes"
                );
                return Err(ProxyError::RequestBodyTooLarge {
                    content_length: content_length_hint,
                    max_bytes,
                });
            }
            // Grow the reservation only for bytes beyond what was already
            // reserved from the declared Content-Length (avoids double-
            // reserving the same bytes when a length was declared).
            let already_reserved = reservation.held_bytes();
            let new_total = accumulated.len() as u64 + data.len() as u64;
            if new_total > already_reserved {
                let growth = new_total - already_reserved;
                if !reservation.try_grow(growth) {
                    // `try_grow` already emitted the rate-limited rejection log
                    // (Requirement 2.6, `InflightLedger::log_rejection_rate_limited`);
                    // don't duplicate it here.
                    ledger.record_aborted_accumulation();
                    return Err(ProxyError::InflightCeilingExceeded {
                        ceiling_bytes: ledger.ceiling_bytes(),
                        requested_bytes: new_total,
                    });
                }
            }
            accumulated.extend_from_slice(&data);
        }
    }

    Ok(Bytes::from(accumulated))
}

/// Connect to target IP address, then apply the pool's socket settings (keepalive,
/// receive buffer, `TCP_NODELAY`) before any TLS handshake.
async fn connect_to_target(
    ip: IpAddr,
    port: u16,
    socket: &crate::config::ConnectionPoolConfig,
) -> Result<TcpStream> {
    let addr = std::net::SocketAddr::new(ip, port);

    debug!("Connecting to {}", addr);

    let tcp_stream = TcpStream::connect(addr).await.map_err(|e| {
        ProxyError::ConnectionError(format!("Failed to connect to {}: {}", addr, e))
    })?;

    crate::https_connector::apply_socket_options(tcp_stream, socket)
}

/// Establish TLS connection
async fn establish_tls(
    tcp_stream: TcpStream,
    hostname: &str,
    tls_connector: &TlsConnector,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    // A dotted bucket's virtual-hosted name is sent and checked as its S3
    // endpoint; the Host header in the request bytes is unchanged.
    let server_name = rustls::pki_types::ServerName::try_from(
        crate::https_connector::upstream_certificate_name(hostname),
    )
    .map_err(|e| ProxyError::TlsError(format!("Invalid server name {}: {}", hostname, e)))?
    .to_owned();

    debug!("Establishing TLS connection to {}", hostname);

    let tls_stream = tls_connector
        .connect(server_name, tcp_stream)
        .await
        .map_err(|e| {
            ProxyError::TlsError(format!("TLS handshake failed to {}: {}", hostname, e))
        })?;

    Ok(tls_stream)
}

/// Establish TLS connection with retry logic
///
/// Attempts to establish a TLS connection with exponential backoff retry.
/// Retries on both TCP connection failures and TLS handshake failures.
///
/// # Arguments
///
/// * `target_ip` - IP address to connect to
/// * `port` - Port number (typically 443)
/// * `hostname` - Hostname for TLS SNI
/// * `tls_connector` - TLS connector instance
/// * `max_retries` - Maximum number of retry attempts (default: 5)
///
/// # Returns
///
/// Returns a TLS stream on success, or ProxyError on failure after all retries
///
/// # Requirements
///
/// - Requirement 1.1: Retry TLS handshake failures up to 5 times
/// - Requirement 1.2: Retry with exponential backoff
/// - Requirement 1.3: Return last error on exhaustion
/// - Requirement 1.4: Stop retrying on success
/// - Requirement 1.5: Treat timeouts as retryable errors
/// - Requirement 2.2: Log each retry attempt with details
/// - Requirement 2.4: Use exponential backoff starting at 100ms
/// - Requirement 3.1: Log warnings with attempt number and error
/// - Requirement 3.3: Log successful recovery after failures
/// - Requirement 3.4: Include hostname and attempt number in logs
pub(crate) async fn establish_tls_with_retry(
    target_ip: IpAddr,
    port: u16,
    hostname: &str,
    tls_connector: &TlsConnector,
    max_retries: usize,
    socket: &crate::config::ConnectionPoolConfig,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let mut last_error = None;
    let mut had_failures = false;

    for attempt in 0..max_retries {
        // Connect to target (Requirement 1.1, 1.5)
        let tcp_stream = match connect_to_target(target_ip, port, socket).await {
            Ok(stream) => stream,
            Err(e) => {
                had_failures = true;
                // Log retry attempt with details (Requirement 2.2, 3.1, 3.4)
                warn!(
                    "TLS connection attempt {} failed for {}: TCP connection error: {}",
                    attempt + 1,
                    hostname,
                    e
                );
                last_error = Some(e);
                if attempt < max_retries - 1 {
                    // Exponential backoff with overflow protection (Requirement 2.4, 29.2, 29.3, 29.4)
                    // Clamp attempt to prevent shift overflow, use checked_shl/saturating_mul, cap at 60s
                    let clamped_attempt = attempt.min(20) as u32;
                    let delay_ms = 100u64
                        .saturating_mul(1u64.checked_shl(clamped_attempt).unwrap_or(u64::MAX))
                        .min(60_000);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
                // Log final failure (Requirement 3.2)
                error!(
                    "TLS connection failed after {} attempts for {}: {}",
                    max_retries,
                    hostname,
                    last_error.as_ref().unwrap()
                );
                return Err(last_error.unwrap());
            }
        };

        // Establish TLS (Requirement 1.1, 1.5)
        match establish_tls(tcp_stream, hostname, tls_connector).await {
            Ok(stream) => {
                // Log successful recovery if we had previous failures (Requirement 3.3)
                if had_failures {
                    info!(
                        "TLS connection succeeded on attempt {} for {} after previous failures",
                        attempt + 1,
                        hostname
                    );
                }
                // Stop retrying on success (Requirement 1.4)
                return Ok(stream);
            }
            Err(e) => {
                had_failures = true;
                // Log retry attempt with details (Requirement 2.2, 3.1, 3.4)
                warn!(
                    "TLS handshake attempt {} failed for {}: {}",
                    attempt + 1,
                    hostname,
                    e
                );
                last_error = Some(e);
                if attempt < max_retries - 1 {
                    // Exponential backoff with overflow protection (Requirement 2.4, 29.2, 29.3, 29.4)
                    // Clamp attempt to prevent shift overflow, use checked_shl/saturating_mul, cap at 60s
                    let clamped_attempt = attempt.min(20) as u32;
                    let delay_ms = 100u64
                        .saturating_mul(1u64.checked_shl(clamped_attempt).unwrap_or(u64::MAX))
                        .min(60_000);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
                // Log final failure (Requirement 3.2)
                error!(
                    "TLS handshake failed after {} attempts for {}: {}",
                    max_retries,
                    hostname,
                    last_error.as_ref().unwrap()
                );
                // Return last error on exhaustion (Requirement 1.3)
                return Err(last_error.unwrap());
            }
        }
    }

    // Should never reach here, but just in case
    Err(last_error.unwrap_or_else(|| ProxyError::TlsError("All retry attempts failed".to_string())))
}

/// Where the final (non-1xx) response head starts, once that head is complete.
/// Any 1xx heads before it are skipped.
fn final_head_start(bytes: &[u8]) -> Result<Option<usize>> {
    let mut pos = 0;
    while pos < bytes.len() {
        let header_end = match find_header_end_from_pos(bytes, pos) {
            Some(end_pos) => end_pos,
            None => return Ok(None),
        };
        let header_str = String::from_utf8_lossy(&bytes[pos..header_end]);
        let status_line = match header_str.lines().next() {
            Some(line) => line,
            None => {
                return Err(ProxyError::HttpError(
                    "Invalid HTTP response: no status line".to_string(),
                ))
            }
        };
        let status_code = parse_status_code(status_line)?;
        if status_code.is_informational() {
            info!(
                "Received informational response: {} - skipping",
                status_code
            );
            // A 1xx response has no body: the next response starts after its head.
            pos = header_end + 4;
            continue;
        }
        return Ok(Some(pos));
    }
    Ok(None)
}

/// Check if we have a complete HTTP response starting from a specific position
fn has_complete_response_from_pos(bytes: &[u8], start_pos: usize) -> bool {
    // Find the end of headers (double CRLF)
    let header_end = match find_header_end_from_pos(bytes, start_pos) {
        Some(pos) => pos,
        None => return false, // Headers not complete yet
    };

    // Parse headers to find Content-Length or Transfer-Encoding
    let header_section = &bytes[start_pos..header_end];
    let header_str = String::from_utf8_lossy(header_section);

    // Check for Content-Length
    for line in header_str.lines() {
        if line.to_lowercase().starts_with("content-length:") {
            if let Some(length_str) = line.split(':').nth(1) {
                if let Ok(content_length) = length_str.trim().parse::<usize>() {
                    let body_start = header_end + 4; // Skip \r\n\r\n
                    let current_body_length = bytes.len().saturating_sub(body_start);
                    return current_body_length >= content_length;
                }
            }
        }

        // Check for chunked encoding
        if line.to_lowercase().starts_with("transfer-encoding:") && line.contains("chunked") {
            // Check for end of chunked encoding (0\r\n\r\n)
            let remaining_bytes = &bytes[start_pos..];
            return remaining_bytes.ends_with(b"0\r\n\r\n")
                || remaining_bytes.ends_with(b"0\r\n\r\n\r\n");
        }
    }

    // If no Content-Length or Transfer-Encoding, assume complete after headers
    // (This handles 204 No Content, 304 Not Modified, etc.)
    true
}

/// Find the end of HTTP headers (position of \r\n\r\n)
fn find_header_end(bytes: &[u8]) -> Option<usize> {
    find_header_end_from_pos(bytes, 0)
}

/// Find the end of HTTP headers starting from a specific position
fn find_header_end_from_pos(bytes: &[u8], start_pos: usize) -> Option<usize> {
    (start_pos..bytes.len().saturating_sub(3)).find(|&i| &bytes[i..i + 4] == b"\r\n\r\n")
}

/// Parse raw HTTP response bytes into a Hyper Response
fn parse_http_response(
    bytes: Vec<u8>,
    method: &hyper::Method,
    path: &str,
) -> Result<(hyper::http::response::Parts, Bytes)> {
    // Owned: the body below is a slice of this buffer, not a copy.
    let bytes = Bytes::from(bytes);
    // Find end of headers
    let header_end = find_header_end(&bytes).ok_or_else(|| {
        ProxyError::HttpError("Invalid HTTP response: no header end found".to_string())
    })?;

    let header_section = &bytes[..header_end];
    let body_start = header_end + 4; // Skip \r\n\r\n

    // Parse status line and headers first to check for chunked encoding
    let header_str = String::from_utf8_lossy(header_section);
    let mut lines = header_str.lines();

    // Parse status line: HTTP/1.1 200 OK
    let status_line = lines.next().ok_or_else(|| {
        ProxyError::HttpError("Invalid HTTP response: no status line".to_string())
    })?;

    let status_code = parse_status_code(status_line)?;

    // Check for chunked encoding
    let mut is_chunked = false;
    let mut content_length: Option<usize> = None;

    for line in lines.clone() {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_lowercase();
            let value = value.trim();

            if name == "transfer-encoding" && value.contains("chunked") {
                is_chunked = true;
            } else if name == "content-length" {
                content_length = value.parse().ok();
            }
        }
    }

    // Extract and decode body based on encoding
    let body_bytes = if body_start < bytes.len() {
        let raw_body = bytes.slice(body_start..);

        if is_chunked {
            debug!("Decoding chunked response body");
            decode_chunked_body(&raw_body)?
        } else if let Some(length) = content_length {
            // Content-Length specified
            let actual_length = std::cmp::min(length, raw_body.len());
            raw_body.slice(..actual_length)
        } else {
            // No encoding specified, use as-is
            raw_body
        }
    } else if is_chunked
        && method != hyper::Method::HEAD
        && !matches!(status_code.as_u16(), 204 | 304)
    {
        // The head promised chunks and none arrived, not even the zero-size one.
        return Err(ProxyError::from(
            ChunkedDecodeError::TruncatedBeforeTerminator,
        ));
    } else {
        Bytes::new()
    };

    // Build response
    let mut response_builder = Response::builder().status(status_code);

    // Parse and add headers (reset iterator)
    let mut lines = header_str.lines();
    lines.next(); // Skip status line

    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim();
            let value = value.trim();

            // Skip Transfer-Encoding header since we've decoded the body
            if name.to_lowercase() == "transfer-encoding" && value.contains("chunked") {
                continue;
            }

            response_builder = response_builder.header(name, value);
        }
    }

    // Add Content-Length header for decoded body
    if is_chunked {
        response_builder = response_builder.header("content-length", body_bytes.len().to_string());
    }

    // Debug log for raw signed request response - detailed logging is in operation handlers
    debug!(
        "Signed {} {} response: status={}, body_len={}",
        method,
        path,
        status_code,
        body_bytes.len()
    );

    // Debug logging for multipart upload responses
    if path.contains("uploads") {
        debug!(
            "CreateMultipartUpload response details: status={}, body_len={}, chunked={}",
            status_code,
            body_bytes.len(),
            is_chunked
        );
        if status_code != StatusCode::OK {
            error!("Non-200 status for CreateMultipartUpload: {}", status_code);
        }
        if is_chunked {
            debug!(
                "Decoded chunked CreateMultipartUpload response: {} bytes",
                body_bytes.len()
            );
        }
    }

    let (parts, ()) = response_builder
        .body(())
        .map_err(|e| {
            error!("Failed to build HTTP response: {}", e);
            ProxyError::HttpError(format!("Failed to build response: {}", e))
        })?
        .into_parts();

    debug!(
        "Successfully built HTTP response with status: {}",
        status_code
    );
    Ok((parts, body_bytes))
}

/// Parse HTTP status code from status line
fn parse_status_code(status_line: &str) -> Result<StatusCode> {
    // Status line format: HTTP/1.1 200 OK
    let parts: Vec<&str> = status_line.split_whitespace().collect();

    if parts.len() < 2 {
        return Err(ProxyError::HttpError(format!(
            "Invalid status line: {}",
            status_line
        )));
    }

    let code_str = parts[1];
    let code = code_str
        .parse::<u16>()
        .map_err(|e| ProxyError::HttpError(format!("Invalid status code {}: {}", code_str, e)))?;

    StatusCode::from_u16(code)
        .map_err(|e| ProxyError::HttpError(format!("Invalid status code {}: {}", code, e)))
}

/// Configuration for chunked transfer decoding bounds.
///
/// Enforces per-chunk and total-body size limits to prevent silent truncation
/// or unbounded memory consumption from malformed chunked responses.
#[derive(Debug, Clone)]
struct ChunkedDecodeConfig {
    /// Maximum allowed size for a single chunk (default: 16 MiB)
    pub max_chunk_size: usize,
    /// Maximum total decoded body size (default: 100 MiB)
    pub max_total_decoded: usize,
}

impl Default for ChunkedDecodeConfig {
    fn default() -> Self {
        Self {
            max_chunk_size: 16 * 1024 * 1024,     // 16 MiB
            max_total_decoded: 100 * 1024 * 1024, // 100 MiB
        }
    }
}

/// Structured error type for chunked transfer decoding failures.
///
/// Callers that receive a `ChunkedDecodeError` must NOT commit the partial body
/// to the cache and must NOT return it to the client as a 200 response.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ChunkedDecodeError {
    /// A single chunk declared a size exceeding the configured maximum.
    ChunkTooLarge { declared: usize, max: usize },
    /// The accumulated decoded body exceeded the configured maximum.
    BodyTooLarge { accumulated: usize, max: usize },
    /// The read cap was reached before the zero-size terminator chunk.
    TruncatedBeforeTerminator,
    /// The chunk size line could not be parsed as a valid hex number.
    MalformedChunkSize(String),
}

impl std::fmt::Display for ChunkedDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChunkTooLarge { declared, max } => {
                write!(
                    f,
                    "Chunk too large: declared {} bytes, max {} bytes",
                    declared, max
                )
            }
            Self::BodyTooLarge { accumulated, max } => {
                write!(
                    f,
                    "Body too large: accumulated {} bytes, max {} bytes",
                    accumulated, max
                )
            }
            Self::TruncatedBeforeTerminator => {
                write!(
                    f,
                    "Truncated before terminator: read cap reached before zero-size chunk"
                )
            }
            Self::MalformedChunkSize(s) => {
                write!(f, "Malformed chunk size: '{}'", s)
            }
        }
    }
}

impl std::error::Error for ChunkedDecodeError {}

impl From<ChunkedDecodeError> for ProxyError {
    fn from(err: ChunkedDecodeError) -> Self {
        ProxyError::HttpError(format!("Chunked decode error: {}", err))
    }
}

/// Decode chunked HTTP response body with bounds checking.
///
/// Parses chunked encoding format:
/// - Each chunk starts with hex size followed by \r\n
/// - Chunk data follows, terminated by \r\n
/// - Final chunk has size 0 followed by \r\n\r\n
///
/// Returns a structured `ChunkedDecodeError` if:
/// - A chunk declares a size exceeding `config.max_chunk_size`
/// - The accumulated decoded body exceeds `config.max_total_decoded`
/// - The data is truncated before the zero-size terminator chunk
/// - A chunk size line cannot be parsed as hex
///
/// # Arguments
/// * `chunked_data` - Raw chunked response body
/// * `config` - Bounds configuration for decoding
///
/// # Returns
/// * `Ok(Bytes)` - Decoded body bytes without chunk markers
/// * `Err(ChunkedDecodeError)` - Structured error describing the failure
fn decode_chunked_body_bounded(
    chunked_data: &[u8],
    config: &ChunkedDecodeConfig,
) -> std::result::Result<Bytes, ChunkedDecodeError> {
    let mut decoded = Vec::new();
    let mut pos = 0;
    // Set only when the zero-size chunk is read. A body that ends at a chunk boundary
    // without it is cut short, however cleanly the cut falls.
    let mut terminated = false;

    debug!("Decoding chunked body: {} bytes", chunked_data.len());

    while pos < chunked_data.len() {
        // Find the end of the chunk size line (look for \r\n)
        let size_line_end = match find_crlf(&chunked_data[pos..]) {
            Some(offset) => pos + offset,
            None => {
                // No CRLF found — data is truncated before we could read a complete chunk header
                return Err(ChunkedDecodeError::TruncatedBeforeTerminator);
            }
        };

        // Parse chunk size (hex)
        let size_str = String::from_utf8_lossy(&chunked_data[pos..size_line_end]);
        let trimmed = size_str.trim();
        let chunk_size = usize::from_str_radix(trimmed, 16)
            .map_err(|_| ChunkedDecodeError::MalformedChunkSize(trimmed.to_string()))?;

        debug!("Chunk size: {} bytes", chunk_size);

        // Move past the size line and \r\n
        pos = size_line_end + 2;

        // If chunk size is 0, this is the final chunk
        if chunk_size == 0 {
            debug!("Found final chunk, decoding complete");
            terminated = true;
            break;
        }

        // Check per-chunk size limit
        if chunk_size > config.max_chunk_size {
            return Err(ChunkedDecodeError::ChunkTooLarge {
                declared: chunk_size,
                max: config.max_chunk_size,
            });
        }

        // Check total decoded body limit
        if decoded.len() + chunk_size > config.max_total_decoded {
            return Err(ChunkedDecodeError::BodyTooLarge {
                accumulated: decoded.len() + chunk_size,
                max: config.max_total_decoded,
            });
        }

        // Check if we have enough data for the chunk (truncation detection)
        if pos + chunk_size > chunked_data.len() {
            return Err(ChunkedDecodeError::TruncatedBeforeTerminator);
        }

        // Extract chunk data
        decoded.extend_from_slice(&chunked_data[pos..pos + chunk_size]);
        pos += chunk_size;

        // Skip the trailing \r\n after chunk data
        if pos + 2 <= chunked_data.len() && &chunked_data[pos..pos + 2] == b"\r\n" {
            pos += 2;
        } else {
            // Missing CRLF after chunk data means truncation
            return Err(ChunkedDecodeError::TruncatedBeforeTerminator);
        }
    }

    if !terminated {
        return Err(ChunkedDecodeError::TruncatedBeforeTerminator);
    }
    debug!("Decoded chunked body: {} bytes total", decoded.len());
    Ok(Bytes::from(decoded))
}

/// Decode chunked HTTP response body with default bounds.
///
/// Convenience wrapper around `decode_chunked_body_bounded` using `ChunkedDecodeConfig::default()`.
/// Returns a `ProxyError` on failure with structured logging.
fn decode_chunked_body(chunked_data: &[u8]) -> Result<Bytes> {
    let config = ChunkedDecodeConfig::default();
    decode_chunked_body_bounded(chunked_data, &config).map_err(|e| {
        warn!(
            error = %e,
            accumulated_bytes = chunked_data.len(),
            "Chunked transfer decode failed"
        );
        ProxyError::from(e)
    })
}

/// Find the position of \r\n in a byte slice
fn find_crlf(data: &[u8]) -> Option<usize> {
    (0..data.len().saturating_sub(1)).find(|&i| &data[i..i + 2] == b"\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    const AUTH_SIGNING_VIA: &str = "AWS4-HMAC-SHA256 Credential=AKID/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;via;x-amz-date, Signature=abcdef";
    const AUTH_NOT_SIGNING_VIA: &str = "AWS4-HMAC-SHA256 Credential=AKID/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcdef";

    #[test]
    fn via_match_is_exact_not_prefix() {
        assert!(via_names_pseudonym("1.1 s3-proxy-abc", "s3-proxy-abc"));
        // A shorter token that is a prefix of another instance's token is not a loop.
        assert!(!via_names_pseudonym("1.1 s3-proxy-abc", "s3-proxy-ab"));
        assert!(!via_names_pseudonym("1.1 s3-proxy-ab", "s3-proxy-abc"));
    }

    #[test]
    fn via_match_finds_entry_in_list_with_comment() {
        assert!(via_names_pseudonym(
            "1.0 fred, 1.1 s3-proxy-abc (chained), 1.1 other",
            "s3-proxy-abc"
        ));
        assert!(!via_names_pseudonym("1.0 fred, 1.1 other", "s3-proxy-abc"));
    }

    #[test]
    fn loop_detected_only_for_this_process_token() {
        let mut own = hyper::HeaderMap::new();
        own.append("via", format!("1.1 {}", via_pseudonym()).parse().unwrap());
        assert!(via_loop_detected(&own));

        let mut foreign = hyper::HeaderMap::new();
        foreign.append("via", "1.1 s3-proxy-0000000000000000".parse().unwrap());
        assert!(!via_loop_detected(&foreign));

        // Second header line is also inspected.
        let mut second_line = hyper::HeaderMap::new();
        second_line.append("via", "1.1 other".parse().unwrap());
        second_line.append("via", format!("1.1 {}", via_pseudonym()).parse().unwrap());
        assert!(via_loop_detected(&second_line));
    }

    #[test]
    fn maybe_add_via_appends_and_preserves_foreign_entry() {
        let mut headers = HashMap::new();
        headers.insert("via".to_string(), "1.1 upstream-proxy".to_string());
        maybe_add_via(&mut headers, Some(AUTH_NOT_SIGNING_VIA));
        assert_eq!(headers.len(), 1);
        assert_eq!(
            headers.get("Via").unwrap(),
            &format!("1.1 upstream-proxy, 1.1 {}", via_pseudonym())
        );
    }

    #[test]
    fn add_via_for_egress_is_idempotent_and_respects_presigned_via() {
        let uri: hyper::Uri = "/b/k".parse().unwrap();
        let mut headers = HashMap::new();
        add_via_for_egress(&mut headers, &uri);
        add_via_for_egress(&mut headers, &uri);
        assert_eq!(headers.len(), 1);
        assert_eq!(headers.get("Via").unwrap(), &append_via(None));

        let presigned: hyper::Uri = "/b/k?X-Amz-SignedHeaders=host%3Bvia&X-Amz-Signature=s"
            .parse()
            .unwrap();
        let mut signed = HashMap::new();
        signed.insert("via".to_string(), "1.1 signed-value".to_string());
        let before = signed.clone();
        add_via_for_egress(&mut signed, &presigned);
        assert_eq!(signed, before);
    }

    #[test]
    fn maybe_add_via_adds_when_absent() {
        let mut headers = HashMap::new();
        maybe_add_via(&mut headers, None);
        assert_eq!(
            headers.get("Via").unwrap(),
            &format!("1.1 {}", via_pseudonym())
        );
    }

    #[test]
    fn maybe_add_via_leaves_signed_via_byte_identical() {
        let mut headers = HashMap::new();
        headers.insert("via".to_string(), "1.1 signed-value".to_string());
        let before = headers.clone();
        maybe_add_via(&mut headers, Some(AUTH_SIGNING_VIA));
        assert_eq!(headers, before);

        let mut none = HashMap::new();
        maybe_add_via(&mut none, Some(AUTH_SIGNING_VIA));
        assert!(none.is_empty());
    }

    const RANGE_SIGNED_HEADER_AUTH: &str = "AWS4-HMAC-SHA256 Credential=TESTACCESSKEY/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=abc123";
    const RANGE_UNSIGNED_HEADER_AUTH: &str = "AWS4-HMAC-SHA256 Credential=TESTACCESSKEY/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123";
    fn presigned_target(signed_headers_param: &str) -> String {
        format!(
            "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&{}&X-Amz-Date=20130524T000000Z&X-Amz-Expires=3600&X-Amz-Signature=abc123",
            signed_headers_param
        )
    }
    #[test]
    fn test_is_range_signed_query_form_encodings() {
        let none = HashMap::new();
        for param in [
            "X-Amz-SignedHeaders=host%3Brange",
            "X-Amz-SignedHeaders=host%3brange",
            "X-Amz-SignedHeaders=host;range",
            "X-Amz-SignedHeaders=range%3Bhost",
            "x-amz-signedheaders=host%3Brange",
            "X-AMZ-SIGNEDHEADERS=host%3BRange",
        ] {
            assert!(
                is_range_signed(&none, &presigned_target(param)),
                "{param} signs range"
            );
        }
        // An absolute-form target is read the same way.
        assert!(is_range_signed(
            &none,
            "https://s3.amazonaws.com/bucket/key?X-Amz-SignedHeaders=host%3Brange"
        ));
    }
    #[test]
    fn test_is_range_signed_query_form_exact_entry_match() {
        let none = HashMap::new();
        for param in [
            "X-Amz-SignedHeaders=host%3Brange-extra",
            "X-Amz-SignedHeaders=host%3Bx-range",
            "X-Amz-SignedHeaders=host%3Bcontent-range",
            "X-Amz-SignedHeaders=host",
            "X-Amz-Other=host%3Brange",
            "range=bytes%3D0-1",
        ] {
            assert!(
                !is_range_signed(&none, &presigned_target(param)),
                "{param} does not sign range"
            );
        }
        assert!(!is_range_signed(&none, "/bucket/key"));
        assert!(!is_range_signed(&none, "/bucket/key?"));
    }
    #[test]
    fn test_is_range_signed_header_form_query_form_both_and_neither() {
        let mut header_signed = HashMap::new();
        header_signed.insert(
            "authorization".to_string(),
            RANGE_SIGNED_HEADER_AUTH.to_string(),
        );
        let mut header_unsigned = HashMap::new();
        header_unsigned.insert(
            "authorization".to_string(),
            RANGE_UNSIGNED_HEADER_AUTH.to_string(),
        );
        let none = HashMap::new();
        let query_signed = presigned_target("X-Amz-SignedHeaders=host%3Brange");
        let query_unsigned = presigned_target("X-Amz-SignedHeaders=host");
        // Header form only.
        assert!(is_range_signed(&header_signed, "/bucket/key"));
        assert!(is_range_signed(&header_signed, &query_unsigned));
        // Query form only.
        assert!(is_range_signed(&none, &query_signed));
        assert!(is_range_signed(&header_unsigned, &query_signed));
        // Both.
        assert!(is_range_signed(&header_signed, &query_signed));
        // Neither.
        assert!(!is_range_signed(&none, "/bucket/key"));
        assert!(!is_range_signed(&header_unsigned, &query_unsigned));
        assert!(!is_range_signed(&none, &query_unsigned));
    }
    #[test]
    fn test_request_target_keeps_the_query() {
        let origin: hyper::Uri = "/bucket/key?X-Amz-SignedHeaders=host%3Brange"
            .parse()
            .unwrap();
        assert_eq!(
            request_target(&origin),
            "/bucket/key?X-Amz-SignedHeaders=host%3Brange"
        );
        let absolute: hyper::Uri = "https://s3.amazonaws.com/bucket/key?a=b".parse().unwrap();
        assert_eq!(request_target(&absolute), "/bucket/key?a=b");
    }
    #[test]
    fn test_is_range_signed_with_range_in_signed_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_range_signed_without_range_in_signed_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(!is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_range_signed_uppercase_authorization_header() {
        let mut headers = HashMap::new();
        headers.insert(
            "Authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_range_signed_no_authorization_header() {
        let headers = HashMap::new();
        assert!(!is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_range_signed_non_sigv4_auth() {
        let mut headers = HashMap::new();
        headers.insert("authorization".to_string(), "Bearer token123".to_string());
        assert!(!is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_range_signed_malformed_no_signed_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(!is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_range_signed_range_at_end_of_signed_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date;range, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_range_signed_range_at_start_of_signed_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=range;host;x-amz-date, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_range_signed_range_only_header() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=range, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_range_signed_x_range_should_not_match() {
        // "x-range" should NOT match "range"
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-range;x-amz-date, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(!is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_range_signed_content_range_should_not_match() {
        // "content-range" should NOT match "range"
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;content-range;x-amz-date, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(!is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_range_signed_signed_headers_at_end_no_comma() {
        // SignedHeaders at end of string without trailing comma
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, Signature=abc123, SignedHeaders=host;range;x-amz-date".to_string() // #gitleaks:allow
        );
        assert!(is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_aws_sigv4_signed() {
        let mut headers = HashMap::new();

        // Test with full valid AWS4-HMAC-SHA256 signature
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123" // #gitleaks:allow
                .to_string(),
        );
        assert!(is_aws_sigv4_signed(&headers));

        // Test with uppercase header name
        headers.clear();
        headers.insert(
            "Authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123" // #gitleaks:allow
                .to_string(),
        );
        assert!(is_aws_sigv4_signed(&headers));

        // Test without signature
        headers.clear();
        assert!(!is_aws_sigv4_signed(&headers));

        // Test with different auth type
        headers.insert("authorization".to_string(), "Bearer token123".to_string());
        assert!(!is_aws_sigv4_signed(&headers));

        // Test with algorithm substring but missing components (Req 12.3)
        headers.clear();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request" // #gitleaks:allow
                .to_string(),
        );
        assert!(!is_aws_sigv4_signed(&headers));

        // Test with algorithm substring embedded in random text (Req 12.1)
        headers.clear();
        headers.insert(
            "authorization".to_string(),
            "something AWS4-HMAC-SHA256 Credential=x, SignedHeaders=host, Signature=abc"
                .to_string(),
        );
        assert!(!is_aws_sigv4_signed(&headers));
    }

    #[test]
    fn test_is_aws_sigv4_signed_accepts_sigv4a() {
        // SigV4A (AWS4-ECDSA-P256-SHA256) is used by default for MRAP requests.
        // The algorithm string differs from classic SigV4 but the rest of the
        // Authorization header follows the same Credential/SignedHeaders/Signature format.
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-ECDSA-P256-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20250101/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-region-set, \
             Signature=abcdef1234567890" // #gitleaks:allow
                .to_string(),
        );
        assert!(is_aws_sigv4_signed(&headers));
    }

    #[test]
    fn test_is_range_signed_with_sigv4a_range_signed() {
        // Range in SignedHeaders under a SigV4A Authorization header should still be detected.
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-ECDSA-P256-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20250101/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date;x-amz-region-set, \
             Signature=abcdef1234567890" // #gitleaks:allow
                .to_string(),
        );
        assert!(is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_range_signed_with_sigv4a_range_not_signed() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-ECDSA-P256-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20250101/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-region-set, \
             Signature=abcdef1234567890" // #gitleaks:allow
                .to_string(),
        );
        assert!(!is_range_signed(&headers, "/bucket/key"));
    }

    #[test]
    fn test_is_sigv4_algorithm_helper() {
        // Full valid SigV4 header
        assert!(is_sigv4_algorithm(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123" // #gitleaks:allow
        ));
        // Full valid SigV4A header
        assert!(is_sigv4_algorithm(
            "AWS4-ECDSA-P256-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123" // #gitleaks:allow
        ));
        // Not SigV4 at all
        assert!(!is_sigv4_algorithm("Bearer token"));
        assert!(!is_sigv4_algorithm(""));
        // Guard against partial/misspelled algorithms
        assert!(!is_sigv4_algorithm("AWS4-ECDSA Credential=..."));
        // Algorithm present but not at start (substring match should fail)
        assert!(!is_sigv4_algorithm(
            "Basic AWS4-HMAC-SHA256 Credential=x, SignedHeaders=host, Signature=abc"
        ));
        // Algorithm at start but missing required components
        assert!(!is_sigv4_algorithm("AWS4-HMAC-SHA256 Credential=x"));
        assert!(!is_sigv4_algorithm(
            "AWS4-HMAC-SHA256 Credential=x, SignedHeaders=host"
        ));
        assert!(!is_sigv4_algorithm(
            "AWS4-HMAC-SHA256 Credential=x, Signature=abc"
        ));
        assert!(!is_sigv4_algorithm(
            "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc"
        ));
        // Algorithm without trailing space (should fail)
        assert!(!is_sigv4_algorithm(
            "AWS4-HMAC-SHA256Credential=x, SignedHeaders=host, Signature=abc"
        ));
    }

    #[test]
    fn test_find_header_end() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHello";
        assert_eq!(find_header_end(response), Some(34));

        let incomplete = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n";
        assert_eq!(find_header_end(incomplete), None);
    }

    #[test]
    fn test_parse_status_code() {
        assert_eq!(
            parse_status_code("HTTP/1.1 200 OK").unwrap(),
            StatusCode::OK
        );
        assert_eq!(
            parse_status_code("HTTP/1.1 404 Not Found").unwrap(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            parse_status_code("HTTP/1.1 500 Internal Server Error").unwrap(),
            StatusCode::INTERNAL_SERVER_ERROR
        );

        assert!(parse_status_code("Invalid").is_err());
        assert!(parse_status_code("HTTP/1.1").is_err());
    }

    #[test]
    fn test_decode_chunked_body() {
        // Test simple chunked response
        let chunked_data = b"5\r\nHello\r\n6\r\n World\r\n0\r\n\r\n";
        let result = decode_chunked_body(chunked_data).unwrap();
        assert_eq!(result, Bytes::from("Hello World"));

        // Test single chunk
        let chunked_data = b"C\r\nHello World!\r\n0\r\n\r\n";
        let result = decode_chunked_body(chunked_data).unwrap();
        assert_eq!(result, Bytes::from("Hello World!"));

        // Test hex chunk size (like 172 from the AWS response)
        let xml_content = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>test</Bucket><Key>test</Key><UploadId>123</UploadId></InitiateMultipartUploadResult>";
        let chunk_size = format!("{:x}", xml_content.len());
        let mut chunked_data = Vec::new();
        chunked_data.extend_from_slice(chunk_size.as_bytes());
        chunked_data.extend_from_slice(b"\r\n");
        chunked_data.extend_from_slice(xml_content);
        chunked_data.extend_from_slice(b"\r\n0\r\n\r\n");

        let result = decode_chunked_body(&chunked_data).unwrap();
        assert_eq!(result, Bytes::from(xml_content.as_slice()));

        // Test empty body
        let chunked_data = b"0\r\n\r\n";
        let result = decode_chunked_body(chunked_data).unwrap();
        assert_eq!(result, Bytes::new());
    }

    #[test]
    fn a_chunked_body_without_the_zero_chunk_is_refused() {
        // Cut exactly after a chunk's trailing CRLF, and after a whole number of chunks.
        for cut in [
            &b"5\r\nHello\r\n"[..],
            &b"5\r\nHello\r\n6\r\n World\r\n"[..],
            &b""[..],
        ] {
            let result = decode_chunked_body_bounded(cut, &ChunkedDecodeConfig::default());
            assert_eq!(
                result,
                Err(ChunkedDecodeError::TruncatedBeforeTerminator),
                "body {:?} has no zero-size chunk",
                String::from_utf8_lossy(cut)
            );
        }
        // The same body with its terminator decodes.
        let whole = decode_chunked_body_bounded(
            b"5\r\nHello\r\n0\r\n\r\n",
            &ChunkedDecodeConfig::default(),
        );
        assert_eq!(whole, Ok(Bytes::from("Hello")));
    }
    #[test]
    fn a_chunked_response_with_no_body_bytes_is_refused_except_where_none_is_allowed() {
        let head = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        assert!(parse_http_response(head.clone(), &hyper::Method::POST, "/b/k").is_err());
        assert!(parse_http_response(head, &hyper::Method::HEAD, "/b/k").is_ok());
        let no_content = b"HTTP/1.1 204 No Content\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        assert!(parse_http_response(no_content, &hyper::Method::DELETE, "/b/k").is_ok());
    }
    /// Serve one connection on loopback: read the request head, write `reply`, close.
    async fn replying_upstream(reply: &'static [u8]) -> UpstreamTransport {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut seen = Vec::new();
                let mut byte = [0u8; 1];
                while !seen.ends_with(b"\r\n\r\n") {
                    if sock.read(&mut byte).await.unwrap_or(0) == 0 {
                        return;
                    }
                    seen.push(byte[0]);
                }
                let _ = sock.write_all(reply).await;
            }
        });
        UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        }
    }
    async fn forward_empty_put(transport: &UpstreamTransport) -> Result<StatusCode> {
        forward_signed_request_streaming(
            &hyper::Method::PUT,
            &"/bucket/key".parse::<hyper::Uri>().unwrap(),
            &hyper::HeaderMap::new(),
            hyper::Version::HTTP_11,
            Full::new(Bytes::new()),
            "example.com",
            transport,
            None,
            100,
            None,
        )
        .await
        .map(|response| response.status())
    }
    /// Every way a forward can fail after its request was sent in full is marked as an
    /// unknown outcome, and keeps the wording the client-visible 502 and 504 bodies show.
    /// A failure before that (no connection) is not marked.
    #[tokio::test]
    async fn every_exit_after_the_request_was_sent_is_an_unknown_outcome() {
        let replies: [(&str, &'static [u8]); 4] = [
            ("closed without a byte", b""),
            ("closed inside the head", b"HTTP/1.1 200 OK\r\nContent-Le"),
            ("not a status line", b"garbage\r\n\r\n"),
            (
                "chunked body cut at a chunk boundary",
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n",
            ),
        ];
        for (what, reply) in replies {
            let transport = replying_upstream(reply).await;
            let error = forward_empty_put(&transport).await.expect_err(what);
            assert!(
                write_outcome_unknown(&error),
                "{what}: a failure after the request was sent must be an unknown outcome: {error}"
            );
            assert!(
                error
                    .to_string()
                    .contains("request sent, outcome unknown: "),
                "{what}: the client-visible wording must be kept: {error}"
            );
        }
        // Nothing listens: the request was never sent.
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = closed.local_addr().unwrap().port();
        drop(closed);
        let transport = UpstreamTransport {
            ip: "127.0.0.1".parse().unwrap(),
            port,
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };
        let error = forward_empty_put(&transport)
            .await
            .expect_err("no connection");
        assert!(
            !write_outcome_unknown(&error),
            "a failure before the request was sent is not an unknown outcome: {error}"
        );
    }
    /// A response that arrives when the ledger is at its ceiling is still held in
    /// memory, so the ledger must show it: the figure and its peak cover every byte read.
    #[tokio::test]
    async fn a_response_that_arrives_at_the_ceiling_is_still_accounted() {
        let ledger = Arc::new(crate::inflight_ledger::InflightLedger::new(1000));
        let _held = ledger.try_reserve(900).expect("room for the held bytes");
        let mut buffer = ResponseBuffer::new(4096, Some(&ledger));
        let mut stream: &[u8] = &[b'x'; 300];
        let mut scratch = vec![0u8; 8192];
        let read = buffer
            .read_from(&mut stream, &mut scratch, "test")
            .await
            .unwrap();
        assert_eq!(read, 300);
        assert_eq!(
            ledger.reserved_bytes(),
            1200,
            "the 300 bytes held must be on the ledger beside the 900 already reserved"
        );
        assert_eq!(
            ledger.peak_reserved_bytes(),
            1200,
            "the peak must cover the bytes held"
        );
        drop(buffer);
        assert_eq!(
            ledger.reserved_bytes(),
            900,
            "dropping the buffer releases them"
        );
    }
    #[test]
    fn test_decode_chunked_body_bounded_chunk_too_large() {
        let config = ChunkedDecodeConfig {
            max_chunk_size: 10,
            max_total_decoded: 100,
        };
        // Declare a chunk of 20 bytes (exceeds max_chunk_size of 10)
        let chunked_data = b"14\r\n01234567890123456789\r\n0\r\n\r\n";
        let result = decode_chunked_body_bounded(chunked_data, &config);
        assert_eq!(
            result,
            Err(ChunkedDecodeError::ChunkTooLarge {
                declared: 20,
                max: 10
            })
        );
    }

    #[test]
    fn test_decode_chunked_body_bounded_body_too_large() {
        let config = ChunkedDecodeConfig {
            max_chunk_size: 100,
            max_total_decoded: 15,
        };
        // Two chunks of 10 bytes each = 20 total, exceeds max_total_decoded of 15
        let chunked_data = b"a\r\n0123456789\r\na\r\n0123456789\r\n0\r\n\r\n";
        let result = decode_chunked_body_bounded(chunked_data, &config);
        assert_eq!(
            result,
            Err(ChunkedDecodeError::BodyTooLarge {
                accumulated: 20,
                max: 15
            })
        );
    }

    #[test]
    fn test_decode_chunked_body_bounded_truncated_before_terminator() {
        let config = ChunkedDecodeConfig::default();
        // Data ends without a zero-size terminator chunk
        let chunked_data = b"5\r\nHello\r\n6\r\n World";
        let result = decode_chunked_body_bounded(chunked_data, &config);
        assert_eq!(result, Err(ChunkedDecodeError::TruncatedBeforeTerminator));
    }

    #[test]
    fn test_decode_chunked_body_bounded_malformed_chunk_size() {
        let config = ChunkedDecodeConfig::default();
        // "ZZ" is not valid hex
        let chunked_data = b"ZZ\r\nHello\r\n0\r\n\r\n";
        let result = decode_chunked_body_bounded(chunked_data, &config);
        assert_eq!(
            result,
            Err(ChunkedDecodeError::MalformedChunkSize("ZZ".to_string()))
        );
    }

    #[test]
    fn test_decode_chunked_body_bounded_success_within_limits() {
        let config = ChunkedDecodeConfig {
            max_chunk_size: 100,
            max_total_decoded: 100,
        };
        let chunked_data = b"5\r\nHello\r\n6\r\n World\r\n0\r\n\r\n";
        let result = decode_chunked_body_bounded(chunked_data, &config).unwrap();
        assert_eq!(result, Bytes::from("Hello World"));
    }

    #[test]
    fn test_decode_chunked_body_bounded_missing_crlf_after_data() {
        let config = ChunkedDecodeConfig::default();
        // Chunk data not followed by \r\n
        let chunked_data = b"5\r\nHello5\r\nWorld\r\n0\r\n\r\n";
        let result = decode_chunked_body_bounded(chunked_data, &config);
        assert_eq!(result, Err(ChunkedDecodeError::TruncatedBeforeTerminator));
    }

    #[test]
    fn test_find_crlf() {
        assert_eq!(find_crlf(b"Hello\r\nWorld"), Some(5));
        assert_eq!(find_crlf(b"\r\n"), Some(0));
        assert_eq!(find_crlf(b"No CRLF here"), None);
        assert_eq!(find_crlf(b"Multiple\r\nCRLF\r\nHere"), Some(8));
    }

    #[tokio::test]
    async fn test_streaming_cap_rejects_declared_length_above_s3_limit_before_connecting() {
        // Requirement 4.3 / 2.6: a declared length above S3's 5 GiB single-part
        // PUT and UploadPart maximum is rejected before any upstream connection.
        let mut headers = hyper::HeaderMap::new();
        let oversized_length = STREAMED_BODY_CAP + 1;
        headers.insert(
            "content-length",
            oversized_length.to_string().parse().unwrap(),
        );

        let transport = UpstreamTransport {
            ip: "127.0.0.1".parse().unwrap(),
            port: 1,
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };

        let result = forward_signed_request_streaming(
            &hyper::Method::PUT,
            &"/bucket/key".parse::<hyper::Uri>().unwrap(),
            &headers,
            hyper::Version::HTTP_11,
            Full::new(Bytes::new()),
            "example.com",
            &transport,
            None,
            STREAMED_BODY_CAP,
            None,
        )
        .await;

        match result {
            Err(ProxyError::RequestBodyTooLarge {
                content_length,
                max_bytes,
            }) => {
                assert_eq!(content_length, Some(oversized_length));
                assert_eq!(max_bytes, STREAMED_BODY_CAP);
            }
            Err(e) => panic!("expected RequestBodyTooLarge, got error: {}", e),
            Ok(_) => panic!("expected RequestBodyTooLarge, got Ok response"),
        }
    }

    #[tokio::test]
    async fn test_streaming_cap_rejects_oversized_body_midstream() {
        // Req 8.2/8.3: with no declared `Content-Length`, the running byte counter
        // trips the cap mid-stream and fails with the same `RequestBodyTooLarge`
        // behaviour, without buffering the whole body. A mock upstream accepts the
        // connection (so the header write succeeds) and drains bytes; the function
        // fails on the first oversized data frame before forwarding it.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
            }
        });

        let transport = UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };

        // 200-byte body, cap 100, no Content-Length header → first frame trips.
        let body = Full::new(Bytes::from(vec![b'x'; 200]));

        let result = forward_signed_request_streaming(
            &hyper::Method::PUT,
            &"/bucket/key".parse::<hyper::Uri>().unwrap(),
            &hyper::HeaderMap::new(),
            hyper::Version::HTTP_11,
            body,
            "example.com",
            &transport,
            None,
            100,
            None,
        )
        .await;

        match result {
            Err(ProxyError::RequestBodyTooLarge {
                content_length,
                max_bytes,
            }) => {
                assert_eq!(content_length, None);
                assert_eq!(max_bytes, 100);
            }
            Err(e) => panic!("expected RequestBodyTooLarge, got error: {}", e),
            Ok(_) => panic!("expected RequestBodyTooLarge, got Ok response"),
        }
    }

    /// Drive `body` through a loopback HTTP/1 connection to obtain a real
    /// `Request<Incoming>`. The returned sender keeps the server connection alive
    /// until the consumer has drained the body; otherwise a multi-megabyte test body
    /// can stop yielding when the service returns.
    async fn incoming_request(
        body: Vec<u8>,
    ) -> (
        Request<hyper::body::Incoming>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        incoming_request_with_details("PUT", "/bucket/key?part=1", None, body).await
    }

    /// Construct a real inbound request with a caller-selected method, target, and
    /// Content-Type so streaming tests exercise the same `Incoming` body the proxy
    /// receives from a client connection.
    async fn incoming_request_with_details(
        method: &str,
        path_and_query: &str,
        content_type: Option<&str>,
        body: Vec<u8>,
    ) -> (
        Request<hyper::body::Incoming>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        use hyper::server::conn::http1;
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;
        use std::convert::Infallible;
        use tokio::sync::oneshot;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = oneshot::channel::<Request<hyper::body::Incoming>>();
        let request_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(request_tx)));
        let (done_tx, done_rx) = oneshot::channel::<()>();
        let done_rx = std::sync::Arc::new(std::sync::Mutex::new(Some(done_rx)));

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let request_tx = request_tx.clone();
            let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                if let Some(sender) = request_tx.lock().unwrap().take() {
                    let _ = sender.send(request);
                }
                let done = done_rx.lock().unwrap().take();
                async move {
                    if let Some(done) = done {
                        let _ = done.await;
                    }
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                }
            });
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });

        let content_length = body.len();
        let method: hyper::Method = method.parse().expect("test HTTP method must be valid");
        let path_and_query = path_and_query.to_owned();
        let content_type = content_type.map(str::to_owned);
        tokio::spawn(async move {
            use hyper_util::client::legacy::Client;
            use hyper_util::rt::TokioExecutor;

            let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
            let mut request_builder = Request::builder()
                .method(method)
                .uri(format!("http://{address}{path_and_query}"))
                .header("content-length", content_length.to_string());
            if let Some(content_type) = content_type {
                request_builder = request_builder.header("content-type", content_type);
            }
            let request = request_builder.body(Full::new(Bytes::from(body))).unwrap();
            let _ = client.request(request).await;
        });

        (request_rx.await.unwrap(), done_tx)
    }

    #[tokio::test]
    async fn streaming_verbatim_forward_handles_body_larger_than_buffering_thresholds() {
        // Task 4: a forward-only caller must pass a body well beyond the historical
        // 8 MiB allocation clamp without materializing it in the wrapper. Keep the
        // Incoming connection alive until the stream has reached the mock upstream.
        const BODY_SIZE: usize = 16 * 1024 * 1024;
        let payload: Vec<u8> = (0..BODY_SIZE).map(|index| (index % 251) as u8).collect();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = tokio::spawn(capture_upstream_body(listener, payload.len()));
        let (request, _connection_keepalive) = incoming_request(payload.clone()).await;

        let transport = UpstreamTransport {
            ip: address.ip(),
            port: address.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            forward_signed_request_streaming_verbatim(
                request,
                "example.com",
                &transport,
                None,
                BODY_SIZE as u64,
            ),
        )
        .await
        .expect("forward-only streaming must not stall on a multi-megabyte body")
        .expect("forward-only streaming must succeed");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(upstream.await.unwrap(), payload);
    }

    #[tokio::test]
    async fn streaming_verbatim_forward_posts_multipart_upload_without_a_cache_tee() {
        // Task 5a / Validates: Requirements 2.5a. A POST object upload is a
        // multipart/form-data envelope, so the forward-only helper must send it
        // verbatim while `tee = None` ensures those envelope bytes cannot become a
        // write-cache entry. The 204 is the mocked direct-S3 result; the proxied
        // path must return that same upstream status.
        let boundary = "----s3-proxy-test-boundary";
        let payload = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\npost-object\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"upload.txt\"\r\nContent-Type: text/plain\r\n\r\nobject bytes from browser form\r\n--{boundary}--\r\n"
        )
        .into_bytes();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = tokio::spawn(capture_upstream_body_with_status(
            listener,
            payload.len(),
            "204 No Content",
        ));
        let (request, _connection_keepalive) = incoming_request_with_details(
            "POST",
            "/bucket?uploads=browser-form",
            Some(&format!("multipart/form-data; boundary={boundary}")),
            payload.clone(),
        )
        .await;

        let transport = UpstreamTransport {
            ip: address.ip(),
            port: address.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };
        let response = forward_signed_request_streaming_verbatim(
            request,
            "example.com",
            &transport,
            None,
            payload.len() as u64,
        )
        .await
        .expect("POST object upload must stream to S3");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(upstream.await.unwrap(), payload);
    }

    /// Mock upstream: accept one connection, read the request line + headers (up to
    /// the `\r\n\r\n` terminator), then read exactly `body_len` body bytes, capture
    /// them, send a minimal `200 OK`, and return the captured body so the test can
    /// assert verbatim forwarding. Reading the whole body before responding mirrors
    /// a real upstream draining the request.
    async fn capture_upstream_body(listener: tokio::net::TcpListener, body_len: usize) -> Vec<u8> {
        capture_upstream_body_with_status(listener, body_len, "200 OK").await
    }

    async fn capture_upstream_body_with_status(
        listener: tokio::net::TcpListener,
        body_len: usize,
        status: &str,
    ) -> Vec<u8> {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];

        // Read until end-of-headers.
        let header_end = loop {
            let n = sock.read(&mut tmp).await.unwrap();
            if n == 0 {
                return Vec::new();
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };

        // Anything past the header terminator is the start of the body; keep reading
        // until we have the full body.
        let mut body = buf[header_end..].to_vec();
        while body.len() < body_len {
            let n = sock.read(&mut tmp).await.unwrap();
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }

        let response = format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n");
        let _ = sock.write_all(response.as_bytes()).await;
        let _ = sock.flush().await;
        body
    }

    #[tokio::test]
    async fn streaming_verbatim_forward_preserves_presigned_put_query() {
        // Task 5 regression: the former unsigned PUT reconstruction omitted this
        // query string, dropping X-Amz-Signature and producing S3 AccessDenied.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            loop {
                let bytes_read = socket.read(&mut buffer).await.unwrap();
                assert!(bytes_read > 0, "upstream received an incomplete request");
                request.extend_from_slice(&buffer[..bytes_read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8(request)
                .unwrap()
                .lines()
                .next()
                .unwrap()
                .to_owned()
        });

        let transport = UpstreamTransport {
            ip: address.ip(),
            port: address.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };
        let uri = "/bucket/object?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=signature";
        let response = forward_signed_request_streaming(
            &hyper::Method::PUT,
            &uri.parse::<hyper::Uri>().unwrap(),
            &hyper::HeaderMap::new(),
            hyper::Version::HTTP_11,
            Full::new(Bytes::from_static(b"payload")),
            "example.com",
            &transport,
            None,
            u64::MAX,
            None,
        )
        .await
        .expect("presigned PUT must forward successfully");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            upstream.await.unwrap(),
            format!("PUT {} HTTP/1.1", uri),
            "the upstream request line must retain every presigning parameter"
        );
    }

    /// Forward one PUT through the streaming serializer with the given client headers and
    /// return the lowercase header lines the mock upstream received.
    async fn streamed_header_lines(uri: &str, client_headers: &[(&str, &str)]) -> Vec<String> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            loop {
                let n = socket.read(&mut buffer).await.unwrap();
                assert!(n > 0, "upstream received an incomplete request");
                request.extend_from_slice(&buffer[..n]);
                if request.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8(request)
                .unwrap()
                .lines()
                .skip(1)
                .take_while(|l| !l.is_empty())
                .map(|l| l.to_ascii_lowercase())
                .collect::<Vec<_>>()
        });
        let transport = UpstreamTransport {
            ip: address.ip(),
            port: address.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };
        let mut headers = hyper::HeaderMap::new();
        for (k, v) in client_headers {
            headers.append(
                hyper::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                hyper::header::HeaderValue::from_str(v).unwrap(),
            );
        }
        forward_signed_request_streaming(
            &hyper::Method::PUT,
            &uri.parse::<hyper::Uri>().unwrap(),
            &headers,
            hyper::Version::HTTP_11,
            Full::new(Bytes::from_static(b"payload")),
            "example.com",
            &transport,
            None,
            u64::MAX,
            None,
        )
        .await
        .expect("forward must succeed");
        upstream.await.unwrap()
    }

    #[tokio::test]
    async fn streaming_forward_via_signed_branch_leaves_signed_via_untouched() {
        let ours = format!("1.1 {}", via_pseudonym());

        // Authorization header signs `via`: forwarded byte-identical, ours not appended.
        let lines = streamed_header_lines(
            "/bucket/key",
            &[
                ("authorization", AUTH_SIGNING_VIA),
                ("via", "1.1 signed-value"),
            ],
        )
        .await;
        assert_eq!(
            lines
                .iter()
                .filter(|l| l.starts_with("via:"))
                .collect::<Vec<_>>(),
            vec!["via: 1.1 signed-value"]
        );

        // Presigned URL signs `via` in X-Amz-SignedHeaders: same.
        let lines = streamed_header_lines(
            "/bucket/key?X-Amz-SignedHeaders=host%3Bvia&X-Amz-Signature=s",
            &[("via", "1.1 signed-value")],
        )
        .await;
        assert_eq!(
            lines
                .iter()
                .filter(|l| l.starts_with("via:"))
                .collect::<Vec<_>>(),
            vec!["via: 1.1 signed-value"]
        );

        // Control: not signed, so ours is appended after the client's.
        let lines = streamed_header_lines(
            "/bucket/key",
            &[
                ("authorization", AUTH_NOT_SIGNING_VIA),
                ("via", "1.1 client"),
            ],
        )
        .await;
        assert_eq!(
            lines
                .iter()
                .filter(|l| l.starts_with("via:"))
                .collect::<Vec<_>>(),
            vec![&format!("via: 1.1 client, {}", ours)]
        );
    }

    /// Capture the exact request bytes written to the mock upstream. The caller
    /// supplies the expected body length so this handles the request-header/body
    /// coalescing that is normal on a loopback TCP connection.
    async fn capture_upstream_request(
        listener: tokio::net::TcpListener,
        body_len: usize,
    ) -> Vec<u8> {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0u8; 4096];
        let header_end = loop {
            let bytes_read = socket.read(&mut buffer).await.unwrap();
            assert!(bytes_read > 0, "upstream received an incomplete request");
            request.extend_from_slice(&buffer[..bytes_read]);
            if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };

        while request.len() - header_end < body_len {
            let bytes_read = socket.read(&mut buffer).await.unwrap();
            assert!(
                bytes_read > 0,
                "upstream received an incomplete request body"
            );
            request.extend_from_slice(&buffer[..bytes_read]);
        }

        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        request
    }

    #[tokio::test]
    async fn converted_forward_only_request_shapes_are_byte_identical() {
        // Task 8 / Validates: Requirements 2.2, 3.4.
        //
        // Every converted call site reaches the same streaming serializer. Cover the
        // concrete request shapes rather than only PUT: unsigned PUT (with or
        // without write caching), POST object upload, signed-PUT/UploadPart bypass,
        // CreateMultipartUpload, unsafe uploadId forwards, and AbortMultipartUpload.
        // The exact raw TCP request is the oracle: changing request-line query,
        // header order/casing/value, or a body byte makes this test fail before S3 can
        // report SignatureDoesNotMatch in production.
        let cases = [
            (
                "unsigned PUT and signed PUT bypass",
                "PUT",
                "/bucket/object?X-Amz-Signature=put-signature",
                b"put-body".as_slice(),
            ),
            (
                "unsigned POST object upload",
                "POST",
                "/bucket?uploads=browser-form",
                b"--form\r\nfile\r\n--form--\r\n".as_slice(),
            ),
            (
                "CreateMultipartUpload",
                "POST",
                "/bucket/object?uploads",
                b"".as_slice(),
            ),
            (
                "unsafe UploadPart forward",
                "PUT",
                "/bucket/object?partNumber=1&uploadId=../unsafe",
                b"part-body".as_slice(),
            ),
            (
                "unsafe CompleteMultipartUpload forward",
                "POST",
                "/bucket/object?uploadId=../unsafe",
                b"<CompleteMultipartUpload/>".as_slice(),
            ),
            (
                "unsafe AbortMultipartUpload forward",
                "DELETE",
                "/bucket/object?uploadId=../unsafe",
                b"".as_slice(),
            ),
            (
                "AbortMultipartUpload",
                "DELETE",
                "/bucket/object?uploadId=safe-upload-id",
                b"".as_slice(),
            ),
            (
                "SSE-C or write-cache-disabled signed PUT",
                "PUT",
                "/bucket/object?x-id=PutObject",
                b"sse-c-body".as_slice(),
            ),
        ];

        for (site, method, path_and_query, payload) in cases {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let upstream = tokio::spawn(capture_upstream_request(listener, payload.len()));

            let mut headers = hyper::HeaderMap::new();
            headers.append("x-amz-meta-first", "first".parse().unwrap());
            headers.append("content-type", "application/octet-stream".parse().unwrap());
            headers.append("x-amz-meta-second", "second".parse().unwrap());
            headers.append("content-length", payload.len().to_string().parse().unwrap());

            let expected_headers: Vec<u8> = headers
                .iter()
                .flat_map(|(name, value)| {
                    format!("{}: {}\r\n", name.as_str(), value.to_str().unwrap()).into_bytes()
                })
                // Loop detection appends this process's `Via` after the client's headers.
                .chain(format!("via: 1.1 {}\r\n", via_pseudonym()).into_bytes())
                .collect();
            let expected = [
                format!("{method} {path_and_query} HTTP/1.1\r\n").into_bytes(),
                expected_headers,
                b"\r\n".to_vec(),
                payload.to_vec(),
            ]
            .concat();

            let transport = UpstreamTransport {
                ip: address.ip(),
                port: address.port(),
                tls: None,
                validated_endpoint: None,
                timeouts: UpstreamTimeouts::default(),
                response_ledger: None,
                health: None,
            };
            let response = forward_signed_request_streaming(
                &method.parse().unwrap(),
                &path_and_query.parse::<hyper::Uri>().unwrap(),
                &headers,
                hyper::Version::HTTP_11,
                Full::new(Bytes::copy_from_slice(payload)),
                "example.com",
                &transport,
                None,
                u64::MAX,
                None,
            )
            .await
            .unwrap_or_else(|error| panic!("{site} forwarding failed: {error}"));

            assert_eq!(response.status(), StatusCode::OK, "{site} status");
            assert_eq!(upstream.await.unwrap(), expected, "{site} raw request");
        }
    }

    #[test]
    fn known_length_buffered_body_preallocates_at_one_times_body_size() {
        // Task 8 / Validates: Requirement 5.3. A known-length buffered body now
        // allocates exactly its bounded length (1.0x). This fails against the former
        // 8 MiB clamp: a 32 MiB body began at 8 MiB and had to grow through doubling
        // reallocations, which measured at 1.67x peak RSS on the fleet.
        const BODY_SIZE: u64 = 32 * 1024 * 1024;
        let capacity = buffered_body_initial_capacity(Some(BODY_SIZE), BODY_SIZE);
        assert_eq!(capacity as u64, BODY_SIZE);
        assert!(
            capacity as u64 <= BODY_SIZE,
            "known-length allocation must stay at or below 1.0x the body size"
        );
    }

    #[tokio::test]
    async fn test_tee_closed_channel_keeps_forwarding() {
        // Req 7.1/7.3: when the cache tee channel is closed (the background cache task
        // is gone), the forward loop drops the tee and keeps streaming the original
        // bytes to the upstream verbatim — a dead cache never fails an upload the
        // upstream would accept.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let payload = Bytes::from(vec![b'z'; 4096]);
        let payload_len = payload.len();
        let upstream = tokio::spawn(capture_upstream_body(listener, payload_len));

        // Bounded tee channel whose receiver is dropped immediately → every send sees
        // `Closed`.
        let (tee_tx, tee_rx) = mpsc::channel::<Bytes>(4);
        drop(tee_rx);

        let transport = UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };

        let result = forward_signed_request_streaming(
            &hyper::Method::PUT,
            &"/bucket/key".parse::<hyper::Uri>().unwrap(),
            &hyper::HeaderMap::new(),
            hyper::Version::HTTP_11,
            Full::new(payload.clone()),
            "example.com",
            &transport,
            None,
            u64::MAX,
            Some(tee_tx),
        )
        .await;

        assert!(
            result.is_ok(),
            "closed cache tee must not fail the upload: {:?}",
            result.err().map(|e| e.to_string())
        );

        let forwarded = upstream.await.unwrap();
        assert_eq!(
            forwarded.len(),
            payload_len,
            "upstream must receive the full body even when the tee is closed"
        );
        assert_eq!(
            forwarded,
            payload.to_vec(),
            "upstream bytes must be verbatim regardless of tee state"
        );
    }

    #[tokio::test]
    async fn test_tee_full_channel_backpressures_then_forwards() {
        // Req 2.2/2.3: with a small bounded channel and a slow cache consumer, the
        // forward loop hits `Full` and switches to an awaited `send` (bounded
        // backpressure) rather than buffering an unbounded queue. Every forwarded
        // frame still reaches both the upstream (verbatim) and the tee (in order).
        //
        // Also asserts the process-wide backpressure counters (cache-eviction-at-scale
        // Requirement 18.4): a >= delta per `testing.md` § "Never assert an exact
        // delta on a process-global counter", since other tests in this binary can
        // concurrently increment the same atomics.
        use futures::stream;
        use http_body_util::StreamBody;
        use hyper::body::Frame;

        let events_before = tee_backpressure_events_total_put();
        let wait_ms_before = tee_backpressure_wait_ms_total_put();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Multi-frame body so the channel can fill (capacity 1).
        let frames: Vec<Bytes> = vec![
            Bytes::from(vec![b'a'; 1024]),
            Bytes::from(vec![b'b'; 1024]),
            Bytes::from(vec![b'c'; 1024]),
            Bytes::from(vec![b'd'; 1024]),
        ];
        let expected: Vec<u8> = frames.iter().flat_map(|f| f.to_vec()).collect();
        let expected_len = expected.len();

        let upstream = tokio::spawn(capture_upstream_body(listener, expected_len));

        let stream_frames: Vec<std::result::Result<Frame<Bytes>, std::io::Error>> =
            frames.iter().cloned().map(|b| Ok(Frame::data(b))).collect();
        let body = StreamBody::new(stream::iter(stream_frames));

        // Capacity-1 channel; a deliberately slow consumer forces the sender to block
        // on `send().await` after the first queued frame.
        let (tee_tx, mut tee_rx) = mpsc::channel::<Bytes>(1);
        let consumer = tokio::spawn(async move {
            let mut received = Vec::new();
            while let Some(b) = tee_rx.recv().await {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                received.extend_from_slice(&b);
            }
            received
        });

        let transport = UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };

        let result = forward_signed_request_streaming(
            &hyper::Method::PUT,
            &"/bucket/key".parse::<hyper::Uri>().unwrap(),
            &hyper::HeaderMap::new(),
            hyper::Version::HTTP_11,
            body,
            "example.com",
            &transport,
            None,
            u64::MAX,
            Some(tee_tx),
        )
        .await;

        assert!(
            result.is_ok(),
            "streamed upload should succeed: {:?}",
            result.err().map(|e| e.to_string())
        );

        let forwarded = upstream.await.unwrap();
        let teed = consumer.await.unwrap();

        assert_eq!(
            forwarded, expected,
            "upstream must receive every byte verbatim"
        );
        assert_eq!(
            teed, expected,
            "the tee must receive every forwarded byte in order under backpressure"
        );

        let events_after = tee_backpressure_events_total_put();
        let wait_ms_after = tee_backpressure_wait_ms_total_put();
        assert!(
            events_after > events_before,
            "expected genuine backpressure with a 1-capacity channel and a slow \
             consumer, but tee_backpressure_events_total_put did not move: \
             before={events_before} after={events_after}"
        );
        assert!(
            wait_ms_after >= wait_ms_before,
            "tee_backpressure_wait_ms_total_put must be monotonic: \
             before={wait_ms_before} after={wait_ms_after}"
        );
    }

    #[tokio::test]
    async fn test_tee_closes_midstream_keeps_forwarding() {
        // Req 7.1/7.3 (cache-failure isolation, Task 4.2): when the cache task bails
        // *mid-drain* — e.g. a disk-full sink write error or an aws-chunked decode
        // error, both of which call `tee_rx.close()` in `run_streaming_cache_write`
        // — the forward loop must see `Closed`, drop the tee, and keep streaming the
        // remaining body to the upstream verbatim. This complements
        // `test_tee_closed_channel_keeps_forwarding` (which closes the channel
        // *before* any frame is sent) by closing it partway through the stream,
        // matching the real mid-stream-failure timing.
        use futures::stream;
        use http_body_util::StreamBody;
        use hyper::body::Frame;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Eight frames so the channel (capacity 1) is mid-stream when it closes.
        let frames: Vec<Bytes> = (0..8u8)
            .map(|i| Bytes::from(vec![b'a' + i; 1024]))
            .collect();
        let expected: Vec<u8> = frames.iter().flat_map(|f| f.to_vec()).collect();
        let expected_len = expected.len();

        let upstream = tokio::spawn(capture_upstream_body(listener, expected_len));

        let stream_frames: Vec<std::result::Result<Frame<Bytes>, std::io::Error>> =
            frames.iter().cloned().map(|b| Ok(Frame::data(b))).collect();
        let body = StreamBody::new(stream::iter(stream_frames));

        // Capacity-1 channel; the consumer drains two frames, then `close()`s the
        // receiver and stops — exactly what the cache task does when it discards the
        // sink and abandons the tee mid-stream.
        let (tee_tx, mut tee_rx) = mpsc::channel::<Bytes>(1);
        let consumer = tokio::spawn(async move {
            let mut drained = 0usize;
            while let Some(_b) = tee_rx.recv().await {
                drained += 1;
                if drained == 2 {
                    tee_rx.close();
                    break;
                }
            }
            drained
        });

        let transport = UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };

        let result = forward_signed_request_streaming(
            &hyper::Method::PUT,
            &"/bucket/key".parse::<hyper::Uri>().unwrap(),
            &hyper::HeaderMap::new(),
            hyper::Version::HTTP_11,
            body,
            "example.com",
            &transport,
            None,
            u64::MAX,
            Some(tee_tx),
        )
        .await;

        assert!(
            result.is_ok(),
            "a tee that closes mid-stream must not fail the upload: {:?}",
            result.err().map(|e| e.to_string())
        );

        let forwarded = upstream.await.unwrap();
        let _ = consumer.await.unwrap();

        assert_eq!(
            forwarded.len(),
            expected_len,
            "upstream must receive the full body even after the tee closes mid-stream"
        );
        assert_eq!(
            forwarded, expected,
            "upstream bytes must be verbatim regardless of when the tee closes"
        );
    }

    /// **Feature: streaming-write-path, Property 1: Verbatim forwarding**
    /// **Validates: Requirements 4.1, 10.2**
    ///
    /// For arbitrary client body bytes split into an arbitrary sequence of frames,
    /// the concatenation of bytes received by the (mock plaintext) upstream equals
    /// the original client body bytes exactly. The cache branch's decoding never
    /// affects the upstream-bound bytes, so this drives the forward with
    /// `tee = None` and `cap = u64::MAX` and asserts the upstream sees the body
    /// verbatim regardless of how the client framed it.
    ///
    /// Reuses the loopback-TCP mock upstream (`capture_upstream_body`) and a
    /// `StreamBody` of `Frame::data(...)` chunks, mirroring
    /// `test_tee_full_channel_backpressures_then_forwards`. quickcheck is
    /// synchronous, so each case drives the async forward via a per-case tokio
    /// runtime (`Runtime::block_on`). Body size is bounded for speed since every
    /// case spins a real loopback listener.
    #[quickcheck_macros::quickcheck]
    fn prop_verbatim_forwarding_arbitrary_frame_splits(
        frames: Vec<Vec<u8>>,
    ) -> quickcheck::TestResult {
        use futures::stream;
        use http_body_util::StreamBody;
        use hyper::body::Frame;

        // Bound the body: each case opens a loopback TCP listener and builds a
        // tokio runtime, so keep arbitrary inputs small for test speed.
        let total: usize = frames.iter().map(|f| f.len()).sum();
        if frames.len() > 32 || total > 32 * 1024 {
            return quickcheck::TestResult::discard();
        }

        // The verbatim oracle: concatenation of every frame, in order.
        let expected: Vec<u8> = frames.iter().flatten().copied().collect();
        let expected_len = expected.len();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let forwarded = rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let upstream = tokio::spawn(capture_upstream_body(listener, expected_len));

            let stream_frames: Vec<std::result::Result<Frame<Bytes>, std::io::Error>> = frames
                .iter()
                .cloned()
                .map(|f| Ok(Frame::data(Bytes::from(f))))
                .collect();
            let body = StreamBody::new(stream::iter(stream_frames));

            let transport = UpstreamTransport {
                ip: addr.ip(),
                port: addr.port(),
                tls: None,
                validated_endpoint: None,
                timeouts: UpstreamTimeouts::default(),
                response_ledger: None,
                health: None,
            };

            let result = forward_signed_request_streaming(
                &hyper::Method::PUT,
                &"/bucket/key".parse::<hyper::Uri>().unwrap(),
                &hyper::HeaderMap::new(),
                hyper::Version::HTTP_11,
                body,
                "example.com",
                &transport,
                None,
                u64::MAX,
                None,
            )
            .await;
            assert!(
                result.is_ok(),
                "streamed upload should succeed: {:?}",
                result.err().map(|e| e.to_string())
            );
            upstream.await.unwrap()
        });

        quickcheck::TestResult::from_bool(forwarded == expected)
    }

    /// Mock **slow** upstream for the bounded-memory test (Task 4.3). Accept one
    /// connection, read the request line + headers (up to `\r\n\r\n`), then read
    /// exactly `body_len` body bytes in chunks with a small delay between reads —
    /// the delay makes the upstream slower than the client so the awaited
    /// `write_all` to the upstream exercises the primary backpressure path
    /// (Req 2.1). Sends a minimal `200 OK` once the body is fully drained and
    /// returns the number of body bytes read so the caller can assert the whole
    /// body was forwarded verbatim.
    async fn slow_drain_upstream(listener: tokio::net::TcpListener, body_len: usize) -> usize {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut tmp = vec![0u8; 64 * 1024];

        // Read until end-of-headers.
        let header_end = loop {
            let n = sock.read(&mut tmp).await.unwrap();
            if n == 0 {
                return 0;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };

        // Anything past the header terminator is the start of the body.
        let mut body_read = buf.len() - header_end;
        while body_read < body_len {
            // Drain slowly to keep the upstream the slower side (backpressure).
            tokio::time::sleep(std::time::Duration::from_micros(50)).await;
            let n = sock.read(&mut tmp).await.unwrap();
            if n == 0 {
                break;
            }
            body_read += n;
        }

        let _ = sock
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await;
        let _ = sock.flush().await;
        body_read
    }

    /// Drive `forward_signed_request_streaming` for one object of `num_frames`
    /// `frame_size`-byte frames against the slow mock upstream, teeing to a bounded
    /// channel of `capacity` with a deliberately slow consumer.
    ///
    /// The live-buffer proxy: the body producer increments a shared counter when a
    /// frame is pulled into the pipeline (handed toward the upstream + tee) and the
    /// consumer decrements it after it has drained the frame (simulating the cache
    /// task processing a frame). The returned `max_outstanding` is the high-water
    /// mark of frames simultaneously live in the pipeline.
    ///
    /// Returns `(max_outstanding, forwarded_len, teed_len, total_len)`.
    async fn run_bounded_memory_case(
        num_frames: usize,
        frame_size: usize,
        capacity: usize,
        consumer_delay: std::time::Duration,
    ) -> (usize, usize, usize, usize) {
        use futures::stream;
        use http_body_util::StreamBody;
        use hyper::body::Frame;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let total_len = num_frames * frame_size;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(slow_drain_upstream(listener, total_len));

        // Live-frame accounting (our unit-level proxy for "live buffer").
        let outstanding = std::sync::Arc::new(AtomicUsize::new(0));
        let max_outstanding = std::sync::Arc::new(AtomicUsize::new(0));

        // Producer: increment at pull time (frame handed into the pipeline) and
        // record the high-water mark. `fetch_add(..) + 1` is the exact live count
        // right after this increment; a concurrent consumer decrement only makes the
        // true value smaller, so the recorded max is a safe upper bound.
        let produced = outstanding.clone();
        let high_water = max_outstanding.clone();
        let frame_iter = (0..num_frames).map(move |i| {
            let cur = produced.fetch_add(1, Ordering::SeqCst) + 1;
            high_water.fetch_max(cur, Ordering::SeqCst);
            let byte = b'a'.wrapping_add((i % 26) as u8);
            Ok::<_, std::io::Error>(Frame::data(Bytes::from(vec![byte; frame_size])))
        });
        let body = StreamBody::new(stream::iter(frame_iter));

        // Small bounded tee channel + a deliberately slow consumer: forces the
        // channel to fill so the forward loop exercises the `Full` → awaited `send`
        // backpressure path. The consumer holds exactly one frame "in processing" at
        // a time (decrementing only after the delay).
        let (tee_tx, mut tee_rx) = mpsc::channel::<Bytes>(capacity);
        let consumed = outstanding.clone();
        let consumer = tokio::spawn(async move {
            let mut teed_len = 0usize;
            while let Some(b) = tee_rx.recv().await {
                tokio::time::sleep(consumer_delay).await;
                teed_len += b.len();
                consumed.fetch_sub(1, Ordering::SeqCst);
            }
            teed_len
        });

        let transport = UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };

        let result = forward_signed_request_streaming(
            &hyper::Method::PUT,
            &"/bucket/key".parse::<hyper::Uri>().unwrap(),
            &hyper::HeaderMap::new(),
            hyper::Version::HTTP_11,
            body,
            "example.com",
            &transport,
            None,
            u64::MAX,
            Some(tee_tx),
        )
        .await;
        assert!(
            result.is_ok(),
            "streamed upload should succeed: {:?}",
            result.err().map(|e| e.to_string())
        );

        let forwarded_len = upstream.await.unwrap();
        let teed_len = consumer.await.unwrap();
        let max = max_outstanding.load(Ordering::SeqCst);
        (max, forwarded_len, teed_len, total_len)
    }

    #[tokio::test]
    async fn test_bounded_memory_independent_of_object_size() {
        // Task 4.3 / Design Property 2 — bounded memory (Req 1.2, 1.3, 2.2, 10.1).
        //
        // Stream a large synthetic body through `forward_signed_request_streaming`
        // against a mock SLOW upstream while teeing to a small bounded channel with a
        // slow consumer. Directly measuring RSS in a unit test is impractical, so we
        // use a unit-level proxy for "live buffer": the high-water mark of frames
        // simultaneously live in the pipeline (incremented when a frame is pulled
        // toward the upstream + tee, decremented when the consumer drains it).
        //
        // The structural invariant of the bounded `mpsc` channel guarantees this
        // high-water mark never exceeds `capacity + 2`: at most `capacity` frames
        // buffered in the channel, at most one frame held by the forward loop (being
        // written to the upstream / blocked in the awaited `send`), and at most one
        // frame the consumer is actively processing. Combined with the awaited
        // upstream `write_all` (one in-flight frame), this proves the live buffer is
        // the Per_Connection_Buffer and does NOT grow with the total object size.
        let frame_size = 32 * 1024; // 32 KiB frames
        let capacity = 4; // Per_Connection_Buffer ≈ capacity * frame_size = 128 KiB
        let bound = capacity + 2;
        let consumer_delay = std::time::Duration::from_micros(200);

        // Small object: 24 frames = 768 KiB (already ≫ the 128 KiB buffer).
        let (small_max, small_fwd, small_teed, small_total) =
            run_bounded_memory_case(24, frame_size, capacity, consumer_delay).await;

        // Large object: 192 frames = 6 MiB — 8× the small object, ~48× the buffer.
        let (large_max, large_fwd, large_teed, large_total) =
            run_bounded_memory_case(192, frame_size, capacity, consumer_delay).await;

        // The pipeline completed: both objects were streamed verbatim to the upstream
        // and fully tee'd to the cache consumer, so the bound below is over a real,
        // completed run rather than a stalled one.
        assert_eq!(small_fwd, small_total, "small object fully forwarded");
        assert_eq!(small_teed, small_total, "small object fully tee'd");
        assert_eq!(large_fwd, large_total, "large object fully forwarded");
        assert_eq!(large_teed, large_total, "large object fully tee'd");

        // Headline assertion (Req 1.2, 2.2): the live-buffer high-water mark stays
        // within the Per_Connection_Buffer bound for the small object.
        assert!(
            small_max <= bound,
            "small-object live buffer {} exceeded bound {}",
            small_max,
            bound
        );

        // Independence from object size (Req 1.3, 10.1): the SAME fixed bound — a
        // function only of the channel capacity, not the frame count — holds for the
        // 8×-larger object. Peak live memory does not grow with object size.
        assert!(
            large_max <= bound,
            "large-object live buffer {} exceeded the object-size-independent bound {}",
            large_max,
            bound
        );

        // Sanity: a slow consumer on a small channel must actually exercise the
        // backpressure path (the channel filled), otherwise the bound is vacuous.
        assert!(
            large_max >= 2,
            "expected the bounded channel to fill under a slow consumer, got max={}",
            large_max
        );
    }

    // ---------------------------------------------------------------------
    // chunked-request-framing (GitHub issue #19, second half): a chunked
    // request body must be re-framed on the wire.
    // ---------------------------------------------------------------------

    /// Mock upstream for the chunked-framing fixture: accept one connection,
    /// read the request line + headers (up to `\r\n\r\n`), then read raw body
    /// bytes for a bounded settle window (there is no reliable "body length" to
    /// wait for here — the point of this fixture is to capture whatever framing,
    /// correct or not, the proxy actually puts on the wire) and return the raw
    /// captured bytes so the test can assert on their shape directly.
    async fn capture_upstream_raw_body(
        listener: tokio::net::TcpListener,
        settle: std::time::Duration,
    ) -> Vec<u8> {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];

        let header_end = loop {
            let n = sock.read(&mut tmp).await.unwrap();
            if n == 0 {
                return Vec::new();
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };

        let mut body = buf[header_end..].to_vec();
        // Keep draining until the client goes quiet for `settle`, rather than
        // waiting for a known length: this fixture's whole point is to observe
        // whatever bytes actually landed on the wire, including a shape that
        // never sends a valid terminator.
        loop {
            match tokio::time::timeout(settle, sock.read(&mut tmp)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => body.extend_from_slice(&tmp[..n]),
                Ok(Err(_)) => break,
                Err(_) => break, // settle window elapsed with no more data
            }
        }

        let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        let _ = sock.write_all(response.as_bytes()).await;
        let _ = sock.flush().await;
        body
    }

    /// Build the genuine `Transfer-Encoding: chunked` body with a trailer
    /// section that Phase 1 requires (R10.3): a multi-frame `StreamBody` ending
    /// in a `Frame::trailers`, mirroring the inner aws-chunked-over-outer-HTTP
    /// shape pyarrow sends. This is deliberately NOT `Full::new(Bytes::from(...))`
    /// (single data frame, exact size hint, no trailers — the one shape a raw
    /// write handles correctly) and NOT `permit_body::ChunkedBytes` (exists to
    /// make hyper emit Content-Length framing instead of chunked).
    fn chunked_body_with_trailer(
        chunks: Vec<Bytes>,
        trailer_name: &'static str,
        trailer_value: &'static str,
    ) -> impl Body<Data = Bytes, Error = std::io::Error> + Unpin {
        use futures::stream;
        use http_body_util::StreamBody;
        use hyper::body::Frame;

        let mut trailer_map = hyper::HeaderMap::new();
        trailer_map.insert(
            hyper::header::HeaderName::from_static(trailer_name),
            hyper::header::HeaderValue::from_static(trailer_value),
        );

        let mut frames: Vec<std::result::Result<Frame<Bytes>, std::io::Error>> =
            chunks.into_iter().map(|c| Ok(Frame::data(c))).collect();
        frames.push(Ok(Frame::trailers(trailer_map)));

        StreamBody::new(stream::iter(frames))
    }

    /// Headers for a request that genuinely advertises `Transfer-Encoding:
    /// chunked` on the outbound side, mirroring pyarrow's captured shape
    /// (Phase 0.2/0.3 measurement): `content-encoding: aws-chunked`,
    /// `x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER`, and
    /// `x-amz-trailer` naming the trailer this fixture actually sends.
    fn pyarrow_shaped_chunked_headers() -> hyper::HeaderMap {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            hyper::header::TRANSFER_ENCODING,
            hyper::header::HeaderValue::from_static("chunked"),
        );
        headers.insert(
            hyper::header::CONTENT_ENCODING,
            hyper::header::HeaderValue::from_static("aws-chunked"),
        );
        headers.insert(
            hyper::header::HeaderName::from_static("x-amz-content-sha256"),
            hyper::header::HeaderValue::from_static("STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
        );
        headers.insert(
            hyper::header::HeaderName::from_static("x-amz-decoded-content-length"),
            hyper::header::HeaderValue::from_static("11"),
        );
        headers.insert(
            hyper::header::HeaderName::from_static("x-amz-trailer"),
            hyper::header::HeaderValue::from_static("x-amz-checksum-crc64nvme"),
        );
        headers
    }

    #[tokio::test]
    async fn chunked_request_is_forwarded_with_valid_http_chunked_framing() {
        // R10.3/R10.4: drive `forward_signed_request_streaming` with a genuine
        // `Transfer-Encoding: chunked` body carrying a trailer section, against a
        // stub upstream that records raw received bytes, and assert the bytes
        // received are correctly HTTP-chunked and terminated (R1.1, R2.1, R2.3).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(capture_upstream_raw_body(
            listener,
            std::time::Duration::from_millis(300),
        ));

        let body = chunked_body_with_trailer(
            vec![Bytes::from_static(b"hello"), Bytes::from_static(b"world")],
            "x-amz-checksum-crc64nvme",
            "abcd1234",
        );
        let headers = pyarrow_shaped_chunked_headers();

        let transport = UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };

        let result = forward_signed_request_streaming(
            &hyper::Method::PUT,
            &"/bucket/key?partNumber=1&uploadId=up"
                .parse::<hyper::Uri>()
                .unwrap(),
            &headers,
            hyper::Version::HTTP_11,
            body,
            "example.com",
            &transport,
            None,
            u64::MAX,
            None,
        )
        .await;
        assert!(
            result.is_ok(),
            "chunked upload should succeed: {:?}",
            result.err().map(|e| e.to_string())
        );

        let received = upstream.await.unwrap();

        // R1.1: each data frame is HTTP-chunked — hex length, CRLF, bytes, CRLF.
        let expected =
            b"5\r\nhello\r\n5\r\nworld\r\n0\r\nx-amz-checksum-crc64nvme: abcd1234\r\n\r\n";
        assert_eq!(
            received,
            expected,
            "received bytes must be valid HTTP chunked framing with the trailer \
             section after the terminating zero chunk, got: {:?}",
            String::from_utf8_lossy(&received)
        );
    }

    #[tokio::test]
    async fn chunked_request_without_trailers_ends_with_zero_chunk_only() {
        // R2.3: when there are no trailers, the body must end with `0\r\n\r\n` and
        // nothing after it.
        use futures::stream;
        use http_body_util::StreamBody;
        use hyper::body::Frame;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(capture_upstream_raw_body(
            listener,
            std::time::Duration::from_millis(300),
        ));

        let frames: Vec<std::result::Result<Frame<Bytes>, std::io::Error>> =
            vec![Ok(Frame::data(Bytes::from_static(b"abc")))];
        let body = StreamBody::new(stream::iter(frames));

        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            hyper::header::TRANSFER_ENCODING,
            hyper::header::HeaderValue::from_static("chunked"),
        );

        let transport = UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };

        let result = forward_signed_request_streaming(
            &hyper::Method::PUT,
            &"/bucket/key".parse::<hyper::Uri>().unwrap(),
            &headers,
            hyper::Version::HTTP_11,
            body,
            "example.com",
            &transport,
            None,
            u64::MAX,
            None,
        )
        .await;
        assert!(result.is_ok(), "chunked upload should succeed");

        let received = upstream.await.unwrap();
        assert_eq!(received, b"3\r\nabc\r\n0\r\n\r\n");
    }

    #[tokio::test]
    async fn content_length_body_is_forwarded_raw_byte_identical_to_today() {
        // R1.2/R2.7 companion: a Content-Length body (no Transfer-Encoding header
        // at all) must be written byte-identically to today — no framing added.
        // This is the non-regression assertion that must pass BOTH before and
        // after the R1 fix: a raw write is already correct for this shape.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let payload = Bytes::from_static(b"hello world, this is a content-length body");
        let payload_len = payload.len();
        let upstream = tokio::spawn(capture_upstream_body(listener, payload_len));

        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            hyper::header::CONTENT_LENGTH,
            hyper::header::HeaderValue::from_str(&payload_len.to_string()).unwrap(),
        );

        let transport = UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };

        let result = forward_signed_request_streaming(
            &hyper::Method::PUT,
            &"/bucket/key".parse::<hyper::Uri>().unwrap(),
            &headers,
            hyper::Version::HTTP_11,
            Full::new(payload.clone()),
            "example.com",
            &transport,
            None,
            u64::MAX,
            None,
        )
        .await;
        assert!(result.is_ok(), "content-length upload should succeed");

        let received = upstream.await.unwrap();
        assert_eq!(
            received,
            payload.to_vec(),
            "a Content-Length body must be written raw, byte-identical to today \
             — no chunk framing added"
        );
    }

    #[tokio::test]
    async fn no_transfer_encoding_header_at_all_is_forwarded_raw() {
        // R1.3 companion: the framing condition is evaluated against the
        // outbound header block actually being sent, not a heuristic. A request
        // with neither Content-Length nor Transfer-Encoding (the unsigned-write
        // multi-frame case with an unknown length) must also go out raw/unframed
        // — there is nothing in the header block advertising chunked framing.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let chunks = [Bytes::from_static(b"aa"), Bytes::from_static(b"bb")];
        let expected: Vec<u8> = chunks.iter().flat_map(|c| c.to_vec()).collect();
        let expected_len = expected.len();
        let upstream = tokio::spawn(capture_upstream_body(listener, expected_len));

        use futures::stream;
        use http_body_util::StreamBody;
        use hyper::body::Frame;
        let frames: Vec<std::result::Result<Frame<Bytes>, std::io::Error>> =
            chunks.iter().cloned().map(|c| Ok(Frame::data(c))).collect();
        let body = StreamBody::new(stream::iter(frames));

        let transport = UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };

        let result = forward_signed_request_streaming(
            &hyper::Method::PUT,
            &"/bucket/key".parse::<hyper::Uri>().unwrap(),
            &hyper::HeaderMap::new(),
            hyper::Version::HTTP_11,
            body,
            "example.com",
            &transport,
            None,
            u64::MAX,
            None,
        )
        .await;
        assert!(result.is_ok(), "unframed upload should succeed");

        let received = upstream.await.unwrap();
        assert_eq!(
            received, expected,
            "a request with no Transfer-Encoding header must be forwarded raw"
        );
    }

    #[tokio::test]
    async fn r4_tee_receives_unframed_data_even_when_outbound_is_chunked() {
        // R4.1/R4.2/R4.3: the cache tee must keep receiving the original
        // unframed `data`, not the framed wire bytes. Assert this directly by
        // comparing the tee's received bytes to the plain concatenation of the
        // input chunks, while the upstream (observed separately) gets the framed
        // wire bytes. This is the assertion the spec calls out as mandatory,
        // because framing `data` before both consumers would corrupt every
        // cached chunked upload rather than merely failing to forward — a
        // plausible-looking cached object rather than a visible error.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(capture_upstream_raw_body(
            listener,
            std::time::Duration::from_millis(300),
        ));

        let chunks = vec![Bytes::from_static(b"foo"), Bytes::from_static(b"bar")];
        let expected_unframed: Vec<u8> = chunks.iter().flat_map(|c| c.to_vec()).collect();
        let body = chunked_body_with_trailer(chunks, "x-amz-checksum-crc64nvme", "deadbeef");
        let headers = pyarrow_shaped_chunked_headers();

        let (tee_tx, mut tee_rx) = mpsc::channel::<Bytes>(8);
        let tee_consumer = tokio::spawn(async move {
            let mut received = Vec::new();
            while let Some(b) = tee_rx.recv().await {
                received.extend_from_slice(&b);
            }
            received
        });

        let transport = UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: None,
        };

        let result = forward_signed_request_streaming(
            &hyper::Method::PUT,
            &"/bucket/key".parse::<hyper::Uri>().unwrap(),
            &headers,
            hyper::Version::HTTP_11,
            body,
            "example.com",
            &transport,
            None,
            u64::MAX,
            Some(tee_tx),
        )
        .await;
        assert!(result.is_ok(), "chunked upload with tee should succeed");

        let upstream_bytes = upstream.await.unwrap();
        let tee_bytes = tee_consumer.await.unwrap();

        // The tee must see the original unframed bytes...
        assert_eq!(
            tee_bytes, expected_unframed,
            "R4.1: the cache tee must receive the frame's original unframed \
             `data`, not HTTP-chunk-framed bytes"
        );
        // ...while the upstream sees the framed wire bytes — proving framing
        // happens only at the socket-write point (R4.2), not by transforming
        // `data` before both consumers.
        assert_ne!(
            upstream_bytes, tee_bytes,
            "R4.2: the upstream must receive different (framed) bytes than the \
             tee (unframed) — if they matched, framing was skipped or applied \
             to both consumers instead of just the socket write"
        );
        assert!(
            upstream_bytes.starts_with(b"3\r\nfoo\r\n3\r\nbar\r\n0\r\n"),
            "upstream must receive valid HTTP chunked framing: {:?}",
            String::from_utf8_lossy(&upstream_bytes)
        );
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// Helper function to generate a valid AWS SigV4 Authorization header
    fn generate_sigv4_auth_header(signed_headers: &[&str]) -> String {
        let signed_headers_str = signed_headers.join(";");
        format!(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders={}, Signature=abc123def456", // #gitleaks:allow
            signed_headers_str
        )
    }

    /// **Feature: signed-range-requests, Property 3: Signed range identification**
    /// **Validates: Requirements 1.3**
    ///
    /// For any Authorization header where SignedHeaders contains "range",
    /// the is_range_signed function should return true.
    #[quickcheck]
    fn prop_signed_range_identification(
        other_headers: Vec<String>,
        include_range: bool,
    ) -> TestResult {
        // Filter out empty strings and "range" from other_headers
        let other_headers: Vec<&str> = other_headers
            .iter()
            .filter(|h| !h.is_empty() && *h != "range" && !h.contains(';') && !h.contains(','))
            .map(|s| s.as_str())
            .take(5) // Limit to 5 headers
            .collect();

        // Build signed headers list
        let mut signed_headers: Vec<&str> = vec!["host", "x-amz-date"];
        signed_headers.extend(other_headers.iter().cloned());

        if include_range {
            signed_headers.push("range");
        }

        let auth_value = generate_sigv4_auth_header(&signed_headers);

        let mut headers = HashMap::new();
        headers.insert("authorization".to_string(), auth_value);

        let result = is_range_signed(&headers, "/bucket/key");

        // Result should match whether we included "range"
        TestResult::from_bool(result == include_range)
    }

    /// **Feature: signed-range-requests, Property 4: Unsigned range handling**
    /// **Validates: Requirements 1.4, 1.5**
    ///
    /// For any Authorization header where SignedHeaders does not contain "range",
    /// or when no Authorization header exists, is_range_signed should return false.
    #[quickcheck]
    fn prop_unsigned_range_handling(
        other_headers: Vec<String>,
        has_auth_header: bool,
    ) -> TestResult {
        // Filter out "range" from other_headers
        let other_headers: Vec<&str> = other_headers
            .iter()
            .filter(|h| !h.is_empty() && *h != "range" && !h.contains(';') && !h.contains(','))
            .map(|s| s.as_str())
            .take(5)
            .collect();

        let mut headers = HashMap::new();

        if has_auth_header {
            // Build signed headers WITHOUT "range"
            let mut signed_headers: Vec<&str> = vec!["host", "x-amz-date"];
            signed_headers.extend(other_headers.iter().cloned());

            let auth_value = generate_sigv4_auth_header(&signed_headers);
            headers.insert("authorization".to_string(), auth_value);
        }

        let result = is_range_signed(&headers, "/bucket/key");

        // Should always return false since we never include "range"
        TestResult::from_bool(!result)
    }

    /// **Feature: signed-range-requests, Property 22: Signature detection failure handling**
    /// **Validates: Requirements 8.1**
    ///
    /// For any malformed Authorization header, is_range_signed should return false
    /// without panicking.
    #[quickcheck]
    fn prop_signature_detection_failure_handling(random_auth_value: String) -> TestResult {
        let mut headers = HashMap::new();
        headers.insert("authorization".to_string(), random_auth_value);

        // Should not panic and should return false for malformed headers
        let _result = is_range_signed(&headers, "/bucket/key");

        // The key property is that it doesn't panic
        TestResult::from_bool(true) // Passes as long as is_range_signed doesn't panic
    }

    /// **Feature: signed-range-requests, Property 3: Exact header matching**
    /// **Validates: Requirements 1.3**
    ///
    /// For any header name that contains "range" as a substring but is not exactly "range",
    /// is_range_signed should return false.
    #[quickcheck]
    fn prop_exact_range_header_matching(prefix: String, suffix: String) -> TestResult {
        // Skip if both prefix and suffix are empty (that would be exactly "range")
        if prefix.is_empty() && suffix.is_empty() {
            return TestResult::discard();
        }

        // Skip if prefix or suffix contains special characters
        if prefix.contains(';')
            || prefix.contains(',')
            || prefix.contains(' ')
            || suffix.contains(';')
            || suffix.contains(',')
            || suffix.contains(' ')
        {
            return TestResult::discard();
        }

        // Create a header name that contains "range" but isn't exactly "range"
        let fake_range_header = format!("{}range{}", prefix, suffix);

        let auth_value = generate_sigv4_auth_header(&["host", &fake_range_header, "x-amz-date"]);

        let mut headers = HashMap::new();
        headers.insert("authorization".to_string(), auth_value);

        let result = is_range_signed(&headers, "/bucket/key");

        // Should return false because the header is not exactly "range"
        TestResult::from_bool(!result)
    }
}

#[cfg(test)]
mod upstream_timeout_tests {
    //! Each wait the signed-write path makes on the upstream ends with a
    //! `TimeoutError` naming its phase, and neither a slow-but-steady upstream nor a
    //! client pausing between body frames is cut off.
    use super::*;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    /// Bound on every test here, well above any bound the tests configure.
    const TEST_BOUND: Duration = Duration::from_secs(10);

    fn transport(
        addr: SocketAddr,
        tls: Option<Arc<TlsConnector>>,
        connect: Duration,
        idle: Duration,
    ) -> UpstreamTransport {
        UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts {
                connect,
                idle,
                ..UpstreamTimeouts::default()
            },
            response_ledger: None,
            health: None,
        }
    }

    async fn forward<B>(
        t: &UpstreamTransport,
        content_length: usize,
        body: B,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: std::fmt::Display,
    {
        let method = hyper::Method::PUT;
        let uri: hyper::Uri = "/bucket/key".parse().unwrap();
        let mut headers = hyper::HeaderMap::new();
        headers.insert("host", "localhost".parse().unwrap());
        headers.insert(
            "content-length",
            content_length.to_string().parse().unwrap(),
        );
        let forwarded = tokio::time::timeout(
            TEST_BOUND,
            forward_signed_request_streaming(
                &method,
                &uri,
                &headers,
                hyper::Version::HTTP_11,
                body,
                "localhost",
                t,
                None,
                u64::MAX,
                None,
            ),
        )
        .await;
        forwarded.expect("the forward must end within the test bound")
    }

    fn timeout_message(r: Result<Response<BoxBody<Bytes, hyper::Error>>>) -> String {
        match r {
            Err(ProxyError::TimeoutError(m)) => m,
            Err(e) => panic!("expected a TimeoutError, got {e}"),
            Ok(resp) => panic!("expected a TimeoutError, got status {}", resp.status()),
        }
    }

    /// Accept one connection and hand it to `serve`, which keeps it as long as it likes.
    async fn origin<F, Fut>(serve: F) -> SocketAddr
    where
        F: FnOnce(TcpStream) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                serve(stream).await;
            }
        });
        addr
    }

    /// Read a request head and `body_len` body bytes.
    async fn read_request(stream: &mut TcpStream, body_len: usize) {
        let mut seen = Vec::new();
        let mut byte = [0u8; 1];
        while !seen.ends_with(b"\r\n\r\n") {
            if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                return;
            }
            seen.push(byte[0]);
        }
        let mut body = vec![0u8; body_len];
        let _ = stream.read_exact(&mut body).await;
    }

    #[tokio::test]
    async fn a_full_accept_backlog_times_out_as_upstream_connect() {
        use socket2::{Domain, Socket, Type};
        let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
        let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        socket.bind(&bind.into()).unwrap();
        socket.listen(0).unwrap();
        let addr = socket.local_addr().unwrap().as_socket().unwrap();
        // Nothing accepts, so once the queue holds its one or two connections the
        // kernel drops further SYNs and a connect waits for retransmissions.
        let mut queued = Vec::new();
        for _ in 0..16 {
            match std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(s) => queued.push(s),
                Err(_) => break,
            }
        }
        assert!(queued.len() < 16, "precondition: the accept queue filled");
        let t = transport(
            addr,
            None,
            Duration::from_millis(300),
            Duration::from_secs(120),
        );
        let started = Instant::now();
        let msg = timeout_message(forward(&t, 0, Full::new(Bytes::new())).await);
        assert!(msg.contains("upstream connect"), "{msg}");
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn an_unanswered_client_hello_times_out_as_upstream_connect() {
        let addr = origin(|stream| async move {
            let _held = stream;
            std::future::pending::<()>().await;
        })
        .await;
        let cfg = crate::https_connector::build_tls_accept_any_config();
        let tls = Arc::new(TlsConnector::from(Arc::new(cfg)));
        let t = transport(
            addr,
            Some(tls),
            Duration::from_millis(300),
            Duration::from_secs(120),
        );
        let msg = timeout_message(forward(&t, 0, Full::new(Bytes::new())).await);
        assert!(msg.contains("upstream connect"), "{msg}");
    }

    const IDLE: Duration = Duration::from_millis(300);

    fn idle_transport(addr: SocketAddr) -> UpstreamTransport {
        transport(addr, None, Duration::from_secs(5), IDLE)
    }

    #[tokio::test]
    async fn an_upstream_that_stops_reading_times_out_as_request_write() {
        let addr = origin(|mut stream| async move {
            read_request(&mut stream, 0).await;
            std::future::pending::<()>().await;
        })
        .await;
        let body_len = 64 * 1024 * 1024;
        let body = Full::new(Bytes::from(vec![0u8; body_len]));
        let msg = timeout_message(forward(&idle_transport(addr), body_len, body).await);
        assert!(msg.contains("upstream request write"), "{msg}");
    }

    #[tokio::test]
    async fn an_upstream_that_never_answers_times_out_as_response_head() {
        let addr = origin(|mut stream| async move {
            read_request(&mut stream, 5).await;
            std::future::pending::<()>().await;
        })
        .await;
        let body = Full::new(Bytes::from_static(b"hello"));
        let msg = timeout_message(forward(&idle_transport(addr), 5, body).await);
        assert!(msg.contains("upstream response head"), "{msg}");
    }

    #[tokio::test]
    async fn an_upstream_that_stalls_after_its_head_times_out_as_response_body() {
        let addr = origin(|mut stream| async move {
            read_request(&mut stream, 5).await;
            let head = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n";
            let _ = stream.write_all(head).await;
            std::future::pending::<()>().await;
        })
        .await;
        let body = Full::new(Bytes::from_static(b"hello"));
        let msg = timeout_message(forward(&idle_transport(addr), 5, body).await);
        assert!(msg.contains("upstream response body"), "{msg}");
    }

    /// An upstream answering a signed PUT with a 64 MiB body: the proxy refuses it once
    /// it passes the buffered-operation bound, holding at most the bound plus one read,
    /// all of it reserved, and releases the reservation.
    #[tokio::test]
    async fn an_oversized_write_response_is_refused_within_the_bound_and_accounted() {
        const BODY: usize = 64 * 1024 * 1024;
        let addr = origin(|mut stream| async move {
            read_request(&mut stream, 5).await;
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", BODY);
            let _ = stream.write_all(head.as_bytes()).await;
            let chunk = vec![b'x'; 1024 * 1024];
            for _ in 0..BODY / chunk.len() {
                if stream.write_all(&chunk).await.is_err() {
                    return;
                }
            }
            std::future::pending::<()>().await;
        })
        .await;
        let ledger = Arc::new(crate::inflight_ledger::InflightLedger::new(1 << 30));
        let mut t = idle_transport(addr);
        t.response_ledger = Some(Arc::clone(&ledger));
        let body = Full::new(Bytes::from_static(b"hello"));
        let result = forward(&t, 5, body).await;
        let held = match &result {
            Ok(r) => format!("a {} response", r.status()),
            Err(e) => e.to_string(),
        };
        let peak = ledger.peak_reserved_bytes();
        eprintln!("oversized write response: peak reserved {peak} bytes; outcome: {held}");
        assert!(
            matches!(&result, Err(ProxyError::HttpError(m)) if m.contains("bound")),
            "the response must be refused at the bound, got {held}"
        );
        let bound = crate::http_proxy::BUFFERED_OPERATION_BOUND;
        assert!(peak > bound / 2, "the buffer was reserved: peak {peak}");
        assert!(
            peak <= bound + 8192,
            "held at most the bound plus one read: {peak}"
        );
        assert_eq!(ledger.reserved_bytes(), 0, "released with the attempt");
    }

    /// The request has been sent when the response arrives, so S3 may have executed the
    /// write: a ledger at its ceiling at that moment must not turn S3's answer into a
    /// shed. The response, larger than the whole ledger, is forwarded whole. The ledger
    /// records it past its ceiling, since it is held in memory, and releases it with the
    /// response.
    #[tokio::test]
    async fn a_write_response_arriving_at_the_ledger_ceiling_is_forwarded() {
        const BODY: usize = 2 * 1024 * 1024;
        let addr = origin(|mut stream| async move {
            read_request(&mut stream, 5).await;
            let body = vec![b'x'; BODY];
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            std::future::pending::<()>().await;
        })
        .await;
        const CEILING: u64 = 1024 * 1024;
        let ledger = Arc::new(crate::inflight_ledger::InflightLedger::new(CEILING));
        let pinned = ledger
            .try_reserve(CEILING)
            .expect("precondition: fill the ledger");
        let mut t = idle_transport(addr);
        t.response_ledger = Some(Arc::clone(&ledger));
        let result = forward(&t, 5, Full::new(Bytes::from_static(b"hello"))).await;
        let response = match result {
            Ok(r) => r,
            Err(e) => panic!("S3's answer must be forwarded, got: {e}"),
        };
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            ledger.reserved_bytes() >= CEILING + BODY as u64,
            "the response held in memory is on the ledger: {}",
            ledger.reserved_bytes()
        );
        assert!(
            ledger.peak_reserved_bytes() >= CEILING + BODY as u64,
            "and so is the peak: {}",
            ledger.peak_reserved_bytes()
        );
        drop(pinned);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(bytes.len(), BODY);
        assert_eq!(ledger.reserved_bytes(), 0, "released with the response");
    }

    /// A second signed write whose request finishes sending while the first write's
    /// response holds the ledger past its ceiling. S3 has the whole second request, so
    /// its answer is forwarded, not refused as a shed.
    #[tokio::test]
    async fn a_write_sent_while_the_ledger_is_past_its_ceiling_is_answered() {
        use std::sync::atomic::{AtomicBool, Ordering};
        const BODY: usize = 2 * 1024 * 1024;
        let first = origin(|mut stream| async move {
            read_request(&mut stream, 5).await;
            let body = vec![b'x'; BODY];
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            std::future::pending::<()>().await;
        })
        .await;
        let read_in_full = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&read_in_full);
        let second = origin(move |mut stream| async move {
            read_request(&mut stream, 5).await;
            seen.store(true, Ordering::SeqCst);
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await;
            std::future::pending::<()>().await;
        })
        .await;
        const CEILING: u64 = 1024 * 1024;
        let ledger = Arc::new(crate::inflight_ledger::InflightLedger::new(CEILING));
        let pinned = ledger
            .try_reserve(CEILING)
            .expect("precondition: fill the ledger");
        let mut t1 = idle_transport(first);
        t1.response_ledger = Some(Arc::clone(&ledger));
        let held = forward(&t1, 5, Full::new(Bytes::from_static(b"hello")))
            .await
            .expect("precondition: the first write's answer is forwarded");
        assert!(
            ledger.reserved_bytes() > CEILING,
            "precondition: the first response holds the ledger past its ceiling: {}",
            ledger.reserved_bytes()
        );
        let rejected_before = ledger.rejected_total();
        let mut t2 = idle_transport(second);
        t2.response_ledger = Some(Arc::clone(&ledger));
        match forward(&t2, 5, Full::new(Bytes::from_static(b"hello"))).await {
            Ok(response) => assert_eq!(response.status(), StatusCode::OK),
            Err(e) => panic!(
                "a write sent in full was refused after sending: {e} (outcome unknown: {})",
                write_outcome_unknown(&e)
            ),
        }
        assert!(
            read_in_full.load(Ordering::SeqCst),
            "precondition: the second origin read the whole request"
        );
        assert_eq!(
            ledger.rejected_total(),
            rejected_before,
            "nothing was refused after the request was sent"
        );
        drop(held);
        drop(pinned);
        assert_eq!(ledger.reserved_bytes(), 0, "released with the responses");
    }

    /// A response within the bound is reserved until the client response is dropped.
    #[tokio::test]
    async fn a_buffered_write_response_holds_its_reservation_until_dropped() {
        let addr = origin(|mut stream| async move {
            read_request(&mut stream, 5).await;
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nabcd")
                .await;
            std::future::pending::<()>().await;
        })
        .await;
        let ledger = Arc::new(crate::inflight_ledger::InflightLedger::new(1 << 30));
        let mut t = idle_transport(addr);
        t.response_ledger = Some(Arc::clone(&ledger));
        let response = forward(&t, 5, Full::new(Bytes::from_static(b"hello")))
            .await
            .expect("a small response is forwarded");
        assert!(ledger.reserved_bytes() > 0, "held while the response lives");
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&bytes[..], b"abcd");
        assert_eq!(ledger.reserved_bytes(), 0, "released with the response");
    }

    /// Control: an upstream that sends one byte every `IDLE / 2`, for longer than
    /// `2 x IDLE` in all, is never cut off.
    #[tokio::test]
    async fn a_slow_but_steady_upstream_is_not_cut_off() {
        let addr = origin(|mut stream| async move {
            read_request(&mut stream, 5).await;
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\n")
                .await;
            for byte in b"abcdef" {
                tokio::time::sleep(IDLE / 2).await;
                let _ = stream.write_all(&[*byte]).await;
            }
            std::future::pending::<()>().await;
        })
        .await;
        let started = Instant::now();
        let body = Full::new(Bytes::from_static(b"hello"));
        let response = forward(&idle_transport(addr), 5, body)
            .await
            .expect("a steady upstream completes");
        assert!(started.elapsed() > IDLE * 2, "{:?}", started.elapsed());
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&bytes[..], b"abcdef");
    }

    /// Control over TLS: an upstream sending one small TLS record every `IDLE / 2`,
    /// for longer than `2 x IDLE` in all, is never cut off.
    #[tokio::test]
    async fn a_slow_but_steady_tls_upstream_is_not_cut_off() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
        let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        let addr = origin(move |stream| async move {
            let Ok(mut tls) = acceptor.accept(stream).await else {
                return;
            };
            let mut seen = Vec::new();
            let mut byte = [0u8; 1];
            while !seen.ends_with(b"\r\n\r\n") {
                if tls.read(&mut byte).await.unwrap_or(0) == 0 {
                    return;
                }
                seen.push(byte[0]);
            }
            let mut body = [0u8; 5];
            let _ = tls.read_exact(&mut body).await;
            let _ = tls
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\n")
                .await;
            let _ = tls.flush().await;
            for byte in b"abcdef" {
                tokio::time::sleep(IDLE / 2).await;
                let _ = tls.write_all(&[*byte]).await;
                let _ = tls.flush().await;
            }
            std::future::pending::<()>().await;
        })
        .await;
        let cfg = crate::https_connector::build_tls_accept_any_config();
        let tls = Arc::new(TlsConnector::from(Arc::new(cfg)));
        let t = transport(addr, Some(tls), Duration::from_secs(5), IDLE);
        let started = Instant::now();
        let body = Full::new(Bytes::from_static(b"hello"));
        let response = forward(&t, 5, body)
            .await
            .expect("a steady TLS upstream completes");
        assert!(started.elapsed() > IDLE * 2, "{:?}", started.elapsed());
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&bytes[..], b"abcdef");
    }

    /// Control: a client that pauses `2 x IDLE` between two body frames is never cut
    /// off, because the timer does not run while the proxy waits on the client.
    #[tokio::test]
    async fn a_client_pausing_between_body_frames_is_not_cut_off() {
        let addr = origin(|mut stream| async move {
            read_request(&mut stream, 6).await;
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await;
            std::future::pending::<()>().await;
        })
        .await;
        let frames = futures::stream::unfold(0u8, |step| async move {
            match step {
                0 => Some((
                    Ok::<_, std::io::Error>(hyper::body::Frame::data(Bytes::from_static(b"abc"))),
                    1,
                )),
                1 => {
                    tokio::time::sleep(IDLE * 2).await;
                    Some((Ok(hyper::body::Frame::data(Bytes::from_static(b"def"))), 2))
                }
                _ => None,
            }
        });
        let body = http_body_util::StreamBody::new(Box::pin(frames));
        let started = Instant::now();
        let response = forward(&idle_transport(addr), 6, body)
            .await
            .expect("a pausing client completes");
        assert!(started.elapsed() >= IDLE * 2, "{:?}", started.elapsed());
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[cfg(test)]
mod signed_write_health_tests {
    use super::*;
    use crate::connection_pool::{ConnectionPoolManager, IpHealthTracker};
    use std::net::SocketAddr;

    fn transport_to(addr: SocketAddr, health: &UpstreamHealth) -> UpstreamTransport {
        UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
            timeouts: UpstreamTimeouts::default(),
            response_ledger: None,
            health: Some(health.clone()),
        }
    }

    /// A signed write whose connect to its address fails records the failure there,
    /// so three failed writes exclude the address from the host's distributor as
    /// three failed GETs do. A later connect that succeeds clears the record.
    #[tokio::test]
    async fn a_failed_signed_write_connect_counts_against_its_address() {
        let host = "signed-write-health.test";
        let refused: SocketAddr = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap()
        };
        let pool = Arc::new(tokio::sync::RwLock::new(
            ConnectionPoolManager::new_with_config(crate::config::ConnectionPoolConfig::default())
                .unwrap(),
        ));
        pool.write()
            .await
            .apply_resolution(host, vec![refused.ip()], None);
        assert_eq!(
            pool.read().await.get_distributed_ip(host),
            Some(refused.ip())
        );
        let health = UpstreamHealth {
            tracker: Arc::new(IpHealthTracker::new(3)),
            pool: Arc::clone(&pool),
            host: host.to_string(),
        };
        let failing = transport_to(refused, &health);
        for _ in 0..3 {
            let opened =
                tokio::time::timeout(Duration::from_secs(30), open_upstream(&failing, host))
                    .await
                    .expect("the connect must give up");
            assert!(opened.is_err(), "nothing listens on {refused}");
        }
        assert!(
            health.tracker.is_unhealthy(&refused.ip()),
            "three failed signed-write connects must mark the address unhealthy"
        );
        let excluded = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if pool.read().await.get_distributed_ip(host).is_none() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(
            excluded.is_ok(),
            "the address must leave the host's distributor"
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let working = transport_to(listener.local_addr().unwrap(), &health);
        let opened = tokio::time::timeout(Duration::from_secs(10), open_upstream(&working, host))
            .await
            .expect("the connect must not hang");
        assert!(opened.is_ok(), "the listener accepts");
        assert!(
            !health.tracker.is_unhealthy(&refused.ip()),
            "a connect that succeeds clears the address's record"
        );
    }
}
