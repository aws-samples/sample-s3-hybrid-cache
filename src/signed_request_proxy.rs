//! Signed Request Proxy Module
//!
//! Handles AWS Signature Version 4 signed requests by preserving the original
//! HTTP request exactly as received, only changing the destination IP address.
//! This prevents signature validation failures that occur when headers are modified.

use crate::cache_types::ObjectMetadata;

use crate::{ProxyError, Result};
use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::body::Body;
use hyper::{Request, Response, StatusCode};
use std::collections::HashMap;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::TlsConnector;
use tracing::{debug, error, info, warn};

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
}

impl UpstreamTransport {
    /// The verified-TLS-on-443 default transport (Secure_Default_Behaviour). A
    /// handshake failure here stays a generic (retryable) error, exactly as before.
    pub fn verified_tls_443(ip: IpAddr, tls_connector: Arc<TlsConnector>) -> Self {
        Self {
            ip,
            port: 443,
            tls: Some(tls_connector),
            validated_endpoint: None,
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
pub enum UpstreamStream {
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

/// Connect to the upstream per the resolved [`UpstreamTransport`], with retry.
///
/// TLS modes go through [`establish_tls_with_retry`] (TCP + TLS handshake);
/// the plaintext mode performs a plain TCP connect with the same retry/backoff
/// policy. `hostname` is the SNI/Host name (used only for the TLS handshake and
/// log lines); the connect target is `transport.ip:transport.port`.
pub async fn connect_upstream_with_retry(
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
                let tcp =
                    connect_tcp_with_retry(transport.ip, transport.port, hostname, max_retries)
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
                )
                .await?;
                Ok(UpstreamStream::Tls(Box::new(tls)))
            }
        }
        None => {
            let tcp =
                connect_tcp_with_retry(transport.ip, transport.port, hostname, max_retries).await?;
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
) -> Result<TcpStream> {
    let mut last_error = None;
    let mut had_failures = false;

    for attempt in 0..max_retries {
        match connect_to_target(target_ip, port).await {
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
pub fn is_sigv4_algorithm(auth: &str) -> bool {
    let has_algorithm_prefix =
        auth.starts_with("AWS4-HMAC-SHA256 ") || auth.starts_with("AWS4-ECDSA-P256-SHA256 ");

    has_algorithm_prefix
        && auth.contains("Credential=")
        && auth.contains("SignedHeaders=")
        && auth.contains("Signature=")
}

/// Check if a request is signed with AWS Signature Version 4 (or SigV4A)
pub fn is_aws_sigv4_signed(headers: &HashMap<String, String>) -> bool {
    headers
        .get("authorization")
        .or_else(|| headers.get("Authorization"))
        .map(|auth| is_sigv4_algorithm(auth))
        .unwrap_or(false)
}

/// Check if the Range header is included in AWS SigV4 SignedHeaders
///
/// This function parses the Authorization header to determine if the Range
/// header was included in the signature calculation. If so, modifying the
/// Range header would invalidate the signature.
///
/// Accepts both SigV4 (`AWS4-HMAC-SHA256`) and SigV4A
/// (`AWS4-ECDSA-P256-SHA256`); the `SignedHeaders=...` portion has identical
/// syntax in both.
///
/// # Arguments
/// * `headers` - Request headers including Authorization
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
pub fn is_range_signed(headers: &HashMap<String, String>) -> bool {
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

    // Check if "range" is in the semicolon-separated list
    // Must be exact match to avoid matching "x-range" or similar (Requirement 1.3, 1.4)
    signed_headers.split(';').any(|h| h == "range")
}

/// Extract metadata from S3 response and request headers
///
/// This function extracts object metadata needed for caching from both the
/// S3 response headers and the original request headers.
///
/// # Arguments
///
/// * `response_headers` - Headers from the S3 response (source of ETag, Last-Modified, Content-Type)
/// * `request_headers` - Headers from the original client request (retained for API compatibility)
/// * `content_length` - Content length of the object
///
/// # Returns
///
/// Returns an `ObjectMetadata` struct with extracted metadata. Missing headers
/// are handled gracefully by using empty strings or None values.
///
/// # Requirements
///
/// - Requirement 3.1: Extract ETag from response headers
/// - Requirement 3.2: Extract Last-Modified from response headers
/// - Requirement 3.3: Extract Content-Type from response headers (fallback: application/octet-stream)
/// - Requirement 3.4: Store metadata alongside cached object
/// - Requirement 3.5: Handle missing headers gracefully
///
/// # Examples
///
/// ```ignore
/// let response_headers = HashMap::from([
///     ("etag".to_string(), "\"abc123\"".to_string()),
///     ("last-modified".to_string(), "Wed, 21 Oct 2015 07:28:00 GMT".to_string()),
///     ("content-type".to_string(), "application/octet-stream".to_string()),
/// ]);
/// let request_headers = HashMap::new();
/// let metadata = extract_metadata(&response_headers, &request_headers, 1024);
/// ```
pub fn extract_metadata(
    response_headers: &HashMap<String, String>,
    _request_headers: &HashMap<String, String>,
    content_length: u64,
) -> ObjectMetadata {
    // Extract ETag from response headers (Requirement 3.1)
    // Try both lowercase and capitalized versions for case-insensitive matching
    let etag = response_headers
        .get("etag")
        .or_else(|| response_headers.get("ETag"))
        .cloned()
        .unwrap_or_else(|| {
            debug!("ETag not found in response headers, using empty string");
            String::new()
        });

    // Extract Last-Modified from response headers (Requirement 3.2)
    // Try both lowercase and capitalized versions for case-insensitive matching
    let last_modified = response_headers
        .get("last-modified")
        .or_else(|| response_headers.get("Last-Modified"))
        .cloned()
        .unwrap_or_else(|| {
            debug!("Last-Modified not found in response headers, using empty string");
            String::new()
        });

    // Extract Content-Type from response headers (Requirement 16 / 3.3)
    // Use S3 response Content-Type to avoid cache poisoning from incorrect client request headers.
    // Fallback to application/octet-stream if absent.
    let content_type = response_headers
        .get("content-type")
        .or_else(|| response_headers.get("Content-Type"))
        .cloned()
        .or_else(|| {
            debug!("Content-Type not found in S3 response headers, using application/octet-stream");
            Some("application/octet-stream".to_string())
        });

    // Log extracted metadata
    info!(
        "Extracted metadata: etag={}, last_modified={}, content_type={:?}, content_length={}",
        etag, last_modified, content_type, content_length
    );

    // Create ObjectMetadata with extracted values (Requirement 3.4, 3.5)
    ObjectMetadata::new(etag, last_modified, content_length, content_type)
}
/// Forward a signed request by preserving the original HTTP request exactly
///
/// This function:
/// 1. Reads the raw HTTP request from the client
/// 2. Establishes a TLS connection to S3
/// 3. Forwards the raw bytes without modification
/// 4. Returns the raw response
///
/// This preserves AWS SigV4 signatures which would otherwise fail if headers are modified.
pub async fn forward_signed_request(
    req: Request<hyper::body::Incoming>,
    target_host: &str,
    transport: &UpstreamTransport,
    proxy_referer: Option<&str>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
    forward_signed_request_bounded(
        req,
        target_host,
        transport,
        proxy_referer,
        default_max_buffered_request_body_bytes(),
    )
    .await
}

/// Default maximum buffered request body size: 128 MiB
fn default_max_buffered_request_body_bytes() -> u64 {
    128 * 1024 * 1024
}

/// Forward a signed request with a configurable body size cap.
///
/// Same as `forward_signed_request` but accepts a `max_body_bytes` parameter
/// for the bounded body read (Requirement 11.1, 11.4).
pub async fn forward_signed_request_bounded(
    req: Request<hyper::body::Incoming>,
    target_host: &str,
    transport: &UpstreamTransport,
    proxy_referer: Option<&str>,
    max_body_bytes: u64,
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

    // Read request body with bounded size (Requirement 11.4). This enforces the cap
    // up front and returns the fully-buffered body, exactly as before.
    let body_bytes = read_request_body_bounded(req, max_body_bytes).await?;

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
pub async fn forward_signed_request_with_body(
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

/// Forward a signed request by **streaming** the body to the upstream
/// frame-by-frame (the Streaming_Write_Path), instead of buffering the whole body
/// in RAM first.
///
/// This is the single streaming forward implementation the signed-write path
/// converges on. Header serialization is **byte-for-byte identical** to
/// [`forward_signed_request_with_body`]: the same request line, the same
/// `headers.iter()` order and casing, and the same Referer-injection rules. This
/// is load-bearing for SigV4 — any drift in header order, casing, or values
/// invalidates the signature (Requirements 4.1, 4.2, 4.3, 4.4). Only the body
/// handling changes: each client body frame is written to the upstream verbatim
/// with an awaited `write_all`, so the proxy never holds the whole body in memory
/// (Requirements 1.1, 1.2, 1.3) and the awaited write provides the primary
/// backpressure to the client (Requirement 2.1).
///
/// The upstream connection, transport selection, and the
/// `UpstreamTlsValidationFailed` mapping are unchanged — they live entirely in
/// [`connect_upstream_with_retry`] (Requirements 5.3, 5.4), which this function
/// calls exactly as the buffered path does.
///
/// # Parameters
/// * `cap` — the Body_Size_Cap (`server.max_buffered_request_body_bytes`),
///   enforced without buffering the whole body (Requirements 8.1, 8.2, 8.3, 8.4):
///   a declared `Content-Length` exceeding `cap` is rejected with HTTP 413
///   `EntityTooLarge` before any body byte is read, and a body whose streamed
///   bytes exceed `cap` mid-stream stops the upload and fails with the same 413.
///   `cap == u64::MAX` means "no cap" (the `CompleteMultipartUpload`/buffered
///   callers) and can never trip either check.
/// * `tee` — optional cache tee (`None` means no caching). When `Some`, each
///   frame forwarded to the upstream is also sent to this bounded channel using
///   the `TeeStream` discipline (`try_send`; on `Full` an awaited `send`; on
///   `Closed` the tee is dropped and forwarding continues verbatim). The bounded
///   channel plus one in-flight frame is the whole per-request streaming memory
///   budget (Req 2.2, 2.3, 7.1, 7.3). The background cache task that *consumes*
///   this channel (incremental decode + `WriteCacheRangeSink`) is wired in a
///   later task; this function only owns the send side.
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
    // up-front Content-Length check in `read_request_body_bounded`. The same
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
                "Streamed request body exceeds max_buffered_request_body_bytes (Content-Length)"
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

    // Headers - preserve exactly as received
    for (name, value) in headers.iter() {
        if let Ok(value_str) = value.to_str() {
            header_block
                .extend_from_slice(format!("{}: {}\r\n", name.as_str(), value_str).as_bytes());
        }
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

    // Establish the upstream connection per the resolved transport (unchanged: TLS
    // on the default/validated/unvalidated paths, plaintext for the http override;
    // the UpstreamTlsValidationFailed mapping is preserved inside this call).
    let mut stream = connect_upstream_with_retry(transport, target_host, 5).await?;

    // Write the request line + headers first.
    stream
        .write_all(&header_block)
        .await
        .map_err(|e| ProxyError::HttpError(format!("Failed to write request headers: {}", e)))?;

    // Stream the body frame-by-frame, verbatim. The awaited `write_all` is the
    // primary backpressure source: the next client frame is not pulled until the
    // upstream socket accepts the current one (Requirement 2.1), and no whole-body
    // buffer is ever materialized (Requirements 1.1, 1.2, 1.3, 4.1, 4.3).
    //
    // `sent` tracks the running count of body bytes forwarded so the Body_Size_Cap
    // is enforced mid-stream without buffering the whole body (Req 8.2, 8.3): on
    // exceeding `cap` we stop reading the client body and fail with the same 413
    // `EntityTooLarge` behaviour `read_request_body_bounded` produces, before
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
    while let Some(frame) = body.frame().await {
        let frame = frame
            .map_err(|e| ProxyError::HttpError(format!("Failed to read request body: {}", e)))?;
        // Only data frames carry body bytes; non-data frames (e.g. trailers) carry no
        // entity bytes to forward.
        if let Ok(data) = frame.into_data() {
            if data.is_empty() {
                continue;
            }
            frame_count += 1;
            if data.len() > max_frame {
                max_frame = data.len();
            }
            // Running cap enforcement (Req 8.2). `saturating_add` makes the
            // `cap == u64::MAX` (no-cap) callers safe: the sum caps at `u64::MAX` and
            // `u64::MAX > u64::MAX` is false, so they never trip this check.
            if sent.saturating_add(data.len() as u64) > cap {
                warn!(
                    sent_bytes = sent,
                    chunk_bytes = data.len(),
                    max_bytes = cap,
                    content_length = ?content_length_hint,
                    "Streamed request body exceeds max_buffered_request_body_bytes"
                );
                return Err(ProxyError::RequestBodyTooLarge {
                    content_length: content_length_hint,
                    max_bytes: cap,
                });
            }
            // Forward the frame to the upstream verbatim. This awaited `write_all`
            // is the primary backpressure source and must happen for every forwarded
            // frame regardless of the tee (Req 4.1, 4.3, 2.1).
            stream.write_all(&data).await.map_err(|e| {
                ProxyError::HttpError(format!("Failed to write request body: {}", e))
            })?;
            sent = sent.saturating_add(data.len() as u64);

            // Tee the forwarded frame to the cache, applying the same bounded-channel
            // discipline as `TeeStream` on the GET path. Only frames actually
            // forwarded to the upstream are tee'd (a frame rejected by the cap check
            // above returns before reaching here, so it is never tee'd). The tee
            // receives a cheap `Bytes` clone (a refcount bump); the upstream write
            // above already used the original bytes verbatim, so the tee can never
            // alter what the upstream receives (Req 7.3).
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
                        if sender.send(frame).await.is_err() {
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

    stream
        .flush()
        .await
        .map_err(|e| ProxyError::HttpError(format!("Failed to flush request: {}", e)))?;

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

    // Read raw response (unchanged).
    let response_bytes = read_response(&mut stream).await?;
    debug!("Received raw HTTP response: {} bytes", response_bytes.len());

    // Parse response (unchanged).
    parse_http_response(&response_bytes, method, path_and_query)
}

/// Read request body from incoming request with a configurable size cap.
///
/// Reads the body frame-by-frame, accumulating bytes up to `max_bytes`.
/// Returns an error that callers translate to HTTP 413 if the body exceeds the cap.
///
/// # Arguments
/// * `req` - The incoming HTTP request
/// * `max_bytes` - Maximum number of bytes to buffer
///
/// # Requirements
/// - Requirement 11.1: Configurable cap on request body buffering
/// - Requirement 11.2: Return HTTP 413 on exceed
/// - Requirement 11.4: Apply cap at every body.collect() callsite
pub async fn read_request_body_bounded(
    req: Request<hyper::body::Incoming>,
    max_bytes: u64,
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

    let mut body = req.into_body();
    let mut accumulated = Vec::with_capacity(
        content_length_hint
            .unwrap_or(8192)
            .min(max_bytes)
            .min(8 * 1024 * 1024) as usize,
    );

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
            accumulated.extend_from_slice(&data);
        }
    }

    Ok(Bytes::from(accumulated))
}

/// Connect to target IP address
async fn connect_to_target(ip: IpAddr, port: u16) -> Result<TcpStream> {
    let addr = std::net::SocketAddr::new(ip, port);

    debug!("Connecting to {}", addr);

    let tcp_stream = TcpStream::connect(addr).await.map_err(|e| {
        ProxyError::ConnectionError(format!("Failed to connect to {}: {}", addr, e))
    })?;

    Ok(tcp_stream)
}

/// Establish TLS connection
async fn establish_tls(
    tcp_stream: TcpStream,
    hostname: &str,
    tls_connector: &TlsConnector,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let hostname_owned = hostname.to_string();
    let server_name = rustls::pki_types::ServerName::try_from(hostname_owned.as_str())
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
pub async fn establish_tls_with_retry(
    target_ip: IpAddr,
    port: u16,
    hostname: &str,
    tls_connector: &TlsConnector,
    max_retries: usize,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let mut last_error = None;
    let mut had_failures = false;

    for attempt in 0..max_retries {
        // Connect to target (Requirement 1.1, 1.5)
        let tcp_stream = match connect_to_target(target_ip, port).await {
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

/// Read HTTP response from the upstream stream, handling 1xx informational responses
async fn read_response<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Vec<u8>> {
    let mut all_bytes = Vec::new();
    let mut buffer = vec![0u8; 8192];

    // Read until we have the complete final response (non-1xx)
    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                all_bytes.extend_from_slice(&buffer[..n]);

                // Check if we have a complete response
                if all_bytes.len() >= 4 {
                    // Try to find and skip any 1xx responses
                    if let Some(final_response) = extract_final_response(&all_bytes)? {
                        return Ok(final_response);
                    }
                }
            }
            Err(e) => {
                return Err(ProxyError::HttpError(format!(
                    "Failed to read response: {}",
                    e
                )));
            }
        }

        // Safety limit to prevent infinite reading
        if all_bytes.len() > 100 * 1024 * 1024 {
            // 100MB limit
            warn!("Response exceeded 100MB, stopping read");
            break;
        }
    }

    Ok(all_bytes)
}

/// Extract the final (non-1xx) response from the byte stream, skipping any 1xx responses
fn extract_final_response(bytes: &[u8]) -> Result<Option<Vec<u8>>> {
    let mut pos = 0;

    while pos < bytes.len() {
        // Find the end of the current response headers
        let header_end = match find_header_end_from_pos(bytes, pos) {
            Some(end_pos) => end_pos,
            None => return Ok(None), // Headers not complete yet
        };

        // Parse the status line to check if this is a 1xx response
        let header_section = &bytes[pos..header_end];
        let header_str = String::from_utf8_lossy(header_section);
        let mut lines = header_str.lines();

        let status_line = match lines.next() {
            Some(line) => line,
            None => {
                return Err(ProxyError::HttpError(
                    "Invalid HTTP response: no status line".to_string(),
                ))
            }
        };

        // Parse status code
        let status_code = parse_status_code(status_line)?;

        // If this is a 1xx response, skip it and continue looking
        if status_code.as_u16() >= 100 && status_code.as_u16() < 200 {
            info!(
                "Received informational response: {} - skipping",
                status_code
            );

            // Skip past this response (headers + any body)
            let body_start = header_end + 4; // Skip \r\n\r\n

            // For 1xx responses, there should be no body, so move to next response
            pos = body_start;
            continue;
        }

        // This is a final response (2xx-5xx), check if it's complete
        if has_complete_response_from_pos(bytes, pos) {
            // Return the final response starting from pos
            return Ok(Some(bytes[pos..].to_vec()));
        } else {
            // Final response not complete yet
            return Ok(None);
        }
    }

    // No complete final response found
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
    bytes: &[u8],
    method: &hyper::Method,
    path: &str,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
    // Find end of headers
    let header_end = find_header_end(bytes).ok_or_else(|| {
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
        let raw_body = &bytes[body_start..];

        if is_chunked {
            debug!("Decoding chunked response body");
            decode_chunked_body(raw_body)?
        } else if let Some(length) = content_length {
            // Content-Length specified
            let actual_length = std::cmp::min(length, raw_body.len());
            Bytes::copy_from_slice(&raw_body[..actual_length])
        } else {
            // No encoding specified, use as-is
            Bytes::copy_from_slice(raw_body)
        }
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

    let response = response_builder
        .body(
            Full::new(body_bytes)
                .map_err(|never| match never {})
                .boxed(),
        )
        .map_err(|e| {
            error!("Failed to build HTTP response: {}", e);
            ProxyError::HttpError(format!("Failed to build response: {}", e))
        })?;

    debug!(
        "Successfully built HTTP response with status: {}",
        status_code
    );
    Ok(response)
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
pub struct ChunkedDecodeConfig {
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
pub enum ChunkedDecodeError {
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

    #[test]
    fn test_is_range_signed_with_range_in_signed_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(is_range_signed(&headers));
    }

    #[test]
    fn test_is_range_signed_without_range_in_signed_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(!is_range_signed(&headers));
    }

    #[test]
    fn test_is_range_signed_uppercase_authorization_header() {
        let mut headers = HashMap::new();
        headers.insert(
            "Authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(is_range_signed(&headers));
    }

    #[test]
    fn test_is_range_signed_no_authorization_header() {
        let headers = HashMap::new();
        assert!(!is_range_signed(&headers));
    }

    #[test]
    fn test_is_range_signed_non_sigv4_auth() {
        let mut headers = HashMap::new();
        headers.insert("authorization".to_string(), "Bearer token123".to_string());
        assert!(!is_range_signed(&headers));
    }

    #[test]
    fn test_is_range_signed_malformed_no_signed_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(!is_range_signed(&headers));
    }

    #[test]
    fn test_is_range_signed_range_at_end_of_signed_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date;range, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(is_range_signed(&headers));
    }

    #[test]
    fn test_is_range_signed_range_at_start_of_signed_headers() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=range;host;x-amz-date, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(is_range_signed(&headers));
    }

    #[test]
    fn test_is_range_signed_range_only_header() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=range, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(is_range_signed(&headers));
    }

    #[test]
    fn test_is_range_signed_x_range_should_not_match() {
        // "x-range" should NOT match "range"
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-range;x-amz-date, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(!is_range_signed(&headers));
    }

    #[test]
    fn test_is_range_signed_content_range_should_not_match() {
        // "content-range" should NOT match "range"
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;content-range;x-amz-date, Signature=abc123".to_string() // #gitleaks:allow
        );
        assert!(!is_range_signed(&headers));
    }

    #[test]
    fn test_is_range_signed_signed_headers_at_end_no_comma() {
        // SignedHeaders at end of string without trailing comma
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, Signature=abc123, SignedHeaders=host;range;x-amz-date".to_string() // #gitleaks:allow
        );
        assert!(is_range_signed(&headers));
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
        assert!(is_range_signed(&headers));
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
        assert!(!is_range_signed(&headers));
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

    #[test]
    fn test_extract_metadata_all_headers_present() {
        let mut response_headers = HashMap::new();
        response_headers.insert("etag".to_string(), "\"abc123\"".to_string());
        response_headers.insert(
            "last-modified".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        );
        response_headers.insert(
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        );

        let request_headers = HashMap::new();

        let metadata = extract_metadata(&response_headers, &request_headers, 1024);

        assert_eq!(metadata.etag, "\"abc123\"");
        assert_eq!(metadata.last_modified, "Wed, 21 Oct 2015 07:28:00 GMT");
        assert_eq!(
            metadata.content_type,
            Some("application/octet-stream".to_string())
        );
        assert_eq!(metadata.content_length, 1024);
    }

    #[test]
    fn test_extract_metadata_case_insensitive() {
        // Test with capitalized header names
        let mut response_headers = HashMap::new();
        response_headers.insert("ETag".to_string(), "\"xyz789\"".to_string());
        response_headers.insert(
            "Last-Modified".to_string(),
            "Thu, 22 Oct 2015 08:30:00 GMT".to_string(),
        );
        response_headers.insert("Content-Type".to_string(), "text/plain".to_string());

        let request_headers = HashMap::new();

        let metadata = extract_metadata(&response_headers, &request_headers, 2048);

        assert_eq!(metadata.etag, "\"xyz789\"");
        assert_eq!(metadata.last_modified, "Thu, 22 Oct 2015 08:30:00 GMT");
        assert_eq!(metadata.content_type, Some("text/plain".to_string()));
        assert_eq!(metadata.content_length, 2048);
    }

    #[test]
    fn test_extract_metadata_missing_headers() {
        // Test with all headers missing (Requirement 3.5)
        // Content-Type should fallback to application/octet-stream per Req 16.3
        let response_headers = HashMap::new();
        let request_headers = HashMap::new();

        let metadata = extract_metadata(&response_headers, &request_headers, 512);

        // Should handle missing headers gracefully
        assert_eq!(metadata.etag, "");
        assert_eq!(metadata.last_modified, "");
        assert_eq!(
            metadata.content_type,
            Some("application/octet-stream".to_string())
        );
        assert_eq!(metadata.content_length, 512);
    }

    #[test]
    fn test_extract_metadata_partial_headers() {
        // Test with some headers present, some missing
        let mut response_headers = HashMap::new();
        response_headers.insert("etag".to_string(), "\"partial123\"".to_string());
        // last-modified missing
        // content-type missing — should fallback to application/octet-stream

        let request_headers = HashMap::new();

        let metadata = extract_metadata(&response_headers, &request_headers, 4096);

        assert_eq!(metadata.etag, "\"partial123\"");
        assert_eq!(metadata.last_modified, ""); // Missing, should be empty
        assert_eq!(
            metadata.content_type,
            Some("application/octet-stream".to_string())
        ); // Missing from response, fallback to default
        assert_eq!(metadata.content_length, 4096);
    }

    #[test]
    fn test_extract_metadata_zero_content_length() {
        // Test with zero content length (empty object)
        let mut response_headers = HashMap::new();
        response_headers.insert("etag".to_string(), "\"empty\"".to_string());
        response_headers.insert(
            "last-modified".to_string(),
            "Fri, 23 Oct 2015 09:00:00 GMT".to_string(),
        );

        let request_headers = HashMap::new();

        let metadata = extract_metadata(&response_headers, &request_headers, 0);

        assert_eq!(metadata.etag, "\"empty\"");
        assert_eq!(metadata.last_modified, "Fri, 23 Oct 2015 09:00:00 GMT");
        assert_eq!(metadata.content_length, 0);
    }

    #[test]
    fn test_extract_metadata_large_content_length() {
        // Test with large content length
        let mut response_headers = HashMap::new();
        response_headers.insert("etag".to_string(), "\"large\"".to_string());
        response_headers.insert(
            "last-modified".to_string(),
            "Sat, 24 Oct 2015 10:00:00 GMT".to_string(),
        );
        response_headers.insert("content-type".to_string(), "video/mp4".to_string());

        let request_headers = HashMap::new();

        let large_size = 5_000_000_000u64; // 5GB
        let metadata = extract_metadata(&response_headers, &request_headers, large_size);

        assert_eq!(metadata.content_length, large_size);
        assert_eq!(metadata.content_type, Some("video/mp4".to_string()));
    }

    #[test]
    fn test_extract_metadata_various_content_types() {
        let mut response_headers = HashMap::new();
        response_headers.insert("etag".to_string(), "\"test\"".to_string());
        response_headers.insert(
            "last-modified".to_string(),
            "Sun, 25 Oct 2015 11:00:00 GMT".to_string(),
        );

        // Test various content types from S3 response
        let content_types = vec![
            "application/json",
            "text/html; charset=utf-8",
            "image/png",
            "application/x-custom",
        ];

        for ct in content_types {
            let mut resp_headers = response_headers.clone();
            resp_headers.insert("content-type".to_string(), ct.to_string());

            let request_headers = HashMap::new();

            let metadata = extract_metadata(&resp_headers, &request_headers, 1024);
            assert_eq!(metadata.content_type, Some(ct.to_string()));
        }
    }

    #[test]
    fn test_extract_metadata_content_type_from_response_not_request() {
        // Req 16: Content-Type must come from S3 response, not client request.
        // A client sending a wrong Content-Type on PUT should not poison the cache.
        let mut response_headers = HashMap::new();
        response_headers.insert("etag".to_string(), "\"resp-etag\"".to_string());
        response_headers.insert("content-type".to_string(), "image/png".to_string());

        let mut request_headers = HashMap::new();
        request_headers.insert("content-type".to_string(), "text/plain".to_string());

        let metadata = extract_metadata(&response_headers, &request_headers, 2048);

        // Should use response Content-Type, not request Content-Type
        assert_eq!(metadata.content_type, Some("image/png".to_string()));
    }

    #[test]
    fn test_extract_metadata_content_type_fallback_when_absent() {
        // Req 16.3: Fallback to application/octet-stream if S3 response has no Content-Type
        let mut response_headers = HashMap::new();
        response_headers.insert("etag".to_string(), "\"no-ct\"".to_string());
        // No content-type in response

        let mut request_headers = HashMap::new();
        request_headers.insert("content-type".to_string(), "text/html".to_string());

        let metadata = extract_metadata(&response_headers, &request_headers, 512);

        // Should fallback to application/octet-stream, NOT use request's text/html
        assert_eq!(
            metadata.content_type,
            Some("application/octet-stream".to_string())
        );
    }

    // --- Body-size cap enforcement on the streaming forward (Req 8) ---

    #[tokio::test]
    async fn test_streaming_cap_rejects_oversized_content_length() {
        // Req 8.1: a declared `Content-Length` exceeding the cap is rejected with the
        // same `RequestBodyTooLarge` error (mapped to HTTP 413 `EntityTooLarge`) that
        // `read_request_body_bounded` returns — before any body byte is read and
        // before an upstream connection is attempted. The transport points at a port
        // nothing listens on; the test proves we never reach it.
        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            "content-length",
            hyper::header::HeaderValue::from_static("1000"),
        );

        let transport = UpstreamTransport {
            ip: "127.0.0.1".parse().unwrap(),
            port: 1,
            tls: None,
            validated_endpoint: None,
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
            100, // cap < declared Content-Length
            None,
        )
        .await;

        match result {
            Err(ProxyError::RequestBodyTooLarge {
                content_length,
                max_bytes,
            }) => {
                assert_eq!(content_length, Some(1000));
                assert_eq!(max_bytes, 100);
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

    /// Mock upstream: accept one connection, read the request line + headers (up to
    /// the `\r\n\r\n` terminator), then read exactly `body_len` body bytes, capture
    /// them, send a minimal `200 OK`, and return the captured body so the test can
    /// assert verbatim forwarding. Reading the whole body before responding mirrors
    /// a real upstream draining the request.
    async fn capture_upstream_body(listener: tokio::net::TcpListener, body_len: usize) -> Vec<u8> {
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

        let _ = sock
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await;
        let _ = sock.flush().await;
        body
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
        use futures::stream;
        use http_body_util::StreamBody;
        use hyper::body::Frame;

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

        let result = is_range_signed(&headers);

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

        let result = is_range_signed(&headers);

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
        let _result = is_range_signed(&headers);

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

        let result = is_range_signed(&headers);

        // Should return false because the header is not exactly "range"
        TestResult::from_bool(!result)
    }
}
