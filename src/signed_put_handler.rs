//! Signed PUT and Multipart Upload Handler
//!
//! Handles AWS SigV4 signed write requests: single-part PUT plus the four
//! multipart upload operations. Every request is forwarded to S3 unmodified
//! (the proxy holds no credentials and cannot re-sign) and cached in parallel.
//!
//! Handler routing (from `handle_signed_put`):
//!
//! | Request                                   | Handler                               |
//! | ----------------------------------------- | ------------------------------------- |
//! | `PUT /key` (non-multipart)                | [`SignedPutHandler::handle_with_caching`] |
//! | `POST /key?uploads`                       | [`SignedPutHandler::handle_create_multipart_upload`] |
//! | `PUT  /key?uploadId=X&partNumber=N`       | [`SignedPutHandler::handle_upload_part`] |
//! | `POST /key?uploadId=X` (no partNumber)    | [`SignedPutHandler::handle_complete_multipart_upload`] |
//! | `DELETE /key?uploadId=X`                  | [`SignedPutHandler::handle_abort_multipart_upload`] |
//!
//! # Multipart upload invariants
//!
//! For the multipart code paths in particular, see
//! [`docs/MULTIPART_UPLOAD.md`](../../../docs/MULTIPART_UPLOAD.md) for the
//! state machine, correctness gates, concurrency semantics, and threat model.
//! Short version:
//!
//! - In-flight state lives under `{cache_dir}/mpus_in_progress/{uploadId}/`. A
//!   part owns its own `part{N}.bin`, `part{N}.json` and `part{N}.lock`; there is
//!   no shared tracker document that each part appends to.
//! - The streaming part path (`open_multipart_part_sink` + its `finalize`) must
//!   hold `part{N}.lock` across both the part-file rename and the part-record
//!   write — same-part-number concurrent writes rely on this. Per part, not per
//!   upload: see [`SignedPutHandler::record_part_blocking`].
//! - `finalize_multipart_upload` first waits (bounded by
//!   [`MULTIPART_COMPLETE_CACHE_WAIT`]) for the records of the parts its own
//!   request body names, then only retains the cache if S3 succeeded, the request
//!   body parses, every requested part is cached locally, every requested ETag
//!   matches its record, and the assembled part count agrees with S3's ETag. Any
//!   miss → no cache entry; whether staging is also deleted depends on the path
//!   (see [`SignedPutHandler::cleanup_incomplete_multipart_cache`]).
//! - `aws_chunked_decoder` is the one true chunk parser for both this handler
//!   and the non-multipart PUT path.

use crate::aws_chunked_decoder;
use crate::capacity_manager::{check_cache_capacity, log_bypass_decision, CacheDecision};
use crate::compression::CompressionHandler;
use crate::metrics::{MetricsManager, RequestType};
use crate::path_safety::is_safe_path_component;
use crate::s3_client::S3ClientApi;
use crate::signed_request_proxy::{
    forward_signed_request_streaming, forward_signed_request_streaming_verbatim,
    forward_signed_request_with_body, UpstreamTransport, STREAMED_BODY_CAP,
};
use crate::{ProxyError, Result};

/// How long `CompleteMultipartUpload` will wait for the part records it needs to
/// appear in the upload tracker before giving up and not caching the object.
///
/// The per-part cache task is fire-and-forget and lags the client's response by
/// design, so on a working cache this wait is the normal path rather than an
/// exception — measured stragglers landed within about three seconds of Complete.
/// Ten seconds leaves room for a slow shared volume without letting a genuinely
/// lost part hold a client-visible request open indefinitely.
///
/// It is worth being explicit that this trades Complete latency for a working
/// cache. Complete previously returned as soon as S3 did; it now may wait for
/// local work. The cost is bounded, only paid on multipart uploads, and the
/// alternative measured outcome was multipart objects never being cached at all.
///
/// Deliberately a fixed internal constant rather than a config field: no operator
/// has needed to tune it, and the shape of the fix does not depend on the value.
/// If a need appears, a field with this as its default is a compatible change.
const MULTIPART_COMPLETE_CACHE_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// Gap between tracker polls while waiting for part records (see
/// [`MULTIPART_COMPLETE_CACHE_WAIT`]). Short enough that the common case adds
/// little latency, long enough not to hammer a network filesystem.
const TRACKER_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// How many part-cache finalizations may hold a `spawn_blocking` thread at
/// once, per process.
///
/// # Why this bound exists, and why the rationale changed
///
/// This started as a fix for a real, measured failure: before per-part records
/// (see [`Self::record_part_blocking`]), every part's finalize took an exclusive
/// cross-instance `flock` on a single shared `upload.lock` and rewrote the WHOLE
/// `upload.meta` tracker, so per-part cost grew with the number of parts already
/// recorded and every in-flight part queued on that one lock. Each waiter occupied
/// a `spawn_blocking` thread for up to its 30-second timeout; the forward path
/// needs that same pool, so the pool was exhausted and the client's upload failed.
/// Measured on a three-proxy fleet at 2,000 parts (2026-08-21, release 2.6.0,
/// against the shared-`upload.lock` design): **1,214 `upload.lock` timeouts and
/// 1,220 part-record failures across the three instances, and the client's upload
/// failed** with `Connection reset by peer` on an `UploadPart`.
///
/// **That mechanism no longer exists.** Finalization now takes a *per-part*
/// `part{N}.lock` (see [`Self::finalize_and_record_cached_part`]), so two writers
/// on different part numbers never contend at all — only same-part-number retries
/// do, which is the correctness gate this was always meant to preserve, not the
/// throughput problem. Re-run at 2,000 parts against the per-part-lock design
/// showed zero lock contention from this cause.
///
/// This semaphore is kept anyway, as a **plain backstop** rather than a fix for a
/// live defect: it caps how many finalizations can occupy a `spawn_blocking`
/// thread simultaneously, independent of why any one of them might be slow (a
/// degraded shared volume, an unusually large part, etc.). It is not load-bearing
/// for the 2,000-part case any more: re-measured against the per-part-lock
/// design, that case succeeds without this semaphore needing to intervene.
///
/// Four is deliberately below the AWS CLI's default upload concurrency of 10, so
/// the queue is bounded for the common client, while leaving enough parallelism
/// that a single slow shared-volume operation does not serialise everything behind
/// it. It does **not** reduce the total serialised time for one upload — see the
/// note on `MULTIPART_COMPLETE_CACHE_WAIT` and the deferred O(n²) work.
static PART_FINALIZE_SLOTS: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(4));
use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::{HeaderMap, Request, Response, StatusCode};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Simple response info for background tasks (status + headers only, no body)
#[derive(Clone, Debug)]
pub(crate) struct ResponseInfo {
    status: StatusCode,
    headers: HeaderMap,
}

impl ResponseInfo {
    fn status(&self) -> StatusCode {
        self.status
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}
use tracing::{debug, error, info, warn};

/// Outcome of the streaming write-cache task
/// ([`SignedPutHandler::run_streaming_cache_write`]).
///
/// Lets callers and unit tests observe whether the streamed object was cached or
/// skipped (and why) without affecting the upload. The spawned wrapper
/// ([`SignedPutHandler::spawn_streaming_cache_write_task`]) discards this value —
/// per Req 7 a cache skip/failure never alters the upload result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StreamingCacheOutcome {
    /// The object was streamed to the sink and committed to the cache.
    Committed,
    /// Caching was skipped without failing the upload. The string is a short,
    /// stable reason for diagnostics/tests (e.g. `"decoded_length_mismatch"`,
    /// `"s3_error"`, `"cache_write_error"`).
    Skipped(&'static str),
}

/// Represents a part from the CompleteMultipartUpload request body XML.
/// Used to parse and validate which parts the client wants to include in the final object.
///
/// # Requirements
/// - Requirement 4.2: Extract the list of (PartNumber, ETag) pairs from the request
#[derive(Debug)]
struct RequestedPart {
    /// The part number (1-indexed) as specified in the request
    part_number: u32,
    /// The ETag of the part, used for validation against cached parts
    etag: String,
}

/// Parse the CompleteMultipartUpload request body XML to extract the list of parts.
///
/// # Arguments
/// * `body` - The raw request body bytes containing the XML
///
/// # Returns
/// * `Ok(Vec<RequestedPart>)` - List of parts with their part numbers and ETags
/// * `Err(ProxyError)` - If the XML is malformed or contains invalid data
///
/// # Requirements
/// - Requirement 4.1: Parse the XML request body before forwarding to S3
/// - Requirement 4.2: Extract the list of (PartNumber, ETag) pairs from the request
/// - Requirement 4.3: If request body is empty or malformed, skip cache finalization and log warning
///
/// # Example XML Format
/// ```xml
/// <CompleteMultipartUpload>
///   <Part>
///     <PartNumber>1</PartNumber>
///     <ETag>"a54357aff0632cce46d942af68356b38"</ETag>
///   </Part>
/// </CompleteMultipartUpload>
/// ```
fn parse_complete_mpu_request(body: &[u8]) -> Result<Vec<RequestedPart>> {
    // Handle empty body gracefully - return empty list
    if body.is_empty() {
        return Ok(Vec::new());
    }

    let body_str = std::str::from_utf8(body)
        .map_err(|e| ProxyError::InvalidRequest(format!("Invalid UTF-8 in request body: {}", e)))?;

    let mut parts = Vec::new();

    // Split by <Part> and skip the first segment (before the first <Part>)
    for part_match in body_str.split("<Part>").skip(1) {
        if let Some(end_idx) = part_match.find("</Part>") {
            let part_xml = &part_match[..end_idx];

            let part_number = extract_xml_value(part_xml, "PartNumber")?
                .parse::<u32>()
                .map_err(|e| {
                    ProxyError::InvalidRequest(format!("Invalid PartNumber value: {}", e))
                })?;

            let etag = extract_xml_value(part_xml, "ETag")?;

            parts.push(RequestedPart { part_number, etag });
        }
    }

    Ok(parts)
}

/// Extract the value of an XML tag from a string.
///
/// # Arguments
/// * `xml` - The XML string to search in
/// * `tag` - The tag name to extract (without angle brackets)
///
/// # Returns
/// * `Ok(String)` - The trimmed value between the opening and closing tags
/// * `Err(ProxyError)` - If the tag is not found
///
/// # Requirements
/// - Requirement 4.2: Extract the list of (PartNumber, ETag) pairs from the request
fn extract_xml_value(xml: &str, tag: &str) -> Result<String> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);

    let start = xml
        .find(&start_tag)
        .ok_or_else(|| ProxyError::InvalidRequest(format!("Missing <{}> tag", tag)))?
        + start_tag.len();

    let end = xml
        .find(&end_tag)
        .ok_or_else(|| ProxyError::InvalidRequest(format!("Missing </{}> tag", tag)))?;

    Ok(xml[start..end].trim().to_string())
}
/// Normalize an ETag by removing surrounding quotes.
///
/// S3 ETags may or may not have surrounding quotes depending on the source.
/// This function ensures consistent comparison by stripping quotes.
///
/// # Arguments
/// * `etag` - The ETag string to normalize
///
/// # Returns
/// The ETag with surrounding quotes removed (if present)
///
/// # Examples
/// ```ignore
/// assert_eq!(normalize_etag("\"abc123\""), "abc123");
/// assert_eq!(normalize_etag("abc123"), "abc123");
/// assert_eq!(normalize_etag("\"\""), "");
/// ```
fn normalize_etag(etag: &str) -> &str {
    etag.trim_matches('"')
}

/// Format bytes into human-readable string (MB with 1 decimal)
fn format_size(bytes: u64) -> String {
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = MB * 1024.0;

    if bytes as f64 >= GB {
        format!("{:.1}GB", bytes as f64 / GB)
    } else if bytes as f64 >= MB {
        format!("{:.1}MB", bytes as f64 / MB)
    } else if bytes >= 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{}B", bytes)
    }
}

/// Truncate upload ID for logging (first 12 chars + ...)
fn truncate_upload_id(upload_id: &str) -> String {
    if upload_id.len() > 12 {
        format!("{}...", &upload_id[..12])
    } else {
        upload_id.to_string()
    }
}

/// Truncate ETag for logging (first 12 chars + suffix if multipart)
fn truncate_etag(etag: &str) -> String {
    // Remove quotes if present
    let etag = etag.trim_matches('"');

    // Check for multipart suffix (e.g., "-10" at the end)
    if let Some(dash_pos) = etag.rfind('-') {
        let suffix = &etag[dash_pos..];
        // If suffix looks like a part count (e.g., "-10"), preserve it
        if suffix.len() > 1 && suffix[1..].chars().all(|c| c.is_ascii_digit()) {
            let hash_part = &etag[..dash_pos];
            if hash_part.len() > 8 {
                return format!("{}...{}", &hash_part[..8], suffix);
            }
            return etag.to_string();
        }
    }

    // Single-part ETag - just truncate
    if etag.len() > 12 {
        format!("{}...", &etag[..12])
    } else {
        etag.to_string()
    }
}

/// Extract bucket and key from cache_key (format: "bucket/key")
fn parse_cache_key(cache_key: &str) -> (&str, &str) {
    match cache_key.split_once('/') {
        Some((bucket, key)) => (bucket, key),
        None => (cache_key, ""),
    }
}

/// SignedPutHandler orchestrates signed PUT request caching
///
/// This handler coordinates the streaming of signed PUT requests to both
/// S3 and the cache simultaneously, ensuring signature preservation while
/// enabling efficient caching.
///
/// # Requirements
///
/// - Requirement 1.1: Stream request body to both S3 and cache simultaneously
/// - Requirement 1.2: Write data in chunks as received
/// - Requirement 1.3: Commit cached data on S3 success
/// - Requirement 1.4: Discard cached data on S3 error
/// - Requirement 8.1: Handle cache write failures gracefully
/// - Requirement 8.2: Clean up cached data on S3 error
pub struct SignedPutHandler {
    /// Base directory for cache storage
    cache_dir: PathBuf,
    /// Compression handler for cache writes
    compression_handler: CompressionHandler,
    /// Current cache usage in bytes
    current_cache_usage: u64,
    /// Maximum cache capacity in bytes
    max_cache_capacity: u64,
    /// Metrics manager for tracking PUT caching operations
    metrics_manager: Option<Arc<RwLock<MetricsManager>>>,
    /// Cache manager for HEAD cache invalidation
    cache_manager: Option<Arc<crate::cache::CacheManager>>,
    /// S3 client for comprehensive response header extraction
    s3_client: Option<Arc<dyn S3ClientApi + Send + Sync>>,
    /// Proxy identification Referer header value (None when disabled)
    proxy_referer: Option<String>,
    /// Maximum CompleteMultipartUpload body size (default: 10 MiB).
    /// The Complete XML body is bounded to this cap; bodies exceeding it are rejected
    /// with HTTP 413, preventing unbounded memory consumption.
    max_complete_body_bytes: u64,
    /// Bounded depth (in frames) of the streaming write-cache tee channel
    /// (`server.write_cache_tee_channel_depth`). One in-flight frame plus this many
    /// queued frames is the whole per-request streaming cache memory budget — see
    /// the streaming-write-path design (Req 1.4, 2.2, 2.3).
    write_cache_tee_channel_depth: usize,
}

impl SignedPutHandler {
    /// Create a new SignedPutHandler
    ///
    /// # Arguments
    ///
    /// * `cache_dir` - Base directory for cache storage
    /// * `compression_handler` - Handler for compressing cached data
    /// * `current_cache_usage` - Current cache usage in bytes
    /// * `max_cache_capacity` - Maximum cache capacity in bytes
    /// * `max_complete_body_bytes` - Maximum CompleteMultipartUpload body size
    pub fn new(
        cache_dir: PathBuf,
        compression_handler: CompressionHandler,
        current_cache_usage: u64,
        max_cache_capacity: u64,
        proxy_referer: Option<String>,
        max_complete_body_bytes: u64,
        write_cache_tee_channel_depth: usize,
    ) -> Self {
        Self {
            cache_dir,
            compression_handler,
            current_cache_usage,
            max_cache_capacity,
            metrics_manager: None,
            cache_manager: None,
            s3_client: None,
            proxy_referer,
            max_complete_body_bytes,
            write_cache_tee_channel_depth,
        }
    }

    /// Set the metrics manager for tracking PUT caching operations
    ///
    /// # Arguments
    ///
    /// * `metrics_manager` - Metrics manager instance
    pub fn set_metrics_manager(&mut self, metrics_manager: Arc<RwLock<MetricsManager>>) {
        self.metrics_manager = Some(metrics_manager);
    }

    /// Set the cache manager for HEAD cache invalidation
    ///
    /// # Arguments
    ///
    /// * `cache_manager` - Cache manager instance
    pub fn set_cache_manager(&mut self, cache_manager: Arc<crate::cache::CacheManager>) {
        self.cache_manager = Some(cache_manager);
    }

    /// Set the S3 client for comprehensive response header extraction
    ///
    /// # Arguments
    ///
    /// * `s3_client` - S3 client instance
    pub fn set_s3_client(&mut self, s3_client: Arc<dyn S3ClientApi + Send + Sync>) {
        self.s3_client = Some(s3_client);
    }

    /// Handle a signed PUT request with caching
    ///
    /// This is the main orchestration method that:
    /// 1. Decides whether to cache based on capacity
    /// 2. Streams the request body to both S3 and cache
    /// 3. Commits or discards the cache based on S3 response
    ///
    /// # Arguments
    ///
    /// * `req` - The incoming HTTP request
    /// * `cache_key` - Cache key for storing the object
    /// * `target_host` - Target S3 hostname
    /// * `transport` - Resolved upstream transport (connect IP/port + TLS-or-plaintext)
    ///
    /// # Returns
    ///
    /// Returns the S3 response, with caching handled transparently
    ///
    /// # Requirements
    ///
    /// - Requirement 1.1: Stream to both S3 and cache simultaneously
    /// - Requirement 1.3: Commit on S3 success
    /// - Requirement 1.4: Discard on S3 error
    /// - Requirement 2.1: Check capacity before caching
    /// - Requirement 8.1: Handle errors gracefully
    /// - Requirement 5.1: Detect and cache UploadPart requests
    /// - Requirement 5.3: Handle CompleteMultipartUpload
    /// - Requirement 4.1: Handle CreateMultipartUpload
    pub async fn handle_signed_put(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        self.handle_put(req, cache_key, target_host, transport)
            .await
    }

    /// Handle a PUT without an Authorization header using the same streaming cache
    /// pipeline as signed PUTs. A cache-capacity bypass still streams verbatim: the
    /// request is forward-only, so buffering it would recreate the unsigned-write
    /// memory cliff.
    pub async fn handle_unsigned_put(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        self.handle_put(req, cache_key, target_host, transport)
            .await
    }

    async fn handle_put(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // Check if this is a multipart upload request
        let uri = req.uri();
        let query = uri.query().unwrap_or("");

        // Detect CreateMultipartUpload request (Requirement 4.1)
        // POST with ?uploads query parameter initiates a multipart upload
        if Self::is_create_multipart_upload(query) {
            return self
                .handle_create_multipart_upload(req, cache_key, target_host, transport)
                .await;
        }

        // Detect UploadPart request (Requirement 5.1)
        if let Some((upload_id, part_number)) = Self::parse_upload_part_query(query) {
            return self
                .handle_upload_part(
                    req,
                    cache_key,
                    target_host,
                    transport,
                    upload_id,
                    part_number,
                )
                .await;
        }

        // Detect AbortMultipartUpload request (Requirement 4.5)
        // AbortMultipartUpload is a DELETE request with uploadId
        // Must check before CompleteMultipartUpload since both have uploadId without partNumber
        if req.method() == hyper::Method::DELETE && Self::is_abort_multipart_upload(query) {
            let upload_id = Self::extract_upload_id(query).unwrap_or_default();
            return self
                .handle_abort_multipart_upload(req, cache_key, target_host, transport, upload_id)
                .await;
        }

        // Detect CompleteMultipartUpload request (Requirement 5.3)
        // CompleteMultipartUpload is a POST request with uploadId
        if Self::is_complete_multipart_upload(query) {
            let upload_id = Self::extract_upload_id(query).unwrap_or_default();
            return self
                .handle_complete_multipart_upload(req, cache_key, target_host, transport, upload_id)
                .await;
        }

        info!("Handling signed PUT request for cache key: {}", cache_key);

        // Extract request headers for metadata and capacity checking
        let request_headers: HashMap<String, String> = req
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        // Extract Content-Length for capacity checking
        let content_length = request_headers
            .get("content-length")
            .or_else(|| request_headers.get("Content-Length"))
            .and_then(|v| v.parse::<u64>().ok());

        debug!(
            "Signed PUT request: cache_key={}, content_length={:?}",
            cache_key, content_length
        );

        // Compute bytes_uploaded for per-bucket traffic accounting (Design §4b).
        // Use x-amz-decoded-content-length for aws-chunked bodies; else Content-Length.
        let is_aws_chunked = aws_chunked_decoder::is_aws_chunked(&request_headers);
        let body_bytes_uploaded = if is_aws_chunked {
            aws_chunked_decoder::get_decoded_content_length(&request_headers).unwrap_or(0)
        } else {
            content_length.unwrap_or(0)
        };

        // Save bucket before cache_key is consumed by the match arms.
        let (bucket_ref, _) = parse_cache_key(&cache_key);
        let bucket_owned = bucket_ref.to_string();

        // Decide whether to cache based on capacity (Requirement 2.1)
        let cache_decision = self.should_cache(content_length);

        let result = match cache_decision {
            CacheDecision::Cache => {
                info!("Caching signed PUT request: {}", cache_key);
                // Stream to both S3 and cache
                self.handle_with_caching(
                    req,
                    cache_key,
                    target_host,
                    transport,
                    request_headers,
                    content_length,
                )
                .await
            }
            CacheDecision::Bypass(reason) => {
                // Log bypass decision (Requirement 2.5)
                log_bypass_decision(&cache_key, &reason);
                debug!("Bypassing cache for signed PUT: {}", cache_key);
                // Record bypassed PUT (Requirement 9.2)
                if let Some(metrics) = &self.metrics_manager {
                    metrics.read().await.record_bypassed_put().await;
                }
                // A cache-capacity bypass is forward-only. Stream it verbatim so
                // cache misses do not buffer an object the cache will not retain.
                // If the client disconnects mid-upload, S3 sees a partial upload and
                // rejects it, matching the cached signed PUT streaming path.
                forward_signed_request_streaming_verbatim(
                    req,
                    &target_host,
                    &transport,
                    self.proxy_referer.as_deref(),
                    STREAMED_BODY_CAP,
                )
                .await
            }
            CacheDecision::StreamWithCapacityCheck => {
                info!("Streaming signed PUT with capacity check: {}", cache_key);
                // Stream with capacity checking during upload
                self.handle_with_streaming_capacity_check(
                    req,
                    cache_key,
                    target_host,
                    transport,
                    request_headers,
                )
                .await
            }
        };

        // Per-bucket traffic accounting — Spec: per-bucket-metrics, Req 2.1, 2.2, 2.3
        // Record once at the completion of the full request-response cycle (Req 2.3).
        // Skip if bucket is empty (Req 2.4). PutObject response body is empty (bytes_served = 0).
        if !bucket_owned.is_empty() {
            if let Some(metrics) = &self.metrics_manager {
                metrics
                    .read()
                    .await
                    .record_bucket_traffic(
                        &bucket_owned,
                        None, // prefix: no per-bucket prefix config in this handler; bucket-level
                        RequestType::Put,
                        0, // bytes_served: PutObject response body is empty
                        0, // bytes_saved: PUT is not a cache hit
                        body_bytes_uploaded,
                    )
                    .await;
            }
        }

        result
    }

    /// Determine whether a PUT request should be cached
    ///
    /// # Arguments
    ///
    /// * `content_length` - Optional Content-Length from request headers
    ///
    /// # Returns
    ///
    /// Returns a CacheDecision indicating whether to cache, bypass, or stream with checks
    ///
    /// # Requirements
    ///
    /// - Requirement 2.1: Check if Content-Length fits within available capacity
    /// - Requirement 2.2: Bypass if Content-Length exceeds capacity
    /// - Requirement 2.3: Stream with capacity check if no Content-Length
    fn should_cache(&self, content_length: Option<u64>) -> CacheDecision {
        check_cache_capacity(
            content_length,
            self.current_cache_usage,
            self.max_cache_capacity,
        )
    }

    /// Handle signed PUT with caching (Content-Length known and fits).
    ///
    /// Streams the request body to the upstream **verbatim** while teeing it to the
    /// write cache incrementally (streaming-write-path Component 5), instead of
    /// buffering the whole object in RAM. `should_cache` already decided this object
    /// fits, so a cache tee is opened when caching is viable; with no tee the body
    /// still streams to the upstream.
    ///
    /// # Requirements
    ///
    /// - Requirement 1.1: Stream the body to S3 without holding the whole object in RAM
    /// - Requirement 6.1: Single-part PUT streams per Requirements 1–5
    /// - Requirement 7.2: A cache skip never alters the forwarded bytes or response
    #[allow(clippy::too_many_arguments)]
    async fn handle_with_caching(
        &self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
        request_headers: HashMap<String, String>,
        content_length: Option<u64>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        self.stream_put_to_upstream(
            req,
            cache_key,
            target_host,
            transport,
            request_headers,
            content_length,
        )
        .await
    }

    /// Stream a single-part PUT body to the upstream verbatim, optionally teeing it
    /// to the write cache. Shared by [`Self::handle_with_caching`] (Content-Length
    /// known and fits) and [`Self::handle_with_streaming_capacity_check`] (no
    /// Content-Length).
    ///
    /// This replaces the former buffer-then-forward implementation
    /// (`read_request_body_bounded` + inline `raw_request` assembly +
    /// `forward_raw_request_to_s3`) with [`forward_signed_request_streaming`]: the
    /// client body frames flow straight to the upstream (the awaited socket write is
    /// the primary backpressure), and the same frames are tee'd to a bounded channel
    /// feeding the incremental write-cache task when caching is viable. The upstream
    /// always receives the original bytes byte-for-byte (SigV4 intact); only the
    /// cache branch decodes aws-chunked (now done incrementally inside the cache
    /// task, not up front).
    ///
    /// Cache viability is decided by [`Self::setup_put_cache_tee`]; when it returns
    /// no tee, `tee = None` is passed and the body still streams to the upstream
    /// (Req 7.2). After the forward returns, the S3 `ResponseInfo`/error is delivered
    /// to the background cache task (if any) exactly as the buffered path did, and
    /// the S3 response is returned to the client unchanged.
    #[allow(clippy::too_many_arguments)]
    async fn stream_put_to_upstream(
        &self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
        request_headers: HashMap<String, String>,
        content_length: Option<u64>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // Extract request components before consuming the body into a stream.
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers = req.headers().clone();
        let version = req.version();
        let body = req.into_body();

        // The cache branch decodes aws-chunked incrementally; the decoded object
        // length comes from `x-amz-decoded-content-length` for aws-chunked, else the
        // Content-Length. The upstream always receives the original bytes verbatim.
        let is_aws_chunked = aws_chunked_decoder::is_aws_chunked(&request_headers);
        let decoded_len = if is_aws_chunked {
            aws_chunked_decoder::get_decoded_content_length(&request_headers)
        } else {
            content_length
        };

        // Decide whether to cache and, if so, open the sink + spawn the cache task.
        // `tee` is the send side handed to the streaming forward; `s3_result_tx` is
        // the channel the cache task waits on for the S3 result.
        let (tee, s3_result_tx) = self
            .setup_put_cache_tee(&cache_key, &request_headers, is_aws_chunked, decoded_len)
            .await;

        // Stream the body to the upstream verbatim (Req 1.1, 4.1). `tee = None`
        // means no caching, but the body still streams (Req 7.2). The body-size cap
        // is enforced without buffering the whole body inside the streaming forward.
        let s3_response = forward_signed_request_streaming(
            &method,
            &uri,
            &headers,
            version,
            body,
            &target_host,
            &transport,
            self.proxy_referer.as_deref(),
            STREAMED_BODY_CAP,
            tee,
        )
        .await;

        // Deliver the S3 result (status + headers, or error) to the background cache
        // task through the oneshot, exactly as the buffered path did. On success the
        // task finalizes + commits the cached range; on error/non-success it discards.
        if let Some(s3_result_tx) = s3_result_tx {
            let response_info = match &s3_response {
                Ok(resp) => Ok(ResponseInfo {
                    status: resp.status(),
                    headers: resp.headers().clone(),
                }),
                Err(e) => Err(e.clone()),
            };
            let _ = s3_result_tx.send(response_info);
        }

        // Return the S3 response to the client unchanged (Req 5.5).
        s3_response
    }

    /// Set up the streaming write-cache tee for a single-part PUT, when caching is
    /// viable. Returns `(tee_sender, s3_result_sender)`:
    ///
    /// - `tee_sender: Some` when a streaming cache sink was opened and a background
    ///   [`Self::run_streaming_cache_write`] task spawned to consume it; pass it to
    ///   [`forward_signed_request_streaming`].
    /// - `s3_result_sender: Some` whenever a background cache task (streaming, or the
    ///   empty-object metadata-only task) is waiting for the S3 result; the caller
    ///   must send the `ResponseInfo`/error into it after the forward returns.
    ///
    /// Both `None` means no caching for this request — the body still streams to the
    /// upstream verbatim (Req 7.2). Caching is skipped (no tee) when: there is no
    /// cache manager; the decoded object length is unknown (e.g. non-chunked with no
    /// Content-Length, or aws-chunked without `x-amz-decoded-content-length`, since
    /// the sink must be sized up front); or write-cache capacity cannot be reserved.
    /// Empty objects (decoded length 0) are cached via the metadata-only buffered
    /// path (the streaming sink rejects a zero-length open), preserving the buffered
    /// path's empty-object cache-hit behaviour.
    async fn setup_put_cache_tee(
        &self,
        cache_key: &str,
        request_headers: &HashMap<String, String>,
        is_aws_chunked: bool,
        decoded_len: Option<u64>,
    ) -> (
        Option<tokio::sync::mpsc::Sender<Bytes>>,
        Option<tokio::sync::oneshot::Sender<Result<ResponseInfo>>>,
    ) {
        // Caching requires a cache manager.
        let cache_manager = match &self.cache_manager {
            Some(cm) => cm.clone(),
            None => return (None, None),
        };

        let decoded_len = match decoded_len {
            Some(n) => n,
            None => {
                // Decoded object length unknown: we cannot size the streaming sink,
                // so skip caching (parity with today's capacity decisions). The body
                // still streams to the upstream.
                debug!(
                    "Streaming PUT: decoded object length unknown, skipping cache: cache_key={}",
                    cache_key
                );
                return (None, None);
            }
        };

        // Empty object: there is no range to stream. Cache via the metadata-only
        // buffered path (the streaming sink rejects a zero-length open), so an
        // immediate post-PUT GET/HEAD still hits, matching the buffered path.
        if decoded_len == 0 {
            let (s3_result_tx, s3_result_rx) =
                tokio::sync::oneshot::channel::<Result<ResponseInfo>>();
            Self::spawn_cache_write_task(
                cache_key.to_string(),
                Bytes::new(),
                s3_result_rx,
                self.cache_dir.clone(),
                self.compression_handler.clone(),
                Some(0),
                request_headers.clone(),
                self.metrics_manager.clone(),
                Some(cache_manager),
                self.s3_client.clone(),
            );
            return (None, Some(s3_result_tx));
        }

        // Reserve write-cache capacity and open the streaming sink. A failed
        // reservation (insufficient capacity) or open simply skips caching — the
        // body still streams verbatim (Req 7.2).
        let sink = match cache_manager
            .open_write_cache_sink(cache_key, decoded_len)
            .await
        {
            Ok(Some(sink)) => sink,
            Ok(None) => {
                debug!(
                    "Streaming PUT: write-cache capacity unavailable, skipping cache: cache_key={}",
                    cache_key
                );
                return (None, None);
            }
            Err(e) => {
                warn!(
                    "Streaming PUT: failed to open write-cache sink, skipping cache (upload unaffected): cache_key={}, error={}",
                    cache_key, e
                );
                return (None, None);
            }
        };

        let ttl = cache_manager.get_effective_put_ttl(cache_key).await;
        let (tee_tx, tee_rx) =
            tokio::sync::mpsc::channel::<Bytes>(self.write_cache_tee_channel_depth);
        let (s3_result_tx, s3_result_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        Self::spawn_streaming_cache_write_task(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_result_rx,
            is_aws_chunked,
            Some(decoded_len),
            ttl,
            request_headers.clone(),
            self.metrics_manager.clone(),
            Some(cache_manager),
            self.s3_client.clone(),
        );

        (Some(tee_tx), Some(s3_result_tx))
    }

    /// Handle signed PUT with streaming capacity check (no Content-Length).
    ///
    /// Streams the request body to the upstream verbatim while teeing it to the
    /// write cache incrementally, instead of buffering the whole object in RAM.
    /// With no Content-Length up front, caching is gated on
    /// `x-amz-decoded-content-length` (aws-chunked, needed to size the sink) and an
    /// atomic write-cache capacity reservation; an unsizable body skips caching but
    /// still streams to the upstream.
    ///
    /// # Requirements
    ///
    /// - Requirement 1.1: Stream the body to S3 without holding the whole object in RAM
    /// - Requirement 6.1: Single-part PUT streams per Requirements 1–5
    /// - Requirement 7.2: A cache skip never alters the forwarded bytes or response
    async fn handle_with_streaming_capacity_check(
        &self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
        request_headers: HashMap<String, String>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // No Content-Length up front: pass `None`, so caching is gated on
        // `x-amz-decoded-content-length` (aws-chunked) — needed to size the sink —
        // and an atomic write-cache reservation. A non-chunked body with no
        // Content-Length cannot be sized, so it skips caching while still streaming
        // verbatim to the upstream. The former up-front
        // `available_capacity` comparison is subsumed by `try_reserve_write_cache`
        // inside `setup_put_cache_tee` → `open_write_cache_sink`.
        self.stream_put_to_upstream(
            req,
            cache_key,
            target_host,
            transport,
            request_headers,
            None,
        )
        .await
    }

    // ============================================================================
    // Multipart Upload Handling Methods
    // ============================================================================

    /// Parse query string to detect UploadPart request
    ///
    /// Returns (upload_id, part_number) if this is an UploadPart request
    ///
    /// # Requirements
    ///
    /// - Requirement 5.1: Detect UploadPart requests
    fn parse_upload_part_query(query: &str) -> Option<(String, u32)> {
        let mut upload_id: Option<String> = None;
        let mut part_number: Option<u32> = None;

        for param in query.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                match key {
                    "uploadId" => upload_id = Some(value.to_string()),
                    "partNumber" => part_number = value.parse().ok(),
                    _ => {}
                }
            }
        }

        match (upload_id, part_number) {
            (Some(id), Some(num)) => Some((id, num)),
            _ => None,
        }
    }

    /// Check if this is a CompleteMultipartUpload request
    ///
    /// # Requirements
    ///
    /// - Requirement 5.3: Handle CompleteMultipartUpload
    fn is_complete_multipart_upload(query: &str) -> bool {
        query.contains("uploadId") && !query.contains("partNumber")
    }

    /// Extract upload ID from query string
    fn extract_upload_id(query: &str) -> Option<String> {
        for param in query.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                if key == "uploadId" {
                    return Some(value.to_string());
                }
            }
        }
        None
    }

    /// Check if this is a CreateMultipartUpload request
    ///
    /// CreateMultipartUpload is a POST request with ?uploads query parameter
    /// (no uploadId yet, as that's returned by S3)
    ///
    /// # Requirements
    ///
    /// - Requirement 4.1: Detect CreateMultipartUpload requests
    fn is_create_multipart_upload(query: &str) -> bool {
        // CreateMultipartUpload has "uploads" in query but no uploadId
        // The query is typically just "uploads" or "uploads="
        (query == "uploads"
            || query.starts_with("uploads&")
            || query.starts_with("uploads=")
            || query.contains("&uploads"))
            && !query.contains("uploadId")
    }

    /// Check if this is an AbortMultipartUpload request
    ///
    /// AbortMultipartUpload is a DELETE request with uploadId query parameter
    ///
    /// # Requirements
    ///
    /// - Requirement 4.5: Detect AbortMultipartUpload requests
    fn is_abort_multipart_upload(query: &str) -> bool {
        // AbortMultipartUpload has uploadId in query
        // The query typically contains "uploadId=..."
        query.contains("uploadId") && !query.contains("partNumber")
    }

    /// Extract upload ID from CreateMultipartUpload XML response
    ///
    /// S3 returns the uploadId in the XML body:
    /// ```xml
    /// <InitiateMultipartUploadResult>
    ///   <Bucket>bucket-name</Bucket>
    ///   <Key>object-key</Key>
    ///   <UploadId>upload-id-value</UploadId>
    /// </InitiateMultipartUploadResult>
    /// ```
    fn extract_upload_id_from_xml(xml: &str) -> Option<String> {
        debug!("Attempting to extract UploadId from CreateMultipartUpload XML response (length: {} bytes)", xml.len());

        // Simple XML parsing to extract UploadId value
        // Look for <UploadId>value</UploadId> pattern (case-insensitive)
        let xml_lower = xml.to_lowercase();

        if let Some(start_pos) = xml_lower.find("<uploadid>") {
            let after_tag = &xml[start_pos + 10..]; // Skip "<UploadId>" or "<uploadid>"
            if let Some(end_pos) = after_tag.to_lowercase().find("</uploadid>") {
                let upload_id = after_tag[..end_pos].trim().to_string();
                info!("Successfully extracted UploadId from XML: {}", upload_id);
                return Some(upload_id);
            }
        }

        // Fallback: return None if UploadId not found
        warn!(
            "Failed to extract UploadId from CreateMultipartUpload response XML. XML content: {}",
            if xml.len() > 500 { &xml[..500] } else { xml }
        );
        None
    }

    /// Handle CreateMultipartUpload request
    ///
    /// This method:
    /// 1. Forwards the request to S3
    /// 2. Parses the uploadId from S3 response XML
    /// 3. Creates mpus_in_progress/{uploadId}/upload.meta to track the upload
    /// 4. Returns S3 response unchanged to client
    ///
    /// # Requirements
    ///
    /// - Requirement 4.1: Record uploadId and start time when multipart upload is initiated
    async fn handle_create_multipart_upload(
        &self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        let (bucket, key) = parse_cache_key(&cache_key);

        // Extract Content-Type from request headers before forwarding
        // This is optional - if provided, we cache it for use in CompleteMultipartUpload
        let content_type = req
            .headers()
            .get("content-type")
            .or_else(|| req.headers().get("Content-Type"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // CreateMultipartUpload is forward-only on the request side; only its
        // response XML is inspected below. Stream its request verbatim. A client
        // disconnect can therefore reach S3 as a partial request, matching the main
        // signed PUT streaming path.
        let s3_response = forward_signed_request_streaming_verbatim(
            req,
            &target_host,
            &transport,
            self.proxy_referer.as_deref(),
            STREAMED_BODY_CAP,
        )
        .await?;

        let status = s3_response.status();
        let response_headers = s3_response.headers().clone();

        // Read response body to extract uploadId from XML
        let response_body_bytes = s3_response
            .into_body()
            .collect()
            .await
            .map(|collected| collected.to_bytes())
            .unwrap_or_default();

        if status.is_success() {
            // Parse uploadId from XML response
            let body_str = String::from_utf8_lossy(&response_body_bytes);

            if let Some(upload_id) = Self::extract_upload_id_from_xml(&body_str) {
                // Create tracking file for this multipart upload (with content-type if provided)
                if let Err(e) = self
                    .create_multipart_upload_tracker_with_content_type(
                        &cache_key,
                        &upload_id,
                        content_type.clone(),
                    )
                    .await
                {
                    // Log error but don't fail the request - S3 operation succeeded
                    error!(
                        "CreateMultipartUpload tracker failed: bucket={}, key={}, error={}",
                        bucket, key, e
                    );
                } else {
                    info!(
                        "CreateMultipartUpload: bucket={}, key={}, upload_id={}",
                        bucket,
                        key,
                        truncate_upload_id(&upload_id)
                    );
                }
            } else {
                warn!(
                    "Could not extract uploadId from CreateMultipartUpload response: cache_key={}",
                    cache_key
                );
            }
        } else {
            debug!(
                "CreateMultipartUpload failed at S3: cache_key={}, status={}",
                cache_key, status
            );
        }

        // Rebuild response with the body we read (return S3 response unchanged)
        let mut response_builder = Response::builder().status(status);
        for (name, value) in response_headers.iter() {
            response_builder = response_builder.header(name, value);
        }
        let rebuilt_response = response_builder
            .body(
                Full::new(response_body_bytes)
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .map_err(|e| ProxyError::HttpError(format!("Failed to rebuild response: {}", e)))?;

        // CreateMultipartUpload is a POST and is out of scope for per-bucket traffic
        // (only GET object reads and PUT object/part writes are counted). The part data
        // bytes are recorded on the UploadPart path; the control POSTs are not counted.

        Ok(rebuilt_response)
    }

    /// Create a multipart upload tracker file with optional content-type
    ///
    /// Creates mpus_in_progress/{uploadId}/upload.meta with:
    /// - upload_id
    /// - cache_key
    /// - started_at timestamp
    /// - content_type (if provided in CreateMultipartUpload request)
    /// - empty parts list
    ///
    /// # Requirements
    ///
    /// - Requirement 4.1: Record uploadId and start time
    async fn create_multipart_upload_tracker_with_content_type(
        &self,
        cache_key: &str,
        upload_id: &str,
        content_type: Option<String>,
    ) -> Result<()> {
        use crate::cache_types::MultipartUploadTracker;

        // Validate upload_id from upstream response (defense-in-depth against
        // attacker-controlled upstreams returning path-traversal characters).
        if !crate::path_safety::is_safe_path_component(upload_id) {
            return Err(ProxyError::CacheError(format!(
                "Unsafe upload_id from upstream response: {}",
                upload_id
            )));
        }

        // Create directory for this upload
        let upload_dir = self.cache_dir.join("mpus_in_progress").join(upload_id);
        tokio::fs::create_dir_all(&upload_dir).await.map_err(|e| {
            ProxyError::CacheError(format!(
                "Failed to create multipart upload directory: {}",
                e
            ))
        })?;

        // Create tracker with content-type if provided
        let tracker = MultipartUploadTracker::new_with_content_type(
            upload_id.to_string(),
            cache_key.to_string(),
            content_type,
        );

        // Write tracker to file
        let tracker_path = upload_dir.join("upload.meta");
        let tracker_json = tracker.to_json().map_err(|e| {
            ProxyError::CacheError(format!(
                "Failed to serialize multipart upload tracker: {}",
                e
            ))
        })?;

        tokio::fs::write(&tracker_path, tracker_json)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to write multipart upload tracker: {}", e))
            })?;

        debug!(
            "Created multipart upload tracker: upload_id={}, cache_key={}, content_type={:?}, path={:?}",
            upload_id, cache_key, tracker.content_type, tracker_path
        );

        Ok(())
    }

    /// Handle an `UploadPart` request by streaming the part body to the upstream
    /// verbatim while teeing it to a part-staging cache sink.
    ///
    /// Mirrors the single-part PUT streaming path ([`Self::stream_put_to_upstream`]):
    /// the client body frames flow straight to the upstream (the awaited socket
    /// write is the primary backpressure) and the same frames are tee'd to a bounded
    /// channel feeding [`Self::run_streaming_part_cache_write`], which incrementally
    /// decodes aws-chunked (cache branch only) and stages the part into
    /// `mpus_in_progress/{upload_id}/part{N}.bin`. The upstream always receives the
    /// original bytes byte-for-byte (SigV4 intact). The per-part correctness gate is
    /// preserved: the staged part is finalized and recorded in the `upload.meta`
    /// tracker under `upload.lock` only on S3 success.
    ///
    /// # Requirements
    ///
    /// - Requirement 6.2: `UploadPart` streams per Requirements 1–5 (bounded memory)
    /// - Requirement 5.1/5.2: cache each part as a range file at its byte offset
    /// - Requirement 7.1/7.2: a cache skip/failure never alters the forwarded bytes
    /// - Requirement 8.1/8.2: handle cache write failures gracefully
    #[allow(clippy::too_many_arguments)]
    async fn handle_upload_part(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
        upload_id: String,
        part_number: u32,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // Security: validate uploadId before any filesystem path construction.
        // On reject, forward to S3 unmodified (preserve SigV4 response) and skip cache work.
        if !is_safe_path_component(&upload_id) {
            warn!(
                "UploadPart: rejected unsafe uploadId={}, forwarding to S3 without caching",
                truncate_upload_id(&upload_id)
            );
            // This reject path does not inspect the body. Stream it verbatim so a
            // client disconnect reaches S3 as a partial request, matching other
            // signed forward-only paths.
            return forward_signed_request_streaming_verbatim(
                req,
                &target_host,
                &transport,
                self.proxy_referer.as_deref(),
                STREAMED_BODY_CAP,
            )
            .await;
        }

        // Extract request headers
        let request_headers: HashMap<String, String> = req
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        let content_length = request_headers
            .get("content-length")
            .or_else(|| request_headers.get("Content-Length"))
            .and_then(|v| v.parse::<u64>().ok());

        // Check capacity
        let cache_decision = self.should_cache(content_length);

        // Compute bytes_uploaded for per-bucket traffic accounting (Design §4b).
        // Use x-amz-decoded-content-length for aws-chunked bodies; else Content-Length.
        // This is a header-only check — no I/O, safe to do before the match.
        let body_bytes_uploaded = {
            let is_chunked = aws_chunked_decoder::is_aws_chunked(&request_headers);
            if is_chunked {
                aws_chunked_decoder::get_decoded_content_length(&request_headers).unwrap_or(0)
            } else {
                content_length.unwrap_or(0)
            }
        };

        // Save bucket before cache_key is consumed by the match arms.
        let (bucket_ref, _) = parse_cache_key(&cache_key);
        let bucket_owned = bucket_ref.to_string();

        let result = match cache_decision {
            CacheDecision::Cache | CacheDecision::StreamWithCapacityCheck => {
                // Stream the part body to the upstream verbatim while teeing it to
                // the part-staging cache sink (streaming-write-path Req 6.2). This
                // replaces the former buffer-then-forward implementation
                // (`read_request_body_bounded` + inline `raw_request` assembly +
                // `forward_raw_request_to_s3` + a whole-buffer part cache write):
                // the client body frames flow straight to the upstream (the awaited
                // socket write is the primary backpressure), and the same frames are
                // tee'd to a bounded channel feeding the incremental part-cache task.
                // The upstream always receives the original bytes byte-for-byte
                // (SigV4 intact); only the cache branch decodes aws-chunked (now done
                // incrementally inside the cache task, not up front).
                let method = req.method().clone();
                let uri = req.uri().clone();
                let headers = req.headers().clone();
                let version = req.version();
                let body = req.into_body();

                // The cache branch decodes aws-chunked incrementally; the decoded
                // part length comes from `x-amz-decoded-content-length` for
                // aws-chunked, else the Content-Length. Used only to validate the
                // decoded length at finish (Req 3.4); the upstream always receives
                // the original bytes verbatim.
                let is_aws_chunked = aws_chunked_decoder::is_aws_chunked(&request_headers);
                let decoded_len = if is_aws_chunked {
                    aws_chunked_decoder::get_decoded_content_length(&request_headers)
                } else {
                    content_length
                };

                // Open the part sink + spawn the incremental part-cache task when
                // caching is viable. `tee = None` means no caching, but the body
                // still streams to the upstream (Req 7.2).
                let (tee, s3_result_tx) = self
                    .setup_upload_part_cache_tee(
                        &cache_key,
                        &upload_id,
                        part_number,
                        is_aws_chunked,
                        decoded_len,
                    )
                    .await;

                // Stream the original body to the upstream verbatim (Req 1.1, 4.1),
                // enforcing the body-size cap without buffering the whole body.
                let s3_response = forward_signed_request_streaming(
                    &method,
                    &uri,
                    &headers,
                    version,
                    body,
                    &target_host,
                    &transport,
                    self.proxy_referer.as_deref(),
                    STREAMED_BODY_CAP,
                    tee,
                )
                .await;

                // Deliver the S3 result (status + headers, or error) to the
                // background part-cache task. On success it finalizes the part under
                // `upload.lock` and records the tracker with the response ETag; on
                // error/non-success/skip it discards the staged part — the per-part
                // correctness gate (commit only on S3 success) is preserved.
                if let Some(s3_result_tx) = s3_result_tx {
                    let response_info = match &s3_response {
                        Ok(resp) => Ok(ResponseInfo {
                            status: resp.status(),
                            headers: resp.headers().clone(),
                        }),
                        Err(e) => Err(e.clone()),
                    };
                    let _ = s3_result_tx.send(response_info);
                }

                // Return the S3 response to the client unchanged (Req 5.5).
                s3_response
            }
            CacheDecision::Bypass(reason) => {
                log_bypass_decision(&cache_key, &reason);
                // Record bypassed PUT (Requirement 9.2)
                if let Some(metrics) = &self.metrics_manager {
                    metrics.read().await.record_bypassed_put().await;
                }
                // This bypass arm is forward-only. Stream the original frames
                // instead of retaining an object the cache declined to store. A
                // mid-upload disconnect now reaches S3 as a partial upload, matching
                // the cached UploadPart path.
                forward_signed_request_streaming_verbatim(
                    req,
                    &target_host,
                    &transport,
                    self.proxy_referer.as_deref(),
                    STREAMED_BODY_CAP,
                )
                .await
            }
        };

        // Per-bucket traffic accounting — Spec: per-bucket-metrics, Req 2.1, 2.2, 2.3
        // UploadPart maps to RequestType::Put (Design §4a). Record once at the completion
        // of the full request-response cycle (Req 2.3). Skip if bucket empty (Req 2.4).
        // Response body for UploadPart is empty (bytes_served = 0).
        if !bucket_owned.is_empty() {
            if let Some(metrics) = &self.metrics_manager {
                metrics
                    .read()
                    .await
                    .record_bucket_traffic(
                        &bucket_owned,
                        None,             // prefix: no per-bucket prefix config in this handler
                        RequestType::Put, // UploadPart is a PUT operation
                        0,                // bytes_served: UploadPart response body is empty
                        0,                // bytes_saved: PUT is not a cache hit
                        body_bytes_uploaded,
                    )
                    .await;
            }
        }

        result
    }

    /// Cache an upload part as a range file. **Test-support only** (`#[cfg(test)]`):
    /// production caches parts through the streaming part sink
    /// (`open_multipart_part_sink` + `MultipartPartSink::finalize`), which
    /// consults per-bucket cache rules and streams rather than buffering. This
    /// buffered one-shot writer is retained solely as the part-population helper
    /// for the multipart test suite (cleanup, finalize, GET-from-cache, and the
    /// same-part-race concurrency regression); it holds `part{N}.lock` across the
    /// part-file rename and the part-record write, the same correctness gate the
    /// sink enforces. It is compiled only under `cfg(test)` and never ships in
    /// the production binary. Spec: compression-followup-fixes Requirement 4.
    #[cfg(test)]
    pub async fn cache_upload_part(
        &mut self,
        cache_key: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
        etag: &str,
    ) -> Result<()> {
        use crate::cache_types::CachedPartInfo;
        use fs2::FileExt;

        // Ensure multipart tracking directory exists
        let multipart_dir = self.cache_dir.join("mpus_in_progress").join(upload_id);
        tokio::fs::create_dir_all(&multipart_dir)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to create multipart directory: {}", e))
            })?;

        // Store part data in the upload-specific directory (isolated per upload_id)
        let part_file_path = multipart_dir.join(format!("part{}.bin", part_number));

        // Compress the part data (no shared state touched, safe outside the lock).
        let should_compress = self.compression_handler.is_compression_enabled();
        let compression_result =
            self.compression_handler
                .compress_with_metadata(data, cache_key, should_compress);

        // Acquire the PER-PART lock before writing the part file and its record, so
        // a racing same-part-number write cannot leave the on-disk bytes out of sync
        // with the recorded ETag (the invariant exercised by the concurrency test).
        // Per part rather than per upload: the invariant is per part, and a
        // per-upload lock serialised every part of every concurrent upload.
        let lock_file_path = Self::part_lock_path(&multipart_dir, part_number);

        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_file_path)
            .map_err(|e| ProxyError::CacheError(format!("Failed to open lock file: {}", e)))?;

        lock_file.lock_exclusive().map_err(|e| {
            ProxyError::CacheError(format!("Failed to acquire per-part lock: {}", e))
        })?;

        // Write part file atomically using temp file + rename (inside the lock)
        let temp_part_file_path = part_file_path.with_extension("tmp");
        tokio::fs::write(&temp_part_file_path, &compression_result.data)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to write temporary part file: {}", e))
            })?;

        tokio::fs::rename(&temp_part_file_path, &part_file_path)
            .await
            .map_err(|e| ProxyError::CacheError(format!("Failed to rename part file: {}", e)))?;

        // Create part info for tracker (path is deterministic from upload_id + part_number)
        let part_info = CachedPartInfo::new(
            part_number,
            data.len() as u64,
            etag.to_string(),
            compression_result.algorithm.clone(),
        );

        // Read existing tracker or create new one
        // One small file per part. Nothing shared is read or rewritten, so this
        // costs the same at ten parts and at ten thousand.
        let multipart_dir_owned = multipart_dir.clone();
        let part_info_owned = part_info.clone();
        tokio::task::spawn_blocking(move || {
            Self::record_part_blocking(&multipart_dir_owned, part_number, &part_info_owned)
        })
        .await
        .map_err(|e| ProxyError::CacheError(format!("Part record task panicked: {}", e)))??;

        drop(lock_file);
        Ok(())
    }

    /// Path of the per-part record for `part_number`.
    ///
    /// Sits beside the part's data file (`part{N}.bin`) and holds that part's
    /// [`CachedPartInfo`] — size, ETag and compression algorithm — as its own small
    /// JSON document.
    fn part_record_path(multipart_dir: &std::path::Path, part_number: u32) -> std::path::PathBuf {
        multipart_dir.join(format!("part{}.json", part_number))
    }

    /// Path of the per-part lock for `part_number`.
    fn part_lock_path(multipart_dir: &std::path::Path, part_number: u32) -> std::path::PathBuf {
        multipart_dir.join(format!("part{}.lock", part_number))
    }

    /// Record one part, without rewriting anything shared.
    ///
    /// # Why per-part files rather than one tracker
    ///
    /// Every `UploadPart` used to take an exclusive cross-instance `flock` on
    /// `upload.lock`, read the WHOLE `upload.meta`, append one entry, and write the
    /// whole file back. That is O(n²) bytes over an upload and, worse, it serialises
    /// every part of every concurrent upload on one lock on a network filesystem.
    ///
    /// Measured at 2,000 parts on a three-proxy fleet: **1,214 `upload.lock`
    /// timeouts, 1,220 part-record failures, and the client's upload FAILED** with
    /// `Connection reset by peer`, because each timed-out finalize held a
    /// `spawn_blocking` thread for its full 30-second timeout and the forward path
    /// needs that same pool. At ten concurrent parts the same queueing left three
    /// records unlanded after ten seconds, so the object was not cached at all.
    ///
    /// A part now owns its own filenames — `part{N}.bin`, `part{N}.json`,
    /// `part{N}.lock` — so per-part cost is O(1) and no part contends with any
    /// other. Finalisation reads the directory instead of one growing document
    /// ([`Self::load_tracker`]).
    ///
    /// # The correctness gate is preserved, at per-part scope
    ///
    /// The § 2 invariant is that a retried part with different bytes must never
    /// leave the on-disk file and the recorded ETag disagreeing. That still holds:
    /// the caller publishes `part{N}.bin` and writes `part{N}.json` inside one
    /// critical section, now guarded by `part{N}.lock`. Two writers racing the SAME
    /// part number still serialise; two writers on DIFFERENT part numbers no longer
    /// serialise at all, which is the whole point. The lock is per part rather than
    /// per upload because the invariant is per part — nothing about it ever needed
    /// to exclude a different part number.
    fn record_part_blocking(
        multipart_dir: &std::path::Path,
        part_number: u32,
        part_info: &crate::cache_types::CachedPartInfo,
    ) -> Result<()> {
        use std::io::Write;

        let record_path = Self::part_record_path(multipart_dir, part_number);
        let json = serde_json::to_string(part_info).map_err(|e| {
            ProxyError::CacheError(format!("Failed to serialize part record: {}", e))
        })?;

        // Atomic: tmp + fsync + rename (Req 6.2). The tmp name carries the part
        // number so two parts cannot collide on it.
        let tmp_path = multipart_dir.join(format!("part{}.json.tmp", part_number));
        let mut f = std::fs::File::create(&tmp_path).map_err(|e| {
            ProxyError::CacheError(format!("Failed to create temp part record: {}", e))
        })?;
        f.write_all(json.as_bytes()).map_err(|e| {
            ProxyError::CacheError(format!("Failed to write temp part record: {}", e))
        })?;
        f.sync_all()
            .map_err(|e| ProxyError::CacheError(format!("Failed to fsync part record: {}", e)))?;
        drop(f);

        std::fs::rename(&tmp_path, &record_path)
            .map_err(|e| ProxyError::CacheError(format!("Failed to rename part record: {}", e)))
    }

    /// Which of `wanted` have a record on disk.
    ///
    /// Stats the specific files rather than listing the directory. A staging directory
    /// holds up to three files per part (`.bin`, `.json`, `.lock`), so at 2,000 parts a
    /// full listing walks 6,000 entries on a network filesystem — and the poll loop
    /// that needs this answer runs every 100 ms. Statting `wanted` is bounded by what
    /// the caller asked about, and short-circuits as soon as one is missing, which is
    /// the common case early in the wait.
    fn present_parts_blocking(
        multipart_dir: &std::path::Path,
        wanted: &std::collections::HashSet<u32>,
    ) -> std::collections::HashSet<u32> {
        wanted
            .iter()
            .copied()
            .filter(|n| Self::part_record_path(multipart_dir, *n).exists())
            .collect()
    }

    /// Which part numbers have a record on disk.
    ///
    /// Existence only — no JSON parsing — because the one hot caller
    /// ([`Self::await_tracker_parts`]) asks nothing else, and it asks repeatedly.
    fn recorded_part_numbers(multipart_dir: &std::path::Path) -> std::collections::HashSet<u32> {
        let mut present = std::collections::HashSet::new();
        if let Ok(entries) = std::fs::read_dir(multipart_dir) {
            for entry in entries.flatten() {
                if let Some(n) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix("part"))
                    .and_then(|rest| rest.strip_suffix(".json"))
                    .and_then(|digits| digits.parse::<u32>().ok())
                {
                    present.insert(n);
                }
            }
        }
        present
    }

    /// Assemble a [`MultipartUploadTracker`] from the upload-level `upload.meta`
    /// plus the per-part records on disk.
    ///
    /// `upload.meta` is written once, at `CreateMultipartUpload`, and carries only
    /// upload-level facts (upload id, cache key, start time, content type). The
    /// parts come from the directory. So the returned value is the same shape every
    /// existing caller expects, assembled rather than parsed from one document.
    ///
    /// A record that fails to parse is skipped with a `warn!` rather than failing
    /// the whole load: one unreadable part should cost that part, not the object.
    /// The missing-parts guard in `finalize_multipart_upload` then declines to
    /// finalise, which is the correct outcome and the same one as if the part had
    /// never landed.
    fn load_tracker_blocking(
        multipart_dir: &std::path::Path,
        upload_id: &str,
        cache_key: &str,
    ) -> Result<crate::cache_types::MultipartUploadTracker> {
        use crate::cache_types::{CachedPartInfo, MultipartUploadTracker};

        // A missing or unparseable `upload.meta` is NOT fatal, and that is deliberate.
        // The per-part path used to create the tracker on demand when it was absent, so
        // an upload whose `CreateMultipartUpload` this fleet never saw — one started
        // before a deploy, or on a volume that lost the file — still cached. Requiring
        // the file here would silently remove that self-healing. The only thing lost by
        // synthesising is upload-level `content_type`, which is cosmetic; the parts
        // come from disk either way.
        let upload_meta_file = multipart_dir.join("upload.meta");
        let mut tracker = match std::fs::read_to_string(&upload_meta_file) {
            Ok(content) => match MultipartUploadTracker::from_json(&content) {
                Ok(tracker) => tracker,
                Err(e) => {
                    warn!(
                        "Unparseable upload.meta at {:?}: {} — rebuilding upload-level fields from the request (parts are unaffected; they come from disk)",
                        upload_meta_file, e
                    );
                    MultipartUploadTracker::new(upload_id.to_string(), cache_key.to_string())
                }
            },
            Err(_) => {
                debug!(
                    "No upload.meta at {:?}; rebuilding upload-level fields from the request",
                    upload_meta_file
                );
                MultipartUploadTracker::new(upload_id.to_string(), cache_key.to_string())
            }
        };

        // Upload-level file is authoritative for everything except the parts, which
        // are rebuilt from disk so a stale `parts` array in an old-format file
        // cannot contribute.
        tracker.parts.clear();
        tracker.total_size = 0;

        for part_number in Self::recorded_part_numbers(multipart_dir) {
            let record_path = Self::part_record_path(multipart_dir, part_number);
            match std::fs::read_to_string(&record_path)
                .map_err(|e| e.to_string())
                .and_then(|s| serde_json::from_str::<CachedPartInfo>(&s).map_err(|e| e.to_string()))
            {
                Ok(info) => tracker.add_part(info),
                Err(e) => warn!(
                    "Skipping unreadable part record {:?}: {} (this part will count as not cached)",
                    record_path, e
                ),
            }
        }

        Ok(tracker)
    }

    /// Handle CompleteMultipartUpload by creating metadata linking all parts
    ///
    /// # Requirements
    ///
    /// - Requirement 5.3: Handle CompleteMultipartUpload
    /// - Requirement 5.4: Create metadata linking all cached parts as ranges
    /// - Requirement 5.5: Mark upload as incomplete on failure without deleting parts
    /// - Requirement 8.1: Handle cache write failures gracefully
    /// - Requirement 8.2: Clean up cached data on S3 error
    /// - Requirement 9.3: Log detailed error information
    async fn handle_complete_multipart_upload(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
        upload_id: String,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // Security: validate uploadId before any filesystem path construction.
        // On reject, forward to S3 unmodified (preserve SigV4 response) and skip cache work.
        if !is_safe_path_component(&upload_id) {
            warn!(
                "CompleteMultipartUpload: rejected unsafe uploadId={}, forwarding to S3 without caching",
                truncate_upload_id(&upload_id)
            );
            // This reject path does not inspect the body. Stream it verbatim so a
            // client disconnect reaches S3 as a partial request, matching other
            // signed forward-only paths.
            return forward_signed_request_streaming_verbatim(
                req,
                &target_host,
                &transport,
                self.proxy_referer.as_deref(),
                STREAMED_BODY_CAP,
            )
            .await;
        }

        let (bucket, key) = parse_cache_key(&cache_key);

        // Buffer the request body before forwarding to S3 (Requirement 4.1)
        // This allows us to parse the XML to extract the requested parts list
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers = req.headers().clone();
        let version = req.version();

        // Read the request body with a bounded cap (Security: prevent unbounded memory
        // consumption from an oversized CompleteMultipartUpload body). The Complete XML
        // lists part numbers and ETags — a few MiB is ample for even the largest uploads
        // (10,000 parts). On overflow, reject with HTTP 413 before forwarding to S3.
        let max_bytes = self.max_complete_body_bytes;
        let content_length_hint = headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());

        // Fast reject if Content-Length already exceeds cap
        if let Some(cl) = content_length_hint {
            if cl > max_bytes {
                warn!(
                    content_length = cl,
                    max_bytes = max_bytes,
                    "CompleteMultipartUpload body exceeds max_complete_body_bytes (Content-Length)"
                );
                return Err(ProxyError::RequestBodyTooLarge {
                    content_length: Some(cl),
                    max_bytes,
                });
            }
        }

        // Reserve against the in-flight ledger for consistency in the ledger
        // total, even though this cap is already correct and does not change
        // (design.md → "Already-bounded sites, unchanged"). The Complete XML
        // is capped at `max_complete_body_bytes` (10 MiB default) with an
        // up-front Content-Length check above and a mid-collect guard below,
        // so this reservation is taken for the declared length (or grown per
        // chunk when absent) purely for observability parity with the other
        // Buffering_Sites — it is not the primary defense against an oversized
        // body, which `RequestBodyTooLarge`/413 above and below remains.
        let ledger = self
            .s3_client
            .as_ref()
            .map(|c| c.get_inflight_ledger())
            .unwrap_or_else(|| Arc::new(crate::inflight_ledger::InflightLedger::disabled()));
        let mut mpu_reservation = match ledger.try_reserve(content_length_hint.unwrap_or(0)) {
            Some(r) => r,
            None => {
                return Err(ProxyError::InflightCeilingExceeded {
                    ceiling_bytes: ledger.ceiling_bytes(),
                    requested_bytes: content_length_hint.unwrap_or(0),
                });
            }
        };

        let mut body = req.into_body();
        let mut accumulated = Vec::with_capacity(
            content_length_hint
                .unwrap_or(8192)
                .min(max_bytes)
                .min(1024 * 1024) as usize,
        );

        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| {
                ProxyError::HttpError(format!(
                    "Failed to read CompleteMultipartUpload body: {}",
                    e
                ))
            })?;
            if let Ok(data) = frame.into_data() {
                if accumulated.len() as u64 + data.len() as u64 > max_bytes {
                    warn!(
                        accumulated_bytes = accumulated.len(),
                        chunk_bytes = data.len(),
                        max_bytes = max_bytes,
                        "CompleteMultipartUpload body exceeds max_complete_body_bytes"
                    );
                    return Err(ProxyError::RequestBodyTooLarge {
                        content_length: content_length_hint,
                        max_bytes,
                    });
                }
                let already_reserved = mpu_reservation.held_bytes();
                let new_total = accumulated.len() as u64 + data.len() as u64;
                if new_total > already_reserved {
                    let growth = new_total - already_reserved;
                    if !mpu_reservation.try_grow(growth) {
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
        let request_body_bytes = Bytes::from(accumulated);

        // Parse the request body to extract the requested parts list (Requirement 4.1, 4.2)
        let requested_parts = match parse_complete_mpu_request(&request_body_bytes) {
            Ok(parts) => {
                debug!(
                    "Parsed CompleteMultipartUpload request: cache_key={}, parts_count={}",
                    cache_key,
                    parts.len()
                );
                Some(parts)
            }
            Err(e) => {
                // If request body is empty or malformed, skip cache finalization (Requirement 4.3)
                warn!(
                    "Failed to parse CompleteMultipartUpload request body: cache_key={}, error={}, will skip cache finalization",
                    cache_key, e
                );
                None
            }
        };

        // Forward the original request body to S3 using the pre-buffered body
        let s3_response = forward_signed_request_with_body(
            method,
            uri,
            headers,
            version,
            request_body_bytes,
            &target_host,
            &transport,
            self.proxy_referer.as_deref(),
        )
        .await?;

        let status = s3_response.status();

        if status.is_success() {
            // Read response body to extract ETag from XML
            // S3 returns CompleteMultipartUpload response as XML with ETag in the body

            let (parts, body) = s3_response.into_parts();

            // Extract response headers for cache metadata
            let response_headers: std::collections::HashMap<String, String> = parts
                .headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();

            // Collect the body bytes
            let body_bytes = body
                .collect()
                .await
                .map_err(|e| ProxyError::HttpError(format!("Failed to read response body: {}", e)))?
                .to_bytes();

            // Extract ETag from XML response body
            // Format: <CompleteMultipartUploadResult><ETag>"etag-value"</ETag>...</CompleteMultipartUploadResult>
            let body_str = String::from_utf8_lossy(&body_bytes);
            let etag = Self::extract_etag_from_xml(&body_str);

            debug!(
                "Extracted ETag from CompleteMultipartUpload response: cache_key={}, etag={}",
                cache_key, etag
            );

            // Create metadata linking all parts as ranges (Requirement 5.4, 8.1, 9.3)
            // Pass the requested_parts to finalize_multipart_upload for filtering (Requirement 4.4, 5.1)
            //
            // A parse failure now genuinely skips finalization, which is what the
            // warning above has always claimed. Previously `None` was passed
            // through to `finalize_multipart_upload`, where it meant "use all
            // cached parts" — disabling the missing-part and ETag guards and
            // letting a tracker holding a SUBSET of parts produce
            // self-consistent metadata for a shorter object. There is no safe
            // way to finalize without the requested part list: it is the only
            // statement of what the completed object actually contains.
            match requested_parts.as_deref() {
                Some(parts) => {
                    if let Err(e) = self
                        .finalize_multipart_upload(
                            &cache_key,
                            &upload_id,
                            &etag,
                            &response_headers,
                            parts,
                        )
                        .await
                    {
                        error!(
                            "CompleteMultipartUpload cache failed: bucket={}, key={}, error={}",
                            bucket, key, e
                        );
                    }
                }
                None => {
                    warn!(
                        "Skipping multipart cache finalization because the CompleteMultipartUpload body did not parse: cache_key={}, upload_id={} (upload unaffected — S3 already accepted it)",
                        cache_key, upload_id
                    );
                }
            }
            // Success log is in finalize_multipart_upload with full details

            // Reconstruct response to return to client
            let response = Response::from_parts(
                parts,
                Full::new(body_bytes)
                    .map_err(|never| match never {})
                    .boxed(),
            );
            Ok(response)
        } else {
            // Mark upload as incomplete but don't delete parts (Requirement 5.5, 8.2, 9.3)
            error!(
                "CompleteMultipartUpload S3 error: bucket={}, key={}, status={}",
                bucket,
                key,
                status.as_u16()
            );
            Ok(s3_response)
        }
    }

    /// Handle AbortMultipartUpload request
    ///
    /// This method:
    /// 1. Forwards the request to S3
    /// 2. Immediately evicts all cached parts for that uploadId
    /// 3. Deletes mpus_in_progress/{uploadId}/
    /// 4. Returns S3 response unchanged to client
    ///
    /// # Requirements
    ///
    /// - Requirement 4.5: Forward to S3 and immediately evict all cached parts for that uploadId
    /// - Requirement 4.6: Return S3 response unchanged to client
    async fn handle_abort_multipart_upload(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
        upload_id: String,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // Security: validate uploadId BEFORE any path construction or remove_dir_all.
        // On reject, forward to S3 unmodified (preserve SigV4 response) and skip cache work.
        if !is_safe_path_component(&upload_id) {
            warn!(
                "AbortMultipartUpload: rejected unsafe uploadId={}, forwarding to S3 without caching",
                truncate_upload_id(&upload_id)
            );
            // This reject path does not inspect the body. Stream it verbatim so a
            // client disconnect reaches S3 as a partial request, matching other
            // signed forward-only paths.
            return forward_signed_request_streaming_verbatim(
                req,
                &target_host,
                &transport,
                self.proxy_referer.as_deref(),
                STREAMED_BODY_CAP,
            )
            .await;
        }

        info!(
            "Handling AbortMultipartUpload: cache_key={}, upload_id={}",
            cache_key, upload_id
        );

        // AbortMultipartUpload is forward-only; cleanup happens after the response,
        // not from its request body. Stream it verbatim so a mid-upload disconnect
        // reaches S3 as a partial request, consistent with signed PUT forwarding.
        let s3_response = forward_signed_request_streaming_verbatim(
            req,
            &target_host,
            &transport,
            self.proxy_referer.as_deref(),
            STREAMED_BODY_CAP,
        )
        .await?;

        // Always clean up cached parts, regardless of S3 response status
        // This ensures we don't leave orphaned cache data
        if let Err(e) = self.cleanup_multipart_upload(&upload_id).await {
            error!(
                "Failed to cleanup multipart upload cache: cache_key={}, upload_id={}, error={}",
                cache_key, upload_id, e
            );
        } else {
            info!(
                "Successfully cleaned up multipart upload cache: cache_key={}, upload_id={}",
                cache_key, upload_id
            );
        }

        // Return S3 response unchanged (Requirement 4.6)
        Ok(s3_response)
    }

    /// Clean up all cached parts and tracking metadata for a multipart upload
    ///
    /// This method:
    /// 1. Acquires lock on upload.meta
    /// 2. Reads all cached parts from tracking metadata
    /// 3. Deletes each part's range file from ranges/{bucket}/{XX}/{YYY}/
    /// 4. Deletes the upload.meta file
    /// 5. Deletes the mpus_in_progress/{uploadId}/ directory
    ///
    /// # Requirements
    ///
    /// - Requirement 4.5: Delete all cached parts for uploadId
    /// - Requirement 8.5: Clean up tracking metadata
    pub async fn cleanup_multipart_upload(&mut self, upload_id: &str) -> Result<()> {
        let multipart_dir = self.cache_dir.join("mpus_in_progress").join(upload_id);

        if !multipart_dir.exists() {
            debug!(
                "Multipart directory not found during cleanup: upload_id={}",
                upload_id
            );
            return Ok(());
        }

        // Parts are stored inside the upload directory, so a single remove_dir_all cleans everything
        if let Err(e) = tokio::fs::remove_dir_all(&multipart_dir).await {
            warn!(
                "Failed to remove multipart directory: upload_id={}, error={}",
                upload_id, e
            );
        } else {
            info!(
                "Cleaned up multipart upload directory: upload_id={}",
                upload_id
            );
        }

        Ok(())
    }

    /// Finalize multipart upload by creating metadata linking all parts as ranges
    ///
    /// This method:
    /// 1. Acquires lock on upload.meta
    /// 2. Reads all parts, sorts by part number
    /// 3. Calculates byte offsets for each part
    /// 4. Renames part files with final offsets
    /// 5. Creates object metadata with final ETag from S3 XML
    /// 6. Sets is_write_cached=true, write_cache_expires_at
    /// 7. Deletes mpus_in_progress/{uploadId}/
    ///
    /// # Arguments
    ///
    /// * `cache_key` - The cache key for the object
    /// * `upload_id` - The multipart upload ID
    /// * `etag` - The final ETag from S3 response
    /// * `response_headers` - Headers from the S3 response
    /// * `requested_parts` - Optional list of parts from the CompleteMultipartUpload request body.
    ///   If provided, only these parts will be included in the final object (Requirement 5.1).
    ///   If None, all cached parts will be used (backward compatibility).
    ///
    /// # Requirements
    ///
    /// - Requirement 3.1: Create object metadata linking all cached parts as ranges
    /// - Requirement 3.2: Calculate final byte offsets for each part
    /// - Requirement 3.3: Store the final ETag from S3 response
    /// - Requirement 3.4: Set the write cache TTL on the completed object
    /// - Requirement 5.1: Use only parts listed in the CompleteMultipartUpload request body
    async fn finalize_multipart_upload(
        &mut self,
        cache_key: &str,
        upload_id: &str,
        etag: &str,
        response_headers: &std::collections::HashMap<String, String>,
        requested_parts: &[RequestedPart],
    ) -> Result<()> {
        use crate::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState};
        use crate::compression::CompressionAlgorithm;
        use fs2::FileExt;

        let multipart_dir = self.cache_dir.join("mpus_in_progress").join(upload_id);

        // Early validation - if we don't have the upload directory, skip caching entirely
        if !multipart_dir.exists() {
            warn!(
                "CompleteMultipartUpload succeeded on S3 but no local upload directory found: cache_key={}, upload_id={}, skipping cache finalization",
                cache_key, upload_id
            );
            return Ok(());
        }

        // Wait for the parts this Complete names to appear in the tracker before
        // evaluating it (the lifecycle race).
        //
        // The per-part cache task is fire-and-forget and deliberately lags the
        // client's response: the forward path answers the client, and only then
        // does the cache task drain the tee, await the S3 result and finalize.
        // Clients send Complete as soon as the last part is acknowledged — 147 ms
        // after the first part was recorded, in the measured case — so Complete
        // used to read a tracker holding 2 of 10 parts, declare the other 8
        // "not cached locally", and `remove_dir_all` the staging directory out
        // from under the 8 tasks still writing into it. Those tasks then failed
        // ENOENT on rename or ESTALE on `upload.lock`, which is where the error
        // flood came from. The loser was predetermined.
        //
        // WHY POLL THE TRACKER RATHER THAN TRACK IN-FLIGHT TASKS. Parts of one
        // upload can be served by several proxies and Complete by a fourth, so
        // an in-process registry of spawned tasks fixes only the case where they
        // happen to share an instance — which on a three-proxy fleet is the
        // uncommon one. The tracker already lives on the shared volume and is
        // already updated under `upload.lock` by whichever instance ran the part
        // task, and this Complete already knows exactly which parts it needs from
        // its own request body. So waiting for the tracker to contain them is
        // fleet-correct with no new shared state, no marker files, and no
        // dependence on process memory. It is also why the parse-failure path
        // above must skip outright: without the requested part list there is no
        // well-defined set to wait for.
        Self::await_tracker_parts(&multipart_dir, cache_key, upload_id, requested_parts).await;

        // Acquire lock on upload.meta
        let lock_file_path = multipart_dir.join("upload.lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_file_path)
            .map_err(|e| ProxyError::CacheError(format!("Failed to open lock file: {}", e)))?;

        lock_file.lock_exclusive().map_err(|e| {
            ProxyError::CacheError(format!("Failed to acquire lock for upload.meta: {}", e))
        })?;

        // Assemble the tracker: upload-level facts from `upload.meta`, parts from
        // the per-part records on disk. This is the only place that pays O(n) for
        // the part set, once per upload, instead of every part paying it.
        // Off the runtime: this reads one small file per part, so at high part counts
        // it is thousands of synchronous round trips to a shared volume.
        let tracker_load = {
            let dir = multipart_dir.clone();
            let upload_id_owned = upload_id.to_string();
            let cache_key_owned = cache_key.to_string();
            tokio::task::spawn_blocking(move || {
                Self::load_tracker_blocking(&dir, &upload_id_owned, &cache_key_owned)
            })
            .await
            .unwrap_or_else(|e| {
                Err(ProxyError::CacheError(format!(
                    "Tracker load task panicked: {}",
                    e
                )))
            })
        };
        let tracker = match tracker_load {
            Ok(tracker) => tracker,
            Err(e) => {
                warn!(
                    "CompleteMultipartUpload succeeded on S3 but failed to load the upload tracker: cache_key={}, upload_id={}, error={}, cleaning up and skipping cache finalization",
                    cache_key, upload_id, e
                );
                drop(lock_file);
                self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                    .await;
                return Ok(());
            }
        };

        if tracker.parts.is_empty() {
            warn!(
                "CompleteMultipartUpload succeeded on S3 but no parts found in upload tracker: cache_key={}, upload_id={}, cleaning up and skipping cache finalization",
                cache_key, upload_id
            );
            drop(lock_file);
            self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                .await;
            return Ok(());
        }

        // Build HashSet of requested part numbers for efficient lookup (Requirements 5.1, 5.2, 5.3)
        //
        // Always the set S3 was asked to assemble, never "whatever the tracker
        // happens to hold". The old `None` fallback to the tracker's own contents
        // made the filter below a no-op and the guards that follow vacuous, so a
        // partially-recorded upload finalized as a short object.
        let requested_part_numbers: std::collections::HashSet<u32> =
            requested_parts.iter().map(|p| p.part_number).collect();

        // Filter cached parts to only those in the request and sort by part number
        let all_cached_parts = tracker.get_sorted_parts();
        let filtered_parts: Vec<&crate::cache_types::CachedPartInfo> = all_cached_parts
            .into_iter()
            .filter(|p| requested_part_numbers.contains(&p.part_number))
            .collect();

        // Check if any requested parts are not cached locally (Requirement 5.4)
        // If a requested part is not in our cache, skip cache finalization
        {
            let cached_part_numbers: std::collections::HashSet<u32> =
                tracker.parts.iter().map(|p| p.part_number).collect();
            let missing_requested: Vec<u32> = requested_parts
                .iter()
                .filter(|p| !cached_part_numbers.contains(&p.part_number))
                .map(|p| p.part_number)
                .collect();

            if !missing_requested.is_empty() {
                // A genuine straggler after the bounded wait above. Degrade to
                // "not cached" quietly and leave the staging directory to the TTL
                // sweep.
                //
                // Deleting it here is what produced the error flood: the parts
                // this instance is declaring missing may still be mid-write, on
                // this instance or another, and `remove_dir_all` pulls the
                // directory (including `upload.lock` and any `.tmp`) out from
                // under them, so they fail ENOENT on rename or ESTALE on the
                // lock. Those errors read like disk faults and are not.
                warn!(
                    "CompleteMultipartUpload succeeded on S3 but parts {:?} were still not recorded after waiting {:?}: cache_key={}, upload_id={}, skipping cache finalization (upload unaffected; staging left for the TTL sweep)",
                    missing_requested, MULTIPART_COMPLETE_CACHE_WAIT, cache_key, upload_id
                );
                drop(lock_file);
                return Ok(());
            }
        }

        // Validate that all filtered parts exist on disk before proceeding
        let ranges_dir = self.cache_dir.join("ranges");
        let sorted_parts = filtered_parts;
        let mut missing_parts = Vec::new();

        for part in &sorted_parts {
            // Parts are stored in the upload directory: mpus_in_progress/{upload_id}/part{N}.bin
            let part_file = multipart_dir.join(format!("part{}.bin", part.part_number));

            if !part_file.exists() {
                missing_parts.push(part.part_number);
            }
        }

        // If any parts are missing, skip caching and clean up
        if !missing_parts.is_empty() {
            warn!(
                "CompleteMultipartUpload succeeded on S3 but missing local parts {:?}: cache_key={}, upload_id={}, cleaning up and skipping cache finalization",
                missing_parts, cache_key, upload_id
            );
            drop(lock_file);
            self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                .await;
            return Ok(());
        }

        // Validate ETags match between request and cached parts (Requirements 9.1, 9.2, 9.3, 9.4)
        // If any ETag mismatches, skip cache finalization but still forward to S3 (already done)
        {
            for requested_part in requested_parts {
                // Find the corresponding cached part
                if let Some(cached_part) = sorted_parts
                    .iter()
                    .find(|p| p.part_number == requested_part.part_number)
                {
                    // Normalize ETags by removing surrounding quotes before comparison
                    let request_etag = normalize_etag(&requested_part.etag);
                    let cached_etag = normalize_etag(&cached_part.etag);

                    if request_etag != cached_etag {
                        warn!(
                            "ETag mismatch for part {}: request_etag={}, cached_etag={}, cache_key={}, upload_id={}, skipping cache finalization",
                            requested_part.part_number,
                            request_etag,
                            cached_etag,
                            cache_key,
                            upload_id
                        );
                        drop(lock_file);
                        self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                            .await;
                        return Ok(());
                    }
                }
            }
        }

        // Delete unreferenced parts - parts cached but not in the CompleteMultipartUpload request
        // (Requirements 6.1, 6.2, 6.3, 6.4)
        let unreferenced_parts: Vec<&crate::cache_types::CachedPartInfo> = tracker
            .parts
            .iter()
            .filter(|p| !requested_part_numbers.contains(&p.part_number))
            .collect();

        if !unreferenced_parts.is_empty() {
            info!(
                "Cleaning up {} unreferenced parts not in CompleteMultipartUpload request: cache_key={}, upload_id={}, parts={:?}",
                unreferenced_parts.len(),
                cache_key,
                upload_id,
                unreferenced_parts.iter().map(|p| p.part_number).collect::<Vec<_>>()
            );

            for part in &unreferenced_parts {
                // Parts are in the upload directory
                let part_file = multipart_dir.join(format!("part{}.bin", part.part_number));
                if part_file.exists() {
                    match tokio::fs::remove_file(&part_file).await {
                        Ok(()) => {
                            debug!(
                                "Deleted unreferenced part {}: cache_key={}, upload_id={}",
                                part.part_number, cache_key, upload_id
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to delete unreferenced part {}: cache_key={}, upload_id={}, error={}",
                                part.part_number, cache_key, upload_id, e
                            );
                        }
                    }
                }
            }
        }

        // Calculate byte offsets from filtered parts (Requirements 5.3, 7.1)
        // This ensures we only use the parts specified in the request
        let byte_offsets: Vec<(u32, u64, u64)> = {
            let mut offsets = Vec::with_capacity(sorted_parts.len());
            let mut current_offset: u64 = 0;
            for part in &sorted_parts {
                let start = current_offset;
                let end = current_offset + part.size - 1;
                offsets.push((part.part_number, start, end));
                current_offset += part.size;
            }
            offsets
        };

        // Invalidate existing cache entries before creating new object metadata (Requirements 4.1, 4.2, 4.3, 5.1, 5.2)
        if let Some(cache_mgr) = &self.cache_manager {
            if let Err(e) = cache_mgr
                .invalidate_cache_unified_for_operation(cache_key, "CompleteMultipartUpload")
                .await
            {
                warn!(
                    "Failed to invalidate cache during CompleteMultipartUpload: cache_key={}, upload_id={}, error={}",
                    cache_key, upload_id, e
                );
                // Continue with operation - don't fail CompleteMultipartUpload due to cache invalidation failure
            } else {
                debug!(
                    "Successfully invalidated cache for CompleteMultipartUpload: cache_key={}, upload_id={}",
                    cache_key, upload_id
                );
            }
        }

        // Rename part files with final byte offsets and create range specs
        let mut range_specs = Vec::new();

        for (part_number, start, end) in &byte_offsets {
            // Find the part info
            let part_info = sorted_parts
                .iter()
                .find(|p| p.part_number == *part_number)
                .ok_or_else(|| {
                    ProxyError::CacheError(format!("Part {} not found in tracker", part_number))
                })?;

            // Part file is in the upload directory
            let old_part_file = multipart_dir.join(format!("part{}.bin", part_number));

            // New range file path (with byte offset suffix) in the sharded ranges directory
            let suffix = format!("_{}-{}.bin", start, end);
            let new_range_file_path =
                crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, &suffix).map_err(
                    |e| {
                        ProxyError::CacheError(format!(
                            "Failed to get sharded path for part {}: {}",
                            part_number, e
                        ))
                    },
                )?;

            // Ensure parent directories exist for the destination
            if let Some(parent) = new_range_file_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    ProxyError::CacheError(format!(
                        "Failed to create parent directories for range file: {}",
                        e
                    ))
                })?;
            }

            // Move part file from upload dir to final range location
            if old_part_file.exists() {
                tokio::fs::rename(&old_part_file, &new_range_file_path)
                    .await
                    .map_err(|e| {
                        ProxyError::CacheError(format!(
                            "Failed to move part file to range file: {}",
                            e
                        ))
                    })?;

                debug!(
                    "Moved part file: {:?} -> {:?}",
                    old_part_file, new_range_file_path
                );
            } else {
                warn!(
                    "Part file not found during finalization: {:?}",
                    old_part_file
                );
            }

            // Calculate relative path from ranges directory
            let range_file_relative_path = new_range_file_path
                .strip_prefix(&ranges_dir)
                .map_err(|e| {
                    ProxyError::CacheError(format!(
                        "Failed to compute relative path for part {}: {}",
                        part_number, e
                    ))
                })?
                .to_string_lossy()
                .to_string();

            // Get file size for compression info (the part file was already framed
            // by the streaming part sink when the part was uploaded)
            let compressed_size = tokio::fs::metadata(&new_range_file_path)
                .await
                .map(|m| m.len())
                .unwrap_or(part_info.size);

            // Create range spec using the actual compression algorithm recorded
            // for the part when it was written
            let range_spec = RangeSpec::new(
                *start,
                *end,
                range_file_relative_path,
                part_info.compression_algorithm.clone(), // actual algorithm recorded at part write
                compressed_size,
                part_info.size,
            );

            range_specs.push(range_spec);

            debug!(
                "Created range for part {}: start={}, end={}, size={} bytes",
                part_number, start, end, part_info.size
            );
        }

        // Cross-check the assembled part count against what S3 itself reports
        // (Requirement 5.3).
        //
        // Offsets and `content_length` below are derived purely by summing the
        // tracker's part sizes and are otherwise never compared with S3, so a
        // tracker holding a subset yields self-consistent metadata describing a
        // SHORTER object — and a later GET then serves the wrong length and the
        // wrong bytes. The guards above make that unreachable, but they are
        // guards on our own bookkeeping; this is the one check anchored to S3.
        //
        // A multipart ETag has the form `"<md5-of-md5s>-<part-count>"`, and that
        // suffix is the only size-related fact S3 returns here — the
        // CompleteMultipartUpload response carries no object length, so a byte
        // comparison would need an extra HEAD on a client-visible path. The count
        // is enough to catch the truncation signature, which is a missing part.
        if let Some((_, s3_part_count)) = etag.trim_matches('"').rsplit_once('-') {
            if let Ok(s3_part_count) = s3_part_count.parse::<usize>() {
                if s3_part_count != sorted_parts.len() {
                    warn!(
                        "CompleteMultipartUpload part-count disagrees with S3: S3 reports {} part(s) via the ETag, we assembled {}: cache_key={}, upload_id={}, skipping cache finalization to avoid caching a truncated object",
                        s3_part_count,
                        sorted_parts.len(),
                        cache_key,
                        upload_id
                    );
                    drop(lock_file);
                    return Ok(());
                }
            }
        }

        // Calculate total size from filtered parts (Requirements 5.3)
        // This ensures the object size matches what S3 returns (only requested parts)
        let total_size: u64 = sorted_parts.iter().map(|p| p.size).sum();
        let now = std::time::SystemTime::now();
        let write_ttl = std::time::Duration::from_secs(86400); // 1 day default

        // Build part_ranges from byte_offsets (Requirements 7.1, 7.2)
        // Maps part number to (start_offset, end_offset) byte range
        let part_ranges: std::collections::HashMap<u32, (u64, u64)> = byte_offsets
            .iter()
            .map(|(part_number, start, end)| (*part_number, (*start, *end)))
            .collect();

        // Create object metadata with write cache fields (Requirements 3.1, 3.3, 3.4)
        // Note: S3 CompleteMultipartUpload doesn't return Last-Modified or the object's Content-Type
        // The XML response has content-type: application/xml which is NOT the object's content-type
        // Use content-type from CreateMultipartUpload request if provided, otherwise leave None
        // (will be learned from first GET/HEAD request to S3)

        // Filter out content-type from response headers - it's the XML response type, not the object type
        // Also filter out content-length as it's the XML response length, not the object size
        let filtered_response_headers: std::collections::HashMap<String, String> = response_headers
            .iter()
            .filter(|(k, _)| {
                let key_lower = k.to_lowercase();
                key_lower != "content-type" && key_lower != "content-length"
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let object_metadata = ObjectMetadata {
            etag: etag.to_string(),
            last_modified: String::new(),
            content_length: total_size,
            content_type: tracker.content_type.clone(), // Use content-type from CreateMultipartUpload if provided
            response_headers: filtered_response_headers,
            upload_state: UploadState::Complete,
            cumulative_size: total_size,
            parts: Vec::new(),
            compression_algorithm: CompressionAlgorithm::Lz4, // Multipart uses per-range compression
            compressed_size: range_specs.iter().map(|r| r.compressed_size).sum(),
            parts_count: Some(sorted_parts.len() as u32), // Use filtered parts count (Requirement 7.3)
            part_ranges,
            upload_id: None,
            is_write_cached: true, // Mark as write-cached
            write_cache_expires_at: Some(now + write_ttl),
            write_cache_created_at: Some(now),
            write_cache_last_accessed: Some(now),
        };

        // Delete old range files if this is overwriting an existing object
        // This prevents disk space leaks and stale data issues
        let metadata_dir = self.cache_dir.join("metadata");

        // Use sharded path for metadata file to match the rest of the codebase
        // Path format: metadata/{bucket}/{XX}/{YYY}/{sanitized_key}.meta
        let metadata_file = crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta")
            .map_err(|e| {
                ProxyError::CacheError(format!(
                    "Failed to get sharded metadata path for cache_key={}: {}",
                    cache_key, e
                ))
            })?;

        if metadata_file.exists() {
            // Read old metadata to get list of old range files
            if let Ok(old_metadata_content) = tokio::fs::read_to_string(&metadata_file).await {
                if let Ok(old_metadata) =
                    serde_json::from_str::<NewCacheMetadata>(&old_metadata_content)
                {
                    info!(
                        "Deleting {} old range files for overwritten object: cache_key={}",
                        old_metadata.ranges.len(),
                        cache_key
                    );

                    // Delete each old range file
                    for old_range in &old_metadata.ranges {
                        let old_range_file = ranges_dir.join(&old_range.file_path);
                        if let Err(e) = tokio::fs::remove_file(&old_range_file).await {
                            warn!(
                                "Failed to delete old range file: file={}, error={}",
                                old_range.file_path, e
                            );
                        } else {
                            debug!("Deleted old range file: {}", old_range.file_path);
                        }
                    }
                }
            }
        }

        let cache_metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata,
            ranges: range_specs,
            created_at: now,
            expires_at: now + write_ttl, // Use write cache TTL
            compression_info: crate::cache_types::CompressionInfo::default(),
            ..Default::default()
        };

        // Write metadata file - create parent directories for sharded path
        if let Some(parent) = metadata_file.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ProxyError::CacheError(format!("Failed to create metadata directory: {}", e))
            })?;
        }

        let metadata_json = serde_json::to_string_pretty(&cache_metadata)
            .map_err(|e| ProxyError::CacheError(format!("Failed to serialize metadata: {}", e)))?;

        // Write metadata file atomically using temp file + rename
        let temp_metadata_file = metadata_file.with_extension("tmp");
        tokio::fs::write(&temp_metadata_file, &metadata_json)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to write temporary metadata file: {}", e))
            })?;

        // Atomically rename to final location
        tokio::fs::rename(&temp_metadata_file, &metadata_file)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to rename metadata file: {}", e))
            })?;

        debug!(
            "Created metadata file atomically: {} ({} ranges)",
            metadata_file.display(),
            cache_metadata.ranges.len()
        );

        // Write journal entries for size tracking
        // The metadata file is written directly for atomicity, but we need journal entries
        // so the consolidator can track the size delta for this multipart upload
        if let Some(cache_mgr) = &self.cache_manager {
            if let Some(consolidator) = cache_mgr.get_journal_consolidator().await {
                consolidator
                    .write_multipart_journal_entries(
                        cache_key,
                        cache_metadata.ranges.clone(),
                        cache_metadata.object_metadata.clone(),
                    )
                    .await;
            }
        }

        // Log the final summary with human-readable format
        let (bucket, key) = parse_cache_key(cache_key);
        info!(
            "CompleteMultipartUpload: bucket={}, key={}, parts={}, total_size={}, etag={}, cached=true",
            bucket,
            key,
            cache_metadata.ranges.len(),
            format_size(total_size),
            truncate_etag(etag)
        );

        // Release lock before cleanup
        drop(lock_file);

        // Clean up multipart directory (delete mpus_in_progress/{uploadId}/)
        if let Err(e) = tokio::fs::remove_dir_all(&multipart_dir).await {
            warn!(
                "Failed to clean up multipart directory: upload_id={}, error={}",
                upload_id, e
            );
        } else {
            debug!("Cleaned up multipart directory: upload_id={}", upload_id);
        }

        Ok(())
    }

    /// Wait, up to [`MULTIPART_COMPLETE_CACHE_WAIT`], for every part named by this
    /// `CompleteMultipartUpload` to appear in the on-disk tracker.
    ///
    /// Returns once the tracker holds them all, or once the bound elapses —
    /// never an error. Exceeding the bound is not a failure of the upload (S3
    /// has already accepted it) and not a failure of this request; it degrades to
    /// "not cached", which the caller's existing missing-parts guard then reports.
    ///
    /// The tracker is read WITHOUT taking `upload.lock`. That is deliberate: the
    /// read is a monotonic "have the parts I need landed yet" poll, and taking the
    /// exclusive lock on every attempt would contend with the very part tasks
    /// being waited for and serialise them behind the waiter. A torn or
    /// half-written tracker simply fails to parse and is treated as "not yet",
    /// which is the correct answer for a poll. The authoritative read still
    /// happens under the lock in the caller.
    async fn await_tracker_parts(
        multipart_dir: &std::path::Path,
        cache_key: &str,
        upload_id: &str,
        requested_parts: &[RequestedPart],
    ) {
        let wanted: std::collections::HashSet<u32> =
            requested_parts.iter().map(|p| p.part_number).collect();

        let deadline = std::time::Instant::now() + MULTIPART_COMPLETE_CACHE_WAIT;
        let mut waited_ms: u64 = 0;

        loop {
            // Existence of `part{N}.json` is the whole question, so this checks for
            // files rather than parsing anything. It used to parse the entire
            // `upload.meta` on every poll, which grew with the part count — so the
            // poll itself got more expensive the more parts there were to wait for,
            // while holding up the very Complete that was waiting.
            //
            // ON A BLOCKING THREAD, and that is not incidental. These are synchronous
            // filesystem calls against a shared network volume, in a loop that ticks
            // every 100 ms. Called directly from this async fn they pin a runtime
            // worker for the duration of every poll, and at high part counts a
            // 2,000-entry staging directory on EFS made that long enough to starve
            // the forward path and fail the client's upload — trading the lock
            // contention this design removed for a different way to lose the same way.
            let dir = multipart_dir.to_path_buf();
            let wanted_now = wanted.clone();
            let present = match tokio::task::spawn_blocking(move || {
                Self::present_parts_blocking(&dir, &wanted_now)
            })
            .await
            {
                Ok(present) => present,
                Err(e) => {
                    warn!(
                        "Part-record poll task failed: {} — treating as not yet landed",
                        e
                    );
                    std::collections::HashSet::new()
                }
            };

            if wanted.is_subset(&present) {
                if waited_ms > 0 {
                    debug!(
                        "Multipart Complete waited {}ms for {} part record(s) to land: cache_key={}, upload_id={}",
                        waited_ms,
                        wanted.len(),
                        cache_key,
                        upload_id
                    );
                }
                return;
            }

            if std::time::Instant::now() >= deadline {
                let missing = wanted.len() - wanted.intersection(&present).count();
                warn!(
                    "Multipart Complete waited {:?} and {} of {} part record(s) still had not landed: cache_key={}, upload_id={} — this object will not be cached (upload unaffected)",
                    MULTIPART_COMPLETE_CACHE_WAIT,
                    missing,
                    wanted.len(),
                    cache_key,
                    upload_id
                );
                return;
            }

            tokio::time::sleep(TRACKER_POLL_INTERVAL).await;
            waited_ms += TRACKER_POLL_INTERVAL.as_millis() as u64;
        }
    }

    /// Clean up incomplete multipart cache data
    ///
    /// This method removes partial cache data when CompleteMultipartUpload succeeds on S3
    /// but the proxy doesn't have complete local state. This prevents serving corrupted
    /// data from incomplete cache entries.
    ///
    /// Note the missing-parts path no longer calls this: deleting the staging
    /// directory while part-cache tasks may still be writing into it is what
    /// produced the ENOENT/ESTALE error cascade. Remaining callers are the ones
    /// where the tracker itself is unusable.
    ///
    /// # Arguments
    ///
    /// * `multipart_dir` - Path to the multipart upload directory
    /// * `upload_id` - The upload ID for logging
    async fn cleanup_incomplete_multipart_cache(
        &self,
        multipart_dir: &std::path::Path,
        upload_id: &str,
    ) {
        // Parts are stored inside the upload directory, so a single remove_dir_all cleans everything
        if let Err(e) = tokio::fs::remove_dir_all(multipart_dir).await {
            warn!(
                "Failed to remove multipart directory during incomplete cleanup: upload_id={}, error={}",
                upload_id, e
            );
        } else {
            info!(
                "Cleaned up incomplete multipart cache: upload_id={}",
                upload_id
            );
        }
    }

    /// Sanitize a cache key for safe path construction
    ///
    /// Removes leading slashes to prevent PathBuf::join() from treating
    /// the key as an absolute path, which would replace the cache directory.
    /// Also handles very long paths by hashing them to ensure filesystem
    /// compatibility.
    ///
    /// # Background
    ///
    /// Rust's `PathBuf::join()` has special behavior with absolute paths:
    /// ```rust
    /// use std::path::PathBuf;
    /// let base = PathBuf::from("/var/cache");
    /// let absolute = "/bucket/key";
    /// let result = base.join(absolute);
    /// // result = "/bucket/key" (NOT "/var/cache/bucket/key")
    /// ```
    ///
    /// This function strips leading slashes to ensure paths are always
    /// constructed relative to the cache directory. It also hashes very
    /// long paths to stay within filesystem limits (typically 255 bytes
    /// per path component).
    ///
    /// # Arguments
    ///
    /// * `cache_key` - The raw cache key (e.g., "/bucket/object")
    ///
    /// # Returns
    ///
    /// A sanitized cache key safe for path joining (e.g., "bucket/object")
    ///
    /// # Examples
    ///
    /// ```
    /// # // This is a private method, so we can't test it directly in doctests
    /// # // The functionality is tested in unit tests
    /// ```
    ///
    /// Extract ETag from CompleteMultipartUpload XML response
    ///
    /// S3 returns the ETag in the XML body, not in headers:
    /// ```text
    /// <CompleteMultipartUploadResult>
    ///   <ETag>"etag-value"</ETag>
    ///   ...
    /// </CompleteMultipartUploadResult>
    /// ```
    fn extract_etag_from_xml(xml: &str) -> String {
        debug!(
            "Attempting to extract ETag from XML response (length: {} bytes)",
            xml.len()
        );

        // Simple XML parsing to extract ETag value
        // Look for <ETag>value</ETag> pattern (case-insensitive)
        let xml_lower = xml.to_lowercase();

        if let Some(start_pos) = xml_lower.find("<etag>") {
            let after_tag = &xml[start_pos + 6..]; // Skip "<ETag>" or "<etag>"
            if let Some(end_pos) = after_tag.to_lowercase().find("</etag>") {
                let etag = after_tag[..end_pos].trim();
                // Remove surrounding quotes if present
                let cleaned_etag = etag.trim_matches('"').to_string();
                debug!("Extracted ETag from XML: {}", cleaned_etag);
                return cleaned_etag;
            }
        }

        // Fallback: return empty string if ETag not found
        warn!(
            "Failed to extract ETag from CompleteMultipartUpload response XML. XML content: {}",
            if xml.len() > 500 { &xml[..500] } else { xml }
        );
        String::new()
    }

    /// Spawn a background task to handle cache writing asynchronously
    ///
    /// This function spawns a tokio task that:
    /// 1. Waits for the S3 result
    /// 2. On S3 success, stores data directly as a single range (0 to content-length-1)
    /// 3. Sets is_write_cached=true and write_cache_expires_at in metadata
    /// 4. On S3 failure, discards any cached data
    ///
    /// # Arguments
    ///
    /// * `cache_key` - Cache key (will be sanitized internally)
    /// * `body_data` - The request body data (already read)
    /// * `s3_result_rx` - Channel receiver for S3 operation result
    /// * `cache_dir` - Cache directory path
    /// * `compression_handler` - Compression handler for cache writes
    /// * `content_length` - Optional content length
    /// * `request_headers` - Request headers for metadata extraction
    /// * `metrics` - Optional metrics manager
    /// * `cache_manager` - Optional cache manager for storing as range with write cache metadata
    ///
    /// # Requirements (write-through-cache-finalization)
    ///
    /// - Requirement 1.1: Store object data as single range (0 to content-length-1)
    /// - Requirement 1.2: Create metadata with ETag and Content-Type from S3 response (Last-Modified learned on first cache-miss GET or first HEAD after PUT)
    /// - Requirement 1.3: Set write cache TTL (default: 1 day)
    /// - Requirement 1.5: Return S3 response unchanged to client (handled by caller)
    /// - Requirement 9.1: Don't cache on S3 failure
    #[allow(clippy::too_many_arguments)]
    fn spawn_cache_write_task(
        cache_key: String,
        body_data: Bytes,
        s3_result_rx: tokio::sync::oneshot::Receiver<Result<ResponseInfo>>,
        _cache_dir: PathBuf,
        _compression_handler: CompressionHandler,
        _content_length: Option<u64>,
        request_headers: HashMap<String, String>,
        metrics: Option<Arc<RwLock<MetricsManager>>>,
        cache_manager: Option<Arc<crate::cache::CacheManager>>,
        s3_client: Option<Arc<dyn S3ClientApi + Send + Sync>>,
    ) {
        tokio::spawn(async move {
            // Track streaming start time
            let start_time = std::time::Instant::now();
            let body_len = body_data.len() as u64;

            // Wait for S3 result first (Requirement 9.1: Don't cache on S3 failure)
            match s3_result_rx.await {
                Ok(Ok(response)) => {
                    let status = response.status();

                    if status.is_success() {
                        // S3 success - store as single range with write cache metadata
                        // (Requirements 1.1, 1.2, 1.3)

                        // Extract metadata from S3 response headers
                        let response_headers: HashMap<String, String> = response
                            .headers()
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                            .collect();

                        // Use S3 client's comprehensive header extraction if available
                        let (etag, last_modified, mut comprehensive_headers) =
                            if let Some(s3_client) = &s3_client {
                                let object_metadata = s3_client
                                    .extract_object_metadata_from_response(&response_headers);
                                (
                                    object_metadata.etag,
                                    object_metadata.last_modified,
                                    object_metadata.response_headers,
                                )
                            } else {
                                // Fallback to manual extraction
                                let etag = response_headers
                                    .get("etag")
                                    .or_else(|| response_headers.get("ETag"))
                                    .cloned()
                                    .unwrap_or_default();

                                // S3 PUT responses don't include Last-Modified - leave empty
                                let last_modified = response_headers
                                    .get("last-modified")
                                    .or_else(|| response_headers.get("Last-Modified"))
                                    .cloned()
                                    .unwrap_or_default();

                                (etag, last_modified, response_headers.clone())
                            };

                        // Merge checksum headers from request if not present in response
                        // Always prefer response headers, but include request checksums as fallback
                        for (key, value) in &request_headers {
                            let key_lower = key.to_lowercase();
                            if (key_lower.starts_with("x-amz-checksum-")
                                || key_lower.starts_with("x-amz-content-sha256")
                                || key_lower == "content-md5")
                                && !comprehensive_headers.contains_key(key)
                            {
                                debug!("Adding checksum header from PUT request: {}", key);
                                comprehensive_headers.insert(key.clone(), value.clone());
                            }
                        }

                        // Get Content-Type from request headers (S3 echoes what was sent)
                        let content_type = request_headers
                            .get("content-type")
                            .or_else(|| request_headers.get("Content-Type"))
                            .cloned();

                        // Store directly as range using CacheManager
                        // (Requirements 3.1, 3.2, 3.3 - unified storage only)
                        if let Some(cache_mgr) = &cache_manager {
                            // Invalidate existing cache entries first
                            if let Err(e) = cache_mgr
                                .invalidate_cache_unified_for_operation(&cache_key, "PUT")
                                .await
                            {
                                warn!(
                                    "Failed to invalidate cache before PUT caching: cache_key={}, error={}",
                                    cache_key, e
                                );
                            }

                            // Store as single range with write cache metadata
                            // (Requirements 1.1, 1.2, 1.3)
                            match cache_mgr
                                .store_put_as_write_cached_range(
                                    &cache_key,
                                    &body_data,
                                    etag.clone(),
                                    last_modified.clone(),
                                    content_type.clone(),
                                    comprehensive_headers.clone(),
                                )
                                .await
                            {
                                Ok(()) => {
                                    let streaming_duration_ms =
                                        start_time.elapsed().as_millis() as u64;
                                    // Get the effective PUT TTL for logging - Requirement 11.1
                                    let effective_ttl =
                                        cache_mgr.get_effective_put_ttl(&cache_key).await;

                                    info!(
                                        "Successfully stored PUT as write-cached range: cache_key={}, size={} bytes, etag={}, ttl={:?}",
                                        cache_key, body_len, etag, effective_ttl
                                    );

                                    // Record successful cache
                                    if let Some(m) = &metrics {
                                        m.read()
                                            .await
                                            .record_cached_put(body_len, streaming_duration_ms)
                                            .await;
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to store PUT as write-cached range: cache_key={}, error={}",
                                        cache_key, e
                                    );
                                    // Record cache failure
                                    if let Some(m) = &metrics {
                                        m.read().await.record_put_cache_failure().await;
                                    }
                                }
                            }
                        } else {
                            // No cache_manager available - cannot cache without unified storage
                            warn!(
                                "Cannot cache PUT: no cache_manager available for unified storage: cache_key={}",
                                cache_key
                            );
                            if let Some(m) = &metrics {
                                m.read().await.record_put_cache_failure().await;
                            }
                        }
                    } else {
                        // S3 returned error - don't cache (Requirement 9.1)
                        debug!(
                            "S3 error response, not caching PUT: cache_key={}, status={}",
                            cache_key, status
                        );
                    }
                }
                Ok(Err(e)) => {
                    // S3 failure - don't cache (Requirement 9.1)
                    debug!(
                        "S3 error, not caching PUT: cache_key={}, error={}",
                        cache_key, e
                    );
                }
                Err(e) => {
                    // Channel closed unexpectedly - don't cache
                    // Expected on a client disconnect; counted by the metric
                    // below rather than shouted about in the log (Req 6.7).
                    debug!(
                        "S3 result channel closed unexpectedly: cache_key={}, error={:?}",
                        cache_key, e
                    );
                    if let Some(m) = &metrics {
                        m.read().await.record_put_cache_failure().await;
                    }
                }
            }
        });
    }

    /// Spawn the streaming write-cache task that consumes the bounded tee channel
    /// fed by `forward_signed_request_streaming` and writes the cached object
    /// incrementally through a [`crate::cache::WriteCacheRangeSink`]
    /// (Component 4 of the streaming-write-path design).
    ///
    /// This is the streaming analog of [`Self::spawn_cache_write_task`]: instead
    /// of receiving a fully-buffered decoded body, it drains `tee_rx`
    /// frame-by-frame as the body streams to the upstream, decoding aws-chunked
    /// incrementally on the cache branch only (the upstream always receives the
    /// original bytes verbatim — Req 4). Per-request cache memory stays bounded by
    /// one in-flight frame plus the channel capacity (Req 1, 2).
    ///
    /// Cache-failure isolation (Req 7): every skip/error path discards the sink
    /// and closes the tee receiver, so the forward loop drops the tee and keeps
    /// streaming verbatim — a cache problem never fails an upload S3 would accept.
    ///
    /// The fire-and-forget return value is intentionally ignored; tests call
    /// [`Self::run_streaming_cache_write`] directly to observe the
    /// [`StreamingCacheOutcome`].
    //
    // Wired into the single-part PUT path via `SignedPutHandler::setup_put_cache_tee`
    // (task 5.1); `handle_upload_part` adopts it in task 6.1.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_streaming_cache_write_task(
        cache_key: String,
        sink: crate::cache::WriteCacheRangeSink,
        tee_rx: tokio::sync::mpsc::Receiver<Bytes>,
        s3_result_rx: tokio::sync::oneshot::Receiver<Result<ResponseInfo>>,
        is_aws_chunked: bool,
        expected_decoded_len: Option<u64>,
        ttl: std::time::Duration,
        request_headers: HashMap<String, String>,
        metrics: Option<Arc<RwLock<MetricsManager>>>,
        cache_manager: Option<Arc<crate::cache::CacheManager>>,
        s3_client: Option<Arc<dyn S3ClientApi + Send + Sync>>,
    ) {
        tokio::spawn(async move {
            let _ = Self::run_streaming_cache_write(
                cache_key,
                sink,
                tee_rx,
                s3_result_rx,
                is_aws_chunked,
                expected_decoded_len,
                ttl,
                request_headers,
                metrics,
                cache_manager,
                s3_client,
            )
            .await;
        });
    }

    /// Drive the streaming write-cache pipeline to completion (the receiver side
    /// of the streaming forward's bounded tee channel).
    ///
    /// Phases:
    /// 1. Drain `tee_rx` as frames arrive. Non-chunked frames are object bytes and
    ///    are written straight to the sink; aws-chunked frames are `push`-ed
    ///    through an [`aws_chunked_decoder::IncrementalAwsChunkedDecoder`] and the
    ///    decoded bytes are written to the sink. A decode or sink-write error skips
    ///    caching (discard) without failing the upload (Req 3.4, 7.1, 7.2).
    /// 2. Once the channel closes (the forward loop finished streaming the body),
    ///    await the S3 result via the existing oneshot result channel.
    /// 3. On S3 success, finish the decoder (chunked case) and validate the decoded
    ///    length against `x-amz-decoded-content-length` when present; on mismatch,
    ///    discard the sink and skip caching (Req 3.4).
    /// 4. Build the write-cache [`crate::cache_types::ObjectMetadata`] from the S3
    ///    response (etag / last-modified / content-type / checksum headers) and
    ///    `commit` the sink with the resolved TTL.
    ///
    /// On any S3 failure / non-success / skip, the sink is discarded (its `.tmp`
    /// is cleaned up) and nothing else happens — the upload already streamed
    /// verbatim and its response is returned by the forward loop, untouched.
    //
    // Wired into the single-part PUT path (task 5.1); also exercised directly by
    // the streaming write-cache task tests below.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_streaming_cache_write(
        cache_key: String,
        sink: crate::cache::WriteCacheRangeSink,
        mut tee_rx: tokio::sync::mpsc::Receiver<Bytes>,
        s3_result_rx: tokio::sync::oneshot::Receiver<Result<ResponseInfo>>,
        is_aws_chunked: bool,
        expected_decoded_len: Option<u64>,
        ttl: std::time::Duration,
        request_headers: HashMap<String, String>,
        metrics: Option<Arc<RwLock<MetricsManager>>>,
        cache_manager: Option<Arc<crate::cache::CacheManager>>,
        s3_client: Option<Arc<dyn S3ClientApi + Send + Sync>>,
    ) -> StreamingCacheOutcome {
        let start_time = std::time::Instant::now();

        // The cache branch decodes aws-chunked incrementally; the upstream leg
        // (the forward loop) always receives the original bytes verbatim.
        let decoder = if is_aws_chunked {
            Some(aws_chunked_decoder::IncrementalAwsChunkedDecoder::new())
        } else {
            None
        };
        // ---- Phase 1: drain the bounded tee channel, decode, write to sink ----
        //
        // `sink.write` performs BLOCKING std::fs I/O (LZ4 compress + `File::write_all`
        // + flush). Running it inline on the async worker pins a Tokio worker thread
        // for the entire upload; on a 2-worker runtime (the default on a 2-vCPU host)
        // two concurrent large PUTs starve the whole runtime — including the /health
        // task and the shutdown handler. We therefore drain the channel and do the
        // blocking writes on a dedicated blocking thread via `blocking_recv`, the same
        // way the rest of the proxy offloads blocking cache I/O. The async worker is
        // free to poll other tasks while this thread blocks on EFS writeback.
        // One-shot return from the drain task (once per upload, not a per-frame hot
        // path), so the size gap between `Continue` and `Skip` is immaterial; boxing
        // the sink would only add a needless allocation.
        #[allow(clippy::large_enum_variant)]
        enum DrainOutcome {
            Continue {
                sink: crate::cache::WriteCacheRangeSink,
                decoder: Option<aws_chunked_decoder::IncrementalAwsChunkedDecoder>,
                decoded_written: u64,
            },
            Skip {
                reason: &'static str,
                record_failure: bool,
            },
        }

        let drain_key = cache_key.clone();
        let drain = tokio::task::spawn_blocking(move || {
            let mut sink = sink;
            let mut decoder = decoder;
            let mut decoded_written: u64 = 0;
            while let Some(frame) = tee_rx.blocking_recv() {
                let write_result = match decoder.as_mut() {
                    Some(dec) => match dec.push(frame.as_ref()) {
                        Ok(decoded) => {
                            let n = decoded.len() as u64;
                            sink.write(&decoded).map(|()| n)
                        }
                        Err(e) => {
                            // aws-chunked decode error → skip caching, keep forwarding
                            // (Req 3.4, 7.2). Not a recorded failure: mirrors the
                            // buffered handler, which logs and bypasses on decode error.
                            warn!(
                                "Streaming cache: aws-chunked decode error, skipping cache (upload unaffected): cache_key={}, error={}",
                                drain_key, e
                            );
                            sink.discard();
                            // Close the receiver so the forward loop drops the tee and
                            // keeps streaming verbatim (no deadlock on a full channel).
                            tee_rx.close();
                            return DrainOutcome::Skip {
                                reason: "decode_error",
                                record_failure: false,
                            };
                        }
                    },
                    None => {
                        let n = frame.len() as u64;
                        sink.write(frame.as_ref()).map(|()| n)
                    }
                };

                match write_result {
                    Ok(n) => decoded_written += n,
                    Err(e) => {
                        // Cache write error (disk full, etc.) → skip caching, keep
                        // forwarding (Req 7.1).
                        warn!(
                            "Streaming cache: sink write error, skipping cache (upload unaffected): cache_key={}, error={}",
                            drain_key, e
                        );
                        sink.discard();
                        tee_rx.close();
                        return DrainOutcome::Skip {
                            reason: "cache_write_error",
                            record_failure: true,
                        };
                    }
                }
            }
            DrainOutcome::Continue {
                sink,
                decoder,
                decoded_written,
            }
        })
        .await
        .unwrap_or_else(|join_err| {
            error!(
                "Streaming cache: drain task panicked, skipping cache (upload unaffected): cache_key={}, error={}",
                cache_key, join_err
            );
            DrainOutcome::Skip {
                reason: "drain_task_panic",
                record_failure: true,
            }
        });

        let (sink, decoder, decoded_written) = match drain {
            DrainOutcome::Continue {
                sink,
                decoder,
                decoded_written,
            } => (sink, decoder, decoded_written),
            DrainOutcome::Skip {
                reason,
                record_failure,
            } => {
                if record_failure {
                    if let Some(m) = &metrics {
                        m.read().await.record_put_cache_failure().await;
                    }
                }
                return StreamingCacheOutcome::Skipped(reason);
            }
        };

        // ---- Phase 2: channel closed (body fully streamed). Await S3 result ----
        let response = match s3_result_rx.await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => {
                // S3 failure → don't cache (Req 7.1). The upload already returned
                // the S3 error to the client via the forward loop.
                debug!(
                    "Streaming cache: S3 error, not caching: cache_key={}, error={}",
                    cache_key, e
                );
                sink.discard();
                return StreamingCacheOutcome::Skipped("s3_error");
            }
            Err(e) => {
                // The forward loop dropped the result sender without sending — treat
                // as a cache failure but never touch the upload.
                // Expected on a client disconnect; counted by the metric below.
                debug!(
                    "Streaming cache: S3 result channel closed unexpectedly: cache_key={}, error={:?}",
                    cache_key, e
                );
                sink.discard();
                if let Some(m) = &metrics {
                    m.read().await.record_put_cache_failure().await;
                }
                return StreamingCacheOutcome::Skipped("s3_channel_closed");
            }
        };

        if !response.status().is_success() {
            // S3 returned an error status → don't cache (Req 7.1).
            debug!(
                "Streaming cache: S3 non-success status, not caching: cache_key={}, status={}",
                cache_key,
                response.status()
            );
            sink.discard();
            return StreamingCacheOutcome::Skipped("s3_non_success");
        }

        // ---- Phase 3 (chunked only): finish + decoded-length validation ----
        if let Some(dec) = decoder {
            match dec.finish() {
                Ok(trailers) => {
                    if let Some(expected) = expected_decoded_len {
                        if trailers.decoded_len != expected {
                            // Decoded length disagrees with x-amz-decoded-content-length:
                            // skip caching, do NOT reject the request, and let the
                            // original bytes (already forwarded verbatim) stand — S3
                            // remains the content-length authority (Req 3.4).
                            warn!(
                                "Streaming cache: decoded-length mismatch, skipping cache (upload unaffected): cache_key={}, expected={}, actual={}",
                                cache_key, expected, trailers.decoded_len
                            );
                            sink.discard();
                            return StreamingCacheOutcome::Skipped("decoded_length_mismatch");
                        }
                    }
                }
                Err(e) => {
                    // Body framing was incomplete at end-of-stream → skip caching.
                    warn!(
                        "Streaming cache: aws-chunked framing incomplete at finish, skipping cache (upload unaffected): cache_key={}, error={}",
                        cache_key, e
                    );
                    sink.discard();
                    return StreamingCacheOutcome::Skipped("decode_finish_error");
                }
            }
        }

        // ---- Phase 4: finalize the range bytes, then write `.meta` immediately ----
        //
        // Read-after-write parity — an immediate GET after a PUT must hit: the buffered
        // write-cache path (`store_put_as_write_cached_range_with_ttl`) finalizes the
        // `.bin` and then writes the `.meta` synchronously via `store_new_metadata`,
        // so an immediate post-PUT GET is a cache hit. `WriteCacheRangeSink::commit`
        // is journal-only — it defers the `.meta` until consolidation, which would
        // make that GET a miss. We therefore mirror the buffered path here:
        // `sink.finalize()` (publish `.bin`) → build metadata → store `.meta` now via
        // `CacheManager::store_streamed_write_cache_metadata`.
        let object_metadata = Self::build_streaming_write_cache_metadata(
            decoded_written,
            &response,
            &request_headers,
            &s3_client,
            ttl,
        );

        // Invalidate any existing cache entry for this key BEFORE publishing the new
        // `.bin`, so invalidation cannot delete the range file we are about to
        // finalize. Mirrors the buffered write-cache path's PUT invalidation so a
        // re-PUT replaces stale ranges/metadata.
        if let Some(cache_mgr) = &cache_manager {
            if let Err(e) = cache_mgr
                .invalidate_cache_unified_for_operation(&cache_key, "PUT")
                .await
            {
                warn!(
                    "Streaming cache: failed to invalidate before commit: cache_key={}, error={}",
                    cache_key, e
                );
            }
        }

        // Finalize the bytes (flush residual batch, validate length, publish the
        // `.bin`). This is blocking std::fs (flush + rename), so run it on a blocking
        // thread rather than the async worker. The sink — and its capacity
        // reservation — is moved in and returned so it stays alive until the explicit
        // `drop(sink)` below, after the `.meta` write, matching the buffered path's
        // reservation lifetime.
        let (sink, finalize_res) = match tokio::task::spawn_blocking(move || {
            let mut sink = sink;
            let res = sink.finalize();
            (sink, res)
        })
        .await
        {
            Ok(pair) => pair,
            Err(join_err) => {
                error!(
                    "Streaming cache: finalize task panicked, object not cached (upload unaffected): cache_key={}, error={}",
                    cache_key, join_err
                );
                if let Some(m) = &metrics {
                    m.read().await.record_put_cache_failure().await;
                }
                return StreamingCacheOutcome::Skipped("commit_error");
            }
        };
        let range_spec = match finalize_res {
            Ok(range_spec) => range_spec,
            Err(e) => {
                // Finalize failed (disk error, length mismatch, etc.). The writer was
                // consumed and its `.tmp` cleaned up; the upload is unaffected
                // (Req 7.1).
                error!(
                    "Streaming cache: finalize failed, object not cached (upload unaffected): cache_key={}, error={}",
                    cache_key, e
                );
                if let Some(m) = &metrics {
                    m.read().await.record_put_cache_failure().await;
                }
                return StreamingCacheOutcome::Skipped("commit_error");
            }
        };

        let outcome = match &cache_manager {
            Some(cache_mgr) => {
                match cache_mgr
                    .store_streamed_write_cache_metadata(
                        &cache_key,
                        range_spec,
                        object_metadata,
                        ttl,
                    )
                    .await
                {
                    Ok(()) => {
                        let duration_ms = start_time.elapsed().as_millis() as u64;
                        info!(
                            "Streaming cache: committed write-cached range (.meta written immediately): cache_key={}, size={} bytes, ttl={:?}",
                            cache_key, decoded_written, ttl
                        );
                        if let Some(m) = &metrics {
                            m.read()
                                .await
                                .record_cached_put(decoded_written, duration_ms)
                                .await;
                        }
                        StreamingCacheOutcome::Committed
                    }
                    Err(e) => {
                        // Metadata write failed; the `.bin` is published but
                        // unreferenced (a future consolidation/GC reclaims it). The
                        // upload is unaffected (Req 7.1).
                        error!(
                            "Streaming cache: metadata store failed, object not cached (upload unaffected): cache_key={}, error={}",
                            cache_key, e
                        );
                        if let Some(m) = &metrics {
                            m.read().await.record_put_cache_failure().await;
                        }
                        StreamingCacheOutcome::Skipped("commit_error")
                    }
                }
            }
            None => {
                // No cache manager wired (unit tests exercise the sink without a
                // manager): the `.bin` is published, but the `.meta` cannot be
                // written without a manager. The range bytes are committed.
                StreamingCacheOutcome::Committed
            }
        };

        // Drop the sink now — after the `.meta` write — to release the capacity
        // reservation, matching the buffered path's reservation lifetime.
        drop(sink);
        outcome
    }

    /// Build the write-cache [`crate::cache_types::ObjectMetadata`] for a streamed
    /// PUT from the S3 response and request headers, identically to the buffered
    /// cache task: ETag and Last-Modified come from the S3 response (via the S3
    /// client's comprehensive header extraction when available), Content-Type from
    /// the request (S3 echoes what was sent), and request checksum headers are
    /// merged in as a fallback. The write-cache tracking fields are stamped with
    /// `now` + the resolved TTL.
    ///
    /// `compressed_size`/`compression_algorithm` are left at their defaults here:
    /// the true per-range compressed size and algorithm are recorded on the
    /// `RangeSpec` that `commit_incremental_range` derives from the sink and writes
    /// to the journal; the object-level fields are not used for size accounting.
    fn build_streaming_write_cache_metadata(
        content_length: u64,
        response: &ResponseInfo,
        request_headers: &HashMap<String, String>,
        s3_client: &Option<Arc<dyn S3ClientApi + Send + Sync>>,
        ttl: std::time::Duration,
    ) -> crate::cache_types::ObjectMetadata {
        let response_headers: HashMap<String, String> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        let (etag, last_modified, mut comprehensive_headers) = if let Some(s3_client) = s3_client {
            let object_metadata =
                s3_client.extract_object_metadata_from_response(&response_headers);
            (
                object_metadata.etag,
                object_metadata.last_modified,
                object_metadata.response_headers,
            )
        } else {
            let etag = response_headers
                .get("etag")
                .or_else(|| response_headers.get("ETag"))
                .cloned()
                .unwrap_or_default();
            // S3 PUT responses don't include Last-Modified - leave empty.
            let last_modified = response_headers
                .get("last-modified")
                .or_else(|| response_headers.get("Last-Modified"))
                .cloned()
                .unwrap_or_default();
            (etag, last_modified, response_headers.clone())
        };

        // Merge checksum headers from the request if not present in the response.
        for (key, value) in request_headers {
            let key_lower = key.to_lowercase();
            if (key_lower.starts_with("x-amz-checksum-")
                || key_lower.starts_with("x-amz-content-sha256")
                || key_lower == "content-md5")
                && !comprehensive_headers.contains_key(key)
            {
                comprehensive_headers.insert(key.clone(), value.clone());
            }
        }

        let content_type = request_headers
            .get("content-type")
            .or_else(|| request_headers.get("Content-Type"))
            .cloned();

        let now = std::time::SystemTime::now();
        crate::cache_types::ObjectMetadata {
            etag,
            last_modified,
            content_length,
            content_type,
            response_headers: comprehensive_headers,
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: content_length,
            is_write_cached: true,
            write_cache_expires_at: Some(now + ttl),
            write_cache_created_at: Some(now),
            write_cache_last_accessed: Some(now),
            ..Default::default()
        }
    }

    /// Set up the streaming part-cache tee for an `UploadPart`, when caching is
    /// viable. Returns `(tee_sender, s3_result_sender)`:
    ///
    /// - `tee_sender: Some` when a part-staging sink was opened and a background
    ///   [`Self::run_streaming_part_cache_write`] task spawned to consume it; pass
    ///   it to [`forward_signed_request_streaming`].
    /// - `s3_result_sender: Some` whenever that task is waiting for the S3 result;
    ///   the caller sends the `ResponseInfo`/error into it after the forward returns.
    ///
    /// Both `None` means no caching for this part — the body still streams to the
    /// upstream verbatim (Req 7.2). Caching is skipped (no tee) when there is no
    /// cache manager or the part sink cannot be opened. Unlike the single-part PUT
    /// sink, a part is not pre-sized and not write-cache-capacity-reserved; the
    /// handler's `should_cache` decision already gated this call.
    async fn setup_upload_part_cache_tee(
        &self,
        cache_key: &str,
        upload_id: &str,
        part_number: u32,
        is_aws_chunked: bool,
        decoded_len: Option<u64>,
    ) -> (
        Option<tokio::sync::mpsc::Sender<Bytes>>,
        Option<tokio::sync::oneshot::Sender<Result<ResponseInfo>>>,
    ) {
        let cache_manager = match &self.cache_manager {
            Some(cm) => cm.clone(),
            None => return (None, None),
        };

        // Open the part-staging sink. A failed open simply skips caching this part —
        // the body still streams verbatim to the upstream (Req 7.2).
        let sink = match cache_manager
            .open_multipart_part_sink(cache_key, upload_id, part_number)
            .await
        {
            Ok(sink) => sink,
            Err(e) => {
                warn!(
                    "Streaming UploadPart: failed to open part sink, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, error={}",
                    cache_key, upload_id, part_number, e
                );
                return (None, None);
            }
        };

        let (tee_tx, tee_rx) =
            tokio::sync::mpsc::channel::<Bytes>(self.write_cache_tee_channel_depth);
        let (s3_result_tx, s3_result_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        Self::spawn_streaming_part_cache_write_task(
            cache_key.to_string(),
            upload_id.to_string(),
            part_number,
            sink,
            tee_rx,
            s3_result_rx,
            is_aws_chunked,
            decoded_len,
            self.cache_dir.clone(),
            self.metrics_manager.clone(),
        );

        (Some(tee_tx), Some(s3_result_tx))
    }

    /// Spawn the background streaming part-cache task. Fire-and-forget: a cache
    /// problem never fails an upload the upstream would accept (Req 7). Tests call
    /// [`Self::run_streaming_part_cache_write`] directly to observe the outcome.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_streaming_part_cache_write_task(
        cache_key: String,
        upload_id: String,
        part_number: u32,
        sink: crate::cache::MultipartPartSink,
        tee_rx: tokio::sync::mpsc::Receiver<Bytes>,
        s3_result_rx: tokio::sync::oneshot::Receiver<Result<ResponseInfo>>,
        is_aws_chunked: bool,
        expected_decoded_len: Option<u64>,
        cache_dir: PathBuf,
        metrics: Option<Arc<RwLock<MetricsManager>>>,
    ) {
        tokio::spawn(async move {
            let _ = Self::run_streaming_part_cache_write(
                cache_key,
                upload_id,
                part_number,
                sink,
                tee_rx,
                s3_result_rx,
                is_aws_chunked,
                expected_decoded_len,
                cache_dir,
                metrics,
            )
            .await;
        });
    }

    /// Drive the streaming part-cache pipeline (the receiver side of the
    /// `UploadPart` forward's bounded tee channel). Mirrors
    /// [`Self::run_streaming_cache_write`] but stages the bytes as a multipart part
    /// and, on S3 success, finalizes the part and records the `upload.meta` tracker
    /// under `upload.lock` (the per-part correctness gate) instead of committing
    /// object metadata. On any S3 failure / non-success / skip, the staged part is
    /// discarded — the upload already streamed verbatim and its response is returned
    /// by the forward loop, untouched (Req 7).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_streaming_part_cache_write(
        cache_key: String,
        upload_id: String,
        part_number: u32,
        sink: crate::cache::MultipartPartSink,
        mut tee_rx: tokio::sync::mpsc::Receiver<Bytes>,
        s3_result_rx: tokio::sync::oneshot::Receiver<Result<ResponseInfo>>,
        is_aws_chunked: bool,
        expected_decoded_len: Option<u64>,
        cache_dir: PathBuf,
        metrics: Option<Arc<RwLock<MetricsManager>>>,
    ) -> StreamingCacheOutcome {
        // The cache branch decodes aws-chunked incrementally; the upstream leg always
        // receives the original bytes verbatim.
        let decoder = if is_aws_chunked {
            Some(aws_chunked_decoder::IncrementalAwsChunkedDecoder::new())
        } else {
            None
        };

        // ---- Phase 1: drain the bounded tee channel, decode, write to sink ----
        //
        // `sink.write` is BLOCKING std::fs I/O; draining + writing on a blocking
        // thread keeps the async workers free. Inline blocking here can pin a Tokio
        // worker for the whole part upload and wedge a 2-worker runtime (see
        // `run_streaming_cache_write` for the full rationale).
        // One-shot return from the drain task (once per part, not a per-frame hot
        // path), so the size gap between `Continue` and `Skip` is immaterial; boxing
        // the sink would only add a needless allocation.
        #[allow(clippy::large_enum_variant)]
        enum PartDrainOutcome {
            Continue {
                sink: crate::cache::MultipartPartSink,
                decoder: Option<aws_chunked_decoder::IncrementalAwsChunkedDecoder>,
            },
            Skip {
                reason: &'static str,
                record_failure: bool,
            },
        }

        let drain_key = cache_key.clone();
        let drain_upload = upload_id.clone();
        let drain = tokio::task::spawn_blocking(move || {
            let mut sink = sink;
            let mut decoder = decoder;
            while let Some(frame) = tee_rx.blocking_recv() {
                let write_result = match decoder.as_mut() {
                    Some(dec) => match dec.push(frame.as_ref()) {
                        Ok(decoded) => sink.write(&decoded),
                        Err(e) => {
                            // aws-chunked decode error → skip caching, keep forwarding
                            // (Req 3.4, 7.2).
                            warn!(
                                "Streaming part cache: aws-chunked decode error, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, error={}",
                                drain_key, drain_upload, part_number, e
                            );
                            sink.discard();
                            tee_rx.close();
                            return PartDrainOutcome::Skip {
                                reason: "decode_error",
                                record_failure: false,
                            };
                        }
                    },
                    None => sink.write(frame.as_ref()),
                };

                if let Err(e) = write_result {
                    // Cache write error (disk full, etc.) → skip caching, keep
                    // forwarding (Req 7.1).
                    warn!(
                        "Streaming part cache: sink write error, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, error={}",
                        drain_key, drain_upload, part_number, e
                    );
                    sink.discard();
                    tee_rx.close();
                    return PartDrainOutcome::Skip {
                        reason: "cache_write_error",
                        record_failure: true,
                    };
                }
            }
            PartDrainOutcome::Continue { sink, decoder }
        })
        .await
        .unwrap_or_else(|join_err| {
            error!(
                "Streaming part cache: drain task panicked, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, error={}",
                cache_key, upload_id, part_number, join_err
            );
            PartDrainOutcome::Skip {
                reason: "drain_task_panic",
                record_failure: true,
            }
        });

        let (sink, decoder) = match drain {
            PartDrainOutcome::Continue { sink, decoder } => (sink, decoder),
            PartDrainOutcome::Skip {
                reason,
                record_failure,
            } => {
                if record_failure {
                    if let Some(m) = &metrics {
                        m.read().await.record_put_cache_failure().await;
                    }
                }
                return StreamingCacheOutcome::Skipped(reason);
            }
        };

        // ---- Phase 2: channel closed (part body fully streamed). Await S3 result --
        let response = match s3_result_rx.await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => {
                debug!(
                    "Streaming part cache: S3 error, not caching: cache_key={}, upload_id={}, part_number={}, error={}",
                    cache_key, upload_id, part_number, e
                );
                sink.discard();
                return StreamingCacheOutcome::Skipped("s3_error");
            }
            Err(e) => {
                // Expected on a client disconnect; counted by the metric below.
                debug!(
                    "Streaming part cache: S3 result channel closed unexpectedly: cache_key={}, upload_id={}, part_number={}, error={:?}",
                    cache_key, upload_id, part_number, e
                );
                sink.discard();
                if let Some(m) = &metrics {
                    m.read().await.record_put_cache_failure().await;
                }
                return StreamingCacheOutcome::Skipped("s3_channel_closed");
            }
        };

        if !response.status().is_success() {
            debug!(
                "Streaming part cache: S3 non-success status, not caching: cache_key={}, upload_id={}, part_number={}, status={}",
                cache_key, upload_id, part_number, response.status()
            );
            sink.discard();
            return StreamingCacheOutcome::Skipped("s3_non_success");
        }

        // ---- Phase 3 (chunked only): finish + decoded-length validation ----
        if let Some(dec) = decoder {
            match dec.finish() {
                Ok(trailers) => {
                    if let Some(expected) = expected_decoded_len {
                        if trailers.decoded_len != expected {
                            // Decoded length disagrees with x-amz-decoded-content-length:
                            // skip caching, do NOT reject (S3 is the length authority),
                            // original bytes already forwarded verbatim (Req 3.4).
                            warn!(
                                "Streaming part cache: decoded-length mismatch, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, expected={}, actual={}",
                                cache_key, upload_id, part_number, expected, trailers.decoded_len
                            );
                            sink.discard();
                            return StreamingCacheOutcome::Skipped("decoded_length_mismatch");
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Streaming part cache: aws-chunked framing incomplete at finish, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, error={}",
                        cache_key, upload_id, part_number, e
                    );
                    sink.discard();
                    return StreamingCacheOutcome::Skipped("decode_finish_error");
                }
            }
        }

        // ---- Phase 4: extract ETag, finalize the part + record tracker under lock -
        let response_headers_map: HashMap<String, String> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let etag = response_headers_map
            .get("etag")
            .or_else(|| response_headers_map.get("ETag"))
            .cloned()
            .unwrap_or_default();

        match Self::finalize_and_record_cached_part(
            &cache_dir,
            &cache_key,
            &upload_id,
            part_number,
            &etag,
            sink,
        )
        .await
        {
            Ok(size) => {
                let (bucket, key) = parse_cache_key(&cache_key);
                info!(
                    "UploadPart: bucket={}, key={}, part={}, size={}",
                    bucket,
                    key,
                    part_number,
                    format_size(size)
                );
                StreamingCacheOutcome::Committed
            }
            Err(e) => {
                // WARN, not ERROR, matching every sibling cache-skip in this
                // function. The client's upload is unaffected — S3 has already
                // accepted the part — so this is a missed caching opportunity, not
                // a failure. It was ERROR, and during the multipart lifecycle race
                // it produced a per-part flood that read like a disk fault on the
                // shared volume; the level implied an operator action that does not
                // exist.
                warn!(
                    "Streaming part cache: failed to record cached part (upload unaffected): cache_key={}, upload_id={}, part_number={}, error={}",
                    cache_key, upload_id, part_number, e
                );
                if let Some(m) = &metrics {
                    m.read().await.record_put_cache_failure().await;
                }
                StreamingCacheOutcome::Skipped("part_record_error")
            }
        }
    }

    /// Finalize a streamed part and write its `part{N}.json` record, holding
    /// `part{N}.lock` across **both** the part-file publish (atomic `.tmp` →
    /// `part{N}.bin` rename, inside [`crate::cache::MultipartPartSink::finalize`])
    /// AND the record write. This is the same correctness gate as
    /// [`Self::cache_upload_part`]: a racing same-part-number write cannot leave the
    /// on-disk bytes and the tracker ETag out of sync. Returns the part's
    /// uncompressed size for logging.
    async fn finalize_and_record_cached_part(
        cache_dir: &std::path::Path,
        cache_key: &str,
        upload_id: &str,
        part_number: u32,
        etag: &str,
        sink: crate::cache::MultipartPartSink,
    ) -> Result<u64> {
        use crate::cache_types::CachedPartInfo;
        use fs2::FileExt;

        // Bound how many finalizations contend for `upload.lock` at once, so the
        // cache path cannot exhaust the blocking pool and fail the client's upload
        // (see `PART_FINALIZE_SLOTS`). Held for the whole critical section below,
        // including the blocking task, and released on every exit path by `Drop`.
        //
        // `acquire()` only fails if the semaphore is closed, which never happens
        // for a process-lifetime static; treat it as a non-fatal cache skip rather
        // than unwrapping.
        let _slot = match PART_FINALIZE_SLOTS.acquire().await {
            Ok(permit) => permit,
            Err(e) => {
                return Err(ProxyError::CacheError(format!(
                    "part finalize slot unavailable: {}",
                    e
                )))
            }
        };

        let multipart_dir = cache_dir.join("mpus_in_progress").join(upload_id);
        // The sink open already created this directory; ensure it regardless.
        tokio::fs::create_dir_all(&multipart_dir)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to create multipart directory: {}", e))
            })?;

        // PER-PART lock, not per-upload. The invariant being protected is per part
        // (this part's bytes must agree with this part's recorded ETag), so nothing
        // here ever needed to exclude a different part number — and excluding them
        // is what serialised every part of every concurrent upload on one lock and
        // failed client uploads at high part counts. See `record_part_blocking`.
        let lock_file_path = Self::part_lock_path(&multipart_dir, part_number);
        let etag_owned = etag.to_string();
        let cache_key_owned = cache_key.to_string();
        let upload_id_owned = upload_id.to_string();

        // The whole critical section — the blocking `flock`, the blocking
        // `sink.finalize()` (.tmp flush + rename), and the part-record write — runs on
        // a blocking thread under a timeout. The advisory lock is on the shared volume;
        // acquiring it (and holding it across the blocking finalize) on an async worker
        // would pin that worker and, across instances, could wedge a 2-worker runtime.
        // This mirrors the timeout+spawn_blocking lock pattern used elsewhere
        // (`disk_cache.rs`). The flock is held across the part publish AND its record
        // write, preserving the per-part correctness gate.
        let join = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            tokio::task::spawn_blocking(move || -> Result<u64> {
                let lock_file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&lock_file_path)
                    .map_err(|e| {
                        ProxyError::CacheError(format!("Failed to open lock file: {}", e))
                    })?;
                lock_file.lock_exclusive().map_err(|e| {
                    ProxyError::CacheError(format!("Failed to acquire per-part lock: {}", e))
                })?;

                // Publish the staged part bytes (flush residual batch + atomic rename)
                // UNDER the lock, so the on-disk part file and its recorded ETag are
                // updated as one critical section — identical correctness gate to
                // `cache_upload_part`.
                let info = sink.finalize()?;
                let uncompressed_size = info.uncompressed_size;

                let part_info = CachedPartInfo::new(
                    part_number,
                    uncompressed_size,
                    etag_owned,
                    info.compression_algorithm,
                );

                // One small file, written once. Nothing shared is read or rewritten,
                // so this costs the same whether the upload has ten parts or ten
                // thousand.
                Self::record_part_blocking(&multipart_dir, part_number, &part_info).map_err(
                    |e| {
                        ProxyError::CacheError(format!(
                            "{} (cache_key={}, upload_id={}, part={})",
                            e, cache_key_owned, upload_id_owned, part_number
                        ))
                    },
                )?;

                // Release lock (also released on drop / early `?` return).
                drop(lock_file);
                Ok(uncompressed_size)
            }),
        )
        .await;

        match join {
            Ok(Ok(Ok(size))) => Ok(size),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(join_err)) => Err(ProxyError::CacheError(format!(
                "Part finalize task panicked: {}",
                join_err
            ))),
            Err(_elapsed) => Err(ProxyError::CacheError(format!(
                "Timed out (30s) acquiring part{}.lock / finalizing streamed part",
                part_number
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Assemble the tracker from disk the way production does — upload-level fields
    /// from `upload.meta` (synthesised if absent) plus the per-part `part{N}.json`
    /// records.
    ///
    /// Tests used to read `upload.meta` directly and trust its `parts` array. They
    /// cannot any more: parts are no longer written into that file, because rewriting
    /// it once per part was O(n²) and serialised every part of every concurrent upload
    /// on one cross-instance lock. This helper is the supported way to ask "what parts
    /// are recorded", and it goes through the same loader the product does so a test
    /// cannot pass against a layout the product does not read.
    fn tracker_from_disk(
        multipart_dir: &std::path::Path,
        cache_key: &str,
    ) -> crate::cache_types::MultipartUploadTracker {
        // The directory name IS the upload id, so derive it rather than hardcoding
        // one: several tests assert on `tracker.upload_id`, and these staging dirs are
        // often created by staging a part directly, without a
        // `CreateMultipartUpload` to write `upload.meta`.
        let upload_id = multipart_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown-upload");
        SignedPutHandler::load_tracker_blocking(multipart_dir, upload_id, cache_key)
            .expect("tracker should load from per-part records")
    }

    fn create_test_handler(temp_dir: &TempDir) -> SignedPutHandler {
        let compression_handler = CompressionHandler::new(1024, true);
        SignedPutHandler::new(
            temp_dir.path().to_path_buf(),
            compression_handler,
            0,
            10 * 1024 * 1024, // 10MB capacity
            None,
            10 * 1024 * 1024, // 10 MiB max complete body
            5,                // write_cache_tee_channel_depth
        )
    }

    /// Mock upstream: accept one connection, drain the request (headers + exactly
    /// `body_len` body bytes), then reply `200 OK`. Modeled on the identical helper
    /// in `signed_request_proxy.rs`'s own tests.
    async fn accept_and_ok(listener: tokio::net::TcpListener, body_len: usize) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut tmp = [0u8; 65536];
        let header_end = loop {
            let n = sock.read(&mut tmp).await.unwrap();
            if n == 0 {
                return;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let mut body_read = buf.len() - header_end;
        while body_read < body_len {
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
    }

    /// Drive `body` through a real loopback HTTP/1 connection to obtain a genuine
    /// `Request<hyper::body::Incoming>` — `Incoming` has no public constructor from
    /// bytes, so every test needing one goes through a real connection (same pattern
    /// as `signed_request_proxy.rs`'s inline tests).
    /// The returned `oneshot::Sender` is a **connection keep-alive guard and must be
    /// held until the caller has finished reading the returned body.** An `Incoming`
    /// is fed by the `serve_connection` future that produced it; if that future
    /// completes, the body stops yielding. Returning the request from the service
    /// and responding immediately therefore only works while the whole body already
    /// arrived in the first read — true for a few KiB, false for anything large. At
    /// 150 MiB it is a race that loses under parallel test load, and it loses by
    /// hanging in the I/O driver forever rather than failing. The service now parks
    /// on this channel instead, so the connection keeps draining the socket for as
    /// long as the caller needs. Dropping the sender releases the connection.
    async fn incoming_upload_part_request(
        uri_path_and_query: &str,
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
        let addr = listener.local_addr().unwrap();
        let (req_tx, req_rx) = oneshot::channel::<Request<hyper::body::Incoming>>();
        let req_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(req_tx)));
        let (done_tx, done_rx) = oneshot::channel::<()>();
        let done_rx = std::sync::Arc::new(std::sync::Mutex::new(Some(done_rx)));

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);
            let req_tx = req_tx.clone();
            let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                if let Some(tx) = req_tx.lock().unwrap().take() {
                    let _ = tx.send(req);
                }
                // Taken out of the mutex synchronously so no lock is held across the
                // await below.
                let parked = done_rx.lock().unwrap().take();
                async move {
                    if let Some(parked) = parked {
                        let _ = parked.await;
                    }
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                }
            });
            let _ = http1::Builder::new().serve_connection(io, service).await;
        });

        let body_len = body.len();
        let uri_path_and_query = uri_path_and_query.to_string();
        tokio::spawn(async move {
            use hyper_util::client::legacy::Client;
            use hyper_util::rt::TokioExecutor;
            let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
            let uri = format!("http://{}{}", addr, uri_path_and_query);
            let req = Request::builder()
                .method("PUT")
                .uri(&uri)
                .header("content-length", body_len.to_string())
                .body(Full::new(Bytes::from(body)))
                .unwrap();
            let _ = client.request(req).await;
        });

        (req_rx.await.unwrap(), done_tx)
    }

    /// Requirement IMA 10.8: an `UploadPart` on a cache-capacity bypass streams
    /// verbatim to the upstream under S3's 5 GiB limit.
    ///
    /// `should_cache` returns `Bypass` whenever available cache capacity is
    /// exhausted (`current_cache_usage == max_cache_capacity` here). The bypass arm
    /// is forward-only and must use the same internal streamed cap as every other
    /// PUT and UploadPart path.
    #[tokio::test]
    async fn test_upload_part_bypass_arm_streams_to_upstream() {
        let temp_dir = TempDir::new().unwrap();
        let compression_handler = CompressionHandler::new(1024, true);
        // current_cache_usage == max_cache_capacity => available capacity is zero,
        // so should_cache(Some(_ > 0)) always returns Bypass regardless of size.
        let mut handler = SignedPutHandler::new(
            temp_dir.path().to_path_buf(),
            compression_handler,
            10 * 1024 * 1024, // current_cache_usage
            10 * 1024 * 1024, // max_cache_capacity (== current usage: forces Bypass)
            None,
            10 * 1024 * 1024,
            5,
        );

        // 16 MiB crosses several network frames without the 150 MiB allocation that
        // wedged the full suite under parallel load on 2026-08-17.
        let part_size = 16 * 1024 * 1024;
        // Mock upstream S3, sized to the part body so it can drain the request.
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream = tokio::spawn(accept_and_ok(upstream_listener, part_size));

        // `_conn_keepalive` must outlive the `handle_upload_part` call below — see the
        // helper's doc comment. Dropping it early closes the connection feeding the
        // request body and starves the upstream stream.
        let (req, _conn_keepalive) = incoming_upload_part_request(
            "/test-object?uploadId=test-upload-bypass&partNumber=1",
            vec![0xABu8; part_size],
        )
        .await;

        let transport = Arc::new(UpstreamTransport {
            ip: upstream_addr.ip(),
            port: upstream_addr.port(),
            tls: None,
            validated_endpoint: None,
        });

        // Bounded so a regression in the body-feeding path fails this test instead of
        // parking the whole suite in the I/O driver with no output. 0.33s is typical.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            handler.handle_upload_part(
                req,
                "test-bucket/test-object".to_string(),
                "test-bucket.s3.amazonaws.com".to_string(),
                transport,
                "test-upload-bypass".to_string(),
                1,
            ),
        )
        .await
        .expect("handle_upload_part did not finish within 120s — the request body most likely stopped being fed (see incoming_upload_part_request's keep-alive guard)");

        // If the cap is enforced too small, handle_upload_part rejects the body before
        // ever connecting to the mock upstream, so the upstream task never completes.
        // Bound the wait rather than hanging forever on that failure mode.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;

        match &result {
            Ok(response) => {
                assert_eq!(
                    response.status(),
                    StatusCode::OK,
                    "UploadPart on a cache-capacity bypass must stream to the upstream \
                     and succeed under S3's 5 GiB limit"
                );
            }
            Err(ProxyError::RequestBodyTooLarge {
                content_length,
                max_bytes,
            }) => {
                panic!(
                    "UploadPart incorrectly rejected with 413 on the cache-capacity bypass: \
                     content_length={:?}, max_bytes={} (expected the streamed 5 GiB cap)",
                    content_length, max_bytes
                );
            }
            Err(e) => panic!("unexpected error: {}", e),
        }
    }

    #[test]
    fn test_should_cache_with_content_length_fits() {
        let temp_dir = TempDir::new().unwrap();
        let handler = create_test_handler(&temp_dir);

        let decision = handler.should_cache(Some(1024));
        assert_eq!(decision, CacheDecision::Cache);
    }

    #[test]
    fn test_should_cache_with_content_length_exceeds() {
        let temp_dir = TempDir::new().unwrap();
        let handler = create_test_handler(&temp_dir);

        let decision = handler.should_cache(Some(20 * 1024 * 1024)); // 20MB
        match decision {
            CacheDecision::Bypass(_) => {}
            _ => panic!("Expected Bypass decision"),
        }
    }

    #[test]
    fn test_should_cache_without_content_length() {
        let temp_dir = TempDir::new().unwrap();
        let handler = create_test_handler(&temp_dir);

        let decision = handler.should_cache(None);
        assert_eq!(decision, CacheDecision::StreamWithCapacityCheck);
    }

    #[test]
    fn test_parse_upload_part_query() {
        // Valid UploadPart query
        let query = "uploadId=test-upload-123&partNumber=1";
        let result = SignedPutHandler::parse_upload_part_query(query);
        assert_eq!(result, Some(("test-upload-123".to_string(), 1)));

        // Valid with different order
        let query = "partNumber=5&uploadId=another-upload";
        let result = SignedPutHandler::parse_upload_part_query(query);
        assert_eq!(result, Some(("another-upload".to_string(), 5)));

        // Missing partNumber
        let query = "uploadId=test-upload-123";
        let result = SignedPutHandler::parse_upload_part_query(query);
        assert_eq!(result, None);

        // Missing uploadId
        let query = "partNumber=1";
        let result = SignedPutHandler::parse_upload_part_query(query);
        assert_eq!(result, None);

        // Invalid partNumber
        let query = "uploadId=test-upload-123&partNumber=invalid";
        let result = SignedPutHandler::parse_upload_part_query(query);
        assert_eq!(result, None);

        // Empty query
        let query = "";
        let result = SignedPutHandler::parse_upload_part_query(query);
        assert_eq!(result, None);
    }

    #[test]
    fn test_is_complete_multipart_upload() {
        // CompleteMultipartUpload query (has uploadId but no partNumber)
        let query = "uploadId=test-upload-123";
        assert!(SignedPutHandler::is_complete_multipart_upload(query));

        // UploadPart query (has both uploadId and partNumber)
        let query = "uploadId=test-upload-123&partNumber=1";
        assert!(!SignedPutHandler::is_complete_multipart_upload(query));

        // No uploadId
        let query = "partNumber=1";
        assert!(!SignedPutHandler::is_complete_multipart_upload(query));

        // Empty query
        let query = "";
        assert!(!SignedPutHandler::is_complete_multipart_upload(query));
    }

    #[test]
    fn test_is_abort_multipart_upload() {
        // AbortMultipartUpload query (has uploadId but no partNumber)
        let query = "uploadId=test-upload-123";
        assert!(SignedPutHandler::is_abort_multipart_upload(query));

        // UploadPart query (has both uploadId and partNumber)
        let query = "uploadId=test-upload-123&partNumber=1";
        assert!(!SignedPutHandler::is_abort_multipart_upload(query));

        // No uploadId
        let query = "partNumber=1";
        assert!(!SignedPutHandler::is_abort_multipart_upload(query));

        // Empty query
        let query = "";
        assert!(!SignedPutHandler::is_abort_multipart_upload(query));

        // Additional parameters with uploadId (should still be true)
        let query = "uploadId=test-upload-123&other=value";
        assert!(SignedPutHandler::is_abort_multipart_upload(query));
    }

    #[test]
    fn test_extract_upload_id() {
        // Valid uploadId
        let query = "uploadId=test-upload-123&partNumber=1";
        let result = SignedPutHandler::extract_upload_id(query);
        assert_eq!(result, Some("test-upload-123".to_string()));

        // uploadId only
        let query = "uploadId=another-upload";
        let result = SignedPutHandler::extract_upload_id(query);
        assert_eq!(result, Some("another-upload".to_string()));

        // No uploadId
        let query = "partNumber=1";
        let result = SignedPutHandler::extract_upload_id(query);
        assert_eq!(result, None);

        // Empty query
        let query = "";
        let result = SignedPutHandler::extract_upload_id(query);
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_cache_upload_part() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object";
        let upload_id = "test-upload-123";
        let part_number = 1;
        let data = b"test data for part 1";
        let etag = "test-etag-1";

        // Cache the part
        let result = handler
            .cache_upload_part(cache_key, upload_id, part_number, data, etag)
            .await;
        assert!(result.is_ok());

        // Verify upload.meta exists with tracker info
        let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        let tracker = tracker_from_disk(&multipart_dir, cache_key);

        assert_eq!(tracker.upload_id, upload_id);
        assert_eq!(tracker.cache_key, cache_key);
        assert_eq!(tracker.parts.len(), 1);
        assert_eq!(tracker.parts[0].part_number, part_number);
        assert_eq!(tracker.parts[0].size, data.len() as u64);
        assert_eq!(tracker.parts[0].etag, etag);
        assert_eq!(tracker.total_size, data.len() as u64);
    }

    /// Test that concurrent UploadPart calls for the *same* part number on the
    /// *same* upload_id cannot leave the on-disk bytes out of sync with the
    /// tracker's ETag.
    ///
    /// This reproduces the pattern a misbehaving or racing client could produce:
    /// two UploadPart requests for part N overlap in time on a shared cache
    /// volume. Without the lock covering both the file write and the tracker
    /// update, interleaved renames and tracker writes can result in a tracker
    /// that references ETag_A while the on-disk bytes are from upload B.
    ///
    /// Uses two separate SignedPutHandler instances pointing at the same cache
    /// dir — the same shape as two proxy instances sharing an EFS volume, which
    /// is where this race would realistically surface. The buffered
    /// `cache_upload_part` helper drives the same `upload.lock` critical section
    /// that the production streaming part sink's `finalize` uses.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cache_upload_part_concurrent_same_part_keeps_file_and_tracker_consistent() {
        let temp_dir = TempDir::new().unwrap();

        let cache_key = "test-bucket/concurrent-part-object";
        let upload_id = "test-upload-concurrent";
        let part_number = 1u32;

        // Two distinct payloads with distinct ETags — the pairing must hold.
        // Using sizes above the compression threshold (1024) so both paths
        // exercise the same compression code.
        let data_a = vec![b'A'; 4096];
        let etag_a = "\"etag-a-1111111111111111111111\"";
        let data_b = vec![b'B'; 4096];
        let etag_b = "\"etag-b-2222222222222222222222\"";

        // Drive many interleavings. Each iteration creates two fresh handlers
        // on the same cache dir and races them via tokio::join!.
        for iteration in 0..16 {
            // Fresh upload_id each iteration so prior iterations can't affect
            // the outcome via stale state.
            let iter_upload_id = format!("{}-iter-{}", upload_id, iteration);

            let temp_path = temp_dir.path().to_path_buf();
            let key = cache_key.to_string();
            let upload = iter_upload_id.clone();
            let data_a_clone = data_a.clone();
            let etag_a_s = etag_a.to_string();
            let data_b_clone = data_b.clone();
            let etag_b_s = etag_b.to_string();

            let handle_a = tokio::spawn(async move {
                let compression_handler = CompressionHandler::new(1024, true);
                let mut handler = SignedPutHandler::new(
                    temp_path,
                    compression_handler,
                    0,
                    10 * 1024 * 1024,
                    None,
                    10 * 1024 * 1024,
                    5,
                );
                handler
                    .cache_upload_part(&key, &upload, part_number, &data_a_clone, &etag_a_s)
                    .await
            });

            let temp_path_b = temp_dir.path().to_path_buf();
            let key_b = cache_key.to_string();
            let upload_b = iter_upload_id.clone();
            let handle_b = tokio::spawn(async move {
                let compression_handler = CompressionHandler::new(1024, true);
                let mut handler = SignedPutHandler::new(
                    temp_path_b,
                    compression_handler,
                    0,
                    10 * 1024 * 1024,
                    None,
                    10 * 1024 * 1024,
                    5,
                );
                handler
                    .cache_upload_part(&key_b, &upload_b, part_number, &data_b_clone, &etag_b_s)
                    .await
            });

            let (res_a, res_b) = tokio::join!(handle_a, handle_b);
            res_a.expect("task A panicked").expect("upload A failed");
            res_b.expect("task B panicked").expect("upload B failed");

            // Read the final tracker state.
            let multipart_dir = temp_dir
                .path()
                .join("mpus_in_progress")
                .join(&iter_upload_id);
            let tracker = tracker_from_disk(&multipart_dir, cache_key);

            assert_eq!(
                tracker.parts.len(),
                1,
                "iteration {}: tracker should have exactly one entry for the part number",
                iteration
            );
            let tracked_part = &tracker.parts[0];
            assert_eq!(tracked_part.part_number, part_number);

            // Read the on-disk part file. It was LZ4-frame-compressed by the
            // winner's call; decompress and compare to the expected raw bytes
            // that correspond to the tracked ETag. This is the core invariant:
            // whatever ETag the tracker recorded, the on-disk bytes MUST be the
            // bytes that that ETag describes.
            let part_file = multipart_dir.join(format!("part{}.bin", part_number));
            let compressed_bytes =
                std::fs::read(&part_file).expect("part file exists after writes");
            let compression_handler = CompressionHandler::new(1024, true);
            let decompressed = compression_handler
                .decompress_data(&compressed_bytes)
                .expect("part file decompresses (frame checksum verifies)");

            let expected_bytes = if tracked_part.etag == etag_a {
                &data_a
            } else if tracked_part.etag == etag_b {
                &data_b
            } else {
                panic!(
                    "iteration {}: tracker has unexpected etag {:?}",
                    iteration, tracked_part.etag
                );
            };

            assert_eq!(
                decompressed.len(),
                expected_bytes.len(),
                "iteration {}: on-disk decompressed size must match the ETag recorded in the tracker",
                iteration
            );
            assert_eq!(
                &decompressed, expected_bytes,
                "iteration {}: on-disk bytes must match the payload for the tracker's ETag",
                iteration
            );
            assert_eq!(
                tracked_part.size,
                expected_bytes.len() as u64,
                "iteration {}: tracker-recorded size must match payload size",
                iteration
            );
        }
    }

    #[tokio::test]
    async fn test_cache_multiple_upload_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object";
        let upload_id = "test-upload-456";

        // Cache multiple parts
        for part_num in 1..=3 {
            let data = format!("test data for part {}", part_num);
            let etag = format!("test-etag-{}", part_num);

            let result = handler
                .cache_upload_part(cache_key, upload_id, part_num, data.as_bytes(), &etag)
                .await;
            assert!(result.is_ok());
        }

        // Verify upload.meta exists with all parts
        let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        let tracker = tracker_from_disk(&multipart_dir, cache_key);

        assert_eq!(tracker.upload_id, upload_id);
        assert_eq!(tracker.cache_key, cache_key);
        assert_eq!(tracker.parts.len(), 3);

        // Each part has data like "test data for part N" which is 20 bytes
        // Total: 60 bytes
        assert_eq!(tracker.total_size, 60);
    }

    #[tokio::test]
    async fn test_cleanup_multipart_upload() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object";
        let upload_id = "test-upload-cleanup";

        // Cache multiple parts first
        for part_num in 1..=2 {
            let data = format!("test data for part {}", part_num);
            let etag = format!("test-etag-{}", part_num);

            let result = handler
                .cache_upload_part(cache_key, upload_id, part_num, data.as_bytes(), &etag)
                .await;
            assert!(result.is_ok());
        }

        // Verify parts exist before cleanup
        let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        let tracker = tracker_from_disk(&multipart_dir, cache_key);

        let mut part_files_exist = 0;
        for part_info in &tracker.parts {
            let part_file = multipart_dir.join(format!("part{}.bin", part_info.part_number));
            if part_file.exists() {
                part_files_exist += 1;
            }
        }
        assert_eq!(part_files_exist, 2);

        // Cleanup the multipart upload
        let result = handler.cleanup_multipart_upload(upload_id).await;
        assert!(result.is_ok());

        // Verify multipart directory is gone (all parts removed with it)
        assert!(!multipart_dir.exists());
    }

    #[tokio::test]
    async fn test_finalize_multipart_upload_with_missing_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object";
        let upload_id = "test-upload-missing-parts";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache only part 1, but not part 2
        let data1 = b"test data for part 1";
        let etag1 = "test-etag-1";
        let result = handler
            .cache_upload_part(cache_key, upload_id, 1, data1, etag1)
            .await;
        assert!(result.is_ok());

        // Record part 2 WITHOUT creating its data file, simulating a part that another
        // proxy instance recorded but whose bytes this instance cannot see. Written as
        // its own `part2.json` record, which is where part state now lives.
        let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        let part2_info = crate::cache_types::CachedPartInfo {
            part_number: 2,
            size: 20,
            etag: "test-etag-2".to_string(),
            compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
        };
        SignedPutHandler::record_part_blocking(&multipart_dir, 2, &part2_info)
            .expect("record part 2 without its data file");

        // Verify part 1 file exists in upload dir but part 2 doesn't
        let part1_file = multipart_dir.join("part1.bin");
        assert!(part1_file.exists());

        let part2_file = multipart_dir.join("part2.bin");
        assert!(!part2_file.exists());

        // Both parts are in the tracker, so the part-record wait is satisfied and
        // the missing-part guard is reached on the strength of part2.bin being
        // absent from disk — which is the case this test is about. Previously this
        // passed `None`, which derived the requested set from the tracker itself;
        // that path is gone because it also disabled the guards.
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: etag1.to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "test-etag-2".to_string(),
            },
        ];

        // Attempt to finalize - should skip caching and clean up
        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        // Should succeed (not fail the operation)
        assert!(result.is_ok());

        // Verify cleanup occurred
        assert!(!multipart_dir.exists()); // Multipart directory should be gone

        // Verify no object metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(!metadata_file.exists());
    }

    #[tokio::test]
    async fn test_finalize_multipart_upload_with_missing_directory() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object";
        let upload_id = "test-upload-no-directory";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Don't create any multipart directory or parts
        // This simulates CompleteMultipartUpload succeeding on S3 but no local state

        // Attempt to finalize - should skip caching gracefully. The requested part
        // list is irrelevant here: the missing directory is checked first.
        let result = handler
            .finalize_multipart_upload(cache_key, upload_id, etag, &response_headers, &[])
            .await;

        // Should succeed (not fail the operation)
        assert!(result.is_ok());

        // Verify no object metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(!metadata_file.exists());
    }

    // ============================================================================
    // Streamed-part → finalize contract (streaming-write-path Task 6.2)
    //
    // Task 6.1 converted `handle_upload_part` to stream the part body and stage it
    // via the streaming part path (`open_multipart_part_sink` →
    // `MultipartPartSink::write` → `finalize_and_record_cached_part`), recording the
    // part in the SAME `upload.meta` tracker schema (`MultipartUploadTracker` +
    // `CachedPartInfo`) and the SAME `mpus_in_progress/{upload_id}/part{N}.bin`
    // location the buffered `cache_upload_part` uses. These tests prove
    // `finalize_multipart_upload` reads streamed parts identically to buffered
    // parts: retain-on-success, and cleanup (no cache entry) on a missing requested
    // part or an ETag mismatch. The buffered-staging variants of these gates are
    // covered above; these lock in the streamed-staging path end-to-end.
    // ============================================================================

    /// Stage an `UploadPart` body through the **streaming** part path, exactly as
    /// `run_streaming_part_cache_write` does on S3 success: open the part sink,
    /// write the (decoded) object bytes, then `finalize_and_record_cached_part`
    /// under `upload.lock`. Writes `part{N}.bin` and the `upload.meta` tracker in
    /// the same on-disk shape as the buffered `cache_upload_part`.
    async fn stage_streamed_part(
        cache_dir: &std::path::Path,
        cache_key: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
        etag: &str,
    ) {
        let cache_mgr = crate::cache::CacheManager::new(
            cache_dir.to_path_buf(),
            false, // ram_cache_enabled
            0,     // max_ram_cache_size
            100,   // compression_threshold
            false, // compression_enabled
        );
        let mut sink = cache_mgr
            .open_multipart_part_sink(cache_key, upload_id, part_number)
            .await
            .expect("open streaming part sink");
        sink.write(data)
            .expect("write part bytes to streaming sink");
        SignedPutHandler::finalize_and_record_cached_part(
            cache_dir,
            cache_key,
            upload_id,
            part_number,
            etag,
            sink,
        )
        .await
        .expect("finalize + record streamed part");
    }

    /// Streamed parts that satisfy every gate (S3 success + all requested parts
    /// cached + every ETag matches) are retained: the object `.meta` is created
    /// with the correct cumulative byte offsets and the in-progress dir is removed.
    #[tokio::test]
    async fn test_finalize_multipart_upload_streamed_parts_retained_on_success() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();

        let cache_key = "test-bucket/streamed-contiguous";
        let upload_id = "streamed-upload-contiguous";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Stage three parts via the STREAMING path (not cache_upload_part).
        let part1_data = vec![1u8; 1024];
        let part2_data = vec![2u8; 2048];
        let part3_data = vec![3u8; 512];
        stage_streamed_part(cache_dir, cache_key, upload_id, 1, &part1_data, "etag1").await;
        stage_streamed_part(cache_dir, cache_key, upload_id, 2, &part2_data, "etag2").await;
        stage_streamed_part(cache_dir, cache_key, upload_id, 3, &part3_data, "etag3").await;

        // The tracker written by the streaming path must be byte-schema-compatible
        // with what finalize reads.
        let multipart_dir = cache_dir.join("mpus_in_progress").join(upload_id);
        let tracker = tracker_from_disk(&multipart_dir, cache_key);
        assert_eq!(tracker.parts.len(), 3);
        assert_eq!(tracker.total_size, 1024 + 2048 + 512);

        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
            RequestedPart {
                part_number: 3,
                etag: "etag3".to_string(),
            },
        ];

        handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await
            .unwrap();

        // Retain: object metadata created with correct cumulative ranges.
        let metadata_dir = cache_dir.join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(
            metadata_file.exists(),
            "object .meta should be created for fully-cached streamed parts"
        );
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_file).unwrap()).unwrap();
        assert_eq!(metadata.object_metadata.parts_count, Some(3));
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&1),
            Some(&(0, 1023))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&2),
            Some(&(1024, 3071))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&3),
            Some(&(3072, 3583))
        );
        assert_eq!(metadata.object_metadata.content_length, 1024 + 2048 + 512);

        // In-progress dir cleaned up after finalization.
        assert!(
            !multipart_dir.exists(),
            "in-progress dir should be removed after finalization"
        );
    }

    /// A requested part that was never streamed/cached locally fails the
    /// "every requested part cached" gate: finalize writes no cache entry.
    ///
    /// It also no longer DELETES the staging directory, and this test asserts
    /// that survival deliberately. Deleting it here is what produced the
    /// ENOENT/ESTALE error cascade: the parts being declared missing may still
    /// be mid-write, on this instance or another, and `remove_dir_all` pulls
    /// the directory (including `upload.lock`) out from under them. Staging is
    /// now left to the TTL sweep. The assertion is inverted from what it
    /// originally checked, which is the point — the old expectation encoded the
    /// behaviour that caused the cascade.
    ///
    /// Note this test necessarily takes `MULTIPART_COMPLETE_CACHE_WAIT` to run:
    /// part 2 never lands in the tracker, so the bounded wait runs to its
    /// deadline before the missing-part guard is reached. That is the wait doing
    /// its job, not a slow test.
    #[tokio::test]
    async fn test_finalize_multipart_upload_streamed_parts_cleanup_on_missing_part() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();

        let cache_key = "test-bucket/streamed-missing";
        let upload_id = "streamed-upload-missing";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Only part 1 is streamed; part 2 is requested but never cached.
        let part1_data = vec![1u8; 1024];
        stage_streamed_part(cache_dir, cache_key, upload_id, 1, &part1_data, "etag1").await;

        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
        ];

        handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await
            .unwrap();

        // No object metadata: the missing part means nothing is cached.
        let metadata_dir = cache_dir.join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(
            !metadata_file.exists(),
            "no cache entry when a requested streamed part is missing"
        );
        // ...but the staging directory SURVIVES. A part this instance is
        // declaring missing may still be mid-write elsewhere, so deleting the
        // directory it is writing into is what produced the ENOENT/ESTALE
        // cascade. The TTL sweep reaps it instead.
        assert!(
            cache_dir.join("mpus_in_progress").join(upload_id).exists(),
            "staging dir must be left for the TTL sweep, not deleted out from \
             under part tasks that may still be writing into it"
        );
    }

    // =====================================================================
    // Phase 2 — the truncation path (Requirement 5.1, 5.2)
    //
    // Both tests below were written AFTER the fix they cover, which is worth
    // recording rather than hiding. Their red side was recovered by temporarily
    // reverting the two Phase 2 changes. One caveat on that revert: the fix
    // removes the defective argument from the signature, so the reverted call
    // site necessarily differs by that argument and the reproduction is not
    // byte-identical to the original defect.
    // =====================================================================

    /// Requirement 5.1 — a `CompleteMultipartUpload` whose XML does not parse
    /// MUST NOT finalize the cache.
    ///
    /// Two halves, because the requirement spans a parse and a decision:
    ///
    /// (a) the parser reports `Err` for malformed part XML, which is what
    ///     drives the caller's skip branch; and
    /// (b) the requested part list is AUTHORITATIVE — finalizing against an
    ///     empty list writes nothing even though the tracker holds a complete,
    ///     healthy three-part upload.
    ///
    /// Half (b) is the one with teeth. The removed `None` fallback derived the
    /// requested set from the tracker itself, so on a parse failure the filter
    /// became a no-op and every guard downstream of it was vacuous — a
    /// partially recorded upload finalized as a short object. With the list
    /// authoritative, "no parts were requested" can only ever mean "cache
    /// nothing".
    #[tokio::test]
    async fn unparseable_complete_body_does_not_finalize_the_cache() {
        // (a) Malformed part XML is an Err, not a silently empty list. This is
        // the branch that must lead to skipping finalization.
        let malformed = br#"<CompleteMultipartUpload>
            <Part><PartNumber>not-a-number</PartNumber><ETag>"e1"</ETag></Part>
        </CompleteMultipartUpload>"#;
        assert!(
            parse_complete_mpu_request(malformed).is_err(),
            "malformed PartNumber must be reported as a parse error, since that \
             is what makes the caller skip cache finalization"
        );

        // (b) A complete, healthy tracker plus an EMPTY requested list must
        // still cache nothing.
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();

        let cache_key = "test-bucket/unparseable-complete";
        let upload_id = "upload-unparseable-complete";
        let response_headers = std::collections::HashMap::new();

        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            1,
            &vec![1u8; 1024],
            "etag1",
        )
        .await;
        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            2,
            &vec![2u8; 2048],
            "etag2",
        )
        .await;
        stage_streamed_part(cache_dir, cache_key, upload_id, 3, &vec![3u8; 512], "etag3").await;

        handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                "\"abc123-3\"",
                &response_headers,
                &[], // what an unparseable body morally yields: nothing requested
            )
            .await
            .unwrap();

        let metadata_file =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        assert!(
            !metadata_file.exists(),
            "an empty requested-part list must cache nothing, even with a fully \
             populated tracker — otherwise the tracker's own contents are acting \
             as a fallback and every guard downstream of the filter is vacuous"
        );
    }

    /// Requirement 5.2 — a tracker holding a SUBSET of the requested parts MUST
    /// NOT produce object metadata whose `content_length` is the subset sum.
    ///
    /// This is the truncation signature itself. Offsets and `content_length` are
    /// derived purely by summing the tracker's part sizes, so a tracker holding
    /// two parts of a three-part upload yields entirely self-consistent metadata
    /// that describes a SHORTER object — and a later GET then serves the wrong
    /// length and the wrong bytes with a success status.
    ///
    /// The assertion is deliberately stronger than "no `.meta` exists": it also
    /// names the specific wrong value (the 3,072-byte two-part sum against a
    /// 3,584-byte object), so a future change that writes metadata here fails
    /// with a message saying what was truncated rather than just that a file
    /// appeared.
    ///
    /// The requested list is EMPTY rather than naming all three parts, and that
    /// choice is what gives the test a reachable red side. A non-empty list
    /// naming part 3 is caught by the pre-existing missing-parts guard even
    /// without this spec's changes, so it would pass either way. An empty list is
    /// the shape an unparseable Complete body produces, and it is where the
    /// removed "use all cached parts" fallback did its damage: it substituted the
    /// tracker's own two parts for the request's three and wrote metadata for a
    /// 3,072-byte object.
    #[tokio::test]
    async fn tracker_holding_a_subset_does_not_finalize_a_short_object() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();

        let cache_key = "test-bucket/subset-truncation";
        let upload_id = "upload-subset-truncation";
        let response_headers = std::collections::HashMap::new();

        // Only parts 1 and 2 land locally; part 3 never does.
        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            1,
            &vec![1u8; 1024],
            "etag1",
        )
        .await;
        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            2,
            &vec![2u8; 2048],
            "etag2",
        )
        .await;

        // No parts requested, and S3's ETag says the real object has three.
        handler
            .finalize_multipart_upload(cache_key, upload_id, "\"abc123-3\"", &response_headers, &[])
            .await
            .unwrap();

        let metadata_file =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();

        if metadata_file.exists() {
            let metadata: crate::cache_types::NewCacheMetadata =
                serde_json::from_str(&std::fs::read_to_string(&metadata_file).unwrap()).unwrap();
            panic!(
                "a tracker holding 2 of 3 requested parts finalized anyway, \
                 producing metadata for a SHORTER object: content_length={} \
                 (the two-part subset sum) against a real object of 3584 bytes. \
                 A later GET would serve {} bytes with HTTP 200.",
                metadata.object_metadata.content_length, metadata.object_metadata.content_length
            );
        }
    }

    // =====================================================================
    // Phase 3 — the lifecycle race (Phase 1 task 3)
    // =====================================================================

    /// The race this spec exists for: `CompleteMultipartUpload` arrives while
    /// per-part cache tasks are still running, and the object must still be
    /// cached with the FULL part set.
    ///
    /// Shape, mirroring the measured production sequence rather than a
    /// convenient one: part 1 has landed (so the staging directory and tracker
    /// exist, as they do by Complete time in the real flow), parts 2 and 3 are
    /// still in flight on their own tasks, and Complete runs concurrently with
    /// them. Before the fix, Complete read a tracker holding only part 1,
    /// declared 2 and 3 "not cached locally", and cached nothing — that is the
    /// red side, recovered by neutralising `await_tracker_parts`.
    ///
    /// The staggered delays are load-bearing. Sequential staging lets each part
    /// task finish before the next request, so the lag Complete races against
    /// never builds and the test passes with or without the fix — the same trap
    /// recorded for the fleet probe, where sequential `s3api upload-part` calls
    /// could not reproduce the race either.
    #[tokio::test(flavor = "multi_thread")]
    async fn complete_arriving_before_part_tasks_finish_still_caches_every_part() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_key = "test-bucket/complete-races-parts";
        let upload_id = "upload-complete-races-parts";
        let response_headers = std::collections::HashMap::new();

        // Part 1 is already recorded, exactly as in the measured case where
        // Complete arrived 147 ms after the first part was recorded.
        stage_streamed_part(
            &cache_dir,
            cache_key,
            upload_id,
            1,
            &vec![1u8; 1024],
            "etag1",
        )
        .await;

        // Parts 2 and 3 are still in flight, landing after Complete has begun.
        let mut stragglers = Vec::new();
        for (part_number, size, etag, delay_ms) in [
            (2u32, 2048usize, "etag2", 150u64),
            (3u32, 512usize, "etag3", 400u64),
        ] {
            let cache_dir = cache_dir.clone();
            stragglers.push(tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                stage_streamed_part(
                    &cache_dir,
                    cache_key,
                    upload_id,
                    part_number,
                    &vec![part_number as u8; size],
                    etag,
                )
                .await;
            }));
        }

        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
            RequestedPart {
                part_number: 3,
                etag: "etag3".to_string(),
            },
        ];

        handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                "\"abc123-3\"",
                &response_headers,
                &requested_parts,
            )
            .await
            .unwrap();

        for straggler in stragglers {
            straggler.await.expect("straggler part task panicked");
        }

        let metadata_file =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        assert!(
            metadata_file.exists(),
            "Complete raced the still-running part tasks and cached NOTHING. \
             This is the defect: it read a tracker holding only part 1 and \
             declared parts 2 and 3 missing, when both landed milliseconds later."
        );

        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_file).unwrap()).unwrap();
        assert_eq!(
            metadata.object_metadata.parts_count,
            Some(3),
            "the cached object must carry the full part set, not the subset that \
             happened to have landed when Complete looked"
        );
        assert_eq!(
            metadata.object_metadata.content_length,
            1024 + 2048 + 512,
            "cached content_length must be the whole object, not a subset sum"
        );
    }

    /// A streamed part whose tracker ETag disagrees with the requested ETag fails
    /// the "every ETag matches" gate: finalize cleans up and writes no cache entry.
    #[tokio::test]
    async fn test_finalize_multipart_upload_streamed_parts_cleanup_on_etag_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();

        let cache_key = "test-bucket/streamed-etag-mismatch";
        let upload_id = "streamed-upload-etag-mismatch";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Stage part 1 with the ETag S3 returned for the streamed bytes.
        let part1_data = vec![1u8; 1024];
        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            1,
            &part1_data,
            "\"streamed-etag\"",
        )
        .await;

        // The completion request claims a DIFFERENT ETag for part 1.
        let requested_parts = vec![RequestedPart {
            part_number: 1,
            etag: "\"different-etag\"".to_string(),
        }];

        handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await
            .unwrap();

        // Cleanup: no object metadata, in-progress dir removed.
        let metadata_dir = cache_dir.join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(
            !metadata_file.exists(),
            "no cache entry on streamed-part ETag mismatch"
        );
        assert!(
            !cache_dir.join("mpus_in_progress").join(upload_id).exists(),
            "in-progress dir should be cleaned up on ETag mismatch"
        );
    }

    // ============================================================================
    // Unit Tests for CompleteMultipartUpload XML Parsing
    // ============================================================================

    /// Test parsing valid CompleteMultipartUpload XML with multiple parts
    /// Requirements: 4.1, 4.2
    #[test]
    fn test_parse_complete_mpu_request_valid_xml() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
    <ETag>"a54357aff0632cce46d942af68356b38"</ETag>
  </Part>
  <Part>
    <PartNumber>3</PartNumber>
    <ETag>"0c78aef83f66abc1fa1e8477f296d394"</ETag>
  </Part>
</CompleteMultipartUpload>"#;

        let parts = parse_complete_mpu_request(xml).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"a54357aff0632cce46d942af68356b38\"");
        assert_eq!(parts[1].part_number, 3);
        assert_eq!(parts[1].etag, "\"0c78aef83f66abc1fa1e8477f296d394\"");
    }

    /// Test parsing XML with single part
    /// Requirements: 4.1, 4.2
    #[test]
    fn test_parse_complete_mpu_request_single_part() {
        let xml = br#"<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
    <ETag>"abc123"</ETag>
  </Part>
</CompleteMultipartUpload>"#;

        let parts = parse_complete_mpu_request(xml).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"abc123\"");
    }

    /// Test parsing empty body returns empty list
    /// Requirements: 4.3
    #[test]
    fn test_parse_complete_mpu_request_empty_body() {
        let parts = parse_complete_mpu_request(b"").unwrap();
        assert!(parts.is_empty());
    }

    /// Test parsing XML with no Part elements returns empty list
    /// Requirements: 4.3
    #[test]
    fn test_parse_complete_mpu_request_no_parts() {
        let xml = b"<CompleteMultipartUpload></CompleteMultipartUpload>";
        let parts = parse_complete_mpu_request(xml).unwrap();
        assert!(parts.is_empty());
    }

    /// Test parsing malformed XML with missing PartNumber
    /// Requirements: 4.3
    #[test]
    fn test_parse_complete_mpu_request_missing_part_number() {
        let xml = br#"<CompleteMultipartUpload>
  <Part>
    <ETag>"abc123"</ETag>
  </Part>
</CompleteMultipartUpload>"#;

        let result = parse_complete_mpu_request(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProxyError::InvalidRequest(_)));
    }

    /// Test parsing malformed XML with missing ETag
    /// Requirements: 4.3
    #[test]
    fn test_parse_complete_mpu_request_missing_etag() {
        let xml = br#"<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
  </Part>
</CompleteMultipartUpload>"#;

        let result = parse_complete_mpu_request(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProxyError::InvalidRequest(_)));
    }

    /// Test parsing XML with invalid PartNumber (non-numeric)
    /// Requirements: 4.3
    #[test]
    fn test_parse_complete_mpu_request_invalid_part_number() {
        let xml = br#"<CompleteMultipartUpload>
  <Part>
    <PartNumber>abc</PartNumber>
    <ETag>"abc123"</ETag>
  </Part>
</CompleteMultipartUpload>"#;

        let result = parse_complete_mpu_request(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProxyError::InvalidRequest(_)));
    }

    /// Test parsing invalid UTF-8 body
    /// Requirements: 4.3
    #[test]
    fn test_parse_complete_mpu_request_invalid_utf8() {
        let invalid_utf8 = vec![0xFF, 0xFE, 0x00, 0x01];
        let result = parse_complete_mpu_request(&invalid_utf8);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProxyError::InvalidRequest(_)));
    }

    /// Test extract_xml_value helper with valid input
    /// Requirements: 4.2
    #[test]
    fn test_extract_xml_value_valid() {
        let xml = "<PartNumber>42</PartNumber><ETag>\"abc\"</ETag>";
        assert_eq!(extract_xml_value(xml, "PartNumber").unwrap(), "42");
        assert_eq!(extract_xml_value(xml, "ETag").unwrap(), "\"abc\"");
    }

    /// Test extract_xml_value with whitespace around value
    /// Requirements: 4.2
    #[test]
    fn test_extract_xml_value_with_whitespace() {
        let xml = "<PartNumber>  42  </PartNumber>";
        assert_eq!(extract_xml_value(xml, "PartNumber").unwrap(), "42");
    }

    /// Test extract_xml_value with missing tag
    /// Requirements: 4.2
    #[test]
    fn test_extract_xml_value_missing_tag() {
        let xml = "<PartNumber>42</PartNumber>";
        let result = extract_xml_value(xml, "ETag");
        assert!(result.is_err());
    }

    /// Test parsing XML with parts in non-sequential order
    /// Requirements: 4.1, 4.2
    #[test]
    fn test_parse_complete_mpu_request_non_sequential_parts() {
        let xml = br#"<CompleteMultipartUpload>
  <Part>
    <PartNumber>5</PartNumber>
    <ETag>"etag5"</ETag>
  </Part>
  <Part>
    <PartNumber>2</PartNumber>
    <ETag>"etag2"</ETag>
  </Part>
  <Part>
    <PartNumber>8</PartNumber>
    <ETag>"etag8"</ETag>
  </Part>
</CompleteMultipartUpload>"#;

        let parts = parse_complete_mpu_request(xml).unwrap();
        assert_eq!(parts.len(), 3);
        // Parts should be in the order they appear in the XML
        assert_eq!(parts[0].part_number, 5);
        assert_eq!(parts[1].part_number, 2);
        assert_eq!(parts[2].part_number, 8);
    }

    // ============================================================================
    // Unit Tests for ETag Normalization and Validation
    // ============================================================================

    /// Test normalize_etag removes surrounding quotes
    /// Requirements: 9.1
    #[test]
    fn test_normalize_etag_with_quotes() {
        assert_eq!(normalize_etag("\"abc123\""), "abc123");
        assert_eq!(
            normalize_etag("\"a54357aff0632cce46d942af68356b38\""),
            "a54357aff0632cce46d942af68356b38"
        );
    }

    /// Test normalize_etag handles ETags without quotes
    /// Requirements: 9.1
    #[test]
    fn test_normalize_etag_without_quotes() {
        assert_eq!(normalize_etag("abc123"), "abc123");
        assert_eq!(
            normalize_etag("a54357aff0632cce46d942af68356b38"),
            "a54357aff0632cce46d942af68356b38"
        );
    }

    /// Test normalize_etag handles empty string and edge cases
    /// Requirements: 9.1
    #[test]
    fn test_normalize_etag_edge_cases() {
        assert_eq!(normalize_etag(""), "");
        assert_eq!(normalize_etag("\"\""), "");
        assert_eq!(normalize_etag("\""), "");
        assert_eq!(normalize_etag("\"abc"), "abc");
        assert_eq!(normalize_etag("abc\""), "abc");
    }

    /// Test ETag validation skips cache finalization on mismatch
    /// Requirements: 9.1, 9.2, 9.3, 9.4
    #[tokio::test]
    async fn test_finalize_multipart_upload_etag_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-etag-mismatch";
        let upload_id = "test-upload-etag-mismatch";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Create multipart directory and upload.meta
        let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        tokio::fs::create_dir_all(&multipart_dir).await.unwrap();

        // Cache a part with one ETag
        let part_data = vec![0u8; 1024];
        let cached_etag = "\"cached-etag-abc123\"";
        handler
            .cache_upload_part(cache_key, upload_id, 1, &part_data, cached_etag)
            .await
            .unwrap();

        // Create requested parts with a DIFFERENT ETag (mismatch)
        let requested_parts = vec![RequestedPart {
            part_number: 1,
            etag: "\"different-etag-xyz789\"".to_string(),
        }];

        // Attempt to finalize - should skip caching due to ETag mismatch
        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        // Should succeed (not fail the operation - Requirement 9.4)
        assert!(result.is_ok());

        // Verify cleanup occurred (Requirement 9.2)
        assert!(!multipart_dir.exists());

        // Verify no object metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(!metadata_file.exists());
    }

    /// Test ETag validation succeeds when ETags match (with quotes normalization)
    /// Requirements: 9.1, 9.2
    #[tokio::test]
    async fn test_finalize_multipart_upload_etag_match_with_quotes() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-etag-match";
        let upload_id = "test-upload-etag-match";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Create multipart directory
        let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        tokio::fs::create_dir_all(&multipart_dir).await.unwrap();

        // Cache a part with quoted ETag
        let part_data = vec![0u8; 1024];
        let cached_etag = "\"abc123\"";
        handler
            .cache_upload_part(cache_key, upload_id, 1, &part_data, cached_etag)
            .await
            .unwrap();

        // Create requested parts with same ETag (also quoted - should match after normalization)
        let requested_parts = vec![RequestedPart {
            part_number: 1,
            etag: "\"abc123\"".to_string(),
        }];

        // Attempt to finalize - should succeed since ETags match
        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        // Should succeed
        assert!(result.is_ok());

        // Verify metadata was created (cache finalization succeeded)
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists());
    }

    /// Test ETag validation handles mixed quote formats
    /// Requirements: 9.1
    #[tokio::test]
    async fn test_finalize_multipart_upload_etag_match_mixed_quotes() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-etag-mixed";
        let upload_id = "test-upload-etag-mixed";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Create multipart directory
        let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        tokio::fs::create_dir_all(&multipart_dir).await.unwrap();

        // Cache a part with quoted ETag
        let part_data = vec![0u8; 1024];
        let cached_etag = "\"abc123\"";
        handler
            .cache_upload_part(cache_key, upload_id, 1, &part_data, cached_etag)
            .await
            .unwrap();

        // Create requested parts with unquoted ETag (should still match after normalization)
        let requested_parts = vec![RequestedPart {
            part_number: 1,
            etag: "abc123".to_string(),
        }];

        // Attempt to finalize - should succeed since normalized ETags match
        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        // Should succeed
        assert!(result.is_ok());

        // Verify metadata was created (cache finalization succeeded)
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists());
    }

    /// Test unreferenced part cleanup during CompleteMultipartUpload
    /// When parts are cached but not included in the CompleteMultipartUpload request,
    /// they should be deleted from disk.
    /// Requirements: 6.1, 6.2, 6.3, 6.4
    #[tokio::test]
    async fn test_finalize_multipart_upload_deletes_unreferenced_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-unreferenced";
        let upload_id = "test-upload-unreferenced";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache parts 1, 2, and 3
        let part1_data = vec![1u8; 1024];
        let part2_data = vec![2u8; 2048];
        let part3_data = vec![3u8; 512];

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "etag1")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 2, &part2_data, "etag2")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 3, &part3_data, "etag3")
            .await
            .unwrap();

        // Verify all part files exist in the upload directory
        let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        let part1_file = multipart_dir.join("part1.bin");
        let part2_file = multipart_dir.join("part2.bin");
        let part3_file = multipart_dir.join("part3.bin");

        assert!(
            part1_file.exists(),
            "Part 1 file should exist before finalization"
        );
        assert!(
            part2_file.exists(),
            "Part 2 file should exist before finalization"
        );
        assert!(
            part3_file.exists(),
            "Part 3 file should exist before finalization"
        );

        // Complete with only parts 1 and 3 (skip part 2)
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 3,
                etag: "etag3".to_string(),
            },
        ];

        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        assert!(result.is_ok());

        // Verify metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists(), "Metadata file should be created");

        // Verify upload directory was cleaned up (part 2 deleted with it)
        assert!(
            !multipart_dir.exists(),
            "Upload directory should be removed after finalization"
        );

        // Verify parts 1 and 3 were moved to final byte offsets in ranges/
        // Part 1: 0-1023, Part 3: 1024-1535 (since part 2 is skipped)
        let ranges_dir = temp_dir.path().join("ranges");
        let final_part1_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_0-1023.bin").unwrap();
        let final_part3_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_1024-1535.bin").unwrap();

        assert!(
            final_part1_file.exists(),
            "Part 1 should be renamed to final byte offset"
        );
        assert!(
            final_part3_file.exists(),
            "Part 3 should be renamed to final byte offset"
        );

        // Verify metadata contains correct part_ranges
        let metadata_content = std::fs::read_to_string(&metadata_file).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.object_metadata.parts_count, Some(2));
        assert_eq!(metadata.object_metadata.part_ranges.len(), 2);
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&1),
            Some(&(0, 1023))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&3),
            Some(&(1024, 1535))
        );
        // Part 2 should not be in part_ranges
        assert!(!metadata.object_metadata.part_ranges.contains_key(&2));
    }

    /// Test part filtering with contiguous parts (all parts in sequence)
    /// Requirements: 5.1, 5.2, 5.3, 7.1
    #[tokio::test]
    async fn test_finalize_multipart_upload_contiguous_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-contiguous";
        let upload_id = "test-upload-contiguous";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache parts 1, 2, and 3 (contiguous)
        let part1_data = vec![1u8; 1024];
        let part2_data = vec![2u8; 2048];
        let part3_data = vec![3u8; 512];

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "etag1")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 2, &part2_data, "etag2")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 3, &part3_data, "etag3")
            .await
            .unwrap();

        // Complete with all parts in order (contiguous)
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
            RequestedPart {
                part_number: 3,
                etag: "etag3".to_string(),
            },
        ];

        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        assert!(result.is_ok());

        // Verify metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists(), "Metadata file should be created");

        // Verify all parts were renamed to final byte offsets
        let ranges_dir = temp_dir.path().join("ranges");
        // Part 1: 0-1023 (1024 bytes)
        // Part 2: 1024-3071 (2048 bytes)
        // Part 3: 3072-3583 (512 bytes)
        let final_part1_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_0-1023.bin").unwrap();
        let final_part2_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_1024-3071.bin").unwrap();
        let final_part3_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_3072-3583.bin").unwrap();

        assert!(
            final_part1_file.exists(),
            "Part 1 should be renamed to final byte offset"
        );
        assert!(
            final_part2_file.exists(),
            "Part 2 should be renamed to final byte offset"
        );
        assert!(
            final_part3_file.exists(),
            "Part 3 should be renamed to final byte offset"
        );

        // Verify metadata contains correct part_ranges with cumulative offsets
        let metadata_content = std::fs::read_to_string(&metadata_file).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.object_metadata.parts_count, Some(3));
        assert_eq!(metadata.object_metadata.part_ranges.len(), 3);
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&1),
            Some(&(0, 1023))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&2),
            Some(&(1024, 3071))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&3),
            Some(&(3072, 3583))
        );

        // Verify total content length
        assert_eq!(
            metadata.object_metadata.content_length,
            1024 + 2048 + 512 // 3584 bytes total
        );
    }

    /// Test cumulative offset calculation with variable-sized parts
    /// Verifies that byte ranges are calculated correctly when parts have different sizes
    /// Requirements: 5.3, 7.1
    #[tokio::test]
    async fn test_finalize_multipart_upload_variable_sized_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-variable-sizes";
        let upload_id = "test-upload-variable-sizes";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache parts with significantly different sizes
        let part1_data = vec![1u8; 5 * 1024 * 1024]; // 5 MB
        let part2_data = vec![2u8; 10 * 1024 * 1024]; // 10 MB
        let part3_data = vec![3u8; 7 * 1024 * 1024]; // 7 MB
        let part4_data = vec![4u8; 3 * 1024 * 1024]; // 3 MB

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "etag1")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 2, &part2_data, "etag2")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 3, &part3_data, "etag3")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 4, &part4_data, "etag4")
            .await
            .unwrap();

        // Complete with all parts
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
            RequestedPart {
                part_number: 3,
                etag: "etag3".to_string(),
            },
            RequestedPart {
                part_number: 4,
                etag: "etag4".to_string(),
            },
        ];

        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        assert!(result.is_ok());

        // Verify metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists(), "Metadata file should be created");

        // Verify metadata contains correct part_ranges with cumulative offsets
        let metadata_content = std::fs::read_to_string(&metadata_file).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        // Calculate expected byte ranges
        let part1_size: u64 = 5 * 1024 * 1024;
        let part2_size: u64 = 10 * 1024 * 1024;
        let part3_size: u64 = 7 * 1024 * 1024;
        let part4_size: u64 = 3 * 1024 * 1024;

        let part1_start: u64 = 0;
        let part1_end: u64 = part1_size - 1;
        let part2_start: u64 = part1_size;
        let part2_end: u64 = part1_size + part2_size - 1;
        let part3_start: u64 = part1_size + part2_size;
        let part3_end: u64 = part1_size + part2_size + part3_size - 1;
        let part4_start: u64 = part1_size + part2_size + part3_size;
        let part4_end: u64 = part1_size + part2_size + part3_size + part4_size - 1;

        assert_eq!(metadata.object_metadata.parts_count, Some(4));
        assert_eq!(metadata.object_metadata.part_ranges.len(), 4);
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&1),
            Some(&(part1_start, part1_end)),
            "Part 1 range should be 0-{}",
            part1_end
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&2),
            Some(&(part2_start, part2_end)),
            "Part 2 range should be {}-{}",
            part2_start,
            part2_end
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&3),
            Some(&(part3_start, part3_end)),
            "Part 3 range should be {}-{}",
            part3_start,
            part3_end
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&4),
            Some(&(part4_start, part4_end)),
            "Part 4 range should be {}-{}",
            part4_start,
            part4_end
        );

        // Verify total content length
        let expected_total = part1_size + part2_size + part3_size + part4_size;
        assert_eq!(
            metadata.object_metadata.content_length, expected_total,
            "Total content length should be {} bytes",
            expected_total
        );
    }

    /// Test part filtering with non-contiguous parts (gaps in part numbers)
    /// Verifies that byte ranges are calculated correctly when parts are not sequential
    /// Requirements: 5.1, 5.2, 5.3, 7.1
    #[tokio::test]
    async fn test_finalize_multipart_upload_non_contiguous_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-non-contiguous";
        let upload_id = "test-upload-non-contiguous";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache parts 1, 3, 5 (non-contiguous - gaps at 2 and 4)
        let part1_data = vec![1u8; 1024];
        let part3_data = vec![3u8; 2048];
        let part5_data = vec![5u8; 512];

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "etag1")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 3, &part3_data, "etag3")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 5, &part5_data, "etag5")
            .await
            .unwrap();

        // Complete with all cached parts (non-contiguous part numbers)
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 3,
                etag: "etag3".to_string(),
            },
            RequestedPart {
                part_number: 5,
                etag: "etag5".to_string(),
            },
        ];

        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        assert!(result.is_ok());

        // Verify metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists(), "Metadata file should be created");

        // Verify all parts were renamed to final byte offsets
        let ranges_dir = temp_dir.path().join("ranges");
        // Part 1: 0-1023 (1024 bytes)
        // Part 3: 1024-3071 (2048 bytes) - starts right after part 1
        // Part 5: 3072-3583 (512 bytes) - starts right after part 3
        let final_part1_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_0-1023.bin").unwrap();
        let final_part3_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_1024-3071.bin").unwrap();
        let final_part5_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_3072-3583.bin").unwrap();

        assert!(
            final_part1_file.exists(),
            "Part 1 should be renamed to final byte offset"
        );
        assert!(
            final_part3_file.exists(),
            "Part 3 should be renamed to final byte offset"
        );
        assert!(
            final_part5_file.exists(),
            "Part 5 should be renamed to final byte offset"
        );

        // Verify metadata contains correct part_ranges
        // Note: part_ranges uses the original part numbers as keys, not sequential indices
        let metadata_content = std::fs::read_to_string(&metadata_file).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.object_metadata.parts_count, Some(3));
        assert_eq!(metadata.object_metadata.part_ranges.len(), 3);
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&1),
            Some(&(0, 1023))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&3),
            Some(&(1024, 3071))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&5),
            Some(&(3072, 3583))
        );

        // Parts 2 and 4 should not exist in part_ranges
        assert!(!metadata.object_metadata.part_ranges.contains_key(&2));
        assert!(!metadata.object_metadata.part_ranges.contains_key(&4));

        // Verify total content length
        assert_eq!(
            metadata.object_metadata.content_length,
            1024 + 2048 + 512 // 3584 bytes total
        );
    }

    /// Test part filtering when subset of cached parts is requested
    /// Verifies that only requested parts are included and byte offsets are recalculated
    /// Requirements: 5.1, 5.2, 5.3, 5.4, 7.1
    #[tokio::test]
    async fn test_finalize_multipart_upload_subset_of_cached_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-subset";
        let upload_id = "test-upload-subset";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache parts 1, 2, 3, 4, 5
        let part1_data = vec![1u8; 1000];
        let part2_data = vec![2u8; 2000];
        let part3_data = vec![3u8; 3000];
        let part4_data = vec![4u8; 4000];
        let part5_data = vec![5u8; 5000];

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "etag1")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 2, &part2_data, "etag2")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 3, &part3_data, "etag3")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 4, &part4_data, "etag4")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 5, &part5_data, "etag5")
            .await
            .unwrap();

        // Complete with only parts 2 and 4 (subset)
        let requested_parts = vec![
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
            RequestedPart {
                part_number: 4,
                etag: "etag4".to_string(),
            },
        ];

        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        assert!(result.is_ok());

        // Verify metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists(), "Metadata file should be created");

        // Verify unreferenced parts were deleted
        let ranges_dir = temp_dir.path().join("ranges");
        let part1_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_part1_0-999.bin")
                .unwrap();
        let part3_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_part3_0-2999.bin")
                .unwrap();
        let part5_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_part5_0-4999.bin")
                .unwrap();

        assert!(
            !part1_file.exists(),
            "Part 1 file should be deleted (unreferenced)"
        );
        assert!(
            !part3_file.exists(),
            "Part 3 file should be deleted (unreferenced)"
        );
        assert!(
            !part5_file.exists(),
            "Part 5 file should be deleted (unreferenced)"
        );

        // Verify requested parts were renamed to final byte offsets
        // Part 2: 0-1999 (2000 bytes) - starts at 0 since it's the first requested part
        // Part 4: 2000-5999 (4000 bytes) - starts right after part 2
        let final_part2_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_0-1999.bin").unwrap();
        let final_part4_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_2000-5999.bin").unwrap();

        assert!(
            final_part2_file.exists(),
            "Part 2 should be renamed to final byte offset"
        );
        assert!(
            final_part4_file.exists(),
            "Part 4 should be renamed to final byte offset"
        );

        // Verify metadata contains correct part_ranges
        let metadata_content = std::fs::read_to_string(&metadata_file).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.object_metadata.parts_count, Some(2));
        assert_eq!(metadata.object_metadata.part_ranges.len(), 2);
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&2),
            Some(&(0, 1999))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&4),
            Some(&(2000, 5999))
        );

        // Parts 1, 3, 5 should not exist in part_ranges
        assert!(!metadata.object_metadata.part_ranges.contains_key(&1));
        assert!(!metadata.object_metadata.part_ranges.contains_key(&3));
        assert!(!metadata.object_metadata.part_ranges.contains_key(&5));

        // Verify total content length (only parts 2 and 4)
        assert_eq!(
            metadata.object_metadata.content_length,
            2000 + 4000 // 6000 bytes total
        );
    }

    /// Test ETag validation with multiple parts - one mismatch should skip all caching
    /// Requirements: 9.1, 9.2, 9.3, 9.4
    #[tokio::test]
    async fn test_finalize_multipart_upload_etag_mismatch_one_of_many() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-etag-one-mismatch";
        let upload_id = "test-upload-etag-one-mismatch";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache parts 1, 2, 3
        let part1_data = vec![1u8; 1024];
        let part2_data = vec![2u8; 2048];
        let part3_data = vec![3u8; 512];

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "\"etag1\"")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 2, &part2_data, "\"etag2\"")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 3, &part3_data, "\"etag3\"")
            .await
            .unwrap();

        // Complete with parts where part 2 has mismatched ETag
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "\"etag1\"".to_string(), // matches
            },
            RequestedPart {
                part_number: 2,
                etag: "\"wrong-etag\"".to_string(), // MISMATCH
            },
            RequestedPart {
                part_number: 3,
                etag: "\"etag3\"".to_string(), // matches
            },
        ];

        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        // Should succeed (operation not failed - Requirement 9.4)
        assert!(result.is_ok());

        // Verify no metadata was created (cache finalization skipped - Requirement 9.2)
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(
            !metadata_file.exists(),
            "Metadata file should NOT be created due to ETag mismatch"
        );

        // Verify multipart directory was cleaned up
        let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        assert!(
            !multipart_dir.exists(),
            "Multipart directory should be cleaned up"
        );
    }

    // ============================================================================
    // Property-Based Tests for Multipart Upload
    // ============================================================================

    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: write-through-cache-finalization, Property 5: Multipart part storage**
    /// *For any* successful UploadPart request, the part data SHALL be stored as a range file,
    /// and the tracking metadata SHALL contain the uploadId, partNumber, size, and ETag.
    /// **Validates: Requirements 2.1, 2.2, 2.5**
    #[quickcheck]
    fn prop_multipart_part_storage(part_number: u8, data_size: u8) -> TestResult {
        // Filter out invalid inputs
        let part_number = (part_number % 100) + 1; // 1-100
        let data_size = (data_size % 100) + 10; // 10-109 bytes

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let mut handler = create_test_handler(&temp_dir);

            let cache_key = "test-bucket/test-object";
            let upload_id = "test-upload-prop";
            let data: Vec<u8> = (0..data_size).collect();
            let etag = format!("\"etag-{}\"", part_number);

            // Cache the part
            let result = handler
                .cache_upload_part(cache_key, upload_id, part_number as u32, &data, &etag)
                .await;

            if result.is_err() {
                return TestResult::failed();
            }

            // Part state lives in per-part records now, not in `upload.meta`.
            let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
            let tracker = tracker_from_disk(&multipart_dir, cache_key);
            // Verify tracker contains correct info
            let part_found = tracker
                .parts
                .iter()
                .find(|p| p.part_number == part_number as u32);

            match part_found {
                Some(part) => {
                    // Verify part info matches
                    TestResult::from_bool(
                        tracker.upload_id == upload_id
                            && tracker.cache_key == cache_key
                            && part.size == data_size as u64
                            && part.etag == etag,
                    )
                }
                None => TestResult::failed(),
            }
        })
    }

    /// **Feature: write-through-cache-finalization, Property 7: Multipart completion creates linked metadata**
    /// *For any* successful CompleteMultipartUpload, the object metadata SHALL contain range entries
    /// for all cached parts with correct byte offsets, and the final ETag from S3.
    /// **Validates: Requirements 3.1, 3.3, 3.4**
    #[quickcheck]
    fn prop_multipart_completion_creates_linked_metadata(part_count: u8) -> TestResult {
        // Filter out invalid inputs
        let part_count = (part_count % 5) + 1; // 1-5 parts

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let mut handler = create_test_handler(&temp_dir);

            let cache_key = "test-bucket/test-object";
            let upload_id = "test-upload-complete";
            // Multipart ETag format, whose `-N` suffix is S3's own statement of the
            // part count. It must agree with the number of parts actually cached:
            // finalization now cross-checks the two and declines rather than
            // caching an object S3 says has a different shape. This fixture used a
            // hardcoded `-5` for every part count, which that check correctly reads
            // as a truncated assembly.
            let final_etag = format!("\"abc123-{}\"", part_count);

            // First, cache multiple parts
            let mut requested_parts = Vec::new();
            for part_num in 1..=part_count {
                let data: Vec<u8> = (0..1024).map(|i| (i + part_num as usize) as u8).collect();
                let etag = format!("\"part-etag-{}\"", part_num);

                let result = handler
                    .cache_upload_part(cache_key, upload_id, part_num as u32, &data, &etag)
                    .await;

                if result.is_err() {
                    return TestResult::failed();
                }
                requested_parts.push(RequestedPart {
                    part_number: part_num as u32,
                    etag,
                });
            }

            // Now finalize the multipart upload
            let test_headers = std::collections::HashMap::new(); // Empty headers for test
            let result = handler
                .finalize_multipart_upload(
                    cache_key,
                    upload_id,
                    &final_etag,
                    &test_headers,
                    &requested_parts,
                )
                .await;

            if result.is_err() {
                return TestResult::failed();
            }

            // Verify the metadata file was created at the sharded path
            let metadata_dir = temp_dir.path().join("metadata");
            let metadata_file =
                match crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta") {
                    Ok(path) => path,
                    Err(_) => return TestResult::failed(),
                };

            if !metadata_file.exists() {
                return TestResult::failed();
            }

            // Read and verify metadata
            let meta_content = match std::fs::read_to_string(&metadata_file) {
                Ok(c) => c,
                Err(_) => return TestResult::failed(),
            };

            let metadata: crate::cache_types::NewCacheMetadata =
                match serde_json::from_str(&meta_content) {
                    Ok(m) => m,
                    Err(_) => return TestResult::failed(),
                };

            // Verify metadata properties
            // 1. Range entries for all parts
            if metadata.ranges.len() != part_count as usize {
                return TestResult::failed();
            }

            // 2. Correct byte offsets (no gaps or overlaps)
            let mut expected_start: u64 = 0;
            for range in &metadata.ranges {
                if range.start != expected_start {
                    return TestResult::failed();
                }
                expected_start = range.end + 1;
            }

            // 3. Final ETag from S3
            if metadata.object_metadata.etag != final_etag {
                return TestResult::failed();
            }

            // 4. is_write_cached=true
            if !metadata.object_metadata.is_write_cached {
                return TestResult::failed();
            }

            // 5. write_cache_expires_at is set
            if metadata.object_metadata.write_cache_expires_at.is_none() {
                return TestResult::failed();
            }

            // 6. Multipart directory should be cleaned up
            let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
            if multipart_dir.exists() {
                return TestResult::failed();
            }

            TestResult::passed()
        })
    }

    /// **Feature: write-through-cache-finalization, Property 9: Abort upload cleanup**
    /// *For any* AbortMultipartUpload request, all cached parts and tracking metadata for that uploadId
    /// SHALL be immediately removed.
    /// **Validates: Requirements 4.5, 8.5**
    #[quickcheck]
    fn prop_abort_upload_cleanup(part_count: u8) -> TestResult {
        // Filter out invalid inputs
        let part_count = (part_count % 5) + 1; // 1-5 parts

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let mut handler = create_test_handler(&temp_dir);

            let cache_key = "test-bucket/test-object-abort";
            let upload_id = "test-upload-abort";

            // First, cache multiple parts
            for part_num in 1..=part_count {
                let data: Vec<u8> = (0..1024).map(|i| (i + part_num as usize) as u8).collect();
                let etag = format!("\"part-etag-{}\"", part_num);

                let result = handler
                    .cache_upload_part(cache_key, upload_id, part_num as u32, &data, &etag)
                    .await;

                if result.is_err() {
                    return TestResult::failed();
                }
            }

            // Verify parts were cached
            let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
            let tracker = tracker_from_disk(&multipart_dir, cache_key);

            for part in &tracker.parts {
                let part_path = multipart_dir.join(format!("part{}.bin", part.part_number));

                // Verify part file exists before cleanup
                if !part_path.exists() {
                    return TestResult::failed();
                }
            }

            // Now cleanup the multipart upload (simulating AbortMultipartUpload)
            let result = handler.cleanup_multipart_upload(upload_id).await;

            if result.is_err() {
                return TestResult::failed();
            }

            // Verify tracking metadata and all parts are deleted (directory removed)
            if multipart_dir.exists() {
                return TestResult::failed();
            }

            TestResult::passed()
        })
    }

    /// **Property 2: Part Filtering Preserves Only Requested Parts**
    /// *For any* set of cached parts and requested parts, the filtered result contains exactly
    /// the intersection of cached and requested parts.
    /// - No parts outside the intersection are included
    /// - All parts in the intersection are included
    /// **Validates: Requirements 5.1, 5.2**
    #[quickcheck]
    fn prop_part_filtering_preserves_only_requested_parts(
        cached_parts_bitmap: u16,
        requested_parts_bitmap: u16,
    ) -> TestResult {
        // Use bitmaps to represent sets of part numbers (1-16)
        // Each bit position represents whether that part number is in the set
        // This gives us good coverage of various set combinations

        // Convert bitmaps to sets of part numbers (1-indexed)
        let cached_part_numbers: std::collections::HashSet<u32> = (0..16u32)
            .filter(|i| (cached_parts_bitmap >> i) & 1 == 1)
            .map(|i| i + 1) // Convert to 1-indexed part numbers
            .collect();

        let requested_part_numbers: std::collections::HashSet<u32> = (0..16u32)
            .filter(|i| (requested_parts_bitmap >> i) & 1 == 1)
            .map(|i| i + 1) // Convert to 1-indexed part numbers
            .collect();

        // Skip trivial cases where both sets are empty
        if cached_part_numbers.is_empty() && requested_part_numbers.is_empty() {
            return TestResult::discard();
        }

        // Create mock CachedPartInfo for each cached part
        let cached_parts: Vec<crate::cache_types::CachedPartInfo> = cached_part_numbers
            .iter()
            .map(|&part_num| {
                crate::cache_types::CachedPartInfo::new_uncompressed(
                    part_num,
                    1024, // arbitrary size
                    format!("\"etag-{}\"", part_num),
                )
            })
            .collect();

        // Create RequestedPart for each requested part
        let requested_parts: Vec<RequestedPart> = requested_part_numbers
            .iter()
            .map(|&part_num| RequestedPart {
                part_number: part_num,
                etag: format!("\"etag-{}\"", part_num),
            })
            .collect();

        // Apply the same filtering logic as finalize_multipart_upload
        // (Requirements 5.1, 5.2, 5.3)
        let requested_set: std::collections::HashSet<u32> =
            requested_parts.iter().map(|p| p.part_number).collect();

        let filtered_parts: Vec<&crate::cache_types::CachedPartInfo> = cached_parts
            .iter()
            .filter(|p| requested_set.contains(&p.part_number))
            .collect();

        // Calculate expected intersection
        let expected_intersection: std::collections::HashSet<u32> = cached_part_numbers
            .intersection(&requested_part_numbers)
            .copied()
            .collect();

        // Extract actual filtered part numbers
        let actual_filtered: std::collections::HashSet<u32> =
            filtered_parts.iter().map(|p| p.part_number).collect();

        // Property 1: Filtered result equals the intersection
        if actual_filtered != expected_intersection {
            return TestResult::failed();
        }

        // Property 2: No parts outside the intersection are included
        for part in &filtered_parts {
            if !expected_intersection.contains(&part.part_number) {
                return TestResult::failed();
            }
        }

        // Property 3: All parts in the intersection are included
        for &part_num in &expected_intersection {
            if !actual_filtered.contains(&part_num) {
                return TestResult::failed();
            }
        }

        // Property 4: Count matches
        if filtered_parts.len() != expected_intersection.len() {
            return TestResult::failed();
        }

        TestResult::passed()
    }

    /// **Property 4: Part Ranges Build Correctly from Sizes**
    /// *For any* ordered list of part sizes, cumulative offsets produce contiguous non-overlapping
    /// ranges where each range length equals the part size.
    /// - No gaps between ranges
    /// - No overlaps between ranges
    /// - Each range length equals the part size
    /// **Validates: Requirement 7.1**
    #[quickcheck]
    fn prop_part_ranges_build_correctly_from_sizes(part_sizes: Vec<u32>) -> TestResult {
        // Filter out zero-sized parts (S3 requires minimum 5MB parts, but for testing
        // we just need non-zero sizes to verify the algorithm)
        let part_sizes: Vec<u64> = part_sizes
            .into_iter()
            .filter(|&s| s > 0)
            .map(|s| s as u64)
            .collect();

        // Skip trivial cases with no parts
        if part_sizes.is_empty() {
            return TestResult::discard();
        }

        // Limit to reasonable number of parts to keep tests fast
        if part_sizes.len() > 100 {
            return TestResult::discard();
        }

        // Build part_ranges using the same algorithm as finalize_multipart_upload
        // (Requirements 5.3, 7.1)
        let byte_offsets: Vec<(u32, u64, u64)> = {
            let mut offsets = Vec::with_capacity(part_sizes.len());
            let mut current_offset: u64 = 0;
            for (idx, &size) in part_sizes.iter().enumerate() {
                let part_number = (idx + 1) as u32; // 1-indexed part numbers
                let start = current_offset;
                let end = current_offset + size - 1;
                offsets.push((part_number, start, end));
                current_offset += size;
            }
            offsets
        };

        // Build part_ranges HashMap (Requirements 7.1, 7.2)
        let part_ranges: std::collections::HashMap<u32, (u64, u64)> = byte_offsets
            .iter()
            .map(|(part_number, start, end)| (*part_number, (*start, *end)))
            .collect();

        // Property 1: Each range length equals the part size
        for (idx, &size) in part_sizes.iter().enumerate() {
            let part_number = (idx + 1) as u32;
            if let Some(&(start, end)) = part_ranges.get(&part_number) {
                let range_length = end - start + 1;
                if range_length != size {
                    return TestResult::failed();
                }
            } else {
                // Part should exist in the map
                return TestResult::failed();
            }
        }

        // Property 2: Ranges are contiguous (no gaps)
        // First range should start at 0
        if let Some(&(start, _)) = part_ranges.get(&1) {
            if start != 0 {
                return TestResult::failed();
            }
        }

        // Each subsequent range should start immediately after the previous one ends
        for idx in 1..part_sizes.len() {
            let prev_part_number = idx as u32;
            let curr_part_number = (idx + 1) as u32;

            if let (Some(&(_, prev_end)), Some(&(curr_start, _))) = (
                part_ranges.get(&prev_part_number),
                part_ranges.get(&curr_part_number),
            ) {
                // Current start should be exactly prev_end + 1 (no gap)
                if curr_start != prev_end + 1 {
                    return TestResult::failed();
                }
            }
        }

        // Property 3: Ranges are non-overlapping
        // Since we verified contiguity above (each starts at prev_end + 1),
        // and each range has positive length, they cannot overlap.
        // But let's verify explicitly by checking no range contains another's start
        let mut sorted_ranges: Vec<(u64, u64)> = part_ranges.values().copied().collect();
        sorted_ranges.sort_by_key(|&(start, _)| start);

        for i in 0..sorted_ranges.len() {
            for j in (i + 1)..sorted_ranges.len() {
                let (_, end_i) = sorted_ranges[i];
                let (start_j, _) = sorted_ranges[j];

                // Range j should start after range i ends (no overlap)
                if start_j <= end_i {
                    return TestResult::failed();
                }
            }
        }

        // Property 4: Total coverage equals sum of all part sizes
        let total_size: u64 = part_sizes.iter().sum();
        if let Some(&(_, last_end)) = part_ranges.get(&(part_sizes.len() as u32)) {
            // Last byte should be at total_size - 1 (0-indexed)
            if last_end != total_size - 1 {
                return TestResult::failed();
            }
        }

        // Property 5: Number of ranges equals number of parts
        if part_ranges.len() != part_sizes.len() {
            return TestResult::failed();
        }

        TestResult::passed()
    }

    /// **Property 5: ETag Validation Rejects Mismatches**
    /// *For any* pair of distinct ETags, comparing them causes cache finalization to be skipped;
    /// for any identical pair (with or without surrounding quotes), finalization proceeds.
    /// - Distinct ETags (after normalization) should not match
    /// - Identical ETags with various quote combinations should match
    /// - Quote normalization correctly strips surrounding quotes
    /// **Validates: Requirements 9.1, 9.2**
    #[quickcheck]
    fn prop_etag_validation_rejects_mismatches(
        etag_base: String,
        other_etag_base: String,
        cached_has_quotes: bool,
        request_has_quotes: bool,
    ) -> TestResult {
        // Filter out empty strings and strings containing quotes (to avoid nested quotes)
        if etag_base.is_empty() || etag_base.contains('"') {
            return TestResult::discard();
        }
        if other_etag_base.is_empty() || other_etag_base.contains('"') {
            return TestResult::discard();
        }

        // Limit string length to keep tests fast
        if etag_base.len() > 64 || other_etag_base.len() > 64 {
            return TestResult::discard();
        }

        // Test 1: Identical ETags with various quote combinations should match
        let cached_etag = if cached_has_quotes {
            format!("\"{}\"", etag_base)
        } else {
            etag_base.clone()
        };

        let request_etag = if request_has_quotes {
            format!("\"{}\"", etag_base)
        } else {
            etag_base.clone()
        };

        // After normalization, identical base ETags should match
        let cached_normalized = normalize_etag(&cached_etag);
        let request_normalized = normalize_etag(&request_etag);

        // Property 1: Identical ETags (same base) should match after normalization
        if cached_normalized != request_normalized {
            return TestResult::failed();
        }

        // Property 2: Both normalized values should equal the original base
        if cached_normalized != etag_base || request_normalized != etag_base {
            return TestResult::failed();
        }

        // Test 2: Distinct ETags should not match (when bases are different)
        if etag_base != other_etag_base {
            let other_cached_etag = if cached_has_quotes {
                format!("\"{}\"", other_etag_base)
            } else {
                other_etag_base.clone()
            };

            let other_normalized = normalize_etag(&other_cached_etag);

            // Property 3: Distinct ETags should not match after normalization
            if request_normalized == other_normalized {
                return TestResult::failed();
            }

            // Property 4: The normalized distinct ETag should equal its base
            if other_normalized != other_etag_base {
                return TestResult::failed();
            }
        }

        // Test 3: Verify the ETag validation logic used in finalize_multipart_upload
        // This simulates the actual comparison done during CompleteMultipartUpload
        let etags_match = normalize_etag(&cached_etag) == normalize_etag(&request_etag);

        // Property 5: Same base ETags should always match regardless of quotes
        if !etags_match {
            return TestResult::failed();
        }

        // Test 4: Verify distinct ETags are rejected
        if etag_base != other_etag_base {
            let distinct_request_etag = if request_has_quotes {
                format!("\"{}\"", other_etag_base)
            } else {
                other_etag_base.clone()
            };

            let distinct_match =
                normalize_etag(&cached_etag) == normalize_etag(&distinct_request_etag);

            // Property 6: Distinct ETags should never match
            if distinct_match {
                return TestResult::failed();
            }
        }

        TestResult::passed()
    }

    // =========================================================================
    // Streaming write-cache task tests (Task 4.1)
    //
    // Exercise `run_streaming_cache_write` directly (the spawned wrapper just
    // discards its outcome) over a real `WriteCacheRangeSink` backed by a temp
    // disk cache. They cover: non-chunked write+commit, aws-chunked decode+commit,
    // decoded-length mismatch → discard, and S3-failure → discard.
    // =========================================================================

    /// Build a configured-enough disk cache manager for sink-backed streaming
    /// cache tests (mirrors `cache::write_cache_range_sink_tests::make_disk_cache`).
    async fn make_streaming_disk_cache(
        temp_dir: &TempDir,
        batch_size: usize,
    ) -> crate::disk_cache::DiskCacheManager {
        let dc = crate::disk_cache::DiskCacheManager::new(
            temp_dir.path().to_path_buf(),
            true, // compression_enabled
            1024, // compression_threshold
            false,
            batch_size,
        );
        dc.initialize().await.unwrap();
        dc
    }

    async fn open_streaming_sink(
        dc: crate::disk_cache::DiskCacheManager,
        cache_key: &str,
        content_length: u64,
    ) -> crate::cache::WriteCacheRangeSink {
        crate::cache::WriteCacheRangeSink::open(
            dc,
            cache_key,
            content_length,
            true,
            Some(crate::write_cache_manager::WriteReservation::noop()),
        )
        .await
        .unwrap()
    }

    fn ok_response_info() -> ResponseInfo {
        let mut headers = HeaderMap::new();
        headers.insert("etag", "\"stream-etag\"".parse().unwrap());
        ResponseInfo {
            status: StatusCode::OK,
            headers,
        }
    }

    /// Build a minimal single-chunk aws-chunked body for `payload`.
    fn aws_chunked_single_chunk(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(format!("{:x};chunk-signature=0\r\n", payload.len()).as_bytes());
        out.extend_from_slice(payload);
        out.extend_from_slice(b"\r\n0;chunk-signature=0\r\n\r\n");
        out
    }

    /// Non-chunked body: frames are object bytes; S3 success → Committed + `.bin`.
    #[tokio::test]
    async fn streaming_cache_non_chunked_commits() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/stream-nonchunked";
        let object: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();

        let dc = make_streaming_disk_cache(&temp_dir, 4096).await;
        let final_path = dc.get_new_range_file_path(cache_key, 0, (object.len() as u64) - 1);
        let sink = open_streaming_sink(dc, cache_key, object.len() as u64).await;

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        // Feed the object in two frames, then close the channel and deliver success.
        tee_tx
            .send(Bytes::copy_from_slice(&object[..3000]))
            .await
            .unwrap();
        tee_tx
            .send(Bytes::copy_from_slice(&object[3000..]))
            .await
            .unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            false,
            None,
            std::time::Duration::from_secs(3600),
            HashMap::new(),
            None,
            None,
            None,
        )
        .await;

        assert_eq!(outcome, StreamingCacheOutcome::Committed);
        assert!(
            final_path.exists(),
            "committed .bin must exist: {:?}",
            final_path
        );
    }

    /// Regression for Requirement 12 (cache writes must not block the async runtime),
    /// acceptance criterion 12.4: the streamed write path, run under a
    /// worker-constrained runtime with concurrent large writes, must keep the runtime
    /// responsive rather than wedging it.
    ///
    /// Why this is deterministic and not a timing race:
    ///
    /// The original incident was that the drain loop's blocking `tee_rx.blocking_recv()`
    /// + blocking `sink.write` ran *inline on the async worker thread*. On the 2-worker
    /// default runtime, two concurrent large PUTs pinned both workers in synchronous
    /// writeback and starved everything else (the `/health` task, the SIGTERM handler).
    /// The fix moves that blocking work onto a `spawn_blocking` thread.
    ///
    /// This test reproduces the structural precondition rather than measuring latency:
    /// each write's body frames are produced by a *separate spawned async task*, and
    /// each write's S3 result is delivered by that same async task — i.e. completing a
    /// write *requires async tasks to be scheduled on a worker while the drain is
    /// running*. We launch more concurrent writes (`N = 4`) than worker threads (2).
    ///
    /// - With the fix: the four drains run on blocking threads, so the two workers stay
    ///   free to poll the four feeder tasks. Frames flow through the bounded
    ///   (depth-2) channels, the feeders deliver the S3 results, and all four writes
    ///   commit. The join completes.
    /// - With the bug (blocking work back on the workers): the first two drains scheduled
    ///   pin both workers inside `blocking_recv`; their feeder tasks can never be polled,
    ///   so no frame is ever sent, `blocking_recv` blocks forever, and the runtime
    ///   deadlocks. The join never completes.
    ///
    /// So correct code completes in milliseconds and a regression *cannot* complete at
    /// all. The `timeout` is only a safety net to turn that deadlock into a loud failure
    /// instead of a hung CI job; it is not a latency assertion.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streaming_cache_writes_do_not_block_async_workers() {
        // More concurrent writes than worker threads, so under the bug the drains
        // exhaust the worker pool and starve the feeders.
        const N: usize = 4;
        // Enough frames through a small bounded channel that the feeder must be
        // repeatedly re-polled (awaiting on a full channel) while the drain consumes —
        // exercising the worker/blocking-thread hand-off, not a single shot.
        const FRAMES: usize = 32;
        const FRAME_LEN: usize = 4096;
        let object_len = (FRAMES * FRAME_LEN) as u64;

        let temp_dir = TempDir::new().unwrap();
        let mut run_handles = Vec::with_capacity(N);

        for i in 0..N {
            let cache_key = format!("test-bucket/stream-noblock-{i}");
            // Distinct disk cache per sink (open takes the manager by value); all share
            // the temp dir with distinct keys.
            let dc = make_streaming_disk_cache(&temp_dir, FRAME_LEN).await;
            let sink = open_streaming_sink(dc, &cache_key, object_len).await;

            // Bounded, deliberately shallow so the feeder backpressures on `send`.
            let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(2);
            let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

            // Feeder: an independent async task. It can only make progress if a worker
            // thread is free to poll it — which is the whole property under test.
            tokio::spawn(async move {
                let frame = Bytes::from(vec![(i % 256) as u8; FRAME_LEN]);
                for _ in 0..FRAMES {
                    if tee_tx.send(frame.clone()).await.is_err() {
                        return;
                    }
                }
                drop(tee_tx);
                let _ = s3_tx.send(Ok(ok_response_info()));
            });

            // Runner: the real streamed write-cache path, spawned as its own task.
            run_handles.push(tokio::spawn(SignedPutHandler::run_streaming_cache_write(
                cache_key,
                sink,
                tee_rx,
                s3_rx,
                false,
                None,
                std::time::Duration::from_secs(3600),
                HashMap::new(),
                None,
                None,
                None,
            )));
        }

        // Safety net only: correct code finishes in milliseconds; a regression deadlocks
        // and would otherwise hang CI forever.
        let joined = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut outcomes = Vec::with_capacity(N);
            for h in run_handles {
                outcomes.push(h.await.unwrap());
            }
            outcomes
        })
        .await
        .expect(
            "streamed write path wedged the worker-constrained runtime: concurrent cache \
             writes blocked the async workers (Requirement 12.4 regression)",
        );

        assert_eq!(joined.len(), N);
        for outcome in joined {
            assert_eq!(
                outcome,
                StreamingCacheOutcome::Committed,
                "every concurrent streamed write must commit while the runtime stays responsive"
            );
        }
    }

    /// aws-chunked body: decoded incrementally, S3 success → Committed + `.bin`.
    #[tokio::test]
    async fn streaming_cache_aws_chunked_commits() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/stream-chunked";
        let payload = b"hello streaming world!";
        let encoded = aws_chunked_single_chunk(payload);

        let dc = make_streaming_disk_cache(&temp_dir, 4096).await;
        let final_path = dc.get_new_range_file_path(cache_key, 0, (payload.len() as u64) - 1);
        let sink = open_streaming_sink(dc, cache_key, payload.len() as u64).await;

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        // Split the encoded body across two frames to exercise incremental push.
        let split = encoded.len() / 2;
        tee_tx
            .send(Bytes::copy_from_slice(&encoded[..split]))
            .await
            .unwrap();
        tee_tx
            .send(Bytes::copy_from_slice(&encoded[split..]))
            .await
            .unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            true,
            Some(payload.len() as u64),
            std::time::Duration::from_secs(3600),
            HashMap::new(),
            None,
            None,
            None,
        )
        .await;

        assert_eq!(outcome, StreamingCacheOutcome::Committed);
        assert!(
            final_path.exists(),
            "committed .bin must exist: {:?}",
            final_path
        );
    }

    /// Decoded length disagrees with the expected (x-amz-decoded-content-length):
    /// the sink is discarded, caching is skipped, and no `.bin` is published.
    #[tokio::test]
    async fn streaming_cache_decoded_length_mismatch_discards() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/stream-mismatch";
        let payload = b"hello streaming world!"; // decodes to 22 bytes
        let encoded = aws_chunked_single_chunk(payload);
        let claimed_len: u64 = 9999; // deliberately wrong

        let dc = make_streaming_disk_cache(&temp_dir, 4096).await;
        let final_path = dc.get_new_range_file_path(cache_key, 0, claimed_len - 1);
        let sink = open_streaming_sink(dc, cache_key, claimed_len).await;

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        tee_tx.send(Bytes::copy_from_slice(&encoded)).await.unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            true,
            Some(claimed_len),
            std::time::Duration::from_secs(3600),
            HashMap::new(),
            None,
            None,
            None,
        )
        .await;

        assert_eq!(
            outcome,
            StreamingCacheOutcome::Skipped("decoded_length_mismatch")
        );
        assert!(
            !final_path.exists(),
            "mismatch must not publish a .bin: {:?}",
            final_path
        );
    }

    /// S3 failure: the sink is discarded, caching is skipped, no `.bin` published.
    #[tokio::test]
    async fn streaming_cache_s3_failure_discards() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/stream-s3-failure";
        let object: Vec<u8> = vec![0x42u8; 4000];

        let dc = make_streaming_disk_cache(&temp_dir, 4096).await;
        let final_path = dc.get_new_range_file_path(cache_key, 0, (object.len() as u64) - 1);
        let sink = open_streaming_sink(dc, cache_key, object.len() as u64).await;

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        tee_tx.send(Bytes::copy_from_slice(&object)).await.unwrap();
        drop(tee_tx);
        // Deliver an S3 error result.
        s3_tx
            .send(Err(ProxyError::HttpError(
                "simulated upstream 500".to_string(),
            )))
            .unwrap();

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            false,
            None,
            std::time::Duration::from_secs(3600),
            HashMap::new(),
            None,
            None,
            None,
        )
        .await;

        assert_eq!(outcome, StreamingCacheOutcome::Skipped("s3_error"));
        assert!(
            !final_path.exists(),
            "S3 failure must not publish a .bin: {:?}",
            final_path
        );
    }

    /// Cache-failure isolation (Task 4.2 / Req 7.1, 7.2): an aws-chunked decode
    /// error mid-drain must discard the sink, **close the tee receiver** so the
    /// forward loop drops the tee and keeps streaming verbatim, and skip caching —
    /// never publishing a `.bin` and never touching the upload. This is the Phase-1
    /// early-return path the 4.1 commit/mismatch/s3-failure tests do not exercise
    /// (those decode successfully or fail only after the channel is drained).
    #[tokio::test]
    async fn streaming_cache_decode_error_closes_tee() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/stream-decode-error";
        // A complete, malformed chunk header line (non-hex size) → `push` errors on
        // the first frame, before any object bytes are written.
        let malformed = b"zzz;chunk-signature=0\r\n";

        let dc = make_streaming_disk_cache(&temp_dir, 4096).await;
        // The claimed length is irrelevant here; the decode error fires first.
        let final_path = dc.get_new_range_file_path(cache_key, 0, 99);
        let sink = open_streaming_sink(dc, cache_key, 100).await;

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        // Hold `s3_tx` without sending: the decode error returns in Phase 1, before
        // the S3 result is ever awaited.
        let (_s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        tee_tx.send(Bytes::from_static(malformed)).await.unwrap();
        // Deliberately keep `tee_tx` alive so we can observe the receiver being
        // closed by the cache task (a real forward loop would see `Closed` on its
        // next send and drop the tee, continuing to stream verbatim).

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            true, // aws-chunked
            Some(100),
            std::time::Duration::from_secs(3600),
            HashMap::new(),
            None,
            None,
            None,
        )
        .await;

        assert_eq!(outcome, StreamingCacheOutcome::Skipped("decode_error"));
        assert!(
            tee_tx.is_closed(),
            "decode error must close the tee receiver so the forward loop drops the \
             tee and keeps forwarding verbatim"
        );
        assert!(
            !final_path.exists(),
            "decode error must not publish a .bin: {:?}",
            final_path
        );
    }

    /// Read-after-write parity — an immediate GET after a PUT must hit: a streamed PUT
    /// committed through `run_streaming_cache_write` against a real `CacheManager`
    /// must write the `.meta` **immediately**, so an immediate post-PUT GET is a
    /// cache hit. This guards the parity fix that finalizes the range and stores the
    /// `.meta` synchronously (via `CacheManager::store_streamed_write_cache_metadata`)
    /// rather than using the journal-only `WriteCacheRangeSink::commit`, which would
    /// defer the `.meta` until consolidation and make that GET a miss.
    #[tokio::test]
    async fn streaming_cache_writes_meta_immediately_for_read_after_write_hit() {
        use crate::cache::CacheManager;
        use crate::cache_types::NewCacheMetadata;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = Arc::new(CacheManager::new(
            temp_dir.path().to_path_buf(),
            false, // ram_cache_enabled — disabled; .meta on disk is what a GET reads
            0,     // RAM cache size
            1024,  // compression_threshold
            true,  // compression_enabled
        ));
        // Wire the journal consolidator into the manager (required before
        // `initialize`, and what `create_configured_disk_cache_manager` does for the
        // real proxy startup path).
        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/stream-read-after-write";
        let object: Vec<u8> = (0..6000u32).map(|i| (i % 97) as u8).collect();

        // Open the sink the same way the single-PUT handler does (Task 5.1).
        let sink = cache_manager
            .open_write_cache_sink(cache_key, object.len() as u64)
            .await
            .expect("open_write_cache_sink should not error")
            .expect("capacity should be available for the sink");

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        tee_tx.send(Bytes::copy_from_slice(&object)).await.unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            false,
            None,
            std::time::Duration::from_secs(3600),
            HashMap::new(),
            None,
            Some(cache_manager.clone()),
            None,
        )
        .await;

        assert_eq!(outcome, StreamingCacheOutcome::Committed);

        // The `.meta` must exist immediately — this is what makes a post-PUT GET hit.
        let meta_path = cache_manager.get_new_metadata_file_path(cache_key);
        assert!(
            meta_path.exists(),
            "streamed PUT must write .meta immediately for read-after-write parity: {:?}",
            meta_path
        );

        let metadata: NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        assert!(
            metadata.object_metadata.is_write_cached,
            "streamed PUT .meta must be marked write-cached"
        );
        assert_eq!(
            metadata.object_metadata.content_length,
            object.len() as u64,
            "cached object length must match the streamed object"
        );
        assert_eq!(
            metadata.ranges.len(),
            1,
            "streamed PUT must produce exactly one range (0..len-1)"
        );
        assert_eq!(metadata.ranges[0].start, 0);
        assert_eq!(metadata.ranges[0].end, object.len() as u64 - 1);
    }

    // =========================================================================
    // Streaming part-cache task tests (Task 6.1)
    //
    // Exercise `run_streaming_part_cache_write` directly (the spawned wrapper just
    // discards its outcome) over a real `MultipartPartSink` backed by a temp cache.
    // They cover: non-chunked stage+record + LZ4 round-trip, aws-chunked decode +
    // decoded-length validation, and S3-failure → discard.
    // =========================================================================

    /// A streamed non-chunked `UploadPart` commits: the part `.bin` is published in
    /// the upload's in-progress dir, the `upload.meta` tracker records the part
    /// (number, decoded size, S3 ETag), and the staged bytes round-trip through the
    /// LZ4 frame decoder — the concatenated-frame format the GET-side range loader
    /// reads after the part is linked into the object.
    #[tokio::test]
    async fn streaming_part_cache_non_chunked_records_part_and_round_trips() {
        use crate::cache::CacheManager;
        use std::io::Read;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = Arc::new(CacheManager::new(
            temp_dir.path().to_path_buf(),
            false,
            0,
            1024,
            true,
        ));
        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/stream-part-object";
        let upload_id = "upload-stream-1";
        let part_number = 2u32;
        let object: Vec<u8> = (0..7000u32).map(|i| (i % 131) as u8).collect();

        let sink = cache_manager
            .open_multipart_part_sink(cache_key, upload_id, part_number)
            .await
            .expect("open_multipart_part_sink should succeed");

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();
        tee_tx
            .send(Bytes::copy_from_slice(&object[..4000]))
            .await
            .unwrap();
        tee_tx
            .send(Bytes::copy_from_slice(&object[4000..]))
            .await
            .unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_part_cache_write(
            cache_key.to_string(),
            upload_id.to_string(),
            part_number,
            sink,
            tee_rx,
            s3_rx,
            false,
            None,
            temp_dir.path().to_path_buf(),
            None,
        )
        .await;
        assert_eq!(outcome, StreamingCacheOutcome::Committed);

        let upload_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        let part_path = upload_dir.join(format!("part{}.bin", part_number));
        assert!(
            part_path.exists(),
            "part file must be published: {:?}",
            part_path
        );

        let tracker = tracker_from_disk(&upload_dir, cache_key);
        assert_eq!(tracker.parts.len(), 1);
        assert_eq!(tracker.parts[0].part_number, part_number);
        assert_eq!(
            tracker.parts[0].size,
            object.len() as u64,
            "tracker must record the decoded part size"
        );
        assert_eq!(normalize_etag(&tracker.parts[0].etag), "stream-etag");

        // The staged bytes round-trip through the LZ4 frame decoder (concatenated
        // frames), proving the part file is readable by the GET-side range loader.
        let compressed = std::fs::read(&part_path).unwrap();
        let mut decoder = lz4_flex::frame::FrameDecoder::new(&compressed[..]);
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(
            decoded, object,
            "decompressed part bytes must equal the streamed object"
        );
    }

    /// A streamed aws-chunked `UploadPart` decodes incrementally: the cached part is
    /// the decoded payload (not the chunked wire bytes), the decoded-length check
    /// against `x-amz-decoded-content-length` passes, and the tracker records the
    /// decoded size.
    #[tokio::test]
    async fn streaming_part_cache_aws_chunked_decodes_and_records() {
        use crate::cache::CacheManager;
        use std::io::Read;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = Arc::new(CacheManager::new(
            temp_dir.path().to_path_buf(),
            false,
            0,
            1024,
            true,
        ));
        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/stream-part-chunked";
        let upload_id = "upload-stream-chunked";
        let part_number = 3u32;
        let payload: Vec<u8> = (0..3000u32).map(|i| (i % 199) as u8).collect();
        let chunked = aws_chunked_single_chunk(&payload);

        let sink = cache_manager
            .open_multipart_part_sink(cache_key, upload_id, part_number)
            .await
            .unwrap();

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();
        tee_tx.send(Bytes::copy_from_slice(&chunked)).await.unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_part_cache_write(
            cache_key.to_string(),
            upload_id.to_string(),
            part_number,
            sink,
            tee_rx,
            s3_rx,
            true,
            Some(payload.len() as u64),
            temp_dir.path().to_path_buf(),
            None,
        )
        .await;
        assert_eq!(outcome, StreamingCacheOutcome::Committed);

        let upload_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        let tracker = tracker_from_disk(&upload_dir, cache_key);
        assert_eq!(
            tracker.parts[0].size,
            payload.len() as u64,
            "tracker must record the DECODED payload length, not the chunked wire length"
        );

        let part_path = upload_dir.join(format!("part{}.bin", part_number));
        let compressed = std::fs::read(&part_path).unwrap();
        let mut decoder = lz4_flex::frame::FrameDecoder::new(&compressed[..]);
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(
            decoded, payload,
            "cached part must be the decoded payload, not the aws-chunked wire bytes"
        );
    }

    /// On S3 failure the streamed part is discarded (Req 7.1): no `part{N}.bin` is
    /// published and no `upload.meta` tracker is written — the per-part correctness
    /// gate (commit only on S3 success) holds on the streamed path.
    #[tokio::test]
    async fn streaming_part_cache_s3_error_discards() {
        use crate::cache::CacheManager;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = Arc::new(CacheManager::new(
            temp_dir.path().to_path_buf(),
            false,
            0,
            1024,
            true,
        ));
        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/stream-part-err";
        let upload_id = "upload-stream-err";
        let part_number = 1u32;

        let sink = cache_manager
            .open_multipart_part_sink(cache_key, upload_id, part_number)
            .await
            .unwrap();

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();
        tee_tx
            .send(Bytes::from_static(b"some part bytes"))
            .await
            .unwrap();
        drop(tee_tx);
        s3_tx
            .send(Err(ProxyError::HttpError("boom".to_string())))
            .unwrap();

        let outcome = SignedPutHandler::run_streaming_part_cache_write(
            cache_key.to_string(),
            upload_id.to_string(),
            part_number,
            sink,
            tee_rx,
            s3_rx,
            false,
            None,
            temp_dir.path().to_path_buf(),
            None,
        )
        .await;
        assert_eq!(outcome, StreamingCacheOutcome::Skipped("s3_error"));

        let upload_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        assert!(
            !upload_dir.join(format!("part{}.bin", part_number)).exists(),
            "no part file should be published on S3 error"
        );
        assert!(
            !upload_dir.join("upload.meta").exists(),
            "no tracker should be written on S3 error"
        );
    }

    // --- uploadId validation tests (Security: path traversal prevention) ---

    #[tokio::test]
    async fn test_cleanup_multipart_upload_rejects_traversal() {
        // Ensure cleanup_multipart_upload with a traversal uploadId does NOT
        // perform remove_dir_all outside the cache directory.
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        // Create a sentinel file outside the mpus_in_progress tree
        let sentinel = temp_dir.path().join("sentinel.txt");
        std::fs::write(&sentinel, "must survive").unwrap();

        // Even though cleanup doesn't validate (the handler does), confirm
        // that a traversal uploadId pointing at the sentinel's parent doesn't
        // delete the sentinel when the mpus_in_progress subdir doesn't exist.
        // This tests the defense-in-depth: the directory simply doesn't exist.
        let result = handler.cleanup_multipart_upload("../../sentinel.txt").await;
        assert!(result.is_ok());
        assert!(
            sentinel.exists(),
            "sentinel file must survive cleanup with traversal uploadId"
        );
    }

    #[tokio::test]
    async fn test_upload_id_validation_rejects_malicious_ids() {
        // Verify is_safe_path_component correctly rejects all dangerous patterns
        // that could be used as uploadId values for path traversal.

        // Traversal attempts
        assert!(!is_safe_path_component("../../etc/passwd"));
        assert!(!is_safe_path_component("../x"));
        assert!(!is_safe_path_component(".."));
        assert!(!is_safe_path_component("foo..bar"));

        // Path separators
        assert!(!is_safe_path_component("a/b"));
        assert!(!is_safe_path_component("a\\b"));

        // Control characters
        assert!(!is_safe_path_component("upload\x00id"));
        assert!(!is_safe_path_component("upload\nid"));

        // Empty
        assert!(!is_safe_path_component(""));

        // Valid S3-like uploadIds still pass
        assert!(is_safe_path_component(
            "VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"
        ));
        assert!(is_safe_path_component(
            "2Hoj0CxQnbMljdfMrU3bYHPJFSRPCmLzSHBfSIz4k"
        ));
        assert!(is_safe_path_component("normal-upload-id-123"));
    }

    #[tokio::test]
    async fn test_cache_upload_part_with_safe_upload_id_works() {
        // Normal uploadId still writes cache data (regression guard)
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object";
        let upload_id = "safe-upload-id-ABC123";
        let part_number = 1;
        let data = b"test data for safe uploadId";
        let etag = "test-etag-safe";

        let result = handler
            .cache_upload_part(cache_key, upload_id, part_number, data, etag)
            .await;
        assert!(result.is_ok());

        // Verify the upload directory was created in the correct location
        let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        assert!(
            multipart_dir.exists(),
            "upload directory should be created for safe uploadId"
        );
    }

    #[tokio::test]
    async fn test_malicious_upload_id_no_directory_created() {
        // A malicious uploadId must NOT create any directory in the cache.
        // (Handler validation rejects before cache_upload_part is called, but
        // if the guard were bypassed, the path would be unsafe.)
        let temp_dir = TempDir::new().unwrap();

        // Create a sentinel directory outside mpus_in_progress
        let outside_dir = temp_dir.path().join("important_data");
        std::fs::create_dir_all(&outside_dir).unwrap();
        let sentinel = outside_dir.join("file.txt");
        std::fs::write(&sentinel, "critical data").unwrap();

        // The handler would reject "../../important_data" via is_safe_path_component
        // but verify the cleanup path also doesn't escape.
        let mut handler = create_test_handler(&temp_dir);
        let result = handler
            .cleanup_multipart_upload("../../important_data")
            .await;
        assert!(result.is_ok());

        // The sentinel must survive
        assert!(
            sentinel.exists(),
            "sentinel file outside cache must survive malicious uploadId cleanup"
        );
        assert!(
            outside_dir.exists(),
            "directory outside cache must survive malicious uploadId cleanup"
        );
    }
}
