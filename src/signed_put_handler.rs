//! Signed PUT Handler Module
//!
//! Orchestrates the caching of AWS SigV4 signed PUT requests by streaming
//! the request body simultaneously to both S3 and the local cache.
//! This preserves signature integrity while enabling efficient caching.

use crate::aws_chunked_decoder;
use crate::capacity_manager::{check_cache_capacity, log_bypass_decision, CacheDecision};
use crate::compression::CompressionHandler;
use crate::log_sampler::LogSampler;
use crate::metrics::MetricsManager;
use crate::s3_client::S3Client;
use crate::signed_request_proxy::{forward_signed_request, forward_signed_request_with_body};
use crate::{ProxyError, Result};
use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::{HeaderMap, Request, Response, StatusCode};
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Simple response info for background tasks (status + headers only, no body)
#[derive(Clone)]
struct ResponseInfo {
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
use tokio_rustls::TlsConnector;
use tracing::{debug, error, info, warn};

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
    s3_client: Option<Arc<S3Client>>,
    /// Log sampler for reducing log volume
    log_sampler: LogSampler,
    /// Proxy identification Referer header value (None when disabled)
    proxy_referer: Option<String>,
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
    pub fn new(
        cache_dir: PathBuf,
        compression_handler: CompressionHandler,
        current_cache_usage: u64,
        max_cache_capacity: u64,
        proxy_referer: Option<String>,
    ) -> Self {
        Self {
            cache_dir,
            compression_handler,
            current_cache_usage,
            max_cache_capacity,
            metrics_manager: None,
            cache_manager: None,
            s3_client: None,
            log_sampler: LogSampler::new(),
            proxy_referer,
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
    pub fn set_s3_client(&mut self, s3_client: Arc<S3Client>) {
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
    /// * `target_ip` - Resolved IP address for S3
    /// * `tls_connector` - TLS connector for S3 connection
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
        target_ip: IpAddr,
        tls_connector: Arc<TlsConnector>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // Check if this is a multipart upload request
        let uri = req.uri();
        let query = uri.query().unwrap_or("");

        // Detect CreateMultipartUpload request (Requirement 4.1)
        // POST with ?uploads query parameter initiates a multipart upload
        if Self::is_create_multipart_upload(query) {
            return self
                .handle_create_multipart_upload(
                    req,
                    cache_key,
                    target_host,
                    target_ip,
                    tls_connector,
                )
                .await;
        }

        // Detect UploadPart request (Requirement 5.1)
        if let Some((upload_id, part_number)) = Self::parse_upload_part_query(query) {
            return self
                .handle_upload_part(
                    req,
                    cache_key,
                    target_host,
                    target_ip,
                    tls_connector,
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
                .handle_abort_multipart_upload(
                    req,
                    cache_key,
                    target_host,
                    target_ip,
                    tls_connector,
                    upload_id,
                )
                .await;
        }

        // Detect CompleteMultipartUpload request (Requirement 5.3)
        // CompleteMultipartUpload is a POST request with uploadId
        if Self::is_complete_multipart_upload(query) {
            let upload_id = Self::extract_upload_id(query).unwrap_or_default();
            return self
                .handle_complete_multipart_upload(
                    req,
                    cache_key,
                    target_host,
                    target_ip,
                    tls_connector,
                    upload_id,
                )
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

        // Decide whether to cache based on capacity (Requirement 2.1)
        let cache_decision = self.should_cache(content_length);

        match cache_decision {
            CacheDecision::Cache => {
                info!("Caching signed PUT request: {}", cache_key);
                // Stream to both S3 and cache
                self.handle_with_caching(
                    req,
                    cache_key,
                    target_host,
                    target_ip,
                    tls_connector,
                    request_headers,
                    content_length,
                )
                .await
            }
            CacheDecision::Bypass(reason) => {
                // Log bypass decision (Requirement 2.5)
                log_bypass_decision(&cache_key, &reason);
                info!("Bypassing cache for signed PUT: {}", cache_key);
                // Record bypassed PUT (Requirement 9.2)
                if let Some(metrics) = &self.metrics_manager {
                    metrics.read().await.record_bypassed_put().await;
                }
                // Forward without caching
                forward_signed_request(req, &target_host, target_ip, &tls_connector, self.proxy_referer.as_deref()).await
            }
            CacheDecision::StreamWithCapacityCheck => {
                info!("Streaming signed PUT with capacity check: {}", cache_key);
                // Stream with capacity checking during upload
                self.handle_with_streaming_capacity_check(
                    req,
                    cache_key,
                    target_host,
                    target_ip,
                    tls_connector,
                    request_headers,
                )
                .await
            }
        }
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

    /// Handle signed PUT with caching (Content-Length known and fits)
    ///
    /// This method reads the body into memory, spawns a background task to handle
    /// caching asynchronously, forwards the request to S3, and returns the S3
    /// response immediately without waiting for cache operations to complete.
    ///
    /// # Requirements
    ///
    /// - Requirement 4.1: Forward request to S3 without waiting for cache write completion
    /// - Requirement 4.2: Return S3 response to client immediately
    /// - Requirement 4.3: Handle cache writes in background tasks
    /// - Requirement 1.1: Continue with upload even if caching fails
    /// - Requirement 1.3: Return S3 response unchanged
    async fn handle_with_caching(
        &self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        target_ip: IpAddr,
        tls_connector: Arc<TlsConnector>,
        request_headers: HashMap<String, String>,
        content_length: Option<u64>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // Extract request components before consuming the body
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers = req.headers().clone();
        let version = req.version();

        // Read the request body once into Bytes (Requirement 1.5)
        // This preserves AWS SigV4 signatures
        use http_body_util::BodyExt;
        let body = req.into_body();
        let collected = body
            .collect()
            .await
            .map_err(|e| ProxyError::HttpError(format!("Failed to read request body: {}", e)))?;
        let body_bytes = collected.to_bytes();

        debug!("Read request body: {} bytes", body_bytes.len());

        // Check for aws-chunked encoding and decode if present
        // (Requirements 1.1, 1.2, 1.4, 1.5, 2.1, 2.3, 4.1, 4.2 from aws-chunked-decoding)
        let (body_for_cache, effective_content_length) = if aws_chunked_decoder::is_aws_chunked(
            &request_headers,
        ) {
            info!(
                "aws-chunked encoding detected: cache_key={}, raw_body_size={}",
                cache_key,
                body_bytes.len()
            );
            match aws_chunked_decoder::decode_aws_chunked(&body_bytes) {
                Ok(decoded) => {
                    let decoded_len = decoded.len() as u64;
                    // Validate against x-amz-decoded-content-length if present (Requirement 1.5)
                    if let Some(expected) =
                        aws_chunked_decoder::get_decoded_content_length(&request_headers)
                    {
                        if decoded_len != expected {
                            warn!(
                                "aws-chunked decode length mismatch: cache_key={}, expected={}, actual={} - skipping cache",
                                cache_key, expected, decoded_len
                            );
                            // Record cache bypass metric (Requirement 4.4)
                            if let Some(metrics) = &self.metrics_manager {
                                let metrics_clone = metrics.clone();
                                tokio::spawn(async move {
                                    metrics_clone
                                        .read()
                                        .await
                                        .record_cache_bypass("aws_chunked_decode_error")
                                        .await;
                                });
                            }
                            // Skip caching but continue with S3 upload (Requirement 4.1, 4.2)
                            (None, content_length)
                        } else {
                            info!(
                                "aws-chunked decoded successfully: cache_key={}, raw_size={}, decoded_size={}",
                                cache_key, body_bytes.len(), decoded_len
                            );
                            (Some(Bytes::from(decoded)), Some(decoded_len))
                        }
                    } else {
                        info!(
                            "aws-chunked decoded (no expected length header): cache_key={}, raw_size={}, decoded_size={}",
                            cache_key, body_bytes.len(), decoded_len
                        );
                        (Some(Bytes::from(decoded)), Some(decoded_len))
                    }
                }
                Err(e) => {
                    // Log warning and skip caching, but still forward to S3 (Requirement 4.1, 4.2)
                    warn!(
                        "aws-chunked decode failed: cache_key={}, error={} - skipping cache",
                        cache_key, e
                    );
                    // Record cache bypass metric (Requirement 4.4)
                    if let Some(metrics) = &self.metrics_manager {
                        let metrics_clone = metrics.clone();
                        tokio::spawn(async move {
                            metrics_clone
                                .read()
                                .await
                                .record_cache_bypass("aws_chunked_decode_error")
                                .await;
                        });
                    }
                    (None, content_length)
                }
            }
        } else {
            // Non-chunked: use body as-is (Requirement 3.1, 3.3 from aws-chunked-decoding)
            (Some(body_bytes.clone()), content_length)
        };

        // Create oneshot channel for S3 result communication
        let (s3_result_tx, s3_result_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        // Spawn background cache task only if we have decoded body (Requirement 4.3)
        // Pass decoded body for caching (Requirement 2.3 from aws-chunked-decoding)
        if let Some(cache_body) = body_for_cache {
            Self::spawn_cache_write_task(
                cache_key.clone(),
                cache_body, // Use decoded body for cache
                s3_result_rx,
                self.cache_dir.clone(),
                self.compression_handler.clone(),
                effective_content_length,
                request_headers.clone(),
                self.metrics_manager.clone(),
                self.cache_manager.clone(),
                self.s3_client.clone(),
            );
        } else {
            // No caching - drop the receiver to signal no cache write
            drop(s3_result_rx);
        }

        // Build raw HTTP request for S3
        let mut raw_request = Vec::new();

        // Request line: METHOD /path?query HTTP/1.1
        let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");

        raw_request.extend_from_slice(
            format!("{} {} {:?}\r\n", method, path_and_query, version).as_bytes(),
        );

        // Headers - preserve exactly as received
        for (name, value) in headers.iter() {
            if let Ok(value_str) = value.to_str() {
                raw_request
                    .extend_from_slice(format!("{}: {}\r\n", name.as_str(), value_str).as_bytes());
            }
        }

        // Inject proxy identification Referer header if conditions are met
        if let Some(ref referer_value) = self.proxy_referer {
            if !headers.contains_key("referer") {
                let should_add = if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
                    if auth.contains("AWS4-HMAC-SHA256") {
                        if let Some(pos) = auth.find("SignedHeaders=") {
                            let after_param = &auth[pos + 14..];
                            let end = after_param.find(',').or_else(|| after_param.find(' ')).unwrap_or(after_param.len());
                            let signed_headers = &after_param[..end];
                            !signed_headers.split(';').any(|h| h == "referer")
                        } else { true }
                    } else { true }
                } else { true };
                if should_add {
                    debug!("Adding proxy identification Referer header to raw streaming PUT request: {}", referer_value);
                    raw_request.extend_from_slice(format!("Referer: {}\r\n", referer_value).as_bytes());
                }
            }
        }

        // End of headers
        raw_request.extend_from_slice(b"\r\n");

        // Body
        if !body_bytes.is_empty() {
            raw_request.extend_from_slice(&body_bytes);
        }

        debug!("Built raw HTTP request: {} bytes", raw_request.len());

        // Forward to S3 immediately without waiting for cache (Requirement 4.1)
        let s3_response = self
            .forward_raw_request_to_s3(raw_request, &target_host, target_ip, &tls_connector)
            .await;

        // Send S3 result info to background task through channel
        // Extract status and headers before sending
        let response_info = match &s3_response {
            Ok(resp) => Ok(ResponseInfo {
                status: resp.status(),
                headers: resp.headers().clone(),
            }),
            Err(e) => Err(e.clone()),
        };
        let _ = s3_result_tx.send(response_info);

        // Return S3 response to client immediately (Requirement 4.2)
        s3_response
    }

    /// Handle signed PUT with streaming capacity check (no Content-Length)
    ///
    /// This method reads the body into memory, spawns a background task to handle
    /// caching asynchronously with capacity checking, forwards the request to S3,
    /// and returns the S3 response immediately without waiting for cache operations
    /// to complete.
    ///
    /// # Requirements
    ///
    /// - Requirement 2.1: Sanitize cache keys before path construction
    /// - Requirement 2.2: Ensure cache paths are within cache directory
    /// - Requirement 4.1: Forward request to S3 without waiting for cache write completion
    /// - Requirement 4.2: Return S3 response to client immediately
    /// - Requirement 4.3: Handle cache writes in background tasks
    /// - Requirement 1.1: Continue with upload even if caching fails
    /// - Requirement 1.3: Return S3 response unchanged
    async fn handle_with_streaming_capacity_check(
        &self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        target_ip: IpAddr,
        tls_connector: Arc<TlsConnector>,
        request_headers: HashMap<String, String>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // Extract request components before consuming the body
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers = req.headers().clone();
        let version = req.version();

        // Read the request body once into Bytes (Requirement 1.5)
        // This preserves AWS SigV4 signatures
        use http_body_util::BodyExt;
        let body = req.into_body();
        let collected = body
            .collect()
            .await
            .map_err(|e| ProxyError::HttpError(format!("Failed to read request body: {}", e)))?;
        let body_bytes = collected.to_bytes();

        debug!("Read request body: {} bytes", body_bytes.len());

        // Check for aws-chunked encoding and decode if present
        // Use decoded content length for capacity checking (Requirement 1.3 from aws-chunked-decoding)
        let (body_for_cache, effective_capacity_length) = if aws_chunked_decoder::is_aws_chunked(
            &request_headers,
        ) {
            info!(
                "aws-chunked encoding detected (streaming): cache_key={}, raw_body_size={}",
                cache_key,
                body_bytes.len()
            );
            match aws_chunked_decoder::decode_aws_chunked(&body_bytes) {
                Ok(decoded) => {
                    let decoded_len = decoded.len() as u64;
                    // Validate against x-amz-decoded-content-length if present
                    if let Some(expected) =
                        aws_chunked_decoder::get_decoded_content_length(&request_headers)
                    {
                        if decoded_len != expected {
                            warn!(
                                "aws-chunked decode length mismatch (streaming): cache_key={}, expected={}, actual={} - skipping cache",
                                cache_key, expected, decoded_len
                            );
                            // Record cache bypass metric (Requirement 4.4)
                            if let Some(metrics) = &self.metrics_manager {
                                let metrics_clone = metrics.clone();
                                tokio::spawn(async move {
                                    metrics_clone
                                        .read()
                                        .await
                                        .record_cache_bypass("aws_chunked_decode_error")
                                        .await;
                                });
                            }
                            (None, None)
                        } else {
                            info!(
                                "aws-chunked decoded successfully (streaming): cache_key={}, raw_size={}, decoded_size={}",
                                cache_key, body_bytes.len(), decoded_len
                            );
                            (Some(Bytes::from(decoded)), Some(decoded_len))
                        }
                    } else {
                        info!(
                            "aws-chunked decoded (streaming, no expected length header): cache_key={}, raw_size={}, decoded_size={}",
                            cache_key, body_bytes.len(), decoded_len
                        );
                        (Some(Bytes::from(decoded)), Some(decoded_len))
                    }
                }
                Err(e) => {
                    warn!(
                        "aws-chunked decode failed (streaming): cache_key={}, error={} - skipping cache",
                        cache_key, e
                    );
                    // Record cache bypass metric (Requirement 4.4)
                    if let Some(metrics) = &self.metrics_manager {
                        let metrics_clone = metrics.clone();
                        tokio::spawn(async move {
                            metrics_clone
                                .read()
                                .await
                                .record_cache_bypass("aws_chunked_decode_error")
                                .await;
                        });
                    }
                    (None, None)
                }
            }
        } else {
            // Non-chunked: use body as-is
            (Some(body_bytes.clone()), Some(body_bytes.len() as u64))
        };

        // Create oneshot channel for S3 result communication
        let (s3_result_tx, s3_result_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        // Calculate available capacity for streaming check
        let available_capacity = self
            .max_cache_capacity
            .saturating_sub(self.current_cache_usage);

        // Spawn background cache task with capacity limit (Requirement 4.3)
        // Use decoded body for caching and decoded length for capacity check
        if let Some(cache_body) = body_for_cache {
            // Use decoded length for capacity checking (Requirement 1.3 from aws-chunked-decoding)
            let capacity_to_check = effective_capacity_length.unwrap_or(cache_body.len() as u64);

            // Only proceed if decoded body fits in available capacity
            if capacity_to_check <= available_capacity {
                Self::spawn_cache_write_task_with_capacity(
                    cache_key.clone(),
                    cache_body, // Use decoded body for cache
                    s3_result_rx,
                    self.cache_dir.clone(),
                    self.compression_handler.clone(),
                    Some(available_capacity),
                    request_headers.clone(),
                    self.metrics_manager.clone(),
                    self.cache_manager.clone(),
                    self.s3_client.clone(),
                );
            } else {
                debug!(
                    "Decoded body exceeds capacity: cache_key={}, decoded_size={}, available={}",
                    cache_key, capacity_to_check, available_capacity
                );
                drop(s3_result_rx);
            }
        } else {
            // No caching - drop the receiver to signal no cache write
            drop(s3_result_rx);
        }

        // Build raw HTTP request for S3
        let mut raw_request = Vec::new();

        // Request line: METHOD /path?query HTTP/1.1
        let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");

        raw_request.extend_from_slice(
            format!("{} {} {:?}\r\n", method, path_and_query, version).as_bytes(),
        );

        // Headers - preserve exactly as received
        for (name, value) in headers.iter() {
            if let Ok(value_str) = value.to_str() {
                raw_request
                    .extend_from_slice(format!("{}: {}\r\n", name.as_str(), value_str).as_bytes());
            }
        }

        // Inject proxy identification Referer header if conditions are met
        if let Some(ref referer_value) = self.proxy_referer {
            if !headers.contains_key("referer") {
                let should_add = if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
                    if auth.contains("AWS4-HMAC-SHA256") {
                        if let Some(pos) = auth.find("SignedHeaders=") {
                            let after_param = &auth[pos + 14..];
                            let end = after_param.find(',').or_else(|| after_param.find(' ')).unwrap_or(after_param.len());
                            let signed_headers = &after_param[..end];
                            !signed_headers.split(';').any(|h| h == "referer")
                        } else { true }
                    } else { true }
                } else { true };
                if should_add {
                    debug!("Adding proxy identification Referer header to raw PUT request: {}", referer_value);
                    raw_request.extend_from_slice(format!("Referer: {}\r\n", referer_value).as_bytes());
                }
            }
        }

        // End of headers
        raw_request.extend_from_slice(b"\r\n");

        // Body
        if !body_bytes.is_empty() {
            raw_request.extend_from_slice(&body_bytes);
        }

        debug!("Built raw HTTP request: {} bytes", raw_request.len());

        // Forward to S3 immediately without waiting for cache (Requirement 4.1)
        let s3_response = self
            .forward_raw_request_to_s3(raw_request, &target_host, target_ip, &tls_connector)
            .await;

        // Send S3 result info to background task through channel
        // Extract status and headers before sending
        let response_info = match &s3_response {
            Ok(resp) => Ok(ResponseInfo {
                status: resp.status(),
                headers: resp.headers().clone(),
            }),
            Err(e) => Err(e.clone()),
        };
        let _ = s3_result_tx.send(response_info);

        // Return S3 response to client immediately (Requirement 4.2)
        s3_response
    }

    /// Forward raw HTTP request to S3 via TLS
    ///
    /// This method establishes a TLS connection to S3 and forwards the raw
    /// HTTP request bytes, then reads and parses the response.
    ///
    /// # Arguments
    ///
    /// * `raw_request` - Complete raw HTTP request bytes
    /// * `target_host` - Target S3 hostname
    /// * `target_ip` - Resolved IP address for S3
    /// * `tls_connector` - TLS connector for S3 connection
    ///
    /// # Returns
    ///
    /// Returns the parsed S3 response
    ///
    /// # Requirements
    ///
    /// - Requirement 8.4: Handle network timeouts
    /// - Requirement 9.3: Log detailed error information
    async fn forward_raw_request_to_s3(
        &self,
        raw_request: Vec<u8>,
        target_host: &str,
        target_ip: IpAddr,
        tls_connector: &TlsConnector,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::time::{timeout, Duration};

        // Establish TLS connection with retry logic (Requirement 8.4, 1.1, 1.2, 2.1, 3.1, 3.4)
        // Use 5 retries with exponential backoff
        let mut tls_stream = crate::signed_request_proxy::establish_tls_with_retry(
            target_ip,
            443,
            target_host,
            tls_connector,
            5,
        )
        .await?;

        // Send raw request with timeout (Requirement 8.4)
        timeout(Duration::from_secs(60), tls_stream.write_all(&raw_request))
            .await
            .map_err(|_| {
                error!(
                    "Request write timeout: target_host={}, request_size={}, timeout=60s",
                    target_host,
                    raw_request.len()
                );
                ProxyError::TimeoutError(format!("Request write timeout to {}", target_host))
            })?
            .map_err(|e| {
                error!(
                    "Failed to write request: target_host={}, request_size={}, error={}",
                    target_host,
                    raw_request.len(),
                    e
                );
                ProxyError::HttpError(format!("Failed to write request: {}", e))
            })?;

        timeout(Duration::from_secs(10), tls_stream.flush())
            .await
            .map_err(|_| {
                error!(
                    "Request flush timeout: target_host={}, timeout=10s",
                    target_host
                );
                ProxyError::TimeoutError(format!("Request flush timeout to {}", target_host))
            })?
            .map_err(|e| {
                error!(
                    "Failed to flush request: target_host={}, error={}",
                    target_host, e
                );
                ProxyError::HttpError(format!("Failed to flush request: {}", e))
            })?;

        debug!("Sent raw HTTP request to S3");

        // Read response with timeout (Requirement 8.4)
        let mut response_bytes = Vec::new();
        let mut buffer = vec![0u8; 8192];
        let read_timeout = Duration::from_secs(300); // 5 minutes for large responses

        loop {
            match timeout(read_timeout, tls_stream.read(&mut buffer)).await {
                Ok(Ok(0)) => break, // EOF
                Ok(Ok(n)) => {
                    response_bytes.extend_from_slice(&buffer[..n]);

                    // Check if we have a complete response
                    if self.has_complete_response(&response_bytes) {
                        break;
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        "Failed to read response: target_host={}, bytes_read={}, error={}",
                        target_host,
                        response_bytes.len(),
                        e
                    );
                    return Err(ProxyError::HttpError(format!(
                        "Failed to read response: {}",
                        e
                    )));
                }
                Err(_) => {
                    error!(
                        "Response read timeout: target_host={}, bytes_read={}, timeout={}s",
                        target_host,
                        response_bytes.len(),
                        read_timeout.as_secs()
                    );
                    return Err(ProxyError::TimeoutError(format!(
                        "Response read timeout from {}",
                        target_host
                    )));
                }
            }

            // Safety limit
            if response_bytes.len() > 100 * 1024 * 1024 {
                warn!(
                    "Response exceeded 100MB, stopping read: target_host={}, bytes_read={}",
                    target_host,
                    response_bytes.len()
                );
                break;
            }
        }

        debug!("Received raw HTTP response: {} bytes", response_bytes.len());

        // Parse response
        self.parse_http_response(&response_bytes)
    }

    /// Check if we have a complete HTTP response (skipping 1xx informational responses)
    fn has_complete_response(&self, bytes: &[u8]) -> bool {
        let mut pos = 0;

        loop {
            // Find the end of headers from current position
            let remaining_bytes = &bytes[pos..];
            let header_end = match self.find_header_end(remaining_bytes) {
                Some(end_pos) => end_pos,
                None => return false, // No complete headers found
            };

            // Parse headers to check status code
            let header_section = &remaining_bytes[..header_end];
            let header_str = String::from_utf8_lossy(header_section);
            let mut lines = header_str.lines();

            // Parse status line
            let status_line = match lines.next() {
                Some(line) => line,
                None => return false,
            };

            // Extract status code
            let status_code = match self.parse_status_code(status_line) {
                Ok(code) => code,
                Err(_) => return false,
            };

            // If this is a 1xx response, skip it and continue looking
            if status_code.as_u16() >= 100 && status_code.as_u16() < 200 {
                // Skip past this response (1xx responses typically have no body)
                pos = pos + header_end + 4; // Skip \r\n\r\n
                continue;
            }

            // This is a final response (2xx-5xx), check if it's complete
            // Check for Content-Length
            for line in lines {
                if line.to_lowercase().starts_with("content-length:") {
                    if let Some(length_str) = line.split(':').nth(1) {
                        if let Ok(content_length) = length_str.trim().parse::<usize>() {
                            let body_start = pos + header_end + 4; // Skip \r\n\r\n
                            let current_body_length = bytes.len().saturating_sub(body_start);
                            return current_body_length >= content_length;
                        }
                    }
                }

                // Check for chunked encoding
                if line.to_lowercase().starts_with("transfer-encoding:") && line.contains("chunked")
                {
                    return bytes.ends_with(b"0\r\n\r\n") || bytes.ends_with(b"0\r\n\r\n\r\n");
                }
            }

            // If no Content-Length or Transfer-Encoding found, assume response is complete
            // This handles cases where the response has no body
            return true;
        }
    }

    /// Find the end of HTTP headers (position of \r\n\r\n)
    fn find_header_end(&self, bytes: &[u8]) -> Option<usize> {
        (0..bytes.len().saturating_sub(3)).find(|&i| &bytes[i..i + 4] == b"\r\n\r\n")
    }

    /// Parse raw HTTP response bytes into a Hyper Response
    fn parse_http_response(&self, bytes: &[u8]) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // Parse response, skipping any 1xx informational responses
        let mut pos = 0;

        loop {
            // Find end of headers from current position
            let remaining_bytes = &bytes[pos..];
            let header_end = self.find_header_end(remaining_bytes).ok_or_else(|| {
                ProxyError::HttpError("Invalid HTTP response: no header end found".to_string())
            })?;

            let header_section = &remaining_bytes[..header_end];
            let body_start = pos + header_end + 4; // Skip \r\n\r\n

            // Parse status line and headers
            let header_str = String::from_utf8_lossy(header_section);
            let mut lines = header_str.lines();

            // Parse status line: HTTP/1.1 200 OK
            let status_line = lines.next().ok_or_else(|| {
                ProxyError::HttpError("Invalid HTTP response: no status line".to_string())
            })?;

            let status_code = self.parse_status_code(status_line)?;

            // If this is a 1xx response, skip it and continue looking for final response
            if status_code.as_u16() >= 100 && status_code.as_u16() < 200 {
                info!(
                    "Received informational response: {} - skipping",
                    status_code
                );

                // For 1xx responses, there should be no body, so move to next response
                pos = body_start;
                continue;
            }

            // This is a final response (2xx-5xx), process it
            let body_bytes = if body_start < bytes.len() {
                Bytes::copy_from_slice(&bytes[body_start..])
            } else {
                Bytes::new()
            };

            // Build response with final status code
            let mut response_builder = Response::builder().status(status_code);

            // Parse and add headers
            for line in lines {
                if let Some((name, value)) = line.split_once(':') {
                    let name = name.trim();
                    let value = value.trim();
                    response_builder = response_builder.header(name, value);
                }
            }

            // Debug log for raw PUT response - detailed logging is in the operation handlers
            debug!(
                "Raw PUT response: status={}, body_len={}",
                status_code,
                body_bytes.len()
            );

            return response_builder
                .body(
                    Full::new(body_bytes)
                        .map_err(|never| match never {})
                        .boxed(),
                )
                .map_err(|e| ProxyError::HttpError(format!("Failed to build response: {}", e)));
        }
    }

    /// Parse HTTP status code from status line
    fn parse_status_code(&self, status_line: &str) -> Result<StatusCode> {
        // Status line format: HTTP/1.1 200 OK
        let parts: Vec<&str> = status_line.split_whitespace().collect();

        if parts.len() < 2 {
            return Err(ProxyError::HttpError(format!(
                "Invalid status line: {}",
                status_line
            )));
        }

        let code_str = parts[1];
        let code = code_str.parse::<u16>().map_err(|e| {
            ProxyError::HttpError(format!("Invalid status code {}: {}", code_str, e))
        })?;

        StatusCode::from_u16(code)
            .map_err(|e| ProxyError::HttpError(format!("Invalid status code {}: {}", code, e)))
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
        target_ip: IpAddr,
        tls_connector: Arc<TlsConnector>,
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

        // Forward request to S3
        let s3_response =
            forward_signed_request(req, &target_host, target_ip, &tls_connector, self.proxy_referer.as_deref()).await?;

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

    /// Handle UploadPart request by caching the part as a range file
    ///
    /// # Requirements
    ///
    /// - Requirement 5.1: Cache each part as a range file
    /// - Requirement 5.2: Calculate byte offset based on previously cached parts
    /// - Requirement 8.1: Handle cache write failures gracefully
    /// - Requirement 8.2: Clean up cached data on S3 error
    /// - Requirement 9.3: Log detailed error information
    async fn handle_upload_part(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        target_ip: IpAddr,
        tls_connector: Arc<TlsConnector>,
        upload_id: String,
        part_number: u32,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
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

        match cache_decision {
            CacheDecision::Cache | CacheDecision::StreamWithCapacityCheck => {
                // Read the request body
                let method = req.method().clone();
                let uri = req.uri().clone();
                let headers = req.headers().clone();
                let version = req.version();

                use http_body_util::BodyExt;
                let body = req.into_body();
                let collected = body.collect().await.map_err(|e| {
                    error!(
                        "Failed to read UploadPart body: cache_key={}, upload_id={}, part_number={}, error={}",
                        cache_key, upload_id, part_number, e
                    );
                    ProxyError::HttpError(format!("Failed to read request body: {}", e))
                })?;
                let original_body_bytes = collected.to_bytes();

                debug!(
                    "Read UploadPart body: {} bytes for part {}, first 20 bytes: {:?}, last 20 bytes: {:?}",
                    original_body_bytes.len(),
                    part_number,
                    &original_body_bytes[..std::cmp::min(20, original_body_bytes.len())],
                    &original_body_bytes[original_body_bytes.len().saturating_sub(20)..]
                );

                // Check if body starts with chunked encoding (AWS CLI sends chunked data)
                // We'll strip it ONLY for caching, but send the original to S3
                // Format: "<hex-size>\r\n<data>\r\n"
                let cache_body_bytes = if original_body_bytes.len() > 10
                    && original_body_bytes[0].is_ascii_hexdigit()
                {
                    // Try to parse chunk size
                    if let Some(first_crlf) =
                        original_body_bytes.windows(2).position(|w| w == b"\r\n")
                    {
                        if let Ok(chunk_size_str) =
                            std::str::from_utf8(&original_body_bytes[..first_crlf])
                        {
                            if let Ok(chunk_size) = usize::from_str_radix(chunk_size_str, 16) {
                                let chunk_start = first_crlf + 2; // Skip "\r\n"
                                let chunk_end = chunk_start + chunk_size;

                                // Verify the chunk ends with "\r\n"
                                if chunk_end + 2 <= original_body_bytes.len()
                                    && &original_body_bytes[chunk_end..chunk_end + 2] == b"\r\n"
                                {
                                    debug!(
                                        "Detected chunked encoding: chunk_size={}, will strip {} header bytes and {} trailer bytes for caching only",
                                        chunk_size, chunk_start, original_body_bytes.len() - chunk_end
                                    );

                                    // Extract just the chunk data for caching
                                    let stripped =
                                        original_body_bytes.slice(chunk_start..chunk_end);

                                    // Log chunked decoding with sampling
                                    if self.log_sampler.should_log("chunked_decode") {
                                        debug!(
                                            part_number = part_number,
                                            original_bytes = original_body_bytes.len(),
                                            stripped_bytes = stripped.len(),
                                            "Chunked encoding stripped for caching"
                                        );
                                    }

                                    stripped
                                } else {
                                    original_body_bytes.clone()
                                }
                            } else {
                                original_body_bytes.clone()
                            }
                        } else {
                            original_body_bytes.clone()
                        }
                    } else {
                        original_body_bytes.clone()
                    }
                } else {
                    original_body_bytes.clone()
                };

                // Forward ORIGINAL body to S3 (with chunked encoding intact)
                let mut raw_request = Vec::new();
                let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");

                raw_request.extend_from_slice(
                    format!("{} {} {:?}\r\n", method, path_and_query, version).as_bytes(),
                );

                for (name, value) in headers.iter() {
                    if let Ok(value_str) = value.to_str() {
                        raw_request.extend_from_slice(
                            format!("{}: {}\r\n", name.as_str(), value_str).as_bytes(),
                        );
                    }
                }

                raw_request.extend_from_slice(b"\r\n");
                if !original_body_bytes.is_empty() {
                    raw_request.extend_from_slice(&original_body_bytes);
                }

                // Forward to S3
                let s3_response = self
                    .forward_raw_request_to_s3(raw_request, &target_host, target_ip, &tls_connector)
                    .await?;

                let status = s3_response.status();
                let response_headers = s3_response.headers().clone();

                // Read response body for both success and error cases
                let response_body_bytes = s3_response
                    .into_body()
                    .collect()
                    .await
                    .map(|collected| collected.to_bytes())
                    .unwrap_or_default();

                if status.is_success() {
                    // Extract ETag from response headers
                    let response_headers_map: HashMap<String, String> = response_headers
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                        .collect();

                    let etag = response_headers_map
                        .get("etag")
                        .or_else(|| response_headers_map.get("ETag"))
                        .cloned()
                        .unwrap_or_default();

                    // Cache the part as a range file (Requirement 5.1, 8.1, 9.3)
                    // Use the stripped body (without chunked encoding) for caching
                    if let Err(e) = self
                        .cache_upload_part(
                            &cache_key,
                            &upload_id,
                            part_number,
                            &cache_body_bytes,
                            &etag,
                        )
                        .await
                    {
                        error!(
                            "Failed to cache UploadPart: cache_key={}, upload_id={}, part_number={}, size={}, error_type={}, error={}",
                            cache_key, upload_id, part_number, cache_body_bytes.len(), Self::classify_error(&e), e
                        );
                    } else {
                        // Log successful UploadPart with concise format
                        let (bucket, key) = parse_cache_key(&cache_key);
                        info!(
                            "UploadPart: bucket={}, key={}, part={}, size={}",
                            bucket,
                            key,
                            part_number,
                            format_size(cache_body_bytes.len() as u64)
                        );
                    }
                } else {
                    // S3 returned error (Requirement 8.2, 9.3)
                    let error_body = String::from_utf8_lossy(&response_body_bytes);
                    error!(
                        "S3 error response for UploadPart: cache_key={}, upload_id={}, part_number={}, status={}, status_code={}, error_body={}",
                        cache_key, upload_id, part_number, status, status.as_u16(), error_body
                    );
                }

                // Rebuild response with the body we read
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
                    .map_err(|e| {
                        ProxyError::HttpError(format!("Failed to rebuild response: {}", e))
                    })?;

                Ok(rebuilt_response)
            }
            CacheDecision::Bypass(reason) => {
                log_bypass_decision(&cache_key, &reason);
                // Record bypassed PUT (Requirement 9.2)
                if let Some(metrics) = &self.metrics_manager {
                    metrics.read().await.record_bypassed_put().await;
                }
                forward_signed_request(req, &target_host, target_ip, &tls_connector, self.proxy_referer.as_deref()).await
            }
        }
    }

    /// Cache an upload part as a range file
    ///
    /// This method:
    /// 1. Stores part data as a range file in ranges/{bucket}/{XX}/{YYY}/
    /// 2. Updates upload.meta with part info (acquires lock first)
    /// 3. Tracks uploadId, partNumber, size, etag
    ///
    /// # Requirements
    ///
    /// - Requirement 2.1: Store part data as range file with part number suffix
    /// - Requirement 2.2: Track uploadId, partNumber, size, etag
    /// - Requirement 2.5: Store each part as a separate range file
    pub async fn cache_upload_part(
        &mut self,
        cache_key: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
        etag: &str,
    ) -> Result<()> {
        use crate::cache_types::{CachedPartInfo, MultipartUploadTracker};
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

        // Compress the part data
        let compression_result = self
            .compression_handler
            .compress_content_aware_with_metadata(data, cache_key);

        // Write part file atomically using temp file + rename
        let temp_part_file_path = part_file_path.with_extension("tmp");
        tokio::fs::write(&temp_part_file_path, &compression_result.data)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to write temporary part file: {}", e))
            })?;

        // Atomically rename to final location
        tokio::fs::rename(&temp_part_file_path, &part_file_path)
            .await
            .map_err(|e| ProxyError::CacheError(format!("Failed to rename part file: {}", e)))?;

        debug!(
            "Stored part {} data: {} bytes compressed to {} bytes at {:?}",
            part_number,
            data.len(),
            compression_result.compressed_size,
            part_file_path
        );

        // Create part info for tracker (path is deterministic from upload_id + part_number)
        let part_info = CachedPartInfo::new(
            part_number,
            data.len() as u64,
            etag.to_string(),
            compression_result.algorithm.clone(),
        );

        // Acquire lock and update upload.meta with part info
        let upload_meta_file = multipart_dir.join("upload.meta");
        let lock_file_path = multipart_dir.join("upload.lock");

        // Create lock file if it doesn't exist
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_file_path)
            .map_err(|e| ProxyError::CacheError(format!("Failed to open lock file: {}", e)))?;

        // Acquire exclusive lock
        lock_file.lock_exclusive().map_err(|e| {
            ProxyError::CacheError(format!("Failed to acquire lock for upload.meta: {}", e))
        })?;

        // Read existing tracker or create new one
        let mut tracker = if upload_meta_file.exists() {
            let meta_content = tokio::fs::read_to_string(&upload_meta_file)
                .await
                .map_err(|e| {
                    ProxyError::CacheError(format!("Failed to read upload metadata: {}", e))
                })?;

            MultipartUploadTracker::from_json(&meta_content).unwrap_or_else(|_| {
                // If parsing fails, create a new tracker
                warn!("Failed to parse existing upload.meta, creating new tracker");
                MultipartUploadTracker::new(upload_id.to_string(), cache_key.to_string())
            })
        } else {
            MultipartUploadTracker::new(upload_id.to_string(), cache_key.to_string())
        };

        // Add part to tracker (handles re-upload of same part number)
        tracker.add_part(part_info);

        // Write updated tracker
        let tracker_json = tracker.to_json().map_err(|e| {
            ProxyError::CacheError(format!("Failed to serialize upload tracker: {}", e))
        })?;

        tokio::fs::write(&upload_meta_file, tracker_json)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to write upload metadata: {}", e))
            })?;

        // Release lock (automatically released when lock_file is dropped)
        drop(lock_file);

        debug!(
            "Updated upload tracker: upload_id={}, part_number={}, total_parts={}, total_size={}",
            upload_id,
            part_number,
            tracker.parts.len(),
            tracker.total_size
        );

        Ok(())
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
        target_ip: IpAddr,
        tls_connector: Arc<TlsConnector>,
        upload_id: String,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        let (bucket, key) = parse_cache_key(&cache_key);

        // Buffer the request body before forwarding to S3 (Requirement 4.1)
        // This allows us to parse the XML to extract the requested parts list
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers = req.headers().clone();
        let version = req.version();

        // Read the request body
        let request_body = req.into_body();
        let request_body_bytes = request_body
            .collect()
            .await
            .map_err(|e| ProxyError::HttpError(format!("Failed to read request body: {}", e)))?
            .to_bytes();

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
            target_ip,
            &tls_connector,
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
            if let Err(e) = self
                .finalize_multipart_upload(
                    &cache_key,
                    &upload_id,
                    &etag,
                    &response_headers,
                    requested_parts.as_deref(),
                )
                .await
            {
                error!(
                    "CompleteMultipartUpload cache failed: bucket={}, key={}, error={}",
                    bucket, key, e
                );
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
        target_ip: IpAddr,
        tls_connector: Arc<TlsConnector>,
        upload_id: String,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        info!(
            "Handling AbortMultipartUpload: cache_key={}, upload_id={}",
            cache_key, upload_id
        );

        // Forward request to S3 first
        let s3_response =
            forward_signed_request(req, &target_host, target_ip, &tls_connector, self.proxy_referer.as_deref()).await?;

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
        requested_parts: Option<&[RequestedPart]>,
    ) -> Result<()> {
        use crate::cache_types::{
            MultipartUploadTracker, NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState,
        };
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

        // Read the upload tracker
        let upload_meta_file = multipart_dir.join("upload.meta");
        let tracker_result = tokio::fs::read_to_string(&upload_meta_file).await;

        let tracker_content = match tracker_result {
            Ok(content) => content,
            Err(e) => {
                warn!(
                    "CompleteMultipartUpload succeeded on S3 but failed to read upload tracker: cache_key={}, upload_id={}, error={}, cleaning up and skipping cache finalization",
                    cache_key, upload_id, e
                );
                drop(lock_file);
                self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                    .await;
                return Ok(());
            }
        };

        let tracker = match MultipartUploadTracker::from_json(&tracker_content) {
            Ok(tracker) => tracker,
            Err(e) => {
                warn!(
                    "CompleteMultipartUpload succeeded on S3 but failed to parse upload tracker: cache_key={}, upload_id={}, error={}, cleaning up and skipping cache finalization",
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
        // If requested_parts is None, use all cached parts (backward compatibility)
        let requested_part_numbers: std::collections::HashSet<u32> = match requested_parts {
            Some(parts) => parts.iter().map(|p| p.part_number).collect(),
            None => tracker.parts.iter().map(|p| p.part_number).collect(),
        };

        // Filter cached parts to only those in the request and sort by part number
        let all_cached_parts = tracker.get_sorted_parts();
        let filtered_parts: Vec<&crate::cache_types::CachedPartInfo> = all_cached_parts
            .into_iter()
            .filter(|p| requested_part_numbers.contains(&p.part_number))
            .collect();

        // Check if any requested parts are not cached locally (Requirement 5.4)
        // If a requested part is not in our cache, skip cache finalization
        if let Some(parts) = requested_parts {
            let cached_part_numbers: std::collections::HashSet<u32> =
                tracker.parts.iter().map(|p| p.part_number).collect();
            let missing_requested: Vec<u32> = parts
                .iter()
                .filter(|p| !cached_part_numbers.contains(&p.part_number))
                .map(|p| p.part_number)
                .collect();

            if !missing_requested.is_empty() {
                warn!(
                    "CompleteMultipartUpload succeeded on S3 but requested parts {:?} not cached locally: cache_key={}, upload_id={}, cleaning up and skipping cache finalization",
                    missing_requested, cache_key, upload_id
                );
                drop(lock_file);
                self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                    .await;
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
        if let Some(parts) = requested_parts {
            for requested_part in parts {
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

            // Get file size for compression info (file is already compressed from cache_upload_part)
            let compressed_size = tokio::fs::metadata(&new_range_file_path)
                .await
                .map(|m| m.len())
                .unwrap_or(part_info.size);

            // Create range spec using the actual compression algorithm from cache_upload_part
            let range_spec = RangeSpec::new(
                *start,
                *end,
                range_file_relative_path,
                part_info.compression_algorithm.clone(), // Use actual algorithm from cache_upload_part
                compressed_size,
                part_info.size,
            );

            range_specs.push(range_spec);

            debug!(
                "Created range for part {}: start={}, end={}, size={} bytes",
                part_number, start, end, part_info.size
            );
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

    /// Clean up incomplete multipart cache data
    ///
    /// This method removes partial cache data when CompleteMultipartUpload succeeds on S3
    /// but the proxy doesn't have complete local state. This prevents serving corrupted
    /// data from incomplete cache entries.
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

    /// Spawn a background task to handle cache writing asynchronously with capacity checking
    ///
    /// This function spawns a tokio task that:
    /// 1. Creates a cache writer with capacity limit
    /// 2. Writes the body data to cache
    /// 3. Waits for the S3 result
    /// 4. Commits or discards based on S3 success/failure
    ///
    /// # Arguments
    ///
    /// * `cache_key` - Cache key (will be sanitized internally)
    /// * `body_data` - The request body data (already read)
    /// * `s3_result_rx` - Channel receiver for S3 operation result
    /// * `cache_dir` - Cache directory path
    /// * `compression_handler` - Compression handler for cache writes
    /// * `capacity_limit` - Optional capacity limit for streaming check
    /// * `request_headers` - Request headers for metadata extraction
    /// * `metrics` - Optional metrics manager
    /// * `cache_manager` - Optional cache manager for HEAD cache invalidation
    ///
    /// # Requirements
    ///
    /// - Requirement 4.3: Handle cache operations in background tasks
    /// - Requirement 4.4: Commit cache on S3 success
    /// - Requirement 4.5: Discard cache on S3 failure
    /// - Requirement 1.2: Log warnings on cache failures
    /// - Requirement 1.4: Record metrics on cache failures
    /// - Requirement 2.3: Stream to cache until completion or capacity reached
    /// - Requirement 2.4: Discard cache if capacity exceeded during streaming
    ///
    /// Updated for write-through-cache-finalization:
    /// - Requirement 1.1: Store object data as single range (0 to content-length-1)
    /// - Requirement 1.2: Create metadata with ETag and Content-Type from S3 response (Last-Modified learned on first cache-miss GET or first HEAD after PUT)
    /// - Requirement 1.3: Set write cache TTL (default: 1 day)
    /// - Requirement 9.1: Don't cache on S3 failure
    fn spawn_cache_write_task_with_capacity(
        cache_key: String,
        body_data: Bytes,
        s3_result_rx: tokio::sync::oneshot::Receiver<Result<ResponseInfo>>,
        _cache_dir: PathBuf,
        _compression_handler: CompressionHandler,
        capacity_limit: Option<u64>,
        request_headers: HashMap<String, String>,
        metrics: Option<Arc<RwLock<MetricsManager>>>,
        cache_manager: Option<Arc<crate::cache::CacheManager>>,
        s3_client: Option<Arc<S3Client>>,
    ) {
        tokio::spawn(async move {
            let start_time = std::time::Instant::now();
            let body_len = body_data.len() as u64;

            // Check capacity limit before proceeding
            if let Some(limit) = capacity_limit {
                if body_len > limit {
                    warn!(
                        "PUT body exceeds capacity limit, bypassing cache: cache_key={}, size={}, limit={}",
                        cache_key, body_len, limit
                    );
                    if let Some(m) = &metrics {
                        m.read().await.record_bypassed_put().await;
                    }
                    return;
                }
            }

            // Wait for S3 result first (Requirement 9.1: Don't cache on S3 failure)
            match s3_result_rx.await {
                Ok(Ok(response)) => {
                    let status = response.status();

                    if status.is_success() {
                        // S3 success - store as single range with write cache metadata
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

                        let content_type = request_headers
                            .get("content-type")
                            .or_else(|| request_headers.get("Content-Type"))
                            .cloned();

                        // Store directly as range using CacheManager
                        // (Requirements 3.1, 3.2, 3.3 - unified storage only)
                        if let Some(cache_mgr) = &cache_manager {
                            if let Err(e) = cache_mgr
                                .invalidate_cache_unified_for_operation(&cache_key, "PUT")
                                .await
                            {
                                warn!(
                                    "Failed to invalidate cache before PUT caching: cache_key={}, error={}",
                                    cache_key, e
                                );
                            }

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
                                    let effective_ttl = cache_mgr.get_effective_put_ttl(&cache_key).await;
                                    info!(
                                        "Successfully stored PUT as write-cached range (with capacity check): cache_key={}, size={} bytes, ttl={:?}",
                                        cache_key, body_len, effective_ttl
                                    );
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
                        debug!(
                            "S3 error response, not caching PUT: cache_key={}, status={}",
                            cache_key, status
                        );
                    }
                }
                Ok(Err(e)) => {
                    debug!(
                        "S3 error, not caching PUT: cache_key={}, error={}",
                        cache_key, e
                    );
                }
                Err(e) => {
                    warn!(
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
        s3_client: Option<Arc<S3Client>>,
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
                                    let effective_ttl = cache_mgr.get_effective_put_ttl(&cache_key).await;

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
                    warn!(
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

    /// Classify error type for detailed logging
    ///
    /// # Requirements
    ///
    /// - Requirement 9.3: Log detailed error information
    fn classify_error(error: &ProxyError) -> &'static str {
        match error {
            ProxyError::IoError(msg) if msg.contains("Disk full") => "disk_full",
            ProxyError::IoError(_) => "io_error",
            ProxyError::HttpError(_) => "http_error",
            ProxyError::ConnectionError(_) => "connection_error",
            ProxyError::CacheError(msg) if msg.contains("Capacity limit exceeded") => {
                "capacity_exceeded"
            }
            ProxyError::CacheError(msg) if msg.contains("Disk full") => "disk_full",
            ProxyError::CacheError(_) => "cache_error",
            ProxyError::CompressionError(_) => "compression_error",
            ProxyError::TlsError(_) => "tls_error",
            ProxyError::ConfigError(_) => "config_error",
            ProxyError::DnsError(_) => "dns_error",
            ProxyError::TimeoutError(_) => "timeout_error",
            ProxyError::SerializationError(_) => "serialization_error",
            ProxyError::LockError(_) => "lock_error",
            ProxyError::LockContention(_) => "lock_contention",
            ProxyError::InvalidRequest(_) => "invalid_request",
            ProxyError::S3Error(_) => "s3_error",
            ProxyError::InternalError(_) => "internal_error",
            ProxyError::ServiceUnavailable(_) => "service_unavailable",
            ProxyError::InvalidRange(_) => "invalid_range",
            ProxyError::SystemError(_) => "system_error",
            ProxyError::RetryAfter(_) => "retry_after",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_handler(temp_dir: &TempDir) -> SignedPutHandler {
        let compression_handler = CompressionHandler::new(1024, true);
        SignedPutHandler::new(
            temp_dir.path().to_path_buf(),
            compression_handler,
            0,
            10 * 1024 * 1024, // 10MB capacity
            None,
        )
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
        let upload_meta_file = multipart_dir.join("upload.meta");
        assert!(upload_meta_file.exists());

        // Read and verify tracker
        let meta_content = std::fs::read_to_string(&upload_meta_file).unwrap();
        let tracker: crate::cache_types::MultipartUploadTracker =
            serde_json::from_str(&meta_content).unwrap();

        assert_eq!(tracker.upload_id, upload_id);
        assert_eq!(tracker.cache_key, cache_key);
        assert_eq!(tracker.parts.len(), 1);
        assert_eq!(tracker.parts[0].part_number, part_number);
        assert_eq!(tracker.parts[0].size, data.len() as u64);
        assert_eq!(tracker.parts[0].etag, etag);
        assert_eq!(tracker.total_size, data.len() as u64);
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
        let upload_meta_file = multipart_dir.join("upload.meta");
        assert!(upload_meta_file.exists());

        // Read and verify tracker
        let meta_content = std::fs::read_to_string(&upload_meta_file).unwrap();
        let tracker: crate::cache_types::MultipartUploadTracker =
            serde_json::from_str(&meta_content).unwrap();

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
        let upload_meta_file = multipart_dir.join("upload.meta");
        assert!(upload_meta_file.exists());

        // Verify part files exist in the upload directory
        let meta_content = std::fs::read_to_string(&upload_meta_file).unwrap();
        let tracker: crate::cache_types::MultipartUploadTracker =
            serde_json::from_str(&meta_content).unwrap();

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

        // Manually add part 2 to the tracker without creating the actual file
        // This simulates the scenario where another proxy instance cached part 2
        let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
        let upload_meta_file = multipart_dir.join("upload.meta");

        // Read existing tracker
        let meta_content = std::fs::read_to_string(&upload_meta_file).unwrap();
        let mut tracker: crate::cache_types::MultipartUploadTracker =
            serde_json::from_str(&meta_content).unwrap();

        // Add part 2 to tracker without creating the file (simulates missing part)
        let part2_info = crate::cache_types::CachedPartInfo {
            part_number: 2,
            size: 20,
            etag: "test-etag-2".to_string(),
            compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
        };
        tracker.parts.push(part2_info);
        tracker.total_size = 40; // 20 + 20

        // Write updated tracker
        let updated_meta = serde_json::to_string_pretty(&tracker).unwrap();
        std::fs::write(&upload_meta_file, updated_meta).unwrap();

        // Verify part 1 file exists in upload dir but part 2 doesn't
        let part1_file = multipart_dir.join("part1.bin");
        assert!(part1_file.exists());

        let part2_file = multipart_dir.join("part2.bin");
        assert!(!part2_file.exists());

        // Attempt to finalize - should skip caching and clean up
        let result = handler
            .finalize_multipart_upload(cache_key, upload_id, etag, &response_headers, None)
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

        // Attempt to finalize - should skip caching gracefully
        let result = handler
            .finalize_multipart_upload(cache_key, upload_id, etag, &response_headers, None)
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
        assert_eq!(normalize_etag("\"a54357aff0632cce46d942af68356b38\""), "a54357aff0632cce46d942af68356b38");
    }

    /// Test normalize_etag handles ETags without quotes
    /// Requirements: 9.1
    #[test]
    fn test_normalize_etag_without_quotes() {
        assert_eq!(normalize_etag("abc123"), "abc123");
        assert_eq!(normalize_etag("a54357aff0632cce46d942af68356b38"), "a54357aff0632cce46d942af68356b38");
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
                Some(&requested_parts),
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
                Some(&requested_parts),
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
                Some(&requested_parts),
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

        assert!(part1_file.exists(), "Part 1 file should exist before finalization");
        assert!(part2_file.exists(), "Part 2 file should exist before finalization");
        assert!(part3_file.exists(), "Part 3 file should exist before finalization");

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
                Some(&requested_parts),
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
        assert!(metadata.object_metadata.part_ranges.get(&2).is_none());
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
                Some(&requested_parts),
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
                Some(&requested_parts),
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
            "Part 1 range should be 0-{}", part1_end
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&2),
            Some(&(part2_start, part2_end)),
            "Part 2 range should be {}-{}", part2_start, part2_end
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&3),
            Some(&(part3_start, part3_end)),
            "Part 3 range should be {}-{}", part3_start, part3_end
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&4),
            Some(&(part4_start, part4_end)),
            "Part 4 range should be {}-{}", part4_start, part4_end
        );

        // Verify total content length
        let expected_total = part1_size + part2_size + part3_size + part4_size;
        assert_eq!(
            metadata.object_metadata.content_length,
            expected_total,
            "Total content length should be {} bytes", expected_total
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
                Some(&requested_parts),
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
        assert!(metadata.object_metadata.part_ranges.get(&2).is_none());
        assert!(metadata.object_metadata.part_ranges.get(&4).is_none());

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
                Some(&requested_parts),
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
        assert!(metadata.object_metadata.part_ranges.get(&1).is_none());
        assert!(metadata.object_metadata.part_ranges.get(&3).is_none());
        assert!(metadata.object_metadata.part_ranges.get(&5).is_none());

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
                Some(&requested_parts),
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

            // Verify upload.meta exists
            let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
            let upload_meta_file = multipart_dir.join("upload.meta");

            if !upload_meta_file.exists() {
                return TestResult::failed();
            }

            // Read and verify tracker
            let meta_content = match std::fs::read_to_string(&upload_meta_file) {
                Ok(c) => c,
                Err(_) => return TestResult::failed(),
            };

            let tracker: crate::cache_types::MultipartUploadTracker =
                match serde_json::from_str(&meta_content) {
                    Ok(t) => t,
                    Err(_) => return TestResult::failed(),
                };

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
            let final_etag = "\"abc123-5\""; // Multipart ETag format

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

            // Now finalize the multipart upload
            let test_headers = std::collections::HashMap::new(); // Empty headers for test
            let result = handler
                .finalize_multipart_upload(cache_key, upload_id, final_etag, &test_headers, None)
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
            let upload_meta_file = multipart_dir.join("upload.meta");

            if !upload_meta_file.exists() {
                return TestResult::failed();
            }

            // Read tracker to get part file paths
            let meta_content = match std::fs::read_to_string(&upload_meta_file) {
                Ok(c) => c,
                Err(_) => return TestResult::failed(),
            };

            let tracker: crate::cache_types::MultipartUploadTracker =
                match serde_json::from_str(&meta_content) {
                    Ok(t) => t,
                    Err(_) => return TestResult::failed(),
                };

            // Verify part files exist in the upload directory before cleanup
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

}
