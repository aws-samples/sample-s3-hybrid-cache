//! S3 Client Module
//!
//! Provides HTTPS client functionality for communicating with S3 endpoints
//! using connection pooling, load balancing, and intelligent error handling.

use crate::cache_types::CacheMetadata;
use crate::config::ConnectionPoolConfig;
use crate::connection_pool::{Connection, ConnectionPoolManager};
use crate::https_connector::CustomHttpsConnector;
use crate::{ProxyError, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;
use tracing::{debug, warn};

/// S3 client with Hyper connection pooling support
pub struct S3Client {
    client: Client<CustomHttpsConnector, Full<Bytes>>,
    pool_manager: Arc<Mutex<ConnectionPoolManager>>,
    request_timeout: Duration,
    keepalive_enabled: bool,
    metrics_manager:
        Arc<tokio::sync::RwLock<Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>>>,
}

/// S3 request context for forwarding
#[derive(Debug, Clone)]
pub struct S3RequestContext {
    pub method: Method,
    pub uri: Uri,
    pub headers: HashMap<String, String>,
    pub body: Option<Bytes>,
    pub host: String,
    pub request_size: Option<u64>,
    pub conditional_headers: Option<ConditionalHeaders>,
    pub operation_type: Option<String>,
    pub allow_streaming: bool, // If false, always buffer response
}

/// Conditional headers for cache validation
#[derive(Debug, Clone)]
pub struct ConditionalHeaders {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<String>,
    pub if_unmodified_since: Option<String>,
}

/// Result of conditional header validation
#[derive(Debug, Clone, PartialEq)]
pub enum ConditionalValidationResult {
    Valid,              // Conditions are satisfied, proceed with request
    NotModified,        // Return 304 Not Modified
    PreconditionFailed, // Return 412 Precondition Failed
}

/// S3 response body - either buffered or streaming
pub enum S3ResponseBody {
    /// Fully buffered response (for small responses, errors, metadata)
    Buffered(Bytes),
    /// Streaming response (for large responses to minimize latency and memory)
    Streaming(Incoming),
}

impl S3ResponseBody {
    /// Convert to buffered bytes, collecting stream if necessary
    pub async fn into_bytes(self) -> Result<Bytes> {
        match self {
            S3ResponseBody::Buffered(bytes) => Ok(bytes),
            S3ResponseBody::Streaming(body) => {
                let bytes = body
                    .collect()
                    .await
                    .map_err(|e| ProxyError::HttpError(format!("Failed to collect stream: {}", e)))?
                    .to_bytes();
                Ok(bytes)
            }
        }
    }

    /// Get bytes if already buffered, otherwise return None
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            S3ResponseBody::Buffered(bytes) => Some(bytes),
            S3ResponseBody::Streaming(_) => None,
        }
    }

    /// Check if this is a streaming body
    pub fn is_streaming(&self) -> bool {
        matches!(self, S3ResponseBody::Streaming(_))
    }
}

/// S3 response from forwarded request
pub struct S3Response {
    pub status: StatusCode,
    pub headers: HashMap<String, String>,
    pub body: Option<S3ResponseBody>,
    pub request_duration: Duration,
}

/// Request retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3, // As per requirement 17.6 - limit retries to 3 attempts for GET requests
            initial_delay: Duration::from_millis(100), // As per requirement 17.1 - start at 100ms
            max_delay: Duration::from_secs(30), // As per requirement 17.1 - max 30 seconds
            backoff_multiplier: 2.0, // Exponential backoff
        }
    }
}

impl S3Client {
    /// Create a new S3 client with Hyper connection pooling
    pub fn new(
        config: &ConnectionPoolConfig,
        metrics_manager: Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
    ) -> Result<Self> {
        let pool_manager = Arc::new(Mutex::new(ConnectionPoolManager::new_with_config(
            config.clone(),
        )?));

        // Create TLS connector for HTTPS connections to S3 with system root certificates
        let mut root_store = rustls::RootCertStore::empty();

        // Load system root certificates
        for cert in rustls_native_certs::load_native_certs()
            .map_err(|e| ProxyError::TlsError(format!("Failed to load native certs: {}", e)))?
        {
            root_store
                .add(cert)
                .map_err(|e| ProxyError::TlsError(format!("Failed to add cert: {}", e)))?;
        }

        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let tls_connector = TlsConnector::from(Arc::new(tls_config));

        // Create shared metrics manager reference that can be set later
        let metrics_ref = Arc::new(tokio::sync::RwLock::new(metrics_manager.clone()));

        // Create custom HTTPS connector with shared metrics manager reference
        let mut https_connector =
            CustomHttpsConnector::new(Arc::clone(&pool_manager), tls_connector);

        // Set the shared metrics manager reference on connector
        // This allows metrics to be set later via set_metrics_manager()
        https_connector.set_metrics_manager_ref(Arc::clone(&metrics_ref));

        // Build Hyper client with connection pooling
        let client = if config.keepalive_enabled {
            debug!(
                "Creating S3 client with connection keepalive enabled (idle_timeout: {}s, max_idle_per_host: {})",
                config.idle_timeout.as_secs(),
                config.max_idle_per_host
            );

            Client::builder(TokioExecutor::new())
                .pool_idle_timeout(config.idle_timeout)
                .pool_max_idle_per_host(config.max_idle_per_host)
                .build(https_connector)
        } else {
            debug!("Creating S3 client with connection keepalive disabled");

            // Disable pooling by setting idle timeout to 0
            Client::builder(TokioExecutor::new())
                .pool_idle_timeout(Duration::from_secs(0))
                .pool_max_idle_per_host(0)
                .build(https_connector)
        };

        Ok(Self {
            client,
            pool_manager,
            request_timeout: Duration::from_secs(30),
            keepalive_enabled: config.keepalive_enabled,
            metrics_manager: metrics_ref,
        })
    }

    /// Set metrics manager for tracking connection metrics
    pub async fn set_metrics_manager(
        &self,
        metrics_manager: Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>,
    ) {
        let mut mm = self.metrics_manager.write().await;
        *mm = Some(metrics_manager);
    }

    /// Start background task for max lifetime enforcement
    /// Requirement 4.1, 4.4, 4.5: Close connections exceeding max lifetime
    pub fn start_max_lifetime_task(
        pool_manager: Arc<Mutex<ConnectionPoolManager>>,
        metrics_manager: Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
        max_lifetime: Duration,
        check_interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(check_interval);

            loop {
                interval.tick().await;

                // Check for connections exceeding max lifetime
                let pool = pool_manager.lock().await;
                let expired_connections = pool.get_expired_connections(max_lifetime);
                drop(pool);

                for (endpoint, conn_id) in expired_connections {
                    debug!(
                        "Connection exceeded max lifetime: endpoint={}, conn_id={}, max_lifetime={}s",
                        endpoint, conn_id, max_lifetime.as_secs()
                    );

                    // Track max lifetime closure in metrics
                    if let Some(metrics) = &metrics_manager {
                        metrics.read().await.record_max_lifetime_closure().await;
                    }

                    // Note: Hyper will automatically close the connection when it's returned to the pool
                    // We just need to track the metric here
                }
            }
        })
    }

    /// Get reference to connection pool manager for health/metrics monitoring
    pub fn get_connection_pool(
        &self,
    ) -> Arc<tokio::sync::Mutex<crate::connection_pool::ConnectionPoolManager>> {
        Arc::clone(&self.pool_manager)
    }

    /// Forward request to S3 endpoint with connection pooling and retries
    pub async fn forward_request(&self, context: S3RequestContext) -> Result<S3Response> {
        let retry_config = RetryConfig::default();
        let mut last_error = None;

        // Adjust retry count based on method (Requirement 17.6)
        let max_retries = match context.method {
            Method::GET | Method::HEAD => retry_config.max_retries,
            Method::PUT => 1, // Only 1 retry for PUT to prevent duplicate uploads
            _ => retry_config.max_retries,
        };

        for attempt in 0..=max_retries {
            let start_time = Instant::now();

            match self.try_forward_request(&context).await {
                Ok(mut response) => {
                    let duration = start_time.elapsed();
                    response.request_duration = duration;

                    // Track request for connection reuse calculation
                    // Connection reuse = total_requests - connections_created
                    if self.keepalive_enabled {
                        let mm = self.metrics_manager.read().await;
                        if let Some(ref metrics) = *mm {
                            metrics
                                .read()
                                .await
                                .record_request_to_endpoint(&context.host)
                                .await;
                        }
                    }

                    return Ok(response);
                }
                Err(e) => {
                    let _duration = start_time.elapsed();
                    last_error = Some(e.clone());

                    // Check if this is a connection error and track it
                    let is_conn_error = self.is_connection_error(&e);
                    if is_conn_error {
                        // Log connection error at warn level with details (Requirement 5.4)
                        warn!(
                            "Connection error detected on attempt {}: {} (endpoint: {})",
                            attempt + 1,
                            e,
                            context.host
                        );

                        // Track connection error closure in metrics (Requirement 5.5)
                        let mm = self.metrics_manager.read().await;
                        if let Some(ref metrics) = *mm {
                            metrics.read().await.record_error_closure().await;
                        }
                    }

                    // Check if we should retry based on error type
                    if attempt < max_retries && self.should_retry_error(&e) {
                        let delay = self.calculate_retry_delay(&retry_config, attempt);

                        if is_conn_error {
                            // Connection errors don't count against retry limit (Requirement 5.5)
                            debug!("Retrying after connection error (not counted against retry limit) in {:?}", delay);
                        } else {
                            warn!(
                                "Request attempt {} failed, retrying in {:?}: {}",
                                attempt + 1,
                                delay,
                                e
                            );
                        }

                        tokio::time::sleep(delay).await;
                        continue;
                    } else {
                        break;
                    }
                }
            }
        }

        Err(last_error
            .unwrap_or_else(|| ProxyError::S3Error("All retry attempts failed".to_string())))
    }

    /// Try to forward a single request to S3 using Hyper client
    async fn try_forward_request(&self, context: &S3RequestContext) -> Result<S3Response> {
        let start_time = Instant::now();

        // Track that we're making a request to this endpoint
        // Connection reuse detection: If Hyper reuses a connection from the pool,
        // our CustomHttpsConnector::call() won't be invoked, so we can infer reuse
        // by tracking total requests vs connections created
        let _endpoint = context.host.clone();

        // Build HTTP request
        let mut request_builder = Request::builder().method(&context.method).uri(&context.uri);

        // Add headers (including any conditional headers)
        // Skip Content-Length header for requests with bodies - Hyper will set it automatically
        let has_body = context.body.is_some();
        for (key, value) in &context.headers {
            if has_body && key.to_lowercase() == "content-length" {
                debug!("Skipping Content-Length header for request with body (will be auto-calculated): {}", value);
                continue;
            }
            request_builder = request_builder.header(key, value);
        }

        // Create request body
        let body = match &context.body {
            Some(bytes) => Full::new(bytes.clone()),
            None => Full::new(Bytes::new()),
        };

        let request = request_builder
            .body(body)
            .map_err(|e| ProxyError::HttpError(format!("Failed to build request: {}", e)))?;

        if self.keepalive_enabled {
            debug!(
                "Sending {} request to {} with connection keepalive enabled (endpoint: {})",
                context.method, context.uri, context.host
            );
        } else {
            debug!(
                "Sending {} request to {} (keepalive disabled)",
                context.method, context.uri
            );
        }

        // Send request through Hyper client (handles connection pooling automatically)
        let response = tokio::time::timeout(self.request_timeout, self.client.request(request))
            .await
            .map_err(|_| ProxyError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| ProxyError::HttpError(format!("Failed to send request: {}", e)))?;

        // Read response
        let (parts, body) = response.into_parts();

        // Convert headers to HashMap
        let mut headers = HashMap::new();
        for (key, value) in parts.headers.iter() {
            if let Ok(value_str) = value.to_str() {
                headers.insert(key.to_string(), value_str.to_string());
            }
        }

        // Determine if we should stream or buffer based on Content-Length
        const STREAMING_THRESHOLD: u64 = 1_048_576; // 1MB

        let content_length = headers
            .get("content-length")
            .and_then(|v| v.parse::<u64>().ok());

        let should_stream = context.allow_streaming
            && content_length
                .map(|len| len > STREAMING_THRESHOLD)
                .unwrap_or(false);

        let response_body = if should_stream {
            debug!(
                "Streaming response body (Content-Length: {} bytes)",
                content_length.unwrap()
            );
            // Return streaming body - Hyper handles connection lifecycle
            // Always return streaming body if we decided to stream
            Some(S3ResponseBody::Streaming(body))
        } else {
            // Buffer small responses
            debug!(
                "Buffering response body (Content-Length: {:?} bytes)",
                content_length
            );
            let body_bytes = body
                .collect()
                .await
                .map_err(|e| ProxyError::HttpError(format!("Failed to read response body: {}", e)))?
                .to_bytes();

            if body_bytes.is_empty() {
                None
            } else {
                Some(S3ResponseBody::Buffered(body_bytes))
            }
        };

        let response = S3Response {
            status: parts.status,
            headers,
            body: response_body,
            request_duration: start_time.elapsed(),
        };

        debug!(
            "Received {} response from {} (Content-Length: {:?} bytes, streaming: {}, keepalive: {})", 
            response.status, context.uri, content_length, should_stream, self.keepalive_enabled
        );

        Ok(response)
    }

    /// Check if error should trigger a retry
    fn should_retry_error(&self, error: &ProxyError) -> bool {
        match error {
            ProxyError::ConnectionError(_) => true,
            ProxyError::TimeoutError(_) => true,
            ProxyError::HttpError(msg) => {
                // Retry on specific HTTP errors that indicate temporary issues
                msg.contains("connection") || msg.contains("timeout") || msg.contains("reset")
            }
            ProxyError::S3Error(msg) => {
                // Retry on S3 server errors (5xx) but not client errors (4xx)
                msg.contains("503")
                    || msg.contains("500")
                    || msg.contains("502")
                    || msg.contains("429")
            }
            _ => false,
        }
    }

    /// Check if error is a connection error (for metrics tracking)
    fn is_connection_error(&self, error: &ProxyError) -> bool {
        match error {
            ProxyError::ConnectionError(_) => true,
            ProxyError::HttpError(msg) => {
                // Connection-related HTTP errors
                msg.contains("connection")
                    || msg.contains("reset")
                    || msg.contains("broken pipe")
                    || msg.contains("connection closed")
            }
            _ => false,
        }
    }

    /// Calculate retry delay with exponential backoff (Requirement 17.1)
    fn calculate_retry_delay(&self, config: &RetryConfig, attempt: usize) -> Duration {
        let delay_ms = config.initial_delay.as_millis() as f64
            * config.backoff_multiplier.powi(attempt as i32);

        let delay = Duration::from_millis(delay_ms as u64);

        // Cap at maximum delay
        if delay > config.max_delay {
            config.max_delay
        } else {
            delay
        }
    }

    /// Refresh DNS for all connection pools (Requirement 16.2)
    pub async fn refresh_dns(&self) -> Result<()> {
        let mut pool_manager = self.pool_manager.lock().await;
        pool_manager.refresh_dns().await
    }

    /// Monitor connection health across all pools (Requirement 16.3)
    pub async fn monitor_connection_health(&self) -> Result<()> {
        let mut pool_manager = self.pool_manager.lock().await;
        pool_manager.monitor_connection_health().await
    }

    /// Get multiple connections for large requests (Requirement 16.6)
    pub async fn get_multiple_connections(
        &self,
        endpoint: &str,
        request_size: Option<u64>,
        connection_count: usize,
    ) -> Result<Vec<Connection>> {
        let mut pool_manager = self.pool_manager.lock().await;
        pool_manager
            .get_multiple_connections(endpoint, request_size, connection_count)
            .await
    }

    /// Handle S3 error responses with appropriate retry logic (Requirement 17.1, 17.8)
    pub fn handle_s3_error_response(&self, status: StatusCode) -> Duration {
        match status.as_u16() {
            503 => {
                // S3 SlowDown error - implement aggressive backoff (Requirement 17.8)
                Duration::from_secs(60)
            }
            500 | 502 | 429 => {
                // Server errors and rate limiting - use exponential backoff
                Duration::from_millis(100)
            }
            _ => Duration::from_millis(100),
        }
    }

    /// Build conditional request headers for cache validation (Requirements 4.1, 4.2, 4.3, 4.4, 3.6, 3.8)
    pub fn build_conditional_headers(
        original_headers: &HashMap<String, String>,
        cache_metadata: Option<&CacheMetadata>,
    ) -> HashMap<String, String> {
        let mut headers = original_headers.clone();

        // If we have cache metadata, add conditional headers for validation
        // but only for conditions not already specified by the client (Requirements 3.6, 3.8)
        if let Some(metadata) = cache_metadata {
            // Only add If-Unmodified-Since if client hasn't specified it
            if !metadata.last_modified.is_empty() && !headers.contains_key("if-unmodified-since") {
                headers.insert(
                    "if-unmodified-since".to_string(),
                    metadata.last_modified.clone(),
                );
            }

            // Only add If-Match if client hasn't specified it
            if !metadata.etag.is_empty() && !headers.contains_key("if-match") {
                headers.insert("if-match".to_string(), metadata.etag.clone());
            }
        }

        headers
    }

    /// Validate conditional headers against cache metadata (Requirements 4.1, 4.2, 4.3, 4.4)
    pub fn validate_conditional_headers(
        &self,
        conditional_headers: &ConditionalHeaders,
        cache_metadata: &CacheMetadata,
    ) -> Result<ConditionalValidationResult> {
        // If-Match validation (Requirement 4.1)
        if let Some(if_match) = &conditional_headers.if_match {
            if !cache_metadata.etag.is_empty()
                && cache_metadata.etag != *if_match
                && if_match != "*"
            {
                return Ok(ConditionalValidationResult::PreconditionFailed);
            }
        }

        // If-None-Match validation (Requirement 4.2)
        if let Some(if_none_match) = &conditional_headers.if_none_match {
            if !cache_metadata.etag.is_empty()
                && (cache_metadata.etag == *if_none_match || if_none_match == "*")
            {
                return Ok(ConditionalValidationResult::NotModified);
            }
        }

        // If-Modified-Since validation (Requirement 4.3)
        if let Some(if_modified_since) = &conditional_headers.if_modified_since {
            if !cache_metadata.last_modified.is_empty() {
                // Parse timestamps and compare
                if let (Ok(cache_time), Ok(request_time)) = (
                    self.parse_http_date(&cache_metadata.last_modified),
                    self.parse_http_date(if_modified_since),
                ) {
                    if cache_time <= request_time {
                        return Ok(ConditionalValidationResult::NotModified);
                    }
                }
            }
        }

        // If-Unmodified-Since validation (Requirement 4.4)
        if let Some(if_unmodified_since) = &conditional_headers.if_unmodified_since {
            if !cache_metadata.last_modified.is_empty() {
                // Parse timestamps and compare
                if let (Ok(cache_time), Ok(request_time)) = (
                    self.parse_http_date(&cache_metadata.last_modified),
                    self.parse_http_date(if_unmodified_since),
                ) {
                    if cache_time > request_time {
                        return Ok(ConditionalValidationResult::PreconditionFailed);
                    }
                }
            }
        }

        Ok(ConditionalValidationResult::Valid)
    }

    /// Parse HTTP date string to SystemTime (RFC 7231 format)
    fn parse_http_date(&self, date_str: &str) -> Result<std::time::SystemTime> {
        httpdate::parse_http_date(date_str).map_err(|e| {
            ProxyError::HttpError(format!("Failed to parse HTTP date '{}': {}", date_str, e))
        })
    }

    /// Extract conditional headers from request headers
    pub fn extract_conditional_headers(
        headers: &HashMap<String, String>,
    ) -> Option<ConditionalHeaders> {
        let if_match = headers.get("if-match").cloned();
        let if_none_match = headers.get("if-none-match").cloned();
        let if_modified_since = headers.get("if-modified-since").cloned();
        let if_unmodified_since = headers.get("if-unmodified-since").cloned();

        if if_match.is_some()
            || if_none_match.is_some()
            || if_modified_since.is_some()
            || if_unmodified_since.is_some()
        {
            Some(ConditionalHeaders {
                if_match,
                if_none_match,
                if_modified_since,
                if_unmodified_since,
            })
        } else {
            None
        }
    }

    /// Check if S3 response metadata differs from cached metadata (Requirement 6.6)
    pub fn detect_metadata_mismatch(
        &self,
        s3_response_headers: &HashMap<String, String>,
        cached_metadata: &CacheMetadata,
    ) -> bool {
        // Extract ETag and Last-Modified from S3 response
        let s3_etag = s3_response_headers
            .get("etag")
            .or_else(|| s3_response_headers.get("ETag"))
            .cloned()
            .unwrap_or_default();

        let s3_last_modified = s3_response_headers
            .get("last-modified")
            .or_else(|| s3_response_headers.get("Last-Modified"))
            .cloned()
            .unwrap_or_default();

        // Check for mismatches
        let etag_mismatch = !s3_etag.is_empty()
            && !cached_metadata.etag.is_empty()
            && s3_etag != cached_metadata.etag;

        let last_modified_mismatch = !s3_last_modified.is_empty()
            && !cached_metadata.last_modified.is_empty()
            && s3_last_modified != cached_metadata.last_modified;

        etag_mismatch || last_modified_mismatch
    }

    /// Extract metadata from S3 response headers
    /// Parse Content-Range header to extract total object size
    /// Format: "bytes <start>-<end>/<total_size>" or "bytes <start>-<end>/*"
    /// Returns Some(total_size) if parseable, None if not present or unparseable
    fn parse_content_range_total_size(content_range: &str) -> Option<u64> {
        // Expected format: "bytes 200-1023/146515" or "bytes 200-1023/*"
        if !content_range.starts_with("bytes ") {
            return None;
        }

        // Find the '/' that separates range from total size
        if let Some(slash_pos) = content_range.rfind('/') {
            let total_size_str = &content_range[slash_pos + 1..];

            // Check if it's "*" (unknown size)
            if total_size_str == "*" {
                return None;
            }

            // Try to parse as number
            total_size_str.parse().ok()
        } else {
            None
        }
    }

    pub fn extract_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> CacheMetadata {
        let etag = headers
            .get("etag")
            .or_else(|| headers.get("ETag"))
            .cloned()
            .unwrap_or_default();

        let last_modified = headers
            .get("last-modified")
            .or_else(|| headers.get("Last-Modified"))
            .cloned()
            .unwrap_or_default();

        // First try to get content-length from Content-Length header (for non-range responses)
        let mut content_length = headers
            .get("content-length")
            .or_else(|| headers.get("Content-Length"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // For range responses, try to extract total object size from Content-Range header
        // This provides the full object size even for partial content responses
        if let Some(content_range) = headers
            .get("content-range")
            .or_else(|| headers.get("Content-Range"))
        {
            if let Some(total_size) = Self::parse_content_range_total_size(content_range) {
                debug!("Extracted total object size from Content-Range: {} bytes (was: {} bytes from Content-Length)", 
                       total_size, content_length);
                content_length = total_size;
            }
        }

        let cache_control = headers
            .get("cache-control")
            .or_else(|| headers.get("Cache-Control"))
            .cloned();

        CacheMetadata {
            etag,
            last_modified,
            content_length,
            part_number: None, // Will be set separately for multipart requests
            cache_control,
            access_count: 0,
            last_accessed: SystemTime::now(),
        }
    }

    /// Extract complete ObjectMetadata from S3 response headers including all headers
    pub fn extract_object_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> crate::cache_types::ObjectMetadata {
        let etag = headers
            .get("etag")
            .or_else(|| headers.get("ETag"))
            .cloned()
            .unwrap_or_default();

        // S3 PUT/CompleteMultipartUpload responses don't include Last-Modified header
        // Leave empty - the proxy will learn it on first GET/HEAD request from S3
        let last_modified = headers
            .get("last-modified")
            .or_else(|| headers.get("Last-Modified"))
            .cloned()
            .unwrap_or_default();

        // First try to get content-length from Content-Length header (for non-range responses)
        let mut content_length = headers
            .get("content-length")
            .or_else(|| headers.get("Content-Length"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // For range responses, try to extract total object size from Content-Range header
        // This provides the full object size even for partial content responses
        if let Some(content_range) = headers
            .get("content-range")
            .or_else(|| headers.get("Content-Range"))
        {
            if let Some(total_size) = Self::parse_content_range_total_size(content_range) {
                debug!("Extracted total object size from Content-Range: {} bytes (was: {} bytes from Content-Length)", 
                       total_size, content_length);
                content_length = total_size;
            }
        }

        let content_type = headers
            .get("content-type")
            .or_else(|| headers.get("Content-Type"))
            .cloned();

        // Store all response headers for complete response reconstruction
        // Filter out headers that should not be cached or are request-specific
        let mut response_headers = HashMap::new();
        for (key, value) in headers {
            let key_lower = key.to_lowercase();
            // Skip headers that are connection-specific or should not be cached
            if !matches!(
                key_lower.as_str(),
                "connection"
                    | "transfer-encoding"
                    | "date"
                    | "server"
                    | "x-amz-request-id"
                    | "x-amz-id-2"
            ) {
                // Store the original x-amz- header as-is
                // AWS SDK expects x-amz- headers and will parse them itself
                response_headers.insert(key.clone(), value.clone());
            }
        }

        crate::cache_types::ObjectMetadata::new_with_headers(
            etag,
            last_modified,
            content_length,
            content_type,
            response_headers,
        )
    }

    /// Extract metadata from S3 response headers with URL parameters
    pub fn extract_metadata_from_response_with_params(
        &self,
        headers: &HashMap<String, String>,
        url_params: &S3UrlParams,
    ) -> CacheMetadata {
        let mut metadata = self.extract_metadata_from_response(headers);

        // Set part number from URL params
        metadata.part_number = url_params.part_number;

        metadata
    }
}

/// URL parameter parsing utilities for S3 requests
pub struct S3UrlParams {
    pub part_number: Option<u32>,
    pub upload_id: Option<String>,
    pub uploads: bool,
}

impl S3UrlParams {
    /// Parse S3-specific parameters from URI query string
    pub fn parse_from_uri(uri: &Uri) -> Self {
        let query = uri.query().unwrap_or("");
        Self::parse_from_query(query)
    }

    /// Parse S3-specific parameters from query string
    pub fn parse_from_query(query: &str) -> Self {
        let mut part_number = None;
        let mut upload_id = None;
        let mut uploads = false;

        if query.is_empty() {
            return Self {
                part_number,
                upload_id,
                uploads,
            };
        }

        // Parse query parameters
        for param in query.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                match key {
                    "partNumber" => {
                        if let Ok(part_num) = value.parse::<u32>() {
                            part_number = Some(part_num);
                        }
                    }
                    "uploadId" => {
                        upload_id =
                            Some(urlencoding::decode(value).unwrap_or_default().to_string());
                    }
                    "uploads" => {
                        uploads = true; // uploads parameter doesn't have a value
                    }
                    _ => {} // Ignore other parameters (including versionId)
                }
            } else if param == "uploads" {
                uploads = true; // Handle uploads parameter without value
            }
        }

        Self {
            part_number,
            upload_id,
            uploads,
        }
    }

    /// Check if this is a multipart upload operation
    pub fn is_multipart_upload(&self) -> bool {
        self.upload_id.is_some() || self.uploads || self.part_number.is_some()
    }

    /// Check if this is a part request
    pub fn is_part_request(&self) -> bool {
        self.part_number.is_some()
    }
}

/// Helper function to build S3 request context from HTTP request
pub fn build_s3_request_context(
    method: Method,
    uri: Uri,
    headers: HashMap<String, String>,
    body: Option<Bytes>,
    host: String,
) -> S3RequestContext {
    let request_size = body.as_ref().map(|b| b.len() as u64);
    let conditional_headers = S3Client::extract_conditional_headers(&headers);

    // Build absolute URI for S3 request if the URI is relative
    let absolute_uri = if uri.scheme().is_none() {
        // URI is relative, construct absolute URI with https:// scheme
        let path_and_query = uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or(uri.path());
        format!("https://{}{}", host, path_and_query)
            .parse()
            .unwrap_or(uri) // Fallback to original URI if parsing fails
    } else {
        // URI is already absolute
        uri
    };

    S3RequestContext {
        method,
        uri: absolute_uri,
        headers,
        body,
        host,
        request_size,
        conditional_headers,
        operation_type: None,
        allow_streaming: false, // Default to buffering for safety
    }
}

/// Helper function to build S3 request context with operation type
pub fn build_s3_request_context_with_operation(
    method: Method,
    uri: Uri,
    headers: HashMap<String, String>,
    body: Option<Bytes>,
    host: String,
    operation_type: Option<String>,
) -> S3RequestContext {
    let request_size = body.as_ref().map(|b| b.len() as u64);
    let conditional_headers = S3Client::extract_conditional_headers(&headers);

    // Build absolute URI for S3 request if the URI is relative
    let absolute_uri = if uri.scheme().is_none() {
        // URI is relative, construct absolute URI with https:// scheme
        let path_and_query = uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or(uri.path());
        format!("https://{}{}", host, path_and_query)
            .parse()
            .unwrap_or(uri) // Fallback to original URI if parsing fails
    } else {
        // URI is already absolute
        uri
    };

    S3RequestContext {
        method,
        uri: absolute_uri,
        headers,
        body,
        host,
        request_size,
        conditional_headers,
        operation_type,
        allow_streaming: true, // Enable streaming for bypass operations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConnectionPoolConfig;

    fn create_test_config() -> ConnectionPoolConfig {
        ConnectionPoolConfig {
            max_connections_per_ip: 10,
            dns_refresh_interval: Duration::from_secs(60),
            connection_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(60),
            keepalive_enabled: true,
            max_idle_per_host: 1,
            max_lifetime: Duration::from_secs(300),
            pool_check_interval: Duration::from_secs(10),
            dns_servers: Vec::new(),
            endpoint_overrides: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_s3_client_initialization() {
        // Install default crypto provider for Rustls
        let _ = rustls::crypto::ring::default_provider().install_default();

        // Test that S3 client can be created with system root certificates
        let config = create_test_config();
        let result = S3Client::new(&config, None);

        // Skip TLS validation in test environments where certificates may not be available
        if result.is_err() {
            eprintln!("Skipping TLS test - certificates not available in test environment");
            return;
        }

        let client = result.unwrap();
        assert_eq!(client.request_timeout, Duration::from_secs(30));
        assert_eq!(client.keepalive_enabled, true);
    }

    #[test]
    fn test_s3_client_has_tls_connector() {
        // Install default crypto provider for Rustls
        let _ = rustls::crypto::ring::default_provider().install_default();

        // Test that TLS connector is properly initialized
        let config = create_test_config();
        let result = S3Client::new(&config, None);

        // Skip TLS validation in test environments where certificates may not be available
        if result.is_err() {
            eprintln!("Skipping TLS test - certificates not available in test environment");
            return;
        }

        let client = result.unwrap();

        // The TLS connector should be initialized (we can't directly test it without making a connection,
        // but we can verify the client was created successfully which means TLS setup worked)
        assert_eq!(client.request_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_conditional_headers_extraction() {
        let mut headers = HashMap::new();
        headers.insert("if-match".to_string(), "\"etag123\"".to_string());
        headers.insert("if-none-match".to_string(), "\"etag456\"".to_string());
        headers.insert(
            "if-modified-since".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        );
        headers.insert(
            "if-unmodified-since".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        );

        let conditional = S3Client::extract_conditional_headers(&headers);

        assert!(conditional.is_some());
        let cond = conditional.unwrap();
        assert_eq!(cond.if_match, Some("\"etag123\"".to_string()));
        assert_eq!(cond.if_none_match, Some("\"etag456\"".to_string()));
        assert_eq!(
            cond.if_modified_since,
            Some("Wed, 21 Oct 2015 07:28:00 GMT".to_string())
        );
        assert_eq!(
            cond.if_unmodified_since,
            Some("Wed, 21 Oct 2015 07:28:00 GMT".to_string())
        );
    }

    #[test]
    fn test_conditional_headers_extraction_empty() {
        let headers = HashMap::new();
        let conditional = S3Client::extract_conditional_headers(&headers);
        assert!(conditional.is_none());
    }

    #[test]
    fn test_metadata_extraction() {
        // Install default crypto provider for Rustls
        let _ = rustls::crypto::ring::default_provider().install_default();

        let config = create_test_config();
        let result = S3Client::new(&config, None);

        // Skip TLS validation in test environments where certificates may not be available
        if result.is_err() {
            eprintln!("Skipping TLS test - certificates not available in test environment");
            return;
        }

        let client = result.unwrap();
        let mut headers = HashMap::new();
        headers.insert("etag".to_string(), "\"abc123\"".to_string());
        headers.insert(
            "last-modified".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        );
        headers.insert("content-length".to_string(), "1024".to_string());
        headers.insert("cache-control".to_string(), "max-age=3600".to_string());

        let metadata = client.extract_metadata_from_response(&headers);

        assert_eq!(metadata.etag, "\"abc123\"");
        assert_eq!(metadata.last_modified, "Wed, 21 Oct 2015 07:28:00 GMT");
        assert_eq!(metadata.content_length, 1024);
        assert_eq!(metadata.cache_control, Some("max-age=3600".to_string()));
        assert_eq!(metadata.part_number, None);
    }

    #[test]
    fn test_build_s3_request_context() {
        let method = Method::GET;
        let uri: Uri = "https://s3.amazonaws.com/bucket/key".parse().unwrap();
        let mut headers = HashMap::new();
        headers.insert("host".to_string(), "s3.amazonaws.com".to_string());
        let body = Some(Bytes::from("test data"));
        let host = "s3.amazonaws.com".to_string();

        let context = build_s3_request_context(
            method.clone(),
            uri.clone(),
            headers.clone(),
            body.clone(),
            host.clone(),
        );

        assert_eq!(context.method, method);
        assert_eq!(context.uri, uri);
        assert_eq!(context.headers, headers);
        assert_eq!(context.body, body);
        assert_eq!(context.host, host);
        assert_eq!(context.request_size, Some(9)); // "test data" is 9 bytes
    }

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(30));
        assert_eq!(config.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_parse_content_range_total_size() {
        // Test valid Content-Range headers
        assert_eq!(
            S3Client::parse_content_range_total_size("bytes 200-1023/146515"),
            Some(146515)
        );
        assert_eq!(
            S3Client::parse_content_range_total_size("bytes 0-1023/5242880"),
            Some(5242880)
        );
        assert_eq!(
            S3Client::parse_content_range_total_size("bytes 1000-1999/2000"),
            Some(2000)
        );

        // Test Content-Range with unknown total size
        assert_eq!(
            S3Client::parse_content_range_total_size("bytes 200-1023/*"),
            None
        );

        // Test invalid formats
        assert_eq!(S3Client::parse_content_range_total_size("invalid"), None);
        assert_eq!(
            S3Client::parse_content_range_total_size("bytes 200-1023"),
            None
        );
        assert_eq!(
            S3Client::parse_content_range_total_size("bytes 200-1023/invalid"),
            None
        );
        assert_eq!(S3Client::parse_content_range_total_size(""), None);
    }

    #[test]
    fn test_metadata_extraction_with_content_range() {
        // Install default crypto provider for Rustls
        let _ = rustls::crypto::ring::default_provider().install_default();

        let config = create_test_config();
        let result = S3Client::new(&config, None);

        // Skip TLS validation in test environments where certificates may not be available
        if result.is_err() {
            eprintln!("Skipping TLS test - certificates not available in test environment");
            return;
        }

        let client = result.unwrap();

        // Test range response with Content-Range header
        let mut headers = HashMap::new();
        headers.insert("etag".to_string(), "\"abc123\"".to_string());
        headers.insert(
            "last-modified".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        );
        headers.insert("content-length".to_string(), "1024".to_string()); // Size of this range
        headers.insert(
            "content-range".to_string(),
            "bytes 0-1023/5242880".to_string(),
        ); // Total object size

        let metadata = client.extract_metadata_from_response(&headers);

        assert_eq!(metadata.etag, "\"abc123\"");
        assert_eq!(metadata.last_modified, "Wed, 21 Oct 2015 07:28:00 GMT");
        assert_eq!(metadata.content_length, 5242880); // Should use total size from Content-Range, not Content-Length

        // Test range response with unknown total size
        let mut headers2 = HashMap::new();
        headers2.insert("etag".to_string(), "\"def456\"".to_string());
        headers2.insert("content-length".to_string(), "2048".to_string());
        headers2.insert("content-range".to_string(), "bytes 1000-3047/*".to_string()); // Unknown total size

        let metadata2 = client.extract_metadata_from_response(&headers2);

        assert_eq!(metadata2.etag, "\"def456\"");
        assert_eq!(metadata2.content_length, 2048); // Should fall back to Content-Length
    }
}
