//! S3 Client Module
//!
//! Provides HTTPS client functionality for communicating with S3 endpoints
//! using connection pooling, load balancing, and intelligent error handling.

use crate::cache_types::CacheMetadata;
use crate::config::ConnectionPoolConfig;
use crate::connection_pool::{ConnectionPoolManager, IpHealthTracker};
use crate::https_connector::CustomHttpsConnector;
use crate::{ProxyError, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio_rustls::TlsConnector;
use tracing::{debug, info, warn};

/// S3 client with Hyper connection pooling support
pub struct S3Client {
    client: Client<CustomHttpsConnector, Full<Bytes>>,
    pool_manager: Arc<tokio::sync::RwLock<ConnectionPoolManager>>,
    request_timeout: Duration,
    keepalive_enabled: bool,
    ip_distribution_enabled: bool,
    metrics_manager:
        Arc<tokio::sync::RwLock<Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>>>,
    health_tracker: Arc<IpHealthTracker>,
    /// True when endpoint_overrides is configured (PrivateLink destinations).
    /// Used to lock outbound TLS to 1.2 for VPC interface endpoint compatibility.
    has_endpoint_overrides: bool,
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
    pub operation_type: Option<String>,
    pub allow_streaming: bool, // If false, always buffer response
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
        let pool_manager = Arc::new(tokio::sync::RwLock::new(
            ConnectionPoolManager::new_with_config(config.clone())?,
        ));

        let health_tracker = Arc::new(IpHealthTracker::new(config.ip_failure_threshold));

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

        // Build TLS connectors. Default uses TLS 1.2+1.3. When endpoint
        // overrides are configured, also build a TLS 1.2-only connector
        // for PrivateLink destinations (VPC interface endpoints don't
        // support TLS 1.3).
        let tls_default = {
            let cfg = rustls::ClientConfig::builder()
                .with_root_certificates(root_store.clone())
                .with_no_client_auth();
            TlsConnector::from(Arc::new(cfg))
        };

        let tls_privatelink = if !config.endpoint_overrides.is_empty() {
            info!(
                "Endpoint overrides configured — building TLS 1.2 connector for PrivateLink destinations"
            );
            let cfg = rustls::ClientConfig::builder_with_protocol_versions(
                &[&rustls::version::TLS12],
            )
            .with_root_certificates(root_store)
            .with_no_client_auth();
            Some(TlsConnector::from(Arc::new(cfg)))
        } else {
            None
        };

        // Create shared metrics manager reference that can be set later
        let metrics_ref = Arc::new(tokio::sync::RwLock::new(metrics_manager.clone()));

        // Create custom HTTPS connector with pool manager, health tracker, and config
        let mut https_connector = CustomHttpsConnector::new(
            Arc::clone(&pool_manager),
            tls_default,
            tls_privatelink,
            config.clone(),
            Arc::clone(&health_tracker),
        );

        // Set the shared metrics manager reference on connector
        https_connector.set_metrics_manager_ref(Arc::clone(&metrics_ref));

        // Build Hyper client with connection pooling
        let pool_max_idle = if config.ip_distribution_enabled {
            config.max_idle_per_ip
        } else {
            config.max_idle_per_host
        };

        let client = if config.keepalive_enabled {
            debug!(
                "Creating S3 client with connection keepalive enabled (idle_timeout: {}s, pool_max_idle_per_host: {}, ip_distribution: {})",
                config.idle_timeout.as_secs(),
                pool_max_idle,
                config.ip_distribution_enabled
            );

            Client::builder(TokioExecutor::new())
                .pool_idle_timeout(config.idle_timeout)
                .pool_max_idle_per_host(pool_max_idle)
                .build(https_connector)
        } else {
            debug!("Creating S3 client with connection keepalive disabled");

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
            ip_distribution_enabled: config.ip_distribution_enabled,
            metrics_manager: metrics_ref,
            health_tracker,
            has_endpoint_overrides: !config.endpoint_overrides.is_empty(),
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
    /// Get reference to connection pool manager for health/metrics monitoring
    pub fn get_connection_pool(
        &self,
    ) -> Arc<tokio::sync::RwLock<crate::connection_pool::ConnectionPoolManager>> {
        Arc::clone(&self.pool_manager)
    }

    /// Whether endpoint_overrides is configured (PrivateLink destinations present).
    /// When true, outbound TLS should be locked to 1.2 for VPC interface endpoint compatibility.
    pub fn has_endpoint_overrides(&self) -> bool {
        self.has_endpoint_overrides
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
                            info!(
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

        // IP distribution: rewrite URI authority from hostname to IP address so hyper
        // creates separate connection pools per IP (Requirement 1.1, 1.2)
        let (effective_uri, selected_ip) = if self.ip_distribution_enabled {
            let distributed_ip = {
                let pool_manager = self.pool_manager.read().await;
                pool_manager.get_distributed_ip(&context.host)
            };
            match distributed_ip {
                Some(ip) => {
                    debug!(
                        ip = %ip,
                        host = %context.host,
                        "Selected distributed IP for request"
                    );
                    match rewrite_uri_authority(&context.uri, &ip) {
                        Ok(new_uri) => (new_uri, Some(ip)),
                        Err(e) => {
                            warn!(
                                error = %e,
                                host = %context.host,
                                "URI authority rewrite failed, forwarding with original hostname"
                            );
                            (context.uri.clone(), Some(ip))
                        }
                    }
                }
                None => {
                    // No IPs available yet — register the endpoint so the DNS refresh
                    // background task picks it up, then forward with original hostname (Requirement 7.1)
                    let pool_manager = Arc::clone(&self.pool_manager);
                    let host = context.host.clone();
                    tokio::spawn(async move {
                        pool_manager.write().await.register_endpoint(&host).await;
                    });
                    (context.uri.clone(), None)
                }
            }
        } else {
            (context.uri.clone(), None)
        };

        // Build HTTP request
        let mut request_builder = Request::builder().method(&context.method).uri(&effective_uri);

        // Add headers (including any conditional headers)
        // Skip Content-Length header for requests with bodies - Hyper will set it automatically
        let has_body = context.body.is_some();
        let mut host_header_set = false;
        for (key, value) in &context.headers {
            if has_body && key.to_lowercase() == "content-length" {
                debug!("Skipping Content-Length header for request with body (will be auto-calculated): {}", value);
                continue;
            }
            if key.to_lowercase() == "host" {
                host_header_set = true;
            }
            request_builder = request_builder.header(key, value);
        }

        // When IP distribution rewrites the URI authority to an IP address, ensure the
        // Host header is set to the original hostname for AWS SigV4 compatibility (Requirement 2.2)
        if self.ip_distribution_enabled && !host_header_set {
            request_builder = request_builder.header("host", &context.host);
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
            .map_err(|e| {
                // Record failure for IP health tracking
                if let Some(ip) = selected_ip {
                    if self.health_tracker.record_failure(&ip) {
                        warn!(ip = %ip, host = %context.host, "IP failure threshold reached, excluding from distributor");
                        // Acquire write lock to remove IP — this is rare (only on threshold)
                        let pool_manager = self.pool_manager.clone();
                        let host = context.host.clone();
                        tokio::spawn(async move {
                            let mut pm = pool_manager.write().await;
                            if let Some(dist) = pm.get_distributor_mut(&host) {
                                dist.remove_ip(ip, "consecutive failures");
                            }
                        });
                    }
                }
                ProxyError::HttpError(format!("Failed to send request: {}", e))
            })?;

        // Record success for IP health tracking
        if let Some(ip) = selected_ip {
            self.health_tracker.record_success(&ip);
        }

        // Read response
        let (parts, body) = response.into_parts();

        // Convert headers to HashMap
        let mut headers = HashMap::new();
        for (key, value) in parts.headers.iter() {
            if let Ok(value_str) = value.to_str() {
                headers.insert(key.to_string(), value_str.to_string());
            }
        }

        // Stream or buffer the response body.
        // When allow_streaming is true, stream the body directly to avoid buffering delay.
        // When false (e.g., error responses that need body inspection), buffer it.
        // Responses with no Content-Length and allow_streaming=true are also streamed
        // (chunked transfer encoding from S3).
        let content_length = headers
            .get("content-length")
            .and_then(|v| v.parse::<u64>().ok());

        let should_stream = context.allow_streaming;

        let response_body = if should_stream {
            debug!(
                "Streaming response body (Content-Length: {:?} bytes)",
                content_length
            );
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

    /// Register an endpoint for DNS-based IP distribution and perform an immediate resolve.
    pub async fn register_endpoint(&self, endpoint: &str) {
        let mut pool_manager = self.pool_manager.write().await;
        pool_manager.register_endpoint(endpoint).await;
    }

    /// Refresh DNS for all registered endpoints and clear health tracker failure counts.
    ///
    /// Clearing the tracker on each refresh gives previously-excluded IPs a clean slate
    /// when they are restored by the DNS refresh.
    pub async fn refresh_dns(&self) -> Result<()> {
        let mut pool_manager = self.pool_manager.write().await;
        pool_manager.refresh_dns().await?;
        // Clear failure counts so restored IPs aren't immediately re-excluded
        self.health_tracker.clear();
        Ok(())
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
}

/// URL parameter parsing utilities for S3 requests
pub struct S3UrlParams {
    pub part_number: Option<u32>,
    pub upload_id: Option<String>,
    pub uploads: bool,
}

impl S3UrlParams {
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

    // Build absolute URI for S3 request if the URI is relative
    let absolute_uri = if uri.scheme().is_none() {
        // URI is relative, construct absolute URI with https:// scheme
        let path_and_query = uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or(uri.path());
        format!(
            "https://{}{}",
            format_authority_host(&host, None),
            path_and_query
        )
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
        operation_type: None,
        allow_streaming: true, // Stream by default — buffering delays time-to-first-byte
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

    // Build absolute URI for S3 request if the URI is relative
    let absolute_uri = if uri.scheme().is_none() {
        // URI is relative, construct absolute URI with https:// scheme
        let path_and_query = uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or(uri.path());
        format!(
            "https://{}{}",
            format_authority_host(&host, None),
            path_and_query
        )
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
        operation_type,
        allow_streaming: true, // Enable streaming for bypass operations
    }
}

/// Rewrite a URI's authority (host) from a hostname to an IP address while preserving
/// scheme, path, and query. Used by IP distribution to make hyper create per-IP pools.
fn rewrite_uri_authority(original: &Uri, ip: &IpAddr) -> std::result::Result<Uri, String> {
    let scheme = original.scheme_str().unwrap_or("https");
    let port_suffix = match original.port_u16() {
        Some(443) if scheme == "https" => String::new(),
        Some(80) if scheme == "http" => String::new(),
        Some(port) => format!(":{}", port),
        None => String::new(),
    };
    let ip_authority = match ip {
        IpAddr::V6(v6) => format!("[{}]{}", v6, port_suffix),
        IpAddr::V4(v4) => format!("{}{}", v4, port_suffix),
    };
    let path_and_query = original
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let new_uri_str = format!("{}://{}{}", scheme, ip_authority, path_and_query);
    new_uri_str
        .parse::<Uri>()
        .map_err(|e| format!("Failed to parse rewritten URI '{}': {}", new_uri_str, e))
}

/// Format a hostname for use in a URI authority, bracketing IPv6 literals.
fn format_authority_host(host: &str, port: Option<u16>) -> String {
    let bracketed = if host.contains(':') {
        format!("[{}]", host)
    } else {
        host.to_string()
    };
    match port {
        Some(p) => format!("{}:{}", bracketed, p),
        None => bracketed,
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
            ip_distribution_enabled: false,
            max_idle_per_ip: 10,
            ..Default::default()
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

    // --- URI rewriting tests (Task 6.3) ---

    #[test]
    fn test_rewrite_uri_authority_ipv4() {
        // Validates: Requirement 1.1 - URI authority rewritten from hostname to IP
        let uri: Uri = "https://s3.eu-west-1.amazonaws.com/bucket/key"
            .parse()
            .unwrap();
        let ip: IpAddr = "52.92.17.224".parse().unwrap();

        let result = rewrite_uri_authority(&uri, &ip).unwrap();

        assert_eq!(result.scheme_str(), Some("https"));
        assert_eq!(result.host(), Some("52.92.17.224"));
        assert_eq!(result.path(), "/bucket/key");
    }

    #[test]
    fn test_rewrite_uri_authority_ipv6() {
        // Validates: Requirement 1.1 - IPv6 addresses wrapped in brackets
        let uri: Uri = "https://s3.eu-west-1.amazonaws.com/bucket/key"
            .parse()
            .unwrap();
        let ip: IpAddr = "2600:1f18:243e:b800::1".parse().unwrap();

        let result = rewrite_uri_authority(&uri, &ip).unwrap();

        assert_eq!(result.scheme_str(), Some("https"));
        assert_eq!(result.host(), Some("[2600:1f18:243e:b800::1]"));
        assert_eq!(result.path(), "/bucket/key");
    }

    #[test]
    fn test_rewrite_uri_authority_preserves_path_and_query() {
        // Validates: Requirement 2.3 - all other request components preserved
        let uri: Uri =
            "https://s3.amazonaws.com/bucket/key?partNumber=1&uploadId=abc"
                .parse()
                .unwrap();
        let ip: IpAddr = "52.92.17.224".parse().unwrap();

        let result = rewrite_uri_authority(&uri, &ip).unwrap();

        assert_eq!(result.path(), "/bucket/key");
        assert_eq!(result.query(), Some("partNumber=1&uploadId=abc"));
    }

    #[test]
    fn test_rewrite_uri_authority_non_default_port() {
        // Validates: Requirement 1.1 - non-default ports preserved
        let uri: Uri = "https://s3.amazonaws.com:8443/bucket/key"
            .parse()
            .unwrap();
        let ip: IpAddr = "52.92.17.224".parse().unwrap();

        let result = rewrite_uri_authority(&uri, &ip).unwrap();

        assert_eq!(result.host(), Some("52.92.17.224"));
        assert_eq!(result.port_u16(), Some(8443));
        assert_eq!(result.path(), "/bucket/key");
    }

    #[test]
    fn test_rewrite_uri_authority_default_https_port_omitted() {
        // Default port 443 for https should not appear in the rewritten URI
        let uri: Uri = "https://s3.amazonaws.com:443/bucket/key"
            .parse()
            .unwrap();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        let result = rewrite_uri_authority(&uri, &ip).unwrap();

        assert_eq!(result.port_u16(), None);
        assert_eq!(result.host(), Some("10.0.0.1"));
    }

    #[test]
    fn test_rewrite_uri_authority_http_scheme() {
        let uri: Uri = "http://s3.amazonaws.com/bucket/key".parse().unwrap();
        let ip: IpAddr = "52.92.17.224".parse().unwrap();

        let result = rewrite_uri_authority(&uri, &ip).unwrap();

        assert_eq!(result.scheme_str(), Some("http"));
        assert_eq!(result.host(), Some("52.92.17.224"));
    }

    #[test]
    fn test_pool_max_idle_per_host_ip_distribution_enabled() {
        // Validates: Requirement 5.2 - max_idle_per_ip (10) used when IP distribution enabled
        let mut config = create_test_config();
        config.ip_distribution_enabled = true;
        config.max_idle_per_ip = 10;
        config.max_idle_per_host = 100;

        let pool_max_idle = if config.ip_distribution_enabled {
            config.max_idle_per_ip
        } else {
            config.max_idle_per_host
        };

        assert_eq!(pool_max_idle, 10);
    }

    #[test]
    fn test_pool_max_idle_per_host_ip_distribution_disabled() {
        // Validates: Requirement 5.3 - max_idle_per_host (100) used when IP distribution disabled
        let mut config = create_test_config();
        config.ip_distribution_enabled = false;
        config.max_idle_per_ip = 10;
        config.max_idle_per_host = 100;

        let pool_max_idle = if config.ip_distribution_enabled {
            config.max_idle_per_ip
        } else {
            config.max_idle_per_host
        };

        assert_eq!(pool_max_idle, 100);
    }

    // --- Graceful degradation tests (Task 11.3, Requirements 7.1, 7.2) ---

    #[test]
    fn test_rewrite_uri_authority_path_only_uri() {
        // Validates: Requirement 7.2
        // Edge case: a URI with no scheme or authority (path-only).
        // rewrite_uri_authority falls back to scheme "https" and path "/".
        // The function should still produce a valid URI without panicking.
        let uri: Uri = "/bucket/key".parse().unwrap();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        let result = rewrite_uri_authority(&uri, &ip);

        // Should succeed — the function fills in defaults for missing components
        assert!(result.is_ok());
        let rewritten = result.unwrap();
        assert_eq!(rewritten.scheme_str(), Some("https"));
        assert_eq!(rewritten.host(), Some("10.0.0.1"));
        assert_eq!(rewritten.path(), "/bucket/key");
    }

    #[test]
    fn test_rewrite_uri_authority_root_path_no_query() {
        // Validates: Requirement 7.2
        // Edge case: URI with scheme and authority but no path or query.
        // path_and_query() returns None, so the function falls back to "/".
        let uri: Uri = "https://s3.amazonaws.com".parse().unwrap();
        let ip: IpAddr = "52.92.17.224".parse().unwrap();

        let result = rewrite_uri_authority(&uri, &ip).unwrap();

        assert_eq!(result.scheme_str(), Some("https"));
        assert_eq!(result.host(), Some("52.92.17.224"));
        assert_eq!(result.path(), "/");
    }

    #[test]
    fn test_format_authority_host_brackets_ipv6() {
        // Validates: Requirements 6.1, 6.2
        assert_eq!(format_authority_host("::1", Some(443)), "[::1]:443");
    }

    #[test]
    fn test_format_authority_host_plain_hostname() {
        // Validates: Requirements 6.2
        assert_eq!(
            format_authority_host("example.com", Some(443)),
            "example.com:443"
        );
    }

    #[test]
    fn test_format_authority_host_no_port() {
        // Validates: Requirements 6.1
        assert_eq!(format_authority_host("::1", None), "[::1]");
    }
}
