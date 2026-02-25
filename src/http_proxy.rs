//! HTTP Proxy Module
//!
//! Handles HTTP/HTTPS requests with intelligent caching, range request support,
//! and S3 API compatibility. This module provides the main proxy functionality
//! including request forwarding, response caching, and client communication.

use crate::{
    cache::CacheManager,
    cache_types::{CacheMetadata, ObjectExpirationResult},
    config::Config,
    inflight_tracker::{FetchRole, InFlightTracker},
    logging::LoggerManager,
    range_handler::{RangeHandler, RangeParseResult, RangeSpec},
    s3_client::{
        build_s3_request_context, build_s3_request_context_with_operation, S3Client,
        S3RequestContext, S3ResponseBody,
    },
    tee_stream::TeeStream,
    Result,
};
use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
use hyper::header::{HeaderName, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Cache bypass mode determined from request headers
///
/// This enum represents the cache bypass behavior requested by the client
/// through Cache-Control or Pragma headers.
///
/// Requirements: 1.1, 2.1
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheBypassMode {
    /// No bypass - use normal cache logic
    None,
    /// Bypass cache lookup but cache the response (Cache-Control: no-cache or Pragma: no-cache)
    NoCache,
    /// Bypass cache lookup and do not cache the response (Cache-Control: no-store)
    NoStore,
}

/// Parse Cache-Control header value to determine bypass mode
///
/// Handles:
/// - Case-insensitive directive matching (Requirement 4.1)
/// - Multiple comma-separated directives (Requirement 4.2)
/// - Whitespace around directives (Requirement 4.3)
/// - Returns NoStore if both no-cache and no-store are present (Requirement 4.4)
/// - Ignores unknown directives (Requirement 4.5)
///
/// Requirements: 4.1, 4.2, 4.3, 4.4, 4.5
pub fn parse_cache_control(value: &str) -> CacheBypassMode {
    let mut has_no_cache = false;
    let mut has_no_store = false;

    for directive in value.split(',') {
        let directive = directive.trim().to_lowercase();
        // Handle directives that may have values (e.g., "max-age=0")
        let directive_name = directive.split('=').next().unwrap_or(&directive);
        match directive_name {
            "no-cache" => has_no_cache = true,
            "no-store" => has_no_store = true,
            _ => {} // Ignore unknown directives (Requirement 4.5)
        }
    }

    // Requirement 4.4: no-store takes precedence (more restrictive)
    if has_no_store {
        CacheBypassMode::NoStore
    } else if has_no_cache {
        CacheBypassMode::NoCache
    } else {
        CacheBypassMode::None
    }
}

/// Parse Pragma header for no-cache directive
///
/// Checks for "no-cache" value (case-insensitive)
///
/// Requirements: 3.1, 3.2
pub fn parse_pragma_no_cache(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("no-cache")
}

/// Parse cache bypass headers from request
///
/// Returns the most restrictive bypass mode found:
/// - NoStore > NoCache > None
///
/// Cache-Control takes precedence over Pragma when both are present (Requirement 3.4).
///
/// Requirements: 3.4, 7.3, 7.4
pub fn parse_cache_bypass_headers(
    headers: &HashMap<String, String>,
    config_enabled: bool,
) -> CacheBypassMode {
    // Requirement 7.3, 7.4: If disabled, ignore headers and use normal cache logic
    if !config_enabled {
        return CacheBypassMode::None;
    }

    // Check Cache-Control first (takes precedence per Requirement 3.4)
    if let Some(cache_control) = headers.get("cache-control") {
        let mode = parse_cache_control(cache_control);
        if mode != CacheBypassMode::None {
            return mode;
        }
    }

    // Fall back to Pragma header
    if let Some(pragma) = headers.get("pragma") {
        if parse_pragma_no_cache(pragma) {
            return CacheBypassMode::NoCache;
        }
    }

    CacheBypassMode::None
}

/// Parse Content-Range header to extract byte range and total size
///
/// Parses the format: `bytes start-end/total`
/// Returns `Some((start, end, total))` on success, `None` on invalid format.
///
/// Examples:
/// - "bytes 0-999/5000" -> Some((0, 999, 5000))
/// - "bytes 10485760-15728639/24117248" -> Some((10485760, 15728639, 24117248))
///
/// Requirements: 3.1
pub fn parse_content_range(header: &str) -> Option<(u64, u64, u64)> {
    // Expected format: "bytes start-end/total"
    let header = header.trim();

    // Must start with "bytes "
    let rest = header.strip_prefix("bytes ")?;

    // Split on "/" to get "start-end" and "total"
    let (range_part, total_str) = rest.split_once('/')?;

    // Parse total (handle "*" for unknown total)
    if total_str == "*" {
        return None; // We need the total for part caching
    }
    let total: u64 = total_str.parse().ok()?;

    // Split range on "-" to get start and end
    let (start_str, end_str) = range_part.split_once('-')?;
    let start: u64 = start_str.parse().ok()?;
    let end: u64 = end_str.parse().ok()?;

    // Validate: start <= end and end < total
    if start > end || end >= total {
        return None;
    }

    Some((start, end, total))
}

/// Conditionally add a Referer header for proxy identification in S3 Server Access Logs.
///
/// Adds the header only when:
/// - `proxy_referer` is `Some` (feature enabled)
/// - `referer` key not already present in headers (case-insensitive)
/// - `referer` not listed in the Authorization header's SignedHeaders field
///
/// Requirements: 1.1, 1.4, 3.1, 3.2, 3.3, 3.4
pub fn maybe_add_referer(
    headers: &mut HashMap<String, String>,
    proxy_referer: &Option<String>,
    auth_header: Option<&str>,
) {
    let referer_value = match proxy_referer {
        Some(v) => v,
        None => return,
    };

    // Check if referer already present (case-insensitive key check)
    if headers.keys().any(|k| k.eq_ignore_ascii_case("referer")) {
        return;
    }

    // Check if referer is in SignedHeaders (must not modify signed headers)
    if let Some(auth) = auth_header {
        if auth.contains("AWS4-HMAC-SHA256") {
            if let Some(pos) = auth.find("SignedHeaders=") {
                let after_param = &auth[pos + 14..];
                let end = after_param
                    .find(',')
                    .or_else(|| after_param.find(' '))
                    .unwrap_or(after_param.len());
                let signed_headers = &after_param[..end];
                if signed_headers.split(';').any(|h| h == "referer") {
                    return;
                }
            }
        }
    }

    debug!("Adding proxy identification Referer header: {}", referer_value);
    headers.insert("Referer".to_string(), referer_value.clone());
}

/// Result of detecting a path-style AP/MRAP alias in the request URL.
///
/// When AWS CLI uses `--endpoint-url` with a base AP/MRAP domain, the alias
/// appears as the first path segment. This struct holds the rewritten host,
/// stripped path, and cache key prefix needed to handle such requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathStyleAlias {
    /// Reconstructed upstream host (e.g., `{alias}.s3-accesspoint.{region}.amazonaws.com`)
    pub upstream_host: String,
    /// Path with the alias segment stripped (e.g., `/{remaining}`)
    pub forwarded_path: String,
    /// Cache key prefix — the alias with its reserved suffix intact
    pub cache_key_prefix: String,
}

/// Detect path-style AP/MRAP alias requests and return rewriting info.
///
/// When the Host is a base AP or MRAP domain and the first path segment
/// contains an alias with a reserved suffix (`-s3alias` or `.mrap`), this
/// function returns the reconstructed upstream host, the stripped path, and
/// the cache key prefix.
///
/// Returns `None` if the first path segment does not end with a reserved suffix,
/// guarding against false positive alias detection.
///
/// # AP alias
/// - Host: `s3-accesspoint.{region}.amazonaws.com`
/// - Path: `/{alias-ending-in-s3alias}/{key}`
/// - Result: host `{alias}.s3-accesspoint.{region}.amazonaws.com`,
///   path `/{key}`, cache key prefix `{alias}`
///
/// # MRAP alias
/// - Host: `accesspoint.s3-global.amazonaws.com`
/// - Path: `/{alias}.mrap/{key}`
/// - Result: host `{alias_without_mrap}.accesspoint.s3-global.amazonaws.com`,
///   path `/{key}`, cache key prefix `{alias}.mrap`
pub fn detect_path_style_alias(host: &str, path: &str) -> Option<PathStyleAlias> {
    // Extract the first path segment (after leading '/')
    let trimmed = path.strip_prefix('/')?;
    let (first_segment, remaining) = match trimmed.find('/') {
        Some(pos) => (&trimmed[..pos], &trimmed[pos..]),
        None => (trimmed, ""),
    };

    if first_segment.is_empty() {
        return None;
    }

    // Check for AP alias: host is base s3-accesspoint.{region}.amazonaws.com
    if host.starts_with("s3-accesspoint.") && host.ends_with(".amazonaws.com") {
        if first_segment.ends_with("-s3alias") {
            let region = host
                .strip_prefix("s3-accesspoint.")
                .and_then(|s| s.strip_suffix(".amazonaws.com"))?;
            if region.is_empty() {
                return None;
            }
            let forwarded_path = if remaining.is_empty() {
                "/".to_string()
            } else {
                remaining.to_string()
            };
            return Some(PathStyleAlias {
                upstream_host: format!("{}.s3-accesspoint.{}.amazonaws.com", first_segment, region),
                forwarded_path,
                cache_key_prefix: first_segment.to_string(),
            });
        }
        return None;
    }

    // Check for MRAP alias: host is exactly accesspoint.s3-global.amazonaws.com
    if host == "accesspoint.s3-global.amazonaws.com" {
        if first_segment.ends_with(".mrap") {
            let alias_without_mrap = first_segment.strip_suffix(".mrap")?;
            if alias_without_mrap.is_empty() {
                return None;
            }
            let forwarded_path = if remaining.is_empty() {
                "/".to_string()
            } else {
                remaining.to_string()
            };
            return Some(PathStyleAlias {
                upstream_host: format!(
                    "{}.accesspoint.s3-global.amazonaws.com",
                    alias_without_mrap
                ),
                forwarded_path,
                cache_key_prefix: first_segment.to_string(),
            });
        }
        return None;
    }

    None
}




/// HTTP Proxy server for S3 requests with caching
pub struct HttpProxy {
    listen_addr: SocketAddr,
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    s3_client: Arc<S3Client>,
    range_handler: Arc<RangeHandler>,
    request_semaphore: Arc<Semaphore>,
    active_connections: Arc<AtomicUsize>,
    metrics_manager: Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
    logger_manager: Option<Arc<Mutex<LoggerManager>>>,
    /// In-flight request tracker for download coalescing
    inflight_tracker: Arc<InFlightTracker>,
    /// Pre-built Referer header value for proxy identification in S3 Server Access Logs
    proxy_referer: Option<String>,
}

impl HttpProxy {
    /// Create a new HTTP proxy instance
    pub fn new(listen_addr: SocketAddr, config: Arc<Config>) -> Result<Self> {
        // Convert EvictionAlgorithm to CacheEvictionAlgorithm
        let eviction_algorithm = match config.cache.eviction_algorithm {
            crate::config::EvictionAlgorithm::LRU => crate::cache::CacheEvictionAlgorithm::LRU,
            crate::config::EvictionAlgorithm::TinyLFU => {
                crate::cache::CacheEvictionAlgorithm::TinyLFU
            }
        };

        let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
            config.cache.cache_dir.clone(),
            config.cache.ram_cache_enabled,
            config.cache.max_ram_cache_size,
            config.cache.max_cache_size,
            eviction_algorithm,
            1024, // 1KB compression threshold
            true, // compression enabled
            config.cache.get_ttl,
            config.cache.head_ttl,
            config.cache.put_ttl,
            config.cache.actively_remove_cached_data,
            config.cache.shared_storage.clone(),
            config.cache.write_cache_percent,
            config.cache.write_cache_enabled,
            config.cache.incomplete_upload_ttl,
            config.cache.metadata_cache.clone(),
            config.cache.eviction_trigger_percent,
            config.cache.eviction_target_percent,
            config.cache.read_cache_enabled,
            config.cache.bucket_settings_staleness_threshold,
        ));

        // Create S3 client (metrics will be set later via set_metrics_manager)
        let s3_client = Arc::new(S3Client::new(&config.connection_pool, None)?);

        // Create disk cache manager for new range storage architecture with atomic metadata writes support
        let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
            cache_manager.create_configured_disk_cache_manager(),
        ));

        let range_handler = Arc::new(RangeHandler::new(
            Arc::clone(&cache_manager),
            Arc::clone(&disk_cache_manager),
        ));
        let request_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_requests));
        let active_connections = Arc::new(AtomicUsize::new(0));

        // Build proxy identification Referer header value at startup
        let proxy_referer = if config.server.add_referer_header {
            let hostname = gethostname::gethostname().to_string_lossy().to_string();
            let referer = format!("s3-hybrid-cache/{} ({})", env!("CARGO_PKG_VERSION"), hostname);
            info!("Proxy identification enabled: Referer header will be set to \"{}\"", referer);
            Some(referer)
        } else {
            info!("Proxy identification headers are disabled (add_referer_header = false)");
            None
        };

        Ok(Self {
            listen_addr,
            config,
            cache_manager,
            s3_client,
            range_handler,
            request_semaphore,
            active_connections,
            metrics_manager: None,
            logger_manager: None,
            inflight_tracker: Arc::new(InFlightTracker::new()),
            proxy_referer,
        })
    }

    /// Set the metrics manager for tracking operations
    pub fn set_metrics_manager(
        &mut self,
        metrics_manager: Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>,
    ) {
        self.metrics_manager = Some(metrics_manager.clone());

        // Also set metrics on S3Client for connection keepalive tracking
        let s3_client = Arc::clone(&self.s3_client);
        let metrics = metrics_manager.clone();
        tokio::spawn(async move {
            s3_client.set_metrics_manager(metrics).await;
        });
    }

    /// Set the logger manager for access logging
    pub fn set_logger_manager(&mut self, logger_manager: Arc<Mutex<LoggerManager>>) {
        self.logger_manager = Some(logger_manager);
    }

    /// Get reference to cache manager for health/metrics monitoring
    pub fn get_cache_manager(&self) -> Arc<CacheManager> {
        Arc::clone(&self.cache_manager)
    }

    /// Get reference to S3 client's connection pool for health/metrics monitoring
    pub fn get_connection_pool(
        &self,
    ) -> Arc<tokio::sync::Mutex<crate::connection_pool::ConnectionPoolManager>> {
        self.s3_client.get_connection_pool()
    }

    /// Get reference to compression handler for health/metrics monitoring
    pub fn get_compression_handler(&self) -> Arc<crate::compression::CompressionHandler> {
        self.cache_manager.get_compression_handler()
    }

    /// Start the HTTP proxy server
    pub async fn start(&self, mut shutdown_signal: crate::shutdown::ShutdownSignal) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        info!("HTTP proxy listening on {}", self.listen_addr);

        // Initialize cache manager
        self.cache_manager.initialize().await?;

        // Set cache manager reference in size tracker for GET cache expiration
        self.cache_manager.set_cache_manager_in_tracker().await;

        // Set cache manager reference in journal consolidator for eviction triggering
        self.cache_manager.set_cache_manager_in_consolidator().await;

        // Wire up size tracker to disk cache manager
        if let Some(size_tracker) = self.cache_manager.get_size_tracker().await {
            let disk_cache_manager_arc = self.range_handler.get_disk_cache_manager();
            let mut disk_cache_manager = disk_cache_manager_arc.write().await;
            disk_cache_manager.set_size_tracker(size_tracker);
            info!("Size tracker wired up to disk cache manager for range storage");
        } else {
            warn!(
                "Size tracker not available - cache size tracking will not work for range storage"
            );
        }

        // Note: RAM cache access tracking is now handled by the journal system
        // (CacheHitUpdateBuffer) at the DiskCacheManager level, not via BatchFlushCoordinator

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            debug!("HTTP connection from {}", addr);

                            // Set TCP_NODELAY to disable Nagle's algorithm for lower latency
                            if let Err(e) = stream.set_nodelay(true) {
                                warn!("Failed to set TCP_NODELAY for {}: {}", addr, e);
                            }

                            let config = Arc::clone(&self.config);
                            let cache_manager = Arc::clone(&self.cache_manager);
                            let s3_client = Arc::clone(&self.s3_client);
                            let range_handler = Arc::clone(&self.range_handler);
                            let request_semaphore = Arc::clone(&self.request_semaphore);
                            let active_connections = Arc::clone(&self.active_connections);
                            let metrics_manager = self.metrics_manager.clone();
                            let logger_manager = self.logger_manager.clone();
                            let inflight_tracker = Arc::clone(&self.inflight_tracker);
                            let proxy_referer = self.proxy_referer.clone();

                            tokio::spawn(async move {
                                if let Err(e) = Self::serve_connection(
                                    stream,
                                    addr,
                                    config,
                                    cache_manager,
                                    s3_client,
                                    range_handler,
                                    request_semaphore,
                                    active_connections,
                                    metrics_manager,
                                    logger_manager,
                                    inflight_tracker,
                                    proxy_referer,
                                )
                                .await
                                {
                                    error!("HTTP proxy error for {}: {}", addr, e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept HTTP connection: {}", e);
                        }
                    }
                }
                _ = shutdown_signal.wait_for_shutdown() => {
                    info!("HTTP proxy received shutdown signal, stopping accept loop");
                    break;
                }
            }
        }

        // Drain period: wait for in-flight connections to complete
        let drain_timeout = Duration::from_secs(5);
        let drain_start = std::time::Instant::now();
        let active = self.active_connections.load(Ordering::Relaxed);
        if active > 0 {
            info!("HTTP proxy draining {} active connections (timeout: {:?})", active, drain_timeout);
            while self.active_connections.load(Ordering::Relaxed) > 0
                && drain_start.elapsed() < drain_timeout
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let remaining = self.active_connections.load(Ordering::Relaxed);
            if remaining > 0 {
                warn!("HTTP proxy shutdown with {} connections still active", remaining);
            } else {
                info!("HTTP proxy all connections drained");
            }
        }

        info!("HTTP proxy stopped");
        Ok(())
    }

    /// Serve a single HTTP connection
    async fn serve_connection(
        stream: TcpStream,
        addr: SocketAddr,
        config: Arc<Config>,
        cache_manager: Arc<CacheManager>,
        s3_client: Arc<S3Client>,
        range_handler: Arc<RangeHandler>,
        request_semaphore: Arc<Semaphore>,
        active_connections: Arc<AtomicUsize>,
        metrics_manager: Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
        logger_manager: Option<Arc<Mutex<LoggerManager>>>,
        inflight_tracker: Arc<InFlightTracker>,
        proxy_referer: Option<String>,
    ) -> Result<()> {
        let io = TokioIo::new(stream);

        // Track active connection
        active_connections.fetch_add(1, Ordering::Relaxed);

        let service = service_fn(move |req| {
            let config = Arc::clone(&config);
            let cache_manager = Arc::clone(&cache_manager);
            let s3_client = Arc::clone(&s3_client);
            let range_handler = Arc::clone(&range_handler);
            let request_semaphore = Arc::clone(&request_semaphore);
            let metrics_manager = metrics_manager.clone();
            let logger_manager = logger_manager.clone();
            let inflight_tracker = Arc::clone(&inflight_tracker);
            let proxy_referer = proxy_referer.clone();

            async move {
                Self::handle_request(
                    req,
                    addr,
                    config,
                    cache_manager,
                    s3_client,
                    range_handler,
                    request_semaphore,
                    metrics_manager,
                    logger_manager,
                    inflight_tracker,
                    proxy_referer,
                )
                .await
            }
        });

        if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
            // Check if this is a client-initiated cancellation or write error
            let err_str = err.to_string();
            if err_str.contains("connection closed")
                || err_str.contains("broken pipe")
                || err_str.contains("reset by peer")
                || err_str.contains("error writing")
                || err_str.contains("write error")
                || err.is_canceled()
            {
                debug!("Client disconnected from {}: {}", addr, err);
            } else {
                error!("Error serving HTTP connection from {}: {}", addr, err);
            }
        }

        // Decrement active connection count
        active_connections.fetch_sub(1, Ordering::Relaxed);

        Ok(())
    }

    /// Handle a single HTTP request
    pub async fn handle_request(
        mut req: Request<hyper::body::Incoming>,
        client_addr: SocketAddr,
        config: Arc<Config>,
        cache_manager: Arc<CacheManager>,
        s3_client: Arc<S3Client>,
        range_handler: Arc<RangeHandler>,
        request_semaphore: Arc<Semaphore>,
        metrics_manager: Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
        logger_manager: Option<Arc<Mutex<LoggerManager>>>,
        inflight_tracker: Arc<InFlightTracker>,
        proxy_referer: Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        let start_time = std::time::Instant::now();

        // Acquire semaphore permit for concurrent request limiting
        let _permit = match request_semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                // Return 429 Too Many Requests when limit exceeded
                warn!("Request limit exceeded, returning 429");
                let response = Self::build_error_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    "TooManyRequests",
                    "Request rate exceeded. Please retry after some time.",
                    Some("10"), // Retry-After header
                );
                return Ok(response);
            }
        };

        // Validate Host header (requirement 1.1, 1.3)
        let host = match Self::validate_host_header(&req) {
            Ok(host) => host,
            Err(response) => return Ok(response),
        };

        // Detect path-style AP/MRAP alias requests for logging.
        // We do NOT rewrite the host or path — the request is forwarded to S3
        // exactly as the client sent it, preserving the SigV4 signature.
        // S3 handles path-style AP/MRAP requests natively.
        // The alias naturally appears as the first path segment, which provides
        // correct cache key namespacing without any rewriting.
        if let Some(alias) = detect_path_style_alias(&host, req.uri().path()) {
            debug!(
                "Path-style AP/MRAP alias detected (forwarding as-is): alias={}, path={}",
                alias.cache_key_prefix, req.uri().path()
            );
        }

        // Extract request info for logging
        let method = req.method().clone();
        let uri = req.uri().clone();
        let user_agent = req
            .headers()
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let referer = req
            .headers()
            .get("referer")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let host_header = Some(host.clone());

        debug!("Processing {} {} from host: {}", method, uri, host);

        let metrics_for_recording = metrics_manager.clone();

        // Handle the request based on method
        let (response, served_from_cache) = match method {
            Method::GET | Method::HEAD => {
                let resp = Self::handle_get_head_request(
                    req,
                    host,
                    config.clone(),
                    cache_manager.clone(),
                    s3_client,
                    range_handler,
                    metrics_manager.clone(),
                    inflight_tracker,
                    &proxy_referer,
                )
                .await?;
                // Check if response has cache hit header
                let from_cache =
                    resp.headers().get("x-cache").and_then(|v| v.to_str().ok()) == Some("HIT");
                (resp, from_cache)
            }
            Method::PUT => {
                let resp = Self::handle_put_request(
                    req,
                    host,
                    config.clone(),
                    cache_manager,
                    s3_client,
                    metrics_manager,
                    &proxy_referer,
                )
                .await?;
                (resp, false)
            }
            Method::POST | Method::DELETE => {
                let resp = Self::handle_other_request(
                    req,
                    host,
                    config.clone(),
                    cache_manager,
                    s3_client,
                    metrics_manager,
                    &proxy_referer,
                )
                .await?;
                (resp, false)
            }
            _ => {
                warn!("Unsupported method: {}", method);
                let resp = Self::build_error_response(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "MethodNotAllowed",
                    "The specified method is not allowed against this resource.",
                    None,
                );
                (resp, false)
            }
        };

        // Log access entry if logger is configured
        if let Some(logger) = logger_manager {
            let total_time = start_time.elapsed().as_millis() as u64;
            let status = response.status();
            let object_size = response
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            // For HEAD requests, bytes_sent should be 0 since no body is transmitted
            // For GET requests, bytes_sent equals the actual response body size
            let bytes_sent = if method == Method::HEAD {
                0
            } else {
                object_size
            };

            let error_code = if !status.is_success() {
                Some(format!("{}", status.as_u16()))
            } else {
                None
            };

            if let Ok(logger_guard) = logger.try_lock() {
                let entry = logger_guard.create_access_log_entry(
                    method.as_str(),
                    client_addr.ip().to_string(),
                    uri.to_string(),
                    status.as_u16(),
                    bytes_sent,
                    Some(object_size),
                    total_time,
                    total_time,
                    user_agent,
                    referer,
                    host_header,
                    error_code,
                );

                if let Err(e) = logger_guard.log_access(entry, served_from_cache).await {
                    warn!("Failed to log access entry: {}", e);
                }
            }
        }

        // Record request metrics (feeds /metrics JSON and OTLP export)
        if let Some(mm) = metrics_for_recording.as_ref() {
            let elapsed = start_time.elapsed();
            let success = response.status().is_success();
            mm.read().await.record_request(success, elapsed).await;
        }

        Ok(response)
    }

    /// Convert S3ResponseBody to BoxBody for HTTP response
    fn s3_body_to_box_body(body: S3ResponseBody) -> BoxBody<Bytes, hyper::Error> {
        match body {
            S3ResponseBody::Buffered(bytes) => {
                Full::new(bytes).map_err(|never| match never {}).boxed()
            }
            S3ResponseBody::Streaming(incoming) => incoming
                .map_err(|e| {
                    error!("Stream error: {}", e);
                    e
                })
                .boxed(),
        }
    }

    /// Validate Host header and extract hostname (strips port if present)
    fn validate_host_header(
        req: &Request<hyper::body::Incoming>,
    ) -> std::result::Result<String, Response<BoxBody<Bytes, hyper::Error>>> {
        match req.headers().get("host") {
            Some(host_header) => {
                match host_header.to_str() {
                    Ok(host_with_port) => {
                        // Strip port from host if present (e.g., "s3.amazonaws.com:8081" -> "s3.amazonaws.com")
                        let host = host_with_port.split(':').next().unwrap_or(host_with_port);
                        Ok(host.to_string())
                    }
                    Err(_) => {
                        warn!("Invalid Host header encoding");
                        Err(Self::build_error_response(
                            StatusCode::BAD_REQUEST,
                            "InvalidRequest",
                            "Invalid Host header encoding.",
                            None,
                        ))
                    }
                }
            }
            None => {
                warn!("Missing Host header");
                Err(Self::build_error_response(
                    StatusCode::BAD_REQUEST,
                    "MissingHostHeader",
                    "Host header is required.",
                    None,
                ))
            }
        }
    }

    /// Determine if a request should bypass cache based on S3 operation type
    ///
    /// Returns (should_bypass, operation_type, reason)
    ///
    /// This function is used to identify non-cacheable operations like LIST and metadata
    /// operations that should never be cached regardless of cache bypass headers.
    ///
    /// Requirements: 1.1, 1.2, 2.1, 3.1, 4.1, 5.1, 6.1-6.7, 7.1-7.5
    pub fn should_bypass_cache(
        path: &str,
        query_params: &HashMap<String, String>,
    ) -> (bool, Option<String>, Option<String>) {
        // Requirement 4.1: Check for root path "/" - ListBuckets operation
        if path == "/" {
            return (
                true,
                Some("ListBuckets".to_string()),
                Some("list operation - always fetch fresh data".to_string()),
            );
        }

        // Check for LIST operation parameters
        // Requirements 1.1, 1.2: list-type or delimiter indicates ListObjects
        if query_params.contains_key("list-type") || query_params.contains_key("delimiter") {
            return (
                true,
                Some("ListObjects".to_string()),
                Some("list operation - always fetch fresh data".to_string()),
            );
        }

        // Requirement 2.1: versions parameter indicates ListObjectVersions
        if query_params.contains_key("versions") {
            return (
                true,
                Some("ListObjectVersions".to_string()),
                Some("list operation - always fetch fresh data".to_string()),
            );
        }

        // Requirement 3.1: uploads parameter indicates ListMultipartUploads
        if query_params.contains_key("uploads") {
            return (
                true,
                Some("ListMultipartUploads".to_string()),
                Some("list operation - always fetch fresh data".to_string()),
            );
        }

        // Part-number requests are now cached - removed bypass logic
        // GetObjectPart requests will be handled by the caching system

        // Check for metadata operation parameters (Requirements 6.1-6.7)
        if query_params.contains_key("acl") {
            return (
                true,
                Some("GetObjectAcl".to_string()),
                Some("metadata operation - always fetch fresh data".to_string()),
            );
        }

        if query_params.contains_key("attributes") {
            return (
                true,
                Some("GetObjectAttributes".to_string()),
                Some("metadata operation - always fetch fresh data".to_string()),
            );
        }

        if query_params.contains_key("legal-hold") {
            return (
                true,
                Some("GetObjectLegalHold".to_string()),
                Some("metadata operation - always fetch fresh data".to_string()),
            );
        }

        if query_params.contains_key("object-lock") {
            return (
                true,
                Some("GetObjectLockConfiguration".to_string()),
                Some("metadata operation - always fetch fresh data".to_string()),
            );
        }

        if query_params.contains_key("retention") {
            return (
                true,
                Some("GetObjectRetention".to_string()),
                Some("metadata operation - always fetch fresh data".to_string()),
            );
        }

        if query_params.contains_key("tagging") {
            return (
                true,
                Some("GetObjectTagging".to_string()),
                Some("metadata operation - always fetch fresh data".to_string()),
            );
        }

        if query_params.contains_key("torrent") {
            return (
                true,
                Some("GetObjectTorrent".to_string()),
                Some("metadata operation - always fetch fresh data".to_string()),
            );
        }

        // Requirement 7.5: No non-cacheable parameters found - this is a GetObject request
        // Note: versionId bypass is handled earlier in handle_get_head_request()
        (false, None, None)
    }

    /// Detect if request contains any conditional headers
    /// Requirements: 1.1, 2.1, 3.1, 4.1, 5.1, 5.2
    fn has_conditional_headers(headers: &HashMap<String, String>) -> bool {
        headers.contains_key("if-match")
            || headers.contains_key("if-none-match")
            || headers.contains_key("if-modified-since")
            || headers.contains_key("if-unmodified-since")
    }

    /// Detect GetObjectPart requests and extract part number
    ///
    /// Returns Some(part_number) if:
    /// - Method is GET
    /// - partNumber parameter exists and is valid (positive integer)
    /// - uploadId parameter does NOT exist (that's upload verification)
    ///
    /// Returns None for invalid part numbers or upload verification requests
    ///
    /// Requirements: 1.1, 1.2, 1.3, 1.4
    pub fn is_get_object_part(
        method: &Method,
        query_params: &HashMap<String, String>,
    ) -> Option<u32> {
        // Requirement 1.1: Check for GET method
        if method != Method::GET {
            return None;
        }

        // Requirement 1.4: If uploadId is present, this is upload verification, not download
        if query_params.contains_key("uploadId") {
            return None;
        }

        // Requirement 1.1: Check for partNumber parameter
        if let Some(part_number_str) = query_params.get("partNumber") {
            // Requirement 1.2: Extract and validate part number as u32
            if let Ok(part_number) = part_number_str.parse::<u32>() {
                // Requirement 1.3: Return None for invalid part numbers (zero)
                if part_number > 0 {
                    return Some(part_number);
                }
            }
        }

        // Requirement 1.3: Return None for invalid part numbers (non-numeric, negative, zero)
        None
    }

    /// Handle GET and HEAD requests with caching and range support
    async fn handle_get_head_request(
        req: Request<hyper::body::Incoming>,
        host: String,
        config: Arc<Config>,
        cache_manager: Arc<CacheManager>,
        s3_client: Arc<S3Client>,
        range_handler: Arc<RangeHandler>,
        metrics_manager: Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
        inflight_tracker: Arc<InFlightTracker>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        let method = req.method().clone();
        let uri = req.uri().clone();
        debug!(
            "ENTRY: handle_get_head_request called - method={}, path={}",
            method,
            uri.path()
        );
        let path = uri.path();
        let headers = req.headers();

        // Convert headers to HashMap for easier processing
        let header_map: HashMap<String, String> = headers
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.to_string(), v.to_string())))
            .collect();

        // Check for cache bypass headers (Cache-Control: no-cache/no-store, Pragma: no-cache)
        // Requirements: 1.1, 1.2, 2.1, 2.2, 3.1, 3.2
        let bypass_mode =
            parse_cache_bypass_headers(&header_map, config.cache.cache_bypass_headers_enabled);

        if bypass_mode != CacheBypassMode::None {
            // Determine bypass reason for logging and metrics
            // Requirements: 1.5, 2.4, 5.1, 5.2, 5.3, 5.4
            let (bypass_reason, metrics_reason) = match bypass_mode {
                CacheBypassMode::NoCache => ("no-cache directive", "no-cache directive"),
                CacheBypassMode::NoStore => ("no-store directive", "no-store directive"),
                CacheBypassMode::None => unreachable!(),
            };

            // Log cache bypass at INFO level - Requirements 1.5, 2.4, 5.4
            info!(
                "Cache bypass via header: method={} path={} reason={}",
                method, path, bypass_reason
            );

            // Record metrics - Requirements 5.1, 5.2, 5.3, 5.5
            if let Some(metrics_mgr) = metrics_manager.clone() {
                let reason_owned = metrics_reason.to_string();
                tokio::spawn(async move {
                    let mgr = metrics_mgr.read().await;
                    mgr.record_cache_bypass(&reason_owned).await;
                });
            }

            // Strip cache-control and pragma headers before forwarding to S3
            // Requirements: 6.1, 6.2, 6.3
            let mut forwarded_headers = header_map.clone();
            forwarded_headers.remove("cache-control");
            forwarded_headers.remove("pragma");

            // Parse query parameters to check if this is a non-cacheable operation
            let query_params: HashMap<String, String> = uri
                .query()
                .map(|q| {
                    q.split('&')
                        .filter_map(|pair| {
                            let mut parts = pair.splitn(2, '=');
                            let key = parts.next()?.to_string();
                            let value = parts.next().unwrap_or("").to_string();
                            Some((key, value))
                        })
                        .collect()
                })
                .unwrap_or_default();

            // Check if this operation is normally cacheable
            // LIST operations, metadata operations, etc. should never be cached
            // Requirements: 1.4, 3.4
            let (is_non_cacheable_op, _, _) = Self::should_bypass_cache(path, &query_params);

            // Determine if we should cache the response:
            // - NoStore mode: never cache (Requirement 2.3)
            // - NoCache mode: cache only if the operation is normally cacheable (Requirements 1.3, 3.3)
            let should_cache_response =
                bypass_mode == CacheBypassMode::NoCache && !is_non_cacheable_op;

            if should_cache_response {
                // Forward to S3 and cache the response (skip cache lookup but cache response)
                // Requirements: 1.3, 3.3
                let cache_key = CacheManager::generate_cache_key(path, Some(&host));
                return Self::forward_get_head_to_s3_and_cache(
                    method,
                    uri,
                    host,
                    forwarded_headers,
                    cache_key,
                    cache_manager,
                    s3_client,
                    proxy_referer,
                )
                .await;
            } else {
                // Forward to S3 without caching (Requirement 2.3)
                return Self::forward_get_head_to_s3_without_caching(
                    method,
                    uri,
                    host,
                    forwarded_headers,
                    s3_client,
                    None,
                    proxy_referer,
                )
                .await;
            }
        }

        // Check for Range header first - range requests should be handled by range logic even if they have conditional headers
        let has_range_header = headers.get("range").is_some();

        // Check for conditional headers - Requirements 1.1, 2.1, 3.1, 4.1, 5.1, 5.2, 5.3
        // But only route to conditional path if this is NOT a range request
        if Self::has_conditional_headers(&header_map) && !has_range_header {
            // Requirement 8.1: Log when conditional request is forwarded to S3 with the conditional headers
            let conditional_headers: Vec<String> = header_map
                .iter()
                .filter(|(k, _)| {
                    let key = k.to_lowercase();
                    key == "if-match"
                        || key == "if-none-match"
                        || key == "if-modified-since"
                        || key == "if-unmodified-since"
                })
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();

            info!(
                "Forwarding conditional request to S3: method={} path={} conditional_headers=[{}]",
                method,
                path,
                conditional_headers.join(", ")
            );

            // Generate cache key for conditional request handling
            let cache_key = CacheManager::generate_cache_key(path, Some(&host));

            // Record HEAD cache bypass metrics for conditional headers - Requirement 8.4: Include HEAD request bypasses in statistics
            if method == Method::HEAD {
                if let Some(metrics_mgr) = metrics_manager.clone() {
                    let bypass_reason = "conditional headers - bypass RAM cache".to_string();
                    tokio::spawn(async move {
                        let mgr = metrics_mgr.read().await;
                        mgr.record_cache_bypass(&bypass_reason).await;
                    });
                }
            }

            // Always forward conditional requests to S3 with proper cache management - Requirements 5.1, 5.3, 6.1, 6.2, 6.3
            return Self::forward_conditional_request_to_s3(
                method,
                uri,
                host,
                header_map,
                cache_key,
                cache_manager,
                s3_client,
                proxy_referer,
            )
            .await;
        }

        // Parse query parameters from URI - Requirement 7.1
        let query_params: HashMap<String, String> = uri
            .query()
            .map(|q| {
                q.split('&')
                    .filter_map(|pair| {
                        let mut parts = pair.splitn(2, '=');
                        let key = parts.next()?.to_string();
                        let value = parts.next().unwrap_or("").to_string();
                        Some((key, value))
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Check for expired presigned URLs and reject early
        if let Some(presigned_info) = crate::presigned_url::parse_presigned_url(&query_params) {
            if presigned_info.is_expired() {
                let time_since_expiry = presigned_info
                    .time_since_expiration()
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                info!(
                    "Presigned URL expired: method={} path={} expired_seconds_ago={} signed_at={:?} expires_in={}s",
                    method,
                    path,
                    time_since_expiry,
                    presigned_info.signed_at,
                    presigned_info.expires_in_seconds
                );

                let body_text = "Forbidden: Presigned URL has expired\n";
                let response = Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .header("Content-Type", "text/plain")
                    .header("Content-Length", body_text.len().to_string())
                    .body(
                        Full::new(Bytes::from(body_text))
                            .map_err(|never| match never {})
                            .boxed(),
                    )
                    .unwrap();

                return Ok(response);
            }
        }

        // Check if this request should bypass cache - Requirement 7.1
        // Note: For HEAD requests, only root path (HeadBucket/ListBuckets) bypasses cache
        // HeadObject requests (HEAD to actual objects) are always cached
        let (should_bypass, operation_type, reason) = if method == Method::HEAD {
            // For HEAD requests, only bypass if it's to root path (HeadBucket/ListBuckets)
            if path == "/" {
                (
                    true,
                    Some("ListBuckets".to_string()),
                    Some("list operation - always fetch fresh data".to_string()),
                )
            } else {
                // HeadObject - should be cached
                (false, None, None)
            }
        } else {
            // For GET requests, use full bypass detection
            Self::should_bypass_cache(path, &query_params)
        };

        // If bypass is needed, skip cache and forward directly to S3 - Requirement 7.4
        if should_bypass {
            let op_type = operation_type.as_deref().unwrap_or("Unknown");
            let bypass_reason = reason.as_deref().unwrap_or("unknown reason");

            // Log cache bypass at INFO level - Requirements 8.1, 8.2, 8.3, 8.4
            info!(
                "Bypassing cache: operation={} method={} query={:?} reason={} path={}",
                op_type,
                method,
                uri.query().unwrap_or(""),
                bypass_reason,
                path
            );

            // Record HEAD cache bypass metrics - Requirement 8.4: Include HEAD request bypasses in statistics
            if method == Method::HEAD {
                if let Some(metrics_mgr) = metrics_manager.clone() {
                    let bypass_reason_owned = bypass_reason.to_string();
                    tokio::spawn(async move {
                        let mgr = metrics_mgr.read().await;
                        mgr.record_cache_bypass(&bypass_reason_owned).await;
                    });
                }
            }

            debug!(
                "Forwarding {} operation directly to S3 (bypassing cache)",
                op_type
            );

            // Forward directly to S3 without caching - Requirements 1.4, 2.3, 3.3, 4.3, 5.3, 6.9
            return Self::forward_get_head_to_s3_without_caching(
                method,
                uri,
                host,
                header_map,
                s3_client,
                Some(op_type),
                proxy_referer,
            )
            .await;
        }

        // Cacheable request - proceed with existing cache pipeline - Requirement 7.4
        // Check for Range header - Requirements 3.1, 3.5
        let range_header = headers.get("range").and_then(|h| h.to_str().ok());

        // Generate cache key (path only, versionId is not included)
        let cache_key = CacheManager::generate_cache_key(path, Some(&host));

        debug!("Cache key for {} {}: {}", method, uri, cache_key);

        // Versioned requests bypass cache entirely (no read, no write)
        if query_params.contains_key("versionId") {
            debug!(
                "Versioned request detected, bypassing cache: cache_key={}",
                cache_key
            );

            if let Some(metrics_mgr) = metrics_manager.clone() {
                tokio::spawn(async move {
                    let mgr = metrics_mgr.read().await;
                    mgr.record_cache_bypass("versioned_request").await;
                });
            }

            return Self::forward_get_head_to_s3_without_caching(
                method,
                uri,
                host,
                header_map,
                s3_client,
                Some("GetObject (versioned)"),
                proxy_referer,
            )
            .await;
        }

        // Resolve bucket-level cache settings for read cache control
        // Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7
        let resolved_settings = cache_manager.resolve_settings(path).await;
        if !resolved_settings.read_cache_enabled {
            debug!(
                "Read caching disabled for path={}, source={:?} — streaming directly from S3",
                path, resolved_settings.source
            );
            return Self::forward_get_head_to_s3_without_caching(
                method,
                uri,
                host,
                header_map,
                s3_client,
                Some("read_cache_disabled"),
                proxy_referer,
            )
            .await;
        }

        // Check if this is a GetObjectPart request - Requirements 1.1, 1.2, 1.3, 1.4
        if let Some(part_number) = Self::is_get_object_part(&method, &query_params) {
            debug!(
                "Processing GetObjectPart request: cache_key={}, part_number={}",
                cache_key, part_number
            );

            // Try to serve from cached part - Requirements 4.1, 5.1, 5.4, 5.5
            match cache_manager.lookup_part(&cache_key, part_number).await {
                Ok(Some(cached_part)) => {
                    // Cache HIT - serve cached part with 206 Partial Content
                    // Note: Cache hit logging is handled in cache.rs lookup_part method

                    // Record part cache hit metric - Requirement 8.1
                    if let Some(metrics_manager) = &metrics_manager {
                        metrics_manager
                            .read()
                            .await
                            .record_part_cache_hit(
                                &cache_key,
                                part_number,
                                cached_part.data.len() as u64,
                            )
                            .await;
                    }

                    return Self::serve_cached_part_response(cached_part, method, uri.path()).await;
                }
                Ok(None) => {
                    debug!(
                        "Part cache MISS: cache_key={}, part_number={}",
                        cache_key, part_number
                    );

                    // Record part cache miss metric - Requirement 8.1
                    if let Some(metrics_manager) = &metrics_manager {
                        metrics_manager
                            .read()
                            .await
                            .record_part_cache_miss(&cache_key, part_number)
                            .await;
                    }

                    // IMPORTANT: For part requests without multipart metadata, we MUST go to S3
                    // to get the correct part data with proper Content-Range headers.
                    // We cannot serve from cached ranges because we don't know the part boundaries.
                    // Use download coordination to coalesce concurrent part requests.
                    // Requirement 15.3: Part-number requests use part key for independent tracking
                    return Self::forward_part_with_coordination(
                        method,
                        uri,
                        host,
                        header_map,
                        cache_key,
                        part_number,
                        cache_manager,
                        s3_client,
                        inflight_tracker,
                        config.cache.download_coordination.enabled,
                        config.cache.download_coordination.wait_timeout(),
                        metrics_manager,
                        proxy_referer,
                    )
                    .await;
                }
                Err(e) => {
                    warn!(
                        "Part cache lookup error: cache_key={}, part_number={}, error={}",
                        cache_key, part_number, e
                    );

                    // Record part cache error metric - Requirement 8.5
                    if let Some(metrics_manager) = &metrics_manager {
                        metrics_manager
                            .read()
                            .await
                            .record_part_cache_error(
                                &cache_key,
                                part_number,
                                "lookup",
                                &e.to_string(),
                            )
                            .await;
                    }

                    // IMPORTANT: For part requests with errors, we MUST go to S3
                    // to get the correct part data. Use coordination for coalescing.
                    return Self::forward_part_with_coordination(
                        method,
                        uri,
                        host,
                        header_map,
                        cache_key,
                        part_number,
                        cache_manager,
                        s3_client,
                        inflight_tracker,
                        config.cache.download_coordination.enabled,
                        config.cache.download_coordination.wait_timeout(),
                        metrics_manager,
                        proxy_referer,
                    )
                    .await;
                }
            }
        }

        // Handle range requests
        if let Some(range_str) = range_header {
            debug!("Processing range request: {}", range_str);

            // Get current ETag from cached object metadata for validation
            // Requirements: 2.1, 2.4 - ETag validation in range requests
            let current_etag = match cache_manager.get_object_etag(&cache_key).await {
                Ok(etag) => {
                    if let Some(ref etag_value) = etag {
                        debug!(
                            "Found object ETag for range validation: cache_key={}, etag={}",
                            cache_key, etag_value
                        );
                    } else {
                        debug!(
                            "No object ETag found for range validation: cache_key={}",
                            cache_key
                        );
                    }
                    etag
                }
                Err(e) => {
                    warn!(
                        "Failed to get object ETag for range validation: cache_key={}, error={}",
                        cache_key, e
                    );
                    None
                }
            };

            return Self::handle_range_request(
                method,
                cache_key,
                range_str,
                header_map,
                cache_manager,
                range_handler,
                s3_client,
                host,
                uri,
                config,
                resolved_settings.get_ttl,
                current_etag,
                inflight_tracker,
                metrics_manager.clone(),
                proxy_referer,
            )
            .await;
        }

        // Handle regular (non-range) requests
        // HEAD and GET requests have separate processing paths
        debug!(
            "Processing non-range request: method={}, path={}",
            method,
            uri.path()
        );
        if method == Method::HEAD {
            // Check HEAD cache for HEAD requests using unified cache (RAM first, then disk)
            match cache_manager.get_head_cache_entry_unified(&cache_key).await {
                Ok(Some(head_entry)) => {
                    // Determine cache layer for logging
                    // The unified cache checks MetadataCache (RAM) first, then disk
                    let cache_layer = if cache_manager.is_ram_cache_enabled() {
                        // Check if entry exists in MetadataCache (RAM) to determine layer
                        if cache_manager
                            .get_metadata_cache()
                            .get(&cache_key)
                            .await
                            .is_some()
                        {
                            "RAM cache"
                        } else {
                            "disk cache"
                        }
                    } else {
                        "disk cache"
                    };

                    info!("HEAD {} HIT for {}", cache_layer, uri);

                    // Build response from HEAD cache
                    let mut response_builder = Response::builder().status(StatusCode::OK);

                    // Add cached headers
                    for (key, value) in &head_entry.headers {
                        response_builder = response_builder.header(key, value);
                    }

                    // HEAD requests never include body
                    let response = response_builder
                        .body(
                            Full::new(Bytes::new())
                                .map_err(|never| match never {})
                                .boxed(),
                        )
                        .unwrap();
                    Ok(response)
                }
                Ok(None) => {
                    info!("HEAD cache MISS for {}", uri);

                    // Forward request to S3 and cache response
                    Self::forward_get_head_to_s3_and_cache(
                        method,
                        uri.clone(),
                        host,
                        header_map,
                        cache_key,
                        cache_manager,
                        s3_client,
                        proxy_referer,
                    )
                    .await
                }
                Err(e) => {
                    error!("HEAD cache error for {}: {}", uri, e);

                    // Continue serving by forwarding to S3 (requirement 2.5)
                    warn!("Cache error occurred, falling back to S3 forwarding");
                    Self::forward_get_head_to_s3_and_cache(
                        method,
                        uri.clone(),
                        host,
                        header_map,
                        cache_key,
                        cache_manager,
                        s3_client,
                        proxy_referer,
                    )
                    .await
                }
            }
        } else {
            // GET request without Range header - check if full object is cached using range system
            debug!(
                "Full object GET request: cache_key={}, checking cache first",
                cache_key
            );

            // Load metadata once for pass-through to avoid redundant NFS reads (Requirement 1.1)
            let preloaded_metadata = match cache_manager.get_metadata_cached(&cache_key).await {
                Ok(metadata) => metadata,
                Err(e) => {
                    debug!(
                        "Error getting metadata for full object GET: cache_key={}, error={}",
                        cache_key, e
                    );
                    None
                }
            };

            // Check if we have any cached ranges for this object
            match cache_manager.has_cached_ranges(&cache_key, preloaded_metadata.as_ref()).await {
                Ok(Some((true, total_size))) => {
                    debug!(
                        "Found cached ranges for key: {}, total_size: {} bytes",
                        cache_key, total_size
                    );

                    // Try to serve the full object (0 to total_size-1) from cache
                    let full_range = crate::range_handler::RangeSpec {
                        start: 0,
                        end: total_size - 1, // end is inclusive
                    };

                    // Check if we can serve the full object from cache
                    match range_handler
                        .find_cached_ranges(&cache_key, &full_range, None, preloaded_metadata.as_ref())
                        .await
                    {
                        Ok(overlap) if overlap.can_serve_from_cache => {
                            // Check if cached ranges are expired and need conditional validation (Requirement 2.2)
                            if !overlap.cached_ranges.is_empty() {
                                let disk_cache = range_handler.get_disk_cache_manager();
                                let disk_cache_guard = disk_cache.read().await;

                                // Check object-level expiration
                                let cached_range = &overlap.cached_ranges[0];
                                match disk_cache_guard
                                    .check_object_expiration(&cache_key)
                                    .await
                                {
                                    Ok(ObjectExpirationResult::Expired { last_modified }) => {
                                    debug!(
                                        "Full object expired, performing conditional validation: cache_key={}",
                                        cache_key
                                    );

                                    // Drop the lock before making S3 request
                                    drop(disk_cache_guard);

                                    // Build validation headers
                                    let mut validation_headers = header_map.clone();
                                    if let Some(ref lm) = last_modified {
                                        validation_headers
                                            .insert("if-modified-since".to_string(), lm.clone());
                                    }

                                    // Build S3 request context for conditional validation
                                    let validation_context = crate::s3_client::build_s3_request_context(
                                        method.clone(),
                                        uri.clone(),
                                        validation_headers,
                                        None, // No body
                                        host.clone(),
                                    );

                                    // Make conditional request to S3
                                    match s3_client.forward_request(validation_context).await {
                                        Ok(response) => {
                                            if response.status == StatusCode::NOT_MODIFIED {
                                                // 304 Not Modified - refresh TTL and serve from cache (Requirement 2.3)
                                                debug!(
                                                    "Full object conditional validation returned 304 Not Modified, refreshing TTL: cache_key={}",
                                                    cache_key
                                                );

                                                let mut disk_cache_guard = disk_cache.write().await;
                                                if let Err(e) = disk_cache_guard
                                                    .refresh_object_ttl(
                                                        &cache_key,
                                                        resolved_settings.get_ttl,
                                                    )
                                                    .await
                                                {
                                                    warn!("Failed to refresh full object TTL: {}", e);
                                                }
                                                drop(disk_cache_guard);

                                                // Serve from cache
                                                let header_map: HeaderMap = header_map
                                                    .iter()
                                                    .filter_map(|(k, v)| {
                                                        HeaderName::from_str(k)
                                                            .ok()
                                                            .zip(HeaderValue::from_str(v).ok())
                                                    })
                                                    .collect();
                                                return Self::serve_full_object_from_cache(
                                                    method,
                                                    &full_range,
                                                    &overlap,
                                                    &cache_key,
                                                    cache_manager,
                                                    range_handler,
                                                    s3_client,
                                                    &host,
                                                    uri.path(),
                                                    &header_map,
                                                    config,
                                                )
                                                .await;
                                            } else if response.status == StatusCode::OK {
                                                // 200 OK - data changed, remove stale range and forward to S3 (Requirement 2.4)
                                                debug!(
                                                    "Full object conditional validation returned 200 OK, data changed: cache_key={}, range={}-{}",
                                                    cache_key, cached_range.start, cached_range.end
                                                );

                                                let mut disk_cache_guard = disk_cache.write().await;
                                                if let Err(e) = disk_cache_guard
                                                    .remove_invalidated_range(
                                                        &cache_key,
                                                        cached_range.start,
                                                        cached_range.end,
                                                    )
                                                    .await
                                                {
                                                    warn!("Failed to remove invalidated full object range: {}", e);
                                                }
                                                drop(disk_cache_guard);

                                                // Fall through to forward_get_head_with_coordination
                                            } else {
                                                // Validation returned unexpected status - forward original request to S3 (Requirement 2.5)
                                                warn!(
                                                    "Full object conditional validation returned unexpected status ({}), forwarding to S3: cache_key={}, range={}-{}",
                                                    response.status, cache_key, cached_range.start, cached_range.end
                                                );

                                                let mut disk_cache_guard = disk_cache.write().await;
                                                if let Err(e) = disk_cache_guard
                                                    .remove_invalidated_range(
                                                        &cache_key,
                                                        cached_range.start,
                                                        cached_range.end,
                                                    )
                                                    .await
                                                {
                                                    warn!("Failed to remove invalidated full object range: {}", e);
                                                }
                                                drop(disk_cache_guard);

                                                // Fall through to forward_get_head_with_coordination
                                            }
                                        }
                                        Err(e) => {
                                            // Validation request failed - forward original request to S3
                                            warn!(
                                                "Full object conditional validation request failed ({}), forwarding to S3: cache_key={}, range={}-{}",
                                                e, cache_key, cached_range.start, cached_range.end
                                            );

                                            let mut disk_cache_guard = disk_cache.write().await;
                                            if let Err(e) = disk_cache_guard
                                                .remove_invalidated_range(
                                                    &cache_key,
                                                    cached_range.start,
                                                    cached_range.end,
                                                )
                                                .await
                                            {
                                                warn!("Failed to remove invalidated full object range: {}", e);
                                            }
                                            drop(disk_cache_guard);

                                            // Fall through to forward_get_head_with_coordination
                                        }
                                    }
                                    }
                                    Ok(ObjectExpirationResult::Fresh) => {
                                    // Not expired - serve from cache as before
                                    drop(disk_cache_guard);

                                    // Cache HIT - we can serve the full object from cache
                                    debug!(
                                        operation = "GET",
                                        cache_result = "HIT",
                                        cache_type = "full_object_from_ranges",
                                        path = uri.path(),
                                        size_bytes = total_size,
                                        "Cache operation completed"
                                    );

                                    let header_map: HeaderMap = header_map
                                        .iter()
                                        .filter_map(|(k, v)| {
                                            HeaderName::from_str(k)
                                                .ok()
                                                .zip(HeaderValue::from_str(v).ok())
                                        })
                                        .collect();

                                    return Self::serve_full_object_from_cache(
                                        method,
                                        &full_range,
                                        &overlap,
                                        &cache_key,
                                        cache_manager,
                                        range_handler,
                                        s3_client,
                                        &host,
                                        uri.path(),
                                        &header_map,
                                        config,
                                    )
                                    .await;
                                    }
                                    Err(e) => {
                                        // Unexpected error - log and fall through to S3
                                        warn!(
                                            "Error checking object expiration: cache_key={}, error={}",
                                            cache_key, e
                                        );
                                        drop(disk_cache_guard);
                                    }
                                }
                            } else {
                                // No cached ranges (shouldn't happen if can_serve_from_cache is true, but handle gracefully)
                                debug!(
                                    operation = "GET",
                                    cache_result = "HIT",
                                    cache_type = "full_object_from_ranges",
                                    path = uri.path(),
                                    size_bytes = total_size,
                                    "Cache operation completed"
                                );

                                let header_map: HeaderMap = header_map
                                    .iter()
                                    .filter_map(|(k, v)| {
                                        HeaderName::from_str(k)
                                            .ok()
                                            .zip(HeaderValue::from_str(v).ok())
                                    })
                                    .collect();

                                return Self::serve_full_object_from_cache(
                                    method,
                                    &full_range,
                                    &overlap,
                                    &cache_key,
                                    cache_manager,
                                    range_handler,
                                    s3_client,
                                    &host,
                                    uri.path(),
                                    &header_map,
                                    config,
                                )
                                .await;
                            }
                        }
                        Ok(_) => {
                            // Partial cache coverage - fall through to S3
                            debug!("Partial cache coverage for full object: {}", cache_key);
                        }
                        Err(e) => {
                            // Cache error - log and fall through to S3
                            warn!(
                                "Cache error checking ranges for key {}: {}, forwarding to S3",
                                cache_key, e
                            );
                        }
                    }
                }
                Ok(Some((false, _))) => {
                    debug!("No cached ranges found for key: {}", cache_key);
                }
                Ok(None) => {
                    debug!("No metadata found for key: {}", cache_key);
                }
                Err(e) => {
                    warn!(
                        "Error checking cached ranges for key {}: {}, forwarding to S3",
                        cache_key, e
                    );
                }
            }

            // Cache MISS or no complete cached object - forward to S3
            // Note: cache miss statistics are recorded inside the coordination path
            // (only when an actual S3 fetch occurs, not when a waiter serves from cache)

            // GET requests are independent of HEAD cache state and extract metadata from S3 response
            // Use download coordination if enabled
            Self::forward_get_head_with_coordination(
                method,
                uri.clone(),
                host,
                header_map,
                cache_key,
                cache_manager,
                s3_client,
                inflight_tracker,
                range_handler,
                config.clone(),
                config.cache.download_coordination.enabled,
                config.cache.download_coordination.wait_timeout(),
                metrics_manager,
                proxy_referer,
            )
            .await
        }
    }

    /// Handle PUT requests with write-through caching - Requirements 10.1, 10.4
    async fn handle_put_request(
        req: Request<hyper::body::Incoming>,
        host: String,
        config: Arc<Config>,
        cache_manager: Arc<CacheManager>,
        s3_client: Arc<S3Client>,
        metrics_manager: Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        let uri = req.uri().clone();
        let path = uri.path();
        let query = uri.query().unwrap_or("");

        debug!("PUT request to {} from host: {}", uri, host);

        // Extract headers for signature detection and multipart detection
        let headers: HashMap<String, String> = req
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        // Check if this is an AWS SigV4 signed request
        if crate::signed_request_proxy::is_aws_sigv4_signed(&headers) {
            // Resolve per-bucket write cache settings - Requirements 4.1, 4.2, 4.3, 4.4
            let resolved_settings = cache_manager.resolve_settings(path).await;

            // Check if write caching is enabled for this bucket/prefix
            if !resolved_settings.write_cache_enabled {
                debug!(
                    "Write caching disabled for bucket/prefix, forwarding signed PUT request directly to S3 without caching: path={}, source={:?}",
                    path, resolved_settings.source
                );

                // Forward request to S3 and invalidate cache on success
                let host_for_cache = host.clone();
                let response = Self::forward_signed_request(req, host, s3_client, proxy_referer).await?;

                // If PUT was successful, invalidate cache
                if response.status().is_success() {
                    let cache_key = CacheManager::generate_cache_key(path, Some(&host_for_cache));
                    if let Err(e) = cache_manager
                        .invalidate_cache_unified_for_operation(&cache_key, "PUT")
                        .await
                    {
                        // Log warning but don't fail the PUT request (Requirement 5.2)
                        warn!(
                            "Failed to invalidate cache after successful signed PUT: cache_key={}, error={}",
                            cache_key, e
                        );
                    } else {
                        debug!(
                            "Successfully invalidated cache after signed PUT: cache_key={}",
                            cache_key
                        );
                    }
                }

                return Ok(response);
            }

            debug!("Detected AWS SigV4 signed PUT request, using SignedPutHandler for caching");

            // Generate cache key
            let cache_key = CacheManager::generate_cache_key(path, Some(&host));

            // Get connection pool to resolve DNS and get target IP
            let pool = s3_client.get_connection_pool();
            let mut pool_manager = pool.lock().await;

            // Resolve DNS to get target IP
            match pool_manager.get_connection(&host, None).await {
                Ok(connection) => {
                    let target_ip = connection.ip_address;
                    drop(pool_manager); // Release lock

                    // Create TLS connector
                    let mut root_store = rustls::RootCertStore::empty();

                    // Load system root certificates
                    match rustls_native_certs::load_native_certs() {
                        Ok(certs) => {
                            for cert in certs {
                                if let Err(e) = root_store.add(cert) {
                                    warn!("Failed to add cert: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to load native certs: {}", e);
                            return Ok(Self::build_error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "InternalError",
                                "Failed to load TLS certificates",
                                None,
                            ));
                        }
                    }

                    let tls_config = rustls::ClientConfig::builder()
                        .with_root_certificates(root_store)
                        .with_no_client_auth();

                    let tls_connector =
                        Arc::new(tokio_rustls::TlsConnector::from(Arc::new(tls_config)));

                    // Get current cache usage and max capacity for capacity checking
                    let cache_stats = cache_manager.get_statistics();
                    let current_cache_usage =
                        cache_stats.read_cache_size + cache_stats.write_cache_size;
                    let max_cache_capacity = config.cache.max_cache_size;

                    // Create SignedPutHandler
                    let compression_handler = cache_manager.get_compression_handler();
                    let mut signed_put_handler = crate::signed_put_handler::SignedPutHandler::new(
                        config.cache.cache_dir.clone(),
                        (*compression_handler).clone(),
                        current_cache_usage,
                        max_cache_capacity,
                        proxy_referer.clone(),
                    );

                    // Set metrics manager if available (Requirements 9.1, 9.2, 9.3, 9.4, 9.5)
                    if let Some(metrics) = &metrics_manager {
                        signed_put_handler.set_metrics_manager(metrics.clone());
                    }

                    // Set cache manager for HEAD cache invalidation (Requirements 3.1, 4.1)
                    signed_put_handler.set_cache_manager(Arc::clone(&cache_manager));

                    // Set S3 client for comprehensive response header extraction
                    signed_put_handler.set_s3_client(Arc::clone(&s3_client));

                    // Handle signed PUT with caching (Requirements 1.1, 2.1, 9.1, 9.2)
                    match signed_put_handler
                        .handle_signed_put(req, cache_key, host, target_ip, tls_connector)
                        .await
                    {
                        Ok(response) => Ok(response),
                        Err(e) => {
                            error!("Failed to handle signed PUT with caching: {}", e);
                            Ok(Self::build_error_response(
                                StatusCode::BAD_GATEWAY,
                                "BadGateway",
                                "Failed to forward signed PUT request to S3",
                                None,
                            ))
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to get connection for signed PUT request: {}", e);
                    Ok(Self::build_error_response(
                        StatusCode::BAD_GATEWAY,
                        "BadGateway",
                        "Failed to resolve S3 endpoint",
                        None,
                    ))
                }
            }
        } else {
            // Unsigned PUT request - use existing behavior
            Self::handle_unsigned_put_request(
                req,
                host,
                path,
                query,
                config,
                cache_manager,
                s3_client,
            )
            .await
        }
    }

    /// Handle unsigned PUT requests with write-through caching
    async fn handle_unsigned_put_request(
        req: Request<hyper::body::Incoming>,
        host: String,
        path: &str,
        query: &str,
        config: Arc<Config>,
        cache_manager: Arc<CacheManager>,
        s3_client: Arc<S3Client>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        // Extract headers for multipart detection
        let headers: HashMap<String, String> = req
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        // Check if this is a multipart upload - skip write caching if so
        let is_multipart = cache_manager.is_multipart_upload(&headers, query);
        if is_multipart {
            debug!("Detected multipart upload, skipping write-through caching");
            // Forward directly to S3 without caching
            return Self::forward_put_to_s3_without_caching(req, host, s3_client).await;
        }

        // Read request body for caching
        let body_bytes = match Self::read_request_body(req).await {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to read PUT request body: {}", e);
                return Ok(Self::build_error_response(
                    StatusCode::BAD_REQUEST,
                    "BadRequest",
                    "Failed to read request body",
                    None,
                ));
            }
        };

        // Check write cache configuration and size limits - Requirements 1.2, 1.5, 2.4, 2.5
        let body_size = body_bytes.len() as u64;

        // Resolve per-bucket write cache settings - Requirements 4.1, 4.2, 4.3, 4.4
        let resolved_settings = cache_manager.resolve_settings(path).await;

        // Check if write caching is enabled for this bucket/prefix
        if !resolved_settings.write_cache_enabled {
            debug!(
                "Write caching disabled for bucket/prefix, forwarding PUT request directly to S3 without caching: path={}, size={} bytes, source={:?}",
                path, body_size, resolved_settings.source
            );

            // Forward to S3 and invalidate cache on success
            let cache_key = CacheManager::generate_cache_key(path, Some(&host));
            match Self::forward_put_to_s3_with_body(body_bytes, host, path, headers, s3_client)
                .await
            {
                Ok(response) => {
                    let status = response.status();

                    // Invalidate cache after successful PUT when write caching is disabled
                    if status.is_success() {
                        if let Err(e) = cache_manager
                            .invalidate_cache_unified_for_operation(&cache_key, "PUT")
                            .await
                        {
                            // Log warning but don't fail the request (Requirements 5.1, 5.2)
                            warn!(
                                "Failed to invalidate cache after PUT: cache_key={}, error={}",
                                cache_key, e
                            );
                        }
                    }

                    return Ok(response);
                }
                Err(_) => {
                    return Ok(Self::build_error_response(
                        StatusCode::BAD_GATEWAY,
                        "BadGateway",
                        "Failed to forward request to S3",
                        None,
                    ));
                }
            }
        }

        // Write caching is enabled - check size limits - Requirement 10.5
        let can_cache = cache_manager.can_write_cache_accommodate(body_size)
            && body_size <= config.cache.write_cache_max_object_size;

        if !can_cache {
            debug!(
                "Write cache cannot accommodate PUT request, forwarding without caching: path={}, size={} bytes, max_size={}, can_accommodate={}",
                path, body_size, config.cache.write_cache_max_object_size,
                cache_manager.can_write_cache_accommodate(body_size)
            );

            // Forward to S3 and invalidate cache on success
            let cache_key = CacheManager::generate_cache_key(path, Some(&host));
            match Self::forward_put_to_s3_with_body(body_bytes, host, path, headers, s3_client)
                .await
            {
                Ok(response) => {
                    let status = response.status();

                    // Invalidate cache after successful PUT when write caching cannot accommodate
                    if status.is_success() {
                        if let Err(e) = cache_manager
                            .invalidate_cache_unified_for_operation(&cache_key, "PUT")
                            .await
                        {
                            // Log warning but don't fail the request (Requirements 5.1, 5.2)
                            warn!(
                                "Failed to invalidate cache after PUT: cache_key={}, error={}",
                                cache_key, e
                            );
                        }
                    }

                    return Ok(response);
                }
                Err(_) => {
                    return Ok(Self::build_error_response(
                        StatusCode::BAD_GATEWAY,
                        "BadGateway",
                        "Failed to forward request to S3",
                        None,
                    ));
                }
            }
        }

        // Forward to S3 and cache on success
        let cache_key = CacheManager::generate_cache_key(path, Some(&host));

        match Self::forward_put_to_s3_with_body(
            body_bytes.clone(),
            host.clone(),
            path,
            headers.clone(),
            s3_client,
        )
        .await
        {
            Ok(response) => {
                let status = response.status();

                if status.is_success() {
                    // Cache the PUT request body - Requirement 10.1
                    let metadata = Self::extract_metadata_from_headers(&headers, body_size);

                    if let Err(e) = cache_manager
                        .store_write_cache_entry(&cache_key, &body_bytes, headers, metadata)
                        .await
                    {
                        warn!("Failed to store PUT request in write cache: {}", e);
                        // Don't fail the request due to cache issues
                    } else {
                        debug!("Successfully cached PUT request for key: {}", cache_key);
                    }

                    // Invalidate all cache layers after successful PUT (Requirements 11.1, 11.4)
                    if let Err(e) = cache_manager
                        .invalidate_cache_unified_for_operation(&cache_key, "PUT")
                        .await
                    {
                        // Log warning but don't fail the request (Requirement 3.2)
                        warn!(
                            "Failed to invalidate cache after PUT: cache_key={}, error={}",
                            cache_key, e
                        );
                    }
                } else {
                    // PUT failed, clean up any cached data - Requirement 10.2
                    debug!(
                        "PUT request failed with status {}, cleaning up any cached data",
                        status
                    );
                    if let Err(e) = cache_manager.cleanup_failed_put(&cache_key).await {
                        warn!("Failed to cleanup failed PUT for key {}: {}", cache_key, e);
                    }
                }

                Ok(response)
            }
            Err(e) => {
                error!("Failed to forward PUT request to S3: {:?}", e);
                Ok(Self::build_error_response(
                    StatusCode::BAD_GATEWAY,
                    "BadGateway",
                    "Failed to forward request to S3",
                    None,
                ))
            }
        }
    }

    /// Handle other HTTP methods (POST, DELETE, etc.)
    async fn handle_other_request(
        req: Request<hyper::body::Incoming>,
        host: String,
        config: Arc<Config>,
        cache_manager: Arc<CacheManager>,
        s3_client: Arc<S3Client>,
        metrics_manager: Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        let method = req.method().clone();
        let uri = req.uri().clone();

        debug!("{} request to {} from host: {}", method, uri, host);

        // Forward non-cacheable requests to S3
        let headers: HashMap<String, String> = req
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        // Check if this is an AWS SigV4 signed request
        // If so, check if it's a CompleteMultipartUpload or AbortMultipartUpload that needs special handling
        if crate::signed_request_proxy::is_aws_sigv4_signed(&headers) {
            // Check if this is a CompleteMultipartUpload POST request
            if method == Method::POST {
                let query = uri.query().unwrap_or("");
                // CompleteMultipartUpload has uploadId but no partNumber
                if query.contains("uploadId") && !query.contains("partNumber") {
                    debug!("Detected CompleteMultipartUpload POST request, routing to SignedPutHandler");
                    // Route to PUT handler which will detect and handle CompleteMultipartUpload
                    return Self::handle_put_request(
                        req,
                        host,
                        config,
                        cache_manager,
                        s3_client,
                        metrics_manager,
                        proxy_referer,
                    )
                    .await;
                }
            }

            // Check if this is an AbortMultipartUpload DELETE request (Requirement 4.5)
            if method == Method::DELETE {
                let query = uri.query().unwrap_or("");
                // AbortMultipartUpload has uploadId but no partNumber
                if query.contains("uploadId") && !query.contains("partNumber") {
                    debug!(
                        "Detected AbortMultipartUpload DELETE request, routing to SignedPutHandler"
                    );
                    // Route to PUT handler which will detect and handle AbortMultipartUpload
                    return Self::handle_put_request(
                        req,
                        host,
                        config,
                        cache_manager,
                        s3_client,
                        metrics_manager,
                        proxy_referer,
                    )
                    .await;
                }

                // Signed DELETE of regular object — invalidate cache after successful S3 response
                // (Requirements 5.1, 5.2, 5.3)
                let path = uri.path().to_string();
                let host_for_cache = host.clone();
                let response =
                    Self::forward_signed_request(req, host, s3_client, proxy_referer).await?;
                if response.status().is_success() {
                    let cache_key = CacheManager::generate_cache_key(&path, Some(&host_for_cache));
                    if let Err(e) = cache_manager
                        .invalidate_cache_unified_for_operation(&cache_key, "DELETE")
                        .await
                    {
                        warn!(
                            "Failed to invalidate cache after signed DELETE: cache_key={}, error={}",
                            cache_key, e
                        );
                    }
                }
                return Ok(response);
            }

            debug!(
                "Detected AWS SigV4 signed {} request, forwarding without modification",
                method
            );
            return Self::forward_signed_request(req, host, s3_client, proxy_referer).await;
        }

        // Read request body if present
        let body_bytes = match Self::read_request_body(req).await {
            Ok(bytes) => {
                if bytes.is_empty() {
                    None
                } else {
                    Some(Bytes::from(bytes))
                }
            }
            Err(e) => {
                error!("Failed to read request body: {}", e);
                return Ok(Self::build_error_response(
                    StatusCode::BAD_REQUEST,
                    "BadRequest",
                    "Failed to read request body",
                    None,
                ));
            }
        };

        // Build S3 request context
        let host_for_cache = host.clone();
        let context =
            build_s3_request_context(method.clone(), uri.clone(), headers, body_bytes, host);

        match s3_client.forward_request(context).await {
            Ok(s3_response) => {
                debug!("Successfully forwarded request to S3");

                // For DELETE operations, invalidate cache if the operation was successful
                if method == Method::DELETE && s3_response.status.is_success() {
                    let path = uri.path();
                    let cache_key = CacheManager::generate_cache_key(path, Some(&host_for_cache));

                    // Invalidate all cache layers after successful DELETE (Requirements 11.1, 11.4)
                    if let Err(e) = cache_manager
                        .invalidate_cache_unified_for_operation(&cache_key, "DELETE")
                        .await
                    {
                        // Log warning but don't fail the request
                        warn!(
                            "Failed to invalidate cache after DELETE: cache_key={}, error={}",
                            cache_key, e
                        );
                    }
                }

                Self::convert_s3_response_to_http(s3_response)
            }
            Err(e) => {
                error!("Failed to forward request to S3: {}", e);
                Ok(Self::build_error_response(
                    StatusCode::BAD_GATEWAY,
                    "BadGateway",
                    "Failed to forward request to S3",
                    None,
                ))
            }
        }
    }

    /// Build S3-compatible XML error response
    fn build_error_response(
        status: StatusCode,
        code: &str,
        message: &str,
        retry_after: Option<&str>,
    ) -> Response<BoxBody<Bytes, hyper::Error>> {
        let xml_body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>{}</Code>
    <Message>{}</Message>
    <RequestId>{}</RequestId>
</Error>"#,
            code,
            message,
            Uuid::new_v4()
        );

        let mut response_builder = Response::builder()
            .status(status)
            .header("content-type", "application/xml")
            .header("content-length", xml_body.len());

        if let Some(retry_value) = retry_after {
            response_builder = response_builder.header("retry-after", retry_value);
        }

        response_builder
            .body(
                Full::new(Bytes::from(xml_body))
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .unwrap()
    }

    /// Read request body into bytes
    async fn read_request_body(
        req: Request<hyper::body::Incoming>,
    ) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        use http_body_util::BodyExt;

        let body = req.into_body();
        let body_bytes = body.collect().await?.to_bytes();
        Ok(body_bytes.to_vec())
    }

    /// Forward PUT request to S3 without caching (for multipart uploads)
    async fn forward_put_to_s3_without_caching(
        req: Request<hyper::body::Incoming>,
        host: String,
        s3_client: Arc<S3Client>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        debug!("Forwarding PUT request to S3 without caching");

        // Extract request components
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers: HashMap<String, String> = req
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        // Read body
        let body_bytes = match Self::read_request_body(req).await {
            Ok(bytes) => Some(Bytes::from(bytes)),
            Err(e) => {
                error!("Failed to read request body: {}", e);
                return Ok(Self::build_error_response(
                    StatusCode::BAD_REQUEST,
                    "BadRequest",
                    "Failed to read request body",
                    None,
                ));
            }
        };

        // Build S3 request context
        let context = build_s3_request_context(method, uri, headers, body_bytes, host);

        match s3_client.forward_request(context).await {
            Ok(s3_response) => {
                debug!("Successfully forwarded PUT request to S3");
                // Convert S3Response to HTTP Response
                Self::convert_s3_response_to_http(s3_response)
            }
            Err(e) => {
                error!("Failed to forward PUT request to S3: {}", e);
                Ok(Self::build_error_response(
                    StatusCode::BAD_GATEWAY,
                    "BadGateway",
                    "Failed to forward request to S3",
                    None,
                ))
            }
        }
    }

    /// Forward PUT request to S3 with body data
    async fn forward_put_to_s3_with_body(
        body_bytes: Vec<u8>,
        host: String,
        path: &str,
        headers: HashMap<String, String>,
        s3_client: Arc<S3Client>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        debug!(
            "Forwarding PUT request with body to S3 (size: {} bytes)",
            body_bytes.len()
        );

        // Build URI
        let uri = match format!("https://{}{}", host, path).parse() {
            Ok(uri) => uri,
            Err(e) => {
                error!("Failed to parse URI: {}", e);
                return Ok(Self::build_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    "Failed to build request URI",
                    None,
                ));
            }
        };

        // Build S3 request context
        let context = build_s3_request_context(
            Method::PUT,
            uri,
            headers,
            Some(Bytes::from(body_bytes)),
            host,
        );

        match s3_client.forward_request(context).await {
            Ok(s3_response) => {
                debug!("Successfully forwarded PUT request with body to S3");
                Self::convert_s3_response_to_http(s3_response)
            }
            Err(e) => {
                error!("Failed to forward PUT request with body to S3: {}", e);
                Ok(Self::build_error_response(
                    StatusCode::BAD_GATEWAY,
                    "BadGateway",
                    "Failed to forward request to S3",
                    None,
                ))
            }
        }
    }

    /// Extract metadata from headers for caching
    fn extract_metadata_from_headers(
        headers: &HashMap<String, String>,
        content_length: u64,
    ) -> CacheMetadata {
        CacheMetadata {
            etag: headers.get("etag").cloned().unwrap_or_default(),
            last_modified: headers.get("last-modified").cloned().unwrap_or_default(),
            content_length,
            part_number: None,
            cache_control: headers.get("cache-control").cloned(),
            access_count: 0,
            last_accessed: SystemTime::now(),
        }
    }

    /// Convert S3Response to HTTP Response with streaming support
    fn convert_s3_response_to_http(
        s3_response: crate::s3_client::S3Response,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        let mut response_builder = Response::builder().status(s3_response.status);

        // Add headers
        for (key, value) in s3_response.headers {
            response_builder = response_builder.header(&key, &value);
        }

        // Add body - stream if available, otherwise empty
        let body = match s3_response.body {
            Some(body) => Self::s3_body_to_box_body(body),
            None => Full::new(Bytes::new())
                .map_err(|never| match never {})
                .boxed(),
        };

        Ok(response_builder.body(body).unwrap())
    }

    /// Convert S3Response to HTTP Response with streaming and caching support
    ///
    /// This wraps streaming bodies with TeeStream to enable simultaneous streaming
    /// to client and caching in background.
    async fn convert_s3_response_to_http_with_caching(
        s3_response: crate::s3_client::S3Response,
        cache_key: String,
        range_spec: RangeSpec,
        range_handler: Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        config: Arc<Config>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        let mut response_builder = Response::builder().status(s3_response.status);
        let headers_clone = s3_response.headers.clone();

        // Add headers (skip checksum headers for range requests)
        for (key, value) in &s3_response.headers {
            // Skip checksum headers since they apply to the full object, not the range
            let key_lower = key.to_lowercase();
            if !matches!(
                key_lower.as_str(),
                "x-amz-checksum-crc32"
                    | "x-amz-checksum-crc32c"
                    | "x-amz-checksum-sha1"
                    | "x-amz-checksum-sha256"
                    | "x-amz-checksum-crc64nvme"
                    | "x-amz-checksum-type"
                    | "content-md5"
            ) {
                response_builder = response_builder.header(key, value);
            }
        }

        // Handle body with tee streaming for caching
        let body = match s3_response.body {
            Some(S3ResponseBody::Streaming(incoming)) => {
                // Create channel for cache data
                let (cache_tx, mut cache_rx) = mpsc::channel::<Bytes>(100);

                // Metadata will be extracted from headers in the spawned task using s3_client

                // Spawn background task to collect and cache data
                let cache_key_clone = cache_key.clone();
                let start = range_spec.start;
                let end = range_spec.end;
                let ttl = config.cache.get_ttl;
                let s3_client_clone = s3_client.clone();
                tokio::spawn(async move {
                    let mut accumulated = Vec::new();
                    while let Some(chunk) = cache_rx.recv().await {
                        accumulated.extend_from_slice(&chunk);
                    }

                    if !accumulated.is_empty() {
                        // Use the new method to create ObjectMetadata with all S3 response headers
                        let mut object_metadata =
                            s3_client_clone.extract_object_metadata_from_response(&headers_clone);
                        object_metadata.upload_state = crate::cache_types::UploadState::Complete;
                        object_metadata.cumulative_size = object_metadata.content_length;
                        object_metadata.compression_algorithm =
                            crate::compression::CompressionAlgorithm::Lz4;
                        object_metadata.compressed_size = 0;

                        if let Err(e) = range_handler
                            .store_range_new_storage(
                                &cache_key_clone,
                                start,
                                end,
                                &accumulated,
                                object_metadata,
                                ttl,
                            )
                            .await
                        {
                            warn!("Failed to cache streamed range {}-{}: {}", start, end, e);
                        } else {
                            debug!(
                                "Successfully cached streamed range {}-{} ({} bytes)",
                                start,
                                end,
                                accumulated.len()
                            );
                        }
                    }
                });

                // Convert Incoming to a Stream of Frames
                let frame_stream = futures::stream::unfold(incoming, |mut body| {
                    Box::pin(async move {
                        match body.frame().await {
                            Some(Ok(frame)) => Some((Ok(frame), body)),
                            Some(Err(e)) => Some((Err(e), body)),
                            None => None,
                        }
                    })
                });

                // Wrap with TeeStream
                let tee_stream = TeeStream::new(frame_stream, cache_tx);

                // Convert to BoxBody - StreamBody already implements Body
                BoxBody::new(StreamBody::new(tee_stream))
            }
            Some(S3ResponseBody::Buffered(bytes)) => {
                // Already buffered, just return it
                Full::new(bytes).map_err(|never| match never {}).boxed()
            }
            None => Full::new(Bytes::new())
                .map_err(|never| match never {})
                .boxed(),
        };

        Ok(response_builder.body(body).unwrap())
    }

    /// Handle range requests with caching and conditional headers - Requirements 3.1, 3.2, 3.3, 3.4, 3.6, 3.7, 3.8
    async fn handle_range_request(
        method: Method,
        cache_key: String,
        range_header: &str,
        client_headers: HashMap<String, String>,
        cache_manager: Arc<CacheManager>,
        range_handler: Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        host: String,
        uri: hyper::Uri,
        config: Arc<Config>,
        resolved_get_ttl: std::time::Duration,
        current_etag: Option<String>,
        inflight_tracker: Arc<InFlightTracker>,
        metrics_manager: Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        debug!(
            "[DIAGNOSTIC] handle_range_request called - cache_key: {}, range: {}",
            cache_key, range_header
        );

        // Get content_length from cached metadata if available (for open-ended ranges like "bytes=100-")
        // Also capture the full metadata for pass-through to avoid redundant NFS reads (Requirement 1.1)
        let (content_length, preloaded_metadata) = match cache_manager.get_metadata_cached(&cache_key).await {
            Ok(Some(metadata)) => {
                let len = metadata.object_metadata.content_length;
                debug!(
                    "Found cached content_length for range parsing: cache_key={}, content_length={}",
                    cache_key, len
                );
                // Treat content_length 0 as unknown — don't validate ranges against it
                let cl = if len > 0 { Some(len) } else { None };
                (cl, Some(metadata))
            }
            Ok(None) => {
                debug!(
                    "No cached metadata for range parsing: cache_key={}",
                    cache_key
                );
                (None, None)
            }
            Err(e) => {
                debug!(
                    "Error getting metadata for range parsing: cache_key={}, error={}",
                    cache_key, e
                );
                (None, None)
            }
        };

        // Parse range header with content_length if available
        debug!("[DIAGNOSTIC] Parsing range header: {}", range_header);
        let range_result = range_handler.parse_range_header(range_header, content_length);

        match range_result {
            RangeParseResult::SingleRange(range_spec) => {
                debug!(
                    "[DIAGNOSTIC] Parsed single range: start={}, end={}",
                    range_spec.start, range_spec.end
                );

                // Requirement 2.1: Skip full-object cache check for large files
                // When content_length exceeds the threshold, skip has_cached_ranges + find_cached_ranges(full_range)
                // and proceed directly to range-specific find_cached_ranges.
                // Requirement 2.3: When content_length is unknown (metadata is None), proceed with full-object check as before.
                let skip_full_object_check = preloaded_metadata
                    .as_ref()
                    .map(|m| m.object_metadata.content_length > config.cache.full_object_check_threshold)
                    .unwrap_or(false);

                if skip_full_object_check {
                    debug!(
                        "Skipping full-object cache check for large file: cache_key={}, content_length={}, threshold={}",
                        cache_key,
                        preloaded_metadata.as_ref().map(|m| m.object_metadata.content_length).unwrap_or(0),
                        config.cache.full_object_check_threshold
                    );
                }

                // Requirement 1.3: First check if we have a full object cached that can serve this range
                if !skip_full_object_check {
                debug!("Range request: checking for full object cache first: cache_key={}, requested_range={}-{}", cache_key, range_spec.start, range_spec.end);
                match cache_manager.has_cached_ranges(&cache_key, preloaded_metadata.as_ref()).await {
                    Ok(Some((true, total_size))) => {
                        // We have cached ranges - check if they represent a full object that can serve this range
                        if range_spec.start < total_size && range_spec.end < total_size {
                            debug!("Full object available for range request: cache_key={}, total_size={}, requested_range={}-{}", cache_key, total_size, range_spec.start, range_spec.end);

                            // Create a full object range spec to check if it's completely cached
                            let full_range = crate::range_handler::RangeSpec {
                                start: 0,
                                end: total_size - 1,
                            };

                            // Check if the full object is cached with ETag validation
                            match range_handler
                                .find_cached_ranges(
                                    &cache_key,
                                    &full_range,
                                    current_etag.as_deref(),
                                    preloaded_metadata.as_ref(),
                                )
                                .await
                            {
                                Ok(full_overlap) if full_overlap.can_serve_from_cache => {
                                    debug!(
                                        "Range request served from full object cache: cache_key={}, requested_range={}-{}, full_object_size={} bytes",
                                        cache_key, range_spec.start, range_spec.end, total_size
                                    );

                                    // Filter cached ranges to only include those that overlap with the requested range
                                    let mut filtered_cached_ranges = Vec::new();
                                    for cached_range in &full_overlap.cached_ranges {
                                        // Check if this cached range overlaps with the requested range
                                        if cached_range.start <= range_spec.end
                                            && cached_range.end >= range_spec.start
                                        {
                                            filtered_cached_ranges.push(cached_range.clone());
                                        }
                                    }

                                    // Create filtered overlap with only relevant ranges
                                    let filtered_overlap = crate::range_handler::RangeOverlap {
                                        cached_ranges: filtered_cached_ranges,
                                        missing_ranges: Vec::new(), // No missing ranges since we can serve from cache
                                        can_serve_from_cache: true,
                                    };

                                    // Serve the requested range from the filtered cache ranges
                                    let header_map: HeaderMap = client_headers
                                        .iter()
                                        .filter_map(|(k, v)| {
                                            k.parse::<hyper::header::HeaderName>().ok().and_then(
                                                |name| {
                                                    v.parse::<hyper::header::HeaderValue>()
                                                        .ok()
                                                        .map(|val| (name, val))
                                                },
                                            )
                                        })
                                        .collect();
                                    return Self::serve_range_from_cache(
                                        method,
                                        &range_spec,
                                        &filtered_overlap,
                                        &cache_key,
                                        cache_manager,
                                        range_handler,
                                        s3_client.clone(),
                                        &host,
                                        &uri.to_string(),
                                        &header_map,
                                        config.clone(),
                                        preloaded_metadata.as_ref(),
                                    )
                                    .await;
                                }
                                Ok(_) => {
                                    debug!("Full object not completely cached, falling back to range-specific lookup: cache_key={}", cache_key);
                                }
                                Err(e) => {
                                    debug!("Error checking full object cache, falling back to range-specific lookup: cache_key={}, error={}", cache_key, e);
                                }
                            }
                        } else {
                            debug!("Requested range exceeds cached object size: cache_key={}, total_size={}, requested_range={}-{}", cache_key, total_size, range_spec.start, range_spec.end);
                        }
                    }
                    Ok(Some((false, _))) => {
                        debug!(
                            "No cached ranges found for range request: cache_key={}",
                            cache_key
                        );
                    }
                    Ok(None) => {
                        debug!(
                            "No metadata found for range request: cache_key={}",
                            cache_key
                        );
                    }
                    Err(e) => {
                        debug!("Error checking cached ranges for range request: cache_key={}, error={}", cache_key, e);
                    }
                }
                } // end if !skip_full_object_check

                // Fall back to range-specific cache lookup with ETag validation (Requirement 3.3)
                debug!("[DIAGNOSTIC] Calling find_cached_ranges for cache_key: {}, range: {}-{}, etag: {:?}", cache_key, range_spec.start, range_spec.end, current_etag);
                match range_handler
                    .find_cached_ranges(&cache_key, &range_spec, current_etag.as_deref(), preloaded_metadata.as_ref())
                    .await
                {
                    Ok(overlap) => {
                        debug!("[DIAGNOSTIC] find_cached_ranges returned: cached_ranges={}, missing_ranges={}, can_serve_from_cache={}",
                               overlap.cached_ranges.len(), overlap.missing_ranges.len(), overlap.can_serve_from_cache);

                        // Requirement 4.2: Log when cached ranges are found during full object GET
                        // Include range details (start, end, size) - moved to DEBUG to reduce noise
                        if !overlap.cached_ranges.is_empty() {
                            for cached_range in &overlap.cached_ranges {
                                let range_size = cached_range.end - cached_range.start + 1;
                                debug!(
                                    "Cached range hit: cache_key={}, range={}-{}, size={} bytes, etag={}",
                                    cache_key, cached_range.start, cached_range.end, range_size, cached_range.etag
                                );
                            }
                        }

                        // Log details of cached ranges found (debug level for diagnostics)
                        for (i, cached_range) in overlap.cached_ranges.iter().enumerate() {
                            debug!("[DIAGNOSTIC] Cached range {}: start={}, end={}, etag={}, has_data={}",
                                   i, cached_range.start, cached_range.end, cached_range.etag, !cached_range.data.is_empty());
                        }

                        // Log details of missing ranges
                        for (i, missing_range) in overlap.missing_ranges.iter().enumerate() {
                            debug!(
                                "[DIAGNOSTIC] Missing range {}: start={}, end={}",
                                i, missing_range.start, missing_range.end
                            );
                        }

                        // Check if cached ranges are expired and need conditional validation (Requirement 1.4)
                        if !overlap.cached_ranges.is_empty() {
                            let disk_cache = range_handler.get_disk_cache_manager();
                            let disk_cache_guard = disk_cache.read().await;

                            // Check object-level expiration
                            let cached_range = &overlap.cached_ranges[0];
                            match disk_cache_guard
                                .check_object_expiration(&cache_key)
                                .await
                            {
                                Ok(ObjectExpirationResult::Expired { last_modified }) => {
                                debug!(
                                    "Range expired, performing conditional validation: cache_key={}",
                                    cache_key
                                );

                                // Drop the lock before making S3 request
                                drop(disk_cache_guard);

                                // Build validation headers
                                let mut validation_headers = client_headers.clone();
                                if let Some(ref lm) = last_modified {
                                    validation_headers
                                        .insert("if-modified-since".to_string(), lm.clone());
                                }
                                validation_headers.insert(
                                    "range".to_string(),
                                    format!("bytes={}-{}", range_spec.start, range_spec.end),
                                );

                                // Build S3 request context for conditional validation
                                let validation_context = crate::s3_client::build_s3_request_context(
                                    method.clone(),
                                    uri.clone(),
                                    validation_headers,
                                    None, // No body
                                    host.clone(),
                                );

                                // Make conditional request to S3
                                match s3_client.forward_request(validation_context).await {
                                    Ok(response) => {
                                        if response.status == StatusCode::NOT_MODIFIED {
                                            // 304 Not Modified - refresh TTL and serve from cache (Requirement 1.5, 6.4)
                                            debug!(
                                                "Conditional validation returned 304 Not Modified, refreshing TTL: cache_key={}",
                                                cache_key
                                            );

                                            let mut disk_cache_guard = disk_cache.write().await;
                                            if let Err(e) = disk_cache_guard
                                                .refresh_object_ttl(
                                                    &cache_key,
                                                    resolved_get_ttl,
                                                )
                                                .await
                                            {
                                                warn!("Failed to refresh object TTL: {}", e);
                                            }
                                            drop(disk_cache_guard);

                                            // Serve from cache
                                            let header_map: HeaderMap = client_headers
                                                .iter()
                                                .filter_map(|(k, v)| {
                                                    k.parse::<hyper::header::HeaderName>()
                                                        .ok()
                                                        .and_then(|name| {
                                                            v.parse::<hyper::header::HeaderValue>()
                                                                .ok()
                                                                .map(|val| (name, val))
                                                        })
                                                })
                                                .collect();
                                            return Self::serve_range_from_cache(
                                                method,
                                                &range_spec,
                                                &overlap,
                                                &cache_key,
                                                cache_manager,
                                                range_handler,
                                                s3_client.clone(),
                                                &host,
                                                &uri.to_string(),
                                                &header_map,
                                                config.clone(),
                                                preloaded_metadata.as_ref(),
                                            )
                                            .await;
                                        } else if response.status == StatusCode::OK {
                                            // 200 OK - data changed, remove invalidated range and cache new data (Requirement 1.6)
                                            debug!(
                                                "Conditional validation returned 200 OK, data changed: cache_key={}, range={}-{}",
                                                cache_key, cached_range.start, cached_range.end
                                            );

                                            let mut disk_cache_guard = disk_cache.write().await;
                                            if let Err(e) = disk_cache_guard
                                                .remove_invalidated_range(
                                                    &cache_key,
                                                    cached_range.start,
                                                    cached_range.end,
                                                )
                                                .await
                                            {
                                                warn!("Failed to remove invalidated range: {}", e);
                                            }
                                            drop(disk_cache_guard);

                                            // Forward request to S3 to get and cache new data
                                            return Self::forward_range_request_to_s3(
                                                method,
                                                uri.clone(),
                                                host.clone(),
                                                client_headers.clone(),
                                                cache_key.clone(),
                                                range_spec.clone(),
                                                overlap,
                                                cache_manager,
                                                range_handler.clone(),
                                                s3_client,
                                                config.clone(),
                                                preloaded_metadata.as_ref(),
                                                proxy_referer,
                                            )
                                            .await;
                                        } else {
                                            // Validation returned unexpected status - forward original request to S3
                                            warn!(
                                                "Conditional validation returned unexpected status ({}), forwarding original request to S3: cache_key={}, range={}-{}",
                                                response.status, cache_key, cached_range.start, cached_range.end
                                            );

                                            // Remove the expired range since we couldn't validate it
                                            let mut disk_cache_guard = disk_cache.write().await;
                                            if let Err(e) = disk_cache_guard
                                                .remove_invalidated_range(
                                                    &cache_key,
                                                    cached_range.start,
                                                    cached_range.end,
                                                )
                                                .await
                                            {
                                                warn!("Failed to remove invalidated range: {}", e);
                                            }
                                            drop(disk_cache_guard);

                                            // Forward original request to S3 and cache response if successful
                                            return Self::forward_range_request_to_s3(
                                                method,
                                                uri.clone(),
                                                host.clone(),
                                                client_headers.clone(),
                                                cache_key.clone(),
                                                range_spec.clone(),
                                                overlap,
                                                cache_manager,
                                                range_handler.clone(),
                                                s3_client,
                                                config.clone(),
                                                preloaded_metadata.as_ref(),
                                                proxy_referer,
                                            )
                                            .await;
                                        }
                                    }
                                    Err(e) => {
                                        // Validation request failed - forward original request to S3
                                        warn!(
                                            "Conditional validation request failed ({}), forwarding original request to S3: cache_key={}, range={}-{}",
                                            e, cache_key, cached_range.start, cached_range.end
                                        );

                                        // Remove the expired range since we couldn't validate it
                                        let mut disk_cache_guard = disk_cache.write().await;
                                        if let Err(e) = disk_cache_guard
                                            .remove_invalidated_range(
                                                &cache_key,
                                                cached_range.start,
                                                cached_range.end,
                                            )
                                            .await
                                        {
                                            warn!("Failed to remove invalidated range: {}", e);
                                        }
                                        drop(disk_cache_guard);

                                        // Forward original request to S3 and cache response if successful
                                        return Self::forward_range_request_to_s3(
                                            method,
                                            uri.clone(),
                                            host.clone(),
                                            client_headers.clone(),
                                            cache_key.clone(),
                                            range_spec.clone(),
                                            overlap,
                                            cache_manager,
                                            range_handler.clone(),
                                            s3_client,
                                            config.clone(),
                                            preloaded_metadata.as_ref(),
                                            proxy_referer,
                                        )
                                        .await;
                                    }
                                }
                                }
                                Ok(ObjectExpirationResult::Fresh) => {
                                    // Not expired - fall through to serve from cache
                                    drop(disk_cache_guard);
                                }
                                Err(e) => {
                                    // Unexpected error - log and fall through
                                    warn!(
                                        "Error checking object expiration: cache_key={}, error={}",
                                        cache_key, e
                                    );
                                    drop(disk_cache_guard);
                                }
                            }
                        }

                        if overlap.can_serve_from_cache {
                            // Requirement 4.3: Log when full object is served entirely from cache
                            // Include cache efficiency metrics
                            let total_cached_bytes: u64 = overlap
                                .cached_ranges
                                .iter()
                                .map(|r| r.end - r.start + 1)
                                .sum();
                            let requested_bytes = range_spec.end - range_spec.start + 1;
                            let cache_efficiency = if requested_bytes > 0 {
                                (total_cached_bytes as f64 / requested_bytes as f64) * 100.0
                            } else {
                                100.0
                            };

                            debug!(
                                "Complete cache hit: cache_key={}, range={}-{}, requested_bytes={}, cached_ranges={}, cache_efficiency={:.2}%, s3_requests=0",
                                cache_key, range_spec.start, range_spec.end, requested_bytes,
                                overlap.cached_ranges.len(), cache_efficiency
                            );

                            debug!("[DIAGNOSTIC] Range request can be served entirely from cache - {} cached ranges", overlap.cached_ranges.len());
                            let header_map: HeaderMap = client_headers
                                .iter()
                                .filter_map(|(k, v)| {
                                    k.parse::<hyper::header::HeaderName>()
                                        .ok()
                                        .and_then(|name| {
                                            v.parse::<hyper::header::HeaderValue>()
                                                .ok()
                                                .map(|val| (name, val))
                                        })
                                })
                                .collect();
                            return Self::serve_range_from_cache(
                                method,
                                &range_spec,
                                &overlap,
                                &cache_key,
                                cache_manager,
                                range_handler,
                                s3_client.clone(),
                                &host,
                                &uri.to_string(),
                                &header_map,
                                config.clone(),
                                preloaded_metadata.as_ref(),
                            )
                            .await;
                        } else {
                            debug!("[DIAGNOSTIC] Range request requires partial S3 fetch (cached: {}, missing: {})",
                                  overlap.cached_ranges.len(), overlap.missing_ranges.len());

                            // Record cache miss for disk cache statistics (GET request)
                            // This is a partial or complete miss since we need to fetch from S3
                            cache_manager.update_statistics(false, 0, false);
                            cache_manager.record_bucket_cache_access(&cache_key, false).await;

                            // Check if this is a signed range request (Requirement 1.2, 1.3)
                            // Only check signature when there are cache gaps to avoid overhead
                            let range_is_signed =
                                crate::signed_request_proxy::is_range_signed(&client_headers);

                            if range_is_signed {
                                // Signed range request with cache gaps - forward entire range to S3
                                // Cannot modify Range header without invalidating signature (Requirement 2.1, 2.2, 2.3)

                                // Use download coordination for signed range requests
                                // Requirement 15.3: Range requests use range key for independent tracking
                                return Self::forward_range_with_coordination(
                                    method,
                                    uri,
                                    host,
                                    client_headers,
                                    cache_key,
                                    range_spec,
                                    overlap,
                                    cache_manager,
                                    range_handler,
                                    s3_client,
                                    config.clone(),
                                    true, // is_signed
                                    None, // no preloaded metadata for signed path
                                    inflight_tracker,
                                    metrics_manager,
                                    proxy_referer,
                                )
                                .await;
                            }

                            // Standard (unsigned) range request - fetch only missing ranges (Requirement 1.4)
                            // Build conditional headers for missing portions - Requirement 3.6
                            let conditional_headers = range_handler
                                .build_conditional_headers_for_range(
                                    &client_headers,
                                    &overlap.cached_ranges,
                                );

                            debug!(
                                "Built conditional headers for partial fetch: {:?}",
                                conditional_headers
                            );

                            // Merge conditional headers with original client headers
                            // We need ALL headers (especially Authorization) plus the conditional headers
                            let mut merged_headers = client_headers.clone();
                            for (key, value) in conditional_headers {
                                merged_headers.insert(key, value);
                            }

                            // Use download coordination for unsigned range requests
                            Self::forward_range_with_coordination(
                                method,
                                uri,
                                host,
                                merged_headers,
                                cache_key,
                                range_spec,
                                overlap,
                                cache_manager,
                                range_handler,
                                s3_client,
                                config.clone(),
                                false, // not signed
                                preloaded_metadata.as_ref(),
                                inflight_tracker,
                                metrics_manager,
                                proxy_referer,
                            )
                            .await
                        }
                    }
                    Err(e) => {
                        error!("Error finding cached ranges: {}", e);
                        Ok(Self::build_error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "InternalError",
                            "Error processing range request.",
                            None,
                        ))
                    }
                }
            }
            RangeParseResult::MultipleRanges(_) => {
                warn!("Multiple ranges not supported yet");
                Ok(Self::build_error_response(
                    StatusCode::NOT_IMPLEMENTED,
                    "NotImplemented",
                    "Multiple ranges not supported.",
                    None,
                ))
            }
            RangeParseResult::Invalid(error) => {
                debug!(
                    "Invalid range for cache_key={}, forwarding to S3: {}",
                    cache_key, error
                );
                // Forward to S3 to handle invalid range (requirement 3.5)
                Self::forward_get_head_to_s3_and_cache(
                    method,
                    uri,
                    host,
                    client_headers,
                    cache_key,
                    cache_manager,
                    s3_client,
                    proxy_referer,
                )
                .await
            }
            RangeParseResult::None => {
                // This shouldn't happen since we checked for range header
                warn!("No range found in range header");
                Ok(Self::build_error_response(
                    StatusCode::BAD_REQUEST,
                    "InvalidRange",
                    "No range specification found.",
                    None,
                ))
            }
        }
    }

    /// Serve full object from cache (no Range header in original request)
    async fn serve_full_object_from_cache(
        method: Method,
        range_spec: &RangeSpec,
        overlap: &crate::range_handler::RangeOverlap,
        cache_key: &str,
        cache_manager: Arc<CacheManager>,
        range_handler: Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        host: &str,
        uri: &str,
        headers: &HeaderMap,
        config: Arc<Config>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        // Get the cached data using the same logic as range requests
        let (range_data, _merge_metrics, is_ram_hit) = match Self::get_cached_range_data(
            range_spec,
            overlap,
            cache_key,
            &cache_manager,
            &range_handler,
            s3_client,
            host,
            uri,
            headers,
            &config,
        )
        .await
        {
            Ok(result) => result,
            Err(error_response) => return Ok(error_response),
        };

        // Refresh write cache TTL if this is a write-cached object (Requirements 1.4, 5.2)
        // This is done asynchronously to not block the response
        let cache_manager_clone = cache_manager.clone();
        let cache_key_clone = cache_key.to_string();
        tokio::spawn(async move {
            if let Err(e) = cache_manager_clone
                .refresh_write_cache_ttl(&cache_key_clone)
                .await
            {
                debug!(
                    "Failed to refresh write cache TTL for {}: {}",
                    cache_key_clone, e
                );
            }
        });

        // Get cached metadata to restore original S3 headers
        let cached_metadata = match cache_manager.get_metadata_from_disk(cache_key).await {
            Ok(Some(metadata)) => metadata.object_metadata,
            _ => {
                warn!("Could not retrieve cached metadata for full object response, using minimal headers");
                crate::cache_types::ObjectMetadata::default()
            }
        };

        // Build 200 OK response (not 206 Partial Content)
        let mut response_builder = Response::builder()
            .status(StatusCode::OK)
            .header("content-length", range_data.len().to_string())
            .header("accept-ranges", "bytes");

        // Restore all original S3 headers from cache
        for (key, value) in &cached_metadata.response_headers {
            // Skip headers that should not be included or are already set
            // For full object responses, include checksum headers since they apply to the complete object
            let key_lower = key.to_lowercase();
            if !matches!(
                key_lower.as_str(),
                "content-length"
                    | "content-range"
                    | "accept-ranges"
                    | "connection"
                    | "transfer-encoding"
                    | "date"
                    | "server"
            ) {
                response_builder = response_builder.header(key, value);
            }
        }

        // Add etag from metadata if not present in response_headers
        if !cached_metadata.response_headers.contains_key("etag")
            && !cached_metadata.response_headers.contains_key("ETag")
            && !cached_metadata.etag.is_empty()
        {
            response_builder = response_builder.header("etag", &cached_metadata.etag);
        }

        if !cached_metadata
            .response_headers
            .contains_key("last-modified")
            && !cached_metadata
                .response_headers
                .contains_key("Last-Modified")
            && !cached_metadata.last_modified.is_empty()
        {
            response_builder =
                response_builder.header("last-modified", &cached_metadata.last_modified);
        }

        // For HEAD requests, don't include body
        let body = if method == Method::HEAD {
            Full::new(Bytes::new())
                .map_err(|never| match never {})
                .boxed()
        } else {
            Full::new(Bytes::from(range_data))
                .map_err(|never| match never {})
                .boxed()
        };

        let response = response_builder.body(body).unwrap();

        // Log cache hit at INFO level for observability
        let cache_tier = if is_ram_hit { "RAM" } else { "Disk" };
        info!("GET {} cache HIT for {}", cache_tier, uri);

        Ok(response)
    }

    /// Serve cached part response with 206 Partial Content status
    /// Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 10.1-10.13
    async fn serve_cached_part_response(
        cached_part: crate::cache::CachedPartResponse,
        method: Method,
        uri: &str,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        // Build 206 Partial Content response - Requirement 4.1
        let mut response_builder = Response::builder().status(StatusCode::PARTIAL_CONTENT);

        // Add all headers from cached part response - Requirements 4.2, 4.3, 4.4, 10.1-10.13
        for (key, value) in &cached_part.headers {
            response_builder = response_builder.header(key, value);
        }

        // Calculate size before moving data (unused but kept for potential future use)
        let _size_mib = cached_part.data.len() as f64 / 1_048_576.0;

        // For HEAD requests, don't include body - Requirement 4.6
        let body = if method == Method::HEAD {
            Full::new(Bytes::new())
                .map_err(|never| match never {})
                .boxed()
        } else {
            // Stream response body to client - Requirement 4.5, 4.6
            Full::new(Bytes::from(cached_part.data))
                .map_err(|never| match never {})
                .boxed()
        };

        let response = response_builder.body(body).unwrap();

        // Log cache hit at INFO level for observability
        info!(
            "GET part cache HIT for {} range {}-{}",
            uri, cached_part.start, cached_part.end
        );

        Ok(response)
    }

    /// Serve range request entirely from cache (Range header was present in original request)
    async fn serve_range_from_cache(
        method: Method,
        range_spec: &RangeSpec,
        overlap: &crate::range_handler::RangeOverlap,
        cache_key: &str,
        cache_manager: Arc<CacheManager>,
        range_handler: Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        host: &str,
        uri: &str,
        headers: &HeaderMap,
        config: Arc<Config>,
        preloaded_metadata: Option<&crate::cache_types::NewCacheMetadata>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        debug!(
            "SERVING RANGE FROM CACHE: range={}-{}, cache_key={}",
            range_spec.start, range_spec.end, cache_key
        );

        // Determine range size for streaming threshold check (Requirement 5.5)
        let range_size = range_spec.end - range_spec.start + 1;
        let streaming_threshold = config.cache.disk_streaming_threshold;

        // Check RAM cache before the streaming/buffered decision (Requirements 3.1, 3.2, 3.3, 4.1, 4.2)
        if let Some(ram_data) = cache_manager.get_range_from_ram_cache(
            cache_key,
            range_spec.start,
            range_spec.end,
        ) {
            debug!(
                "RAM cache hit for range {}-{}, serving buffered response",
                range_spec.start, range_spec.end
            );

            // Refresh write cache TTL asynchronously
            let cache_manager_clone = cache_manager.clone();
            let cache_key_clone = cache_key.to_string();
            tokio::spawn(async move {
                if let Err(e) = cache_manager_clone
                    .refresh_write_cache_ttl(&cache_key_clone)
                    .await
                {
                    debug!(
                        "Failed to refresh write cache TTL for {}: {}",
                        cache_key_clone, e
                    );
                }
            });

            // Resolve metadata for response headers
            let cached_metadata = Self::resolve_cached_metadata(
                preloaded_metadata,
                &cache_manager,
                cache_key,
            )
            .await;

            let total_object_size = cached_metadata.content_length;

            // Build Content-Range header value
            let content_range_value =
                range_handler.build_content_range_header(range_spec, total_object_size);

            // Build 206 Partial Content response
            let mut response_builder = Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header("content-length", ram_data.len().to_string())
                .header("content-range", &content_range_value)
                .header("accept-ranges", "bytes");

            // Add S3 headers from cached metadata
            response_builder = Self::add_cached_s3_headers(
                response_builder,
                &cached_metadata,
                range_spec,
                total_object_size,
            );

            // For HEAD requests, don't include body
            let body = if method == Method::HEAD {
                Full::new(Bytes::new())
                    .map_err(|never| match never {})
                    .boxed()
            } else {
                Full::new(Bytes::from(ram_data))
                    .map_err(|never| match never {})
                    .boxed()
            };

            let response = response_builder.body(body).unwrap();

            info!(
                "GET RAM range cache HIT for {} range {}-{}",
                uri, range_spec.start, range_spec.end
            );

            // Record cache hit statistics
            cache_manager.update_statistics(true, range_size, method == Method::HEAD);
            cache_manager.record_bucket_cache_access(&cache_key, true).await;

            return Ok(response);
        }

        // Check if disk streaming conditions are met (Requirements 5.1, 5.2, 5.3, 5.5):
        // - Single cached range (no merge needed)
        // - Range size >= streaming threshold
        // - Not a HEAD request (no body needed)
        let use_streaming = method != Method::HEAD
            && overlap.cached_ranges.len() == 1
            && range_size >= streaming_threshold;

        if use_streaming {
            debug!(
                "Using disk streaming for range {}-{} ({} bytes >= {} threshold), cache_key={}",
                range_spec.start, range_spec.end, range_size, streaming_threshold, cache_key
            );

            // Refresh write cache TTL asynchronously
            let cache_manager_clone = cache_manager.clone();
            let cache_key_clone = cache_key.to_string();
            tokio::spawn(async move {
                if let Err(e) = cache_manager_clone
                    .refresh_write_cache_ttl(&cache_key_clone)
                    .await
                {
                    debug!(
                        "Failed to refresh write cache TTL for {}: {}",
                        cache_key_clone, e
                    );
                }
            });

            // Resolve metadata for headers (Requirement 5.6)
            let cached_metadata = Self::resolve_cached_metadata(
                preloaded_metadata,
                &cache_manager,
                cache_key,
            )
            .await;

            let total_object_size = cached_metadata.content_length;

            // Build Content-Range and Content-Length from metadata before streaming (Requirement 5.6)
            let content_range_value =
                range_handler.build_content_range_header(range_spec, total_object_size);

            let mut response_builder = Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header("content-length", range_size.to_string())
                .header("content-range", &content_range_value)
                .header("accept-ranges", "bytes");

            // Add S3 headers from cached metadata
            response_builder = Self::add_cached_s3_headers(
                response_builder,
                &cached_metadata,
                range_spec,
                total_object_size,
            );

            // Resolve the cache_types::RangeSpec for the cached range
            let cached_range = &overlap.cached_ranges[0];
            let disk_cache = range_handler.get_disk_cache_manager().read().await;

            // Get metadata to find the range spec with file path info
            let disk_range_spec = {
                let metadata = if let Some(preloaded) = preloaded_metadata {
                    Some(preloaded.clone())
                } else {
                    disk_cache.get_metadata(cache_key).await.ok().flatten()
                };

                metadata.and_then(|meta| {
                    meta.ranges
                        .iter()
                        .find(|r| r.start == cached_range.start && r.end == cached_range.end)
                        .cloned()
                })
            };

            // Also check journals for pending ranges (shared storage mode)
            let disk_range_spec = match disk_range_spec {
                Some(spec) => spec,
                None => {
                    match disk_cache
                        .find_pending_journal_ranges(
                            cache_key,
                            cached_range.start,
                            cached_range.end,
                        )
                        .await
                    {
                        Ok(journal_ranges) => {
                            match journal_ranges
                                .into_iter()
                                .find(|r| r.start == cached_range.start && r.end == cached_range.end)
                            {
                                Some(spec) => spec,
                                None => {
                                    debug!(
                                        "Range spec not found for streaming {}-{}, falling back to buffered path",
                                        cached_range.start, cached_range.end
                                    );
                                    drop(disk_cache);
                                    return Self::serve_range_from_cache_buffered(
                                        method,
                                        range_spec,
                                        overlap,
                                        cache_key,
                                        cache_manager,
                                        range_handler,
                                        s3_client,
                                        host,
                                        uri,
                                        headers,
                                        config,
                                        preloaded_metadata,
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(_) => {
                            warn!(
                                "Failed to check journals for streaming {}-{}, falling back to buffered path",
                                cached_range.start, cached_range.end
                            );
                            drop(disk_cache);
                            return Self::serve_range_from_cache_buffered(
                                method,
                                range_spec,
                                overlap,
                                cache_key,
                                cache_manager,
                                range_handler,
                                s3_client,
                                host,
                                uri,
                                headers,
                                config,
                                preloaded_metadata,
                            )
                            .await;
                        }
                    }
                }
            };

            // Stream range data from disk in 512 KiB chunks (Requirement 5.1)
            const DEFAULT_CHUNK_SIZE: usize = 524_288; // 512 KiB
            match disk_cache
                .stream_range_data(&disk_range_spec, DEFAULT_CHUNK_SIZE)
                .await
            {
                Ok(data_stream) => {
                    drop(disk_cache);

                    // Record range access asynchronously
                    let disk_cache_manager = range_handler.get_disk_cache_manager().clone();
                    let cache_key_owned = cache_key.to_string();
                    let range_start = cached_range.start;
                    let range_end = cached_range.end;
                    tokio::spawn(async move {
                        let dc = disk_cache_manager.read().await;
                        if let Err(e) = dc
                            .record_range_access(&cache_key_owned, range_start, range_end)
                            .await
                        {
                            debug!(
                                "Failed to record range access: key={}, range={}-{}, error={}",
                                cache_key_owned, range_start, range_end, e
                            );
                        }
                    });

                    // If the requested range is a sub-range of the cached range, we need to
                    // handle slicing. For streaming, only support exact match or full cached range.
                    // If slicing is needed, the stream handles the full cached range data.
                    // For sub-range requests, fall back to buffered path.
                    if range_spec.start != cached_range.start
                        || range_spec.end != cached_range.end
                    {
                        debug!(
                            "Requested range {}-{} differs from cached range {}-{}, falling back to buffered for slicing",
                            range_spec.start, range_spec.end, cached_range.start, cached_range.end
                        );
                        return Self::serve_range_from_cache_buffered(
                            method,
                            range_spec,
                            overlap,
                            cache_key,
                            cache_manager,
                            range_handler,
                            s3_client,
                            host,
                            uri,
                            headers,
                            config,
                            preloaded_metadata,
                        )
                        .await;
                    }

                    // Bridge the disk stream to a channel-based stream that is Send + Sync.
                    // Spawn a task to read chunks and send Frame<Bytes> through the channel.
                    // Mid-stream errors cause the sender to drop, terminating the connection (Requirement 5.7).
                    let (frame_tx, frame_rx) = mpsc::channel::<std::result::Result<hyper::body::Frame<Bytes>, hyper::Error>>(4);

                    // Prepare RAM cache promotion context (Requirements 2.2, 5.1, 5.2)
                    let max_ram_cache_size = config.cache.max_ram_cache_size;
                    let promotion_cache_manager = cache_manager.clone();
                    let promotion_cache_key = cache_key.to_string();
                    let promotion_start = range_spec.start;
                    let promotion_end = range_spec.end;
                    let promotion_etag = cached_metadata.etag.clone();

                    tokio::spawn(async move {
                        use futures::StreamExt;
                        let mut stream = std::pin::pin!(data_stream);

                        // Collect chunks for RAM cache promotion if range fits
                        let mut promotion_buffer: Option<Vec<u8>> = if range_size <= max_ram_cache_size {
                            Some(Vec::with_capacity(range_size as usize))
                        } else {
                            None
                        };
                        let mut stream_completed = true;

                        while let Some(result) = stream.next().await {
                            match result {
                                Ok(bytes) => {
                                    // Collect chunk data for RAM cache promotion
                                    if let Some(ref mut buf) = promotion_buffer {
                                        buf.extend_from_slice(&bytes);
                                    }

                                    if frame_tx
                                        .send(Ok(hyper::body::Frame::data(bytes)))
                                        .await
                                        .is_err()
                                    {
                                        debug!("Stream receiver dropped, stopping disk read");
                                        stream_completed = false;
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("Mid-stream disk read error, terminating connection: {}", e);
                                    stream_completed = false;
                                    break; // Drop sender, which ends the stream (Requirement 5.7)
                                }
                            }
                        }

                        // Promote to RAM cache after successful streaming (Requirements 2.2, 5.1, 5.2, 6.1, 6.5)
                        if stream_completed {
                            if let Some(buffer) = promotion_buffer {
                                // Check bucket-level RAM cache eligibility before promoting
                                let resolved = promotion_cache_manager.resolve_settings(&promotion_cache_key).await;
                                if !resolved.ram_cache_eligible {
                                    debug!(
                                        "Skipping RAM cache promotion for {}: ram_cache_eligible=false (source={:?})",
                                        promotion_cache_key, resolved.source
                                    );
                                } else {
                                    promotion_cache_manager.promote_range_to_ram_cache(
                                        &promotion_cache_key,
                                        promotion_start,
                                        promotion_end,
                                        &buffer,
                                        promotion_etag,
                                    );
                                }
                            }
                        }
                    });

                    // Create a stream from the channel receiver
                    let frame_stream = futures::stream::unfold(frame_rx, |mut rx| async move {
                        rx.recv().await.map(|item| (item, rx))
                    });

                    let body = BoxBody::new(StreamBody::new(frame_stream));
                    let response = response_builder.body(body).unwrap();

                    info!(
                        "GET Disk (streaming) range cache HIT for {} range {}-{}",
                        uri, range_spec.start, range_spec.end
                    );

                    // Record cache hit statistics for streaming path
                    cache_manager.update_statistics(true, range_size, false);
                    cache_manager.record_bucket_cache_access(&cache_key, true).await;

                    return Ok(response);
                }
                Err(e) => {
                    debug!(
                        "Failed to create stream for range {}-{}: {}, falling back to buffered path",
                        range_spec.start, range_spec.end, e
                    );
                    drop(disk_cache);
                    return Self::serve_range_from_cache_buffered(
                        method,
                        range_spec,
                        overlap,
                        cache_key,
                        cache_manager,
                        range_handler,
                        s3_client,
                        host,
                        uri,
                        headers,
                        config,
                        preloaded_metadata,
                    )
                    .await;
                }
            }
        }

        // Non-streaming (buffered) path for RAM hits, small ranges, or multi-range merges
        Self::serve_range_from_cache_buffered(
            method,
            range_spec,
            overlap,
            cache_key,
            cache_manager,
            range_handler,
            s3_client,
            host,
            uri,
            headers,
            config,
            preloaded_metadata,
        )
        .await
    }

    /// Resolve cached object metadata for response headers.
    /// Uses preloaded metadata if available, otherwise retries from disk.
    async fn resolve_cached_metadata(
        preloaded_metadata: Option<&crate::cache_types::NewCacheMetadata>,
        cache_manager: &Arc<CacheManager>,
        cache_key: &str,
    ) -> crate::cache_types::ObjectMetadata {
        if let Some(preloaded) = preloaded_metadata {
            debug!(
                "Using preloaded metadata for range response headers: cache_key={}",
                cache_key
            );
            return preloaded.object_metadata.clone();
        }

        // Retry with delay if metadata not found - it may still be in flight during concurrent writes
        for attempt in 0..5u64 {
            match cache_manager.get_metadata_from_disk(cache_key).await {
                Ok(Some(metadata)) => {
                    return metadata.object_metadata;
                }
                _ => {
                    if attempt < 4 {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            20 * (attempt + 1),
                        ))
                        .await;
                    }
                }
            }
        }

        warn!(
            "Could not retrieve cached metadata for range response after retries, using minimal headers"
        );
        crate::cache_types::ObjectMetadata::default()
    }

    /// Add cached S3 headers to a response builder.
    /// Shared between streaming and buffered response paths.
    fn add_cached_s3_headers(
        mut response_builder: hyper::http::response::Builder,
        cached_metadata: &crate::cache_types::ObjectMetadata,
        range_spec: &RangeSpec,
        total_object_size: u64,
    ) -> hyper::http::response::Builder {
        // Check if this is a full object range (0 to total_size-1)
        let is_full_object_range = range_spec.start == 0 && range_spec.end == total_object_size - 1;

        for (key, value) in &cached_metadata.response_headers {
            let key_lower = key.to_lowercase();

            let should_skip_checksums = !is_full_object_range
                && matches!(
                    key_lower.as_str(),
                    "x-amz-checksum-crc32"
                        | "x-amz-checksum-crc32c"
                        | "x-amz-checksum-sha1"
                        | "x-amz-checksum-sha256"
                        | "x-amz-checksum-crc64nvme"
                        | "x-amz-checksum-type"
                        | "content-md5"
                        | "checksumcrc32"
                        | "checksumcrc32c"
                        | "checksumsha1"
                        | "checksumsha256"
                        | "checksumcrc64nvme"
                        | "checksumtype"
                );

            if !should_skip_checksums
                && !matches!(
                    key_lower.as_str(),
                    "content-length"
                        | "content-range"
                        | "accept-ranges"
                        | "connection"
                        | "transfer-encoding"
                        | "date"
                        | "server"
                )
            {
                let should_skip_normalized = match key.as_str() {
                    "ServerSideEncryption" => cached_metadata
                        .response_headers
                        .contains_key("x-amz-server-side-encryption"),
                    "VersionId" => cached_metadata
                        .response_headers
                        .contains_key("x-amz-version-id"),
                    "ChecksumType" => cached_metadata
                        .response_headers
                        .contains_key("x-amz-checksum-type"),
                    "ChecksumCRC64NVME" => cached_metadata
                        .response_headers
                        .contains_key("x-amz-checksum-crc64nvme"),
                    "ChecksumCRC32C" => cached_metadata
                        .response_headers
                        .contains_key("x-amz-checksum-crc32c"),
                    "ChecksumCRC32" => cached_metadata
                        .response_headers
                        .contains_key("x-amz-checksum-crc32"),
                    "ChecksumSHA1" => cached_metadata
                        .response_headers
                        .contains_key("x-amz-checksum-sha1"),
                    "ChecksumSHA256" => cached_metadata
                        .response_headers
                        .contains_key("x-amz-checksum-sha256"),
                    _ => false,
                };

                if !should_skip_normalized {
                    response_builder = response_builder.header(key, value);
                }
            }
        }

        // Add basic headers if not present in cached headers
        if !cached_metadata.response_headers.contains_key("etag")
            && !cached_metadata.response_headers.contains_key("ETag")
            && !cached_metadata.etag.is_empty()
        {
            response_builder = response_builder.header("etag", &cached_metadata.etag);
        }

        if !cached_metadata
            .response_headers
            .contains_key("last-modified")
            && !cached_metadata
                .response_headers
                .contains_key("Last-Modified")
            && !cached_metadata.last_modified.is_empty()
        {
            response_builder =
                response_builder.header("last-modified", &cached_metadata.last_modified);
        }

        response_builder
    }

    /// Serve range from cache using buffered (non-streaming) path.
    /// Used for RAM cache hits, small ranges, multi-range merges, or as fallback.
    async fn serve_range_from_cache_buffered(
        method: Method,
        range_spec: &RangeSpec,
        overlap: &crate::range_handler::RangeOverlap,
        cache_key: &str,
        cache_manager: Arc<CacheManager>,
        range_handler: Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        host: &str,
        uri: &str,
        headers: &HeaderMap,
        config: Arc<Config>,
        preloaded_metadata: Option<&crate::cache_types::NewCacheMetadata>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        // Get the cached data using the common logic
        let (range_data, _merge_metrics, is_ram_hit) = match Self::get_cached_range_data(
            range_spec,
            overlap,
            cache_key,
            &cache_manager,
            &range_handler,
            s3_client,
            host,
            uri,
            headers,
            &config,
        )
        .await
        {
            Ok(result) => result,
            Err(error_response) => return Ok(error_response),
        };

        // Refresh write cache TTL if this is a write-cached object (Requirements 1.4, 5.2)
        // This is done asynchronously to not block the response
        let cache_manager_clone = cache_manager.clone();
        let cache_key_clone = cache_key.to_string();
        tokio::spawn(async move {
            if let Err(e) = cache_manager_clone
                .refresh_write_cache_ttl(&cache_key_clone)
                .await
            {
                debug!(
                    "Failed to refresh write cache TTL for {}: {}",
                    cache_key_clone, e
                );
            }
        });

        // Resolve metadata for response headers
        let cached_metadata = Self::resolve_cached_metadata(
            preloaded_metadata,
            &cache_manager,
            cache_key,
        )
        .await;

        // Get total object size from cached metadata for correct Content-Range header
        let total_object_size = cached_metadata.content_length;

        // Build Content-Range header value with correct total object size
        let content_range_value =
            range_handler.build_content_range_header(range_spec, total_object_size);

        // Build 206 Partial Content response
        let mut response_builder = Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header("content-length", range_data.len().to_string())
            .header("content-range", &content_range_value)
            .header("accept-ranges", "bytes");

        // Add S3 headers from cached metadata
        response_builder = Self::add_cached_s3_headers(
            response_builder,
            &cached_metadata,
            range_spec,
            total_object_size,
        );

        // Calculate size before moving range_data
        let _size_mib = range_data.len() as f64 / 1_048_576.0;

        // For HEAD requests, don't include body
        let body = if method == Method::HEAD {
            Full::new(Bytes::new())
                .map_err(|never| match never {})
                .boxed()
        } else {
            Full::new(Bytes::from(range_data))
                .map_err(|never| match never {})
                .boxed()
        };

        let response = response_builder.body(body).unwrap();

        // Log cache hit at INFO level for observability
        let cache_tier = if is_ram_hit { "RAM" } else { "Disk" };
        info!(
            "GET {} range cache HIT for {} range {}-{}",
            cache_tier, uri, range_spec.start, range_spec.end
        );

        Ok(response)
    }

    /// Common logic to get cached range data
    async fn get_cached_range_data(
        range_spec: &RangeSpec,
        overlap: &crate::range_handler::RangeOverlap,
        cache_key: &str,
        cache_manager: &Arc<CacheManager>,
        range_handler: &Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        host: &str,
        uri: &str,
        headers: &HeaderMap,
        config: &Arc<Config>,
    ) -> std::result::Result<
        (Vec<u8>, Option<(u64, u64, usize, f64)>, bool),
        Response<BoxBody<Bytes, hyper::Error>>,
    > {
        // Track whether data came from RAM cache
        let mut is_ram_hit = false;

        // Get the cached data
        let (range_data, merge_metrics) = if overlap.cached_ranges.len() == 1 {
            // Single cached range
            let cached_range = &overlap.cached_ranges[0];

            let data = {
                // Load data from new storage architecture with RAM cache support
                match cache_manager
                    .load_range_data_with_cache(cache_key, cached_range, range_handler)
                    .await
                {
                    Ok((data, ram_hit)) => {
                        is_ram_hit = ram_hit;
                        debug!(
                            "Loaded range data from {}: {} bytes",
                            if ram_hit { "RAM" } else { "disk" },
                            data.len()
                        );
                        data
                    }
                    Err(e) => {
                        debug!("Range file missing ({}), fetching from S3", e);

                        // Fetch the missing range from S3
                        let range_header =
                            format!("bytes={}-{}", cached_range.start, cached_range.end);
                        let mut s3_headers = headers.clone();
                        // Remove any existing Range header to avoid duplicates
                        s3_headers.remove("range");
                        s3_headers.remove("Range");
                        s3_headers.insert("Range", range_header.parse().unwrap());

                        let s3_headers_map: HashMap<String, String> = s3_headers
                            .iter()
                            .map(|(k, v)| {
                                (k.as_str().to_string(), v.to_str().unwrap_or("").to_string())
                            })
                            .collect();

                        // Build absolute URI for S3 request
                        let absolute_uri = match format!("https://{}{}", host, uri).parse() {
                            Ok(uri) => uri,
                            Err(e) => {
                                error!("Failed to parse URI: {}", e);
                                return Err(Self::build_error_response(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "InternalError",
                                    "Failed to build request URI",
                                    None,
                                ));
                            }
                        };

                        let context = S3RequestContext {
                            method: Method::GET,
                            uri: absolute_uri,
                            headers: s3_headers_map,
                            body: None,
                            host: host.to_string(),
                            request_size: None,
                            conditional_headers: None,
                            operation_type: None,
                            allow_streaming: true, // Enable streaming for range fetches
                        };

                        let s3_response = match s3_client.forward_request(context).await {
                            Ok(resp) => resp,
                            Err(e) => {
                                error!("Failed to fetch missing range from S3: {}", e);
                                return Err(Self::build_error_response(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "InternalError",
                                    "Failed to recover missing cache file.",
                                    None,
                                ));
                            }
                        };

                        let fetched_data = match s3_response.body {
                            Some(body) => match body.into_bytes().await {
                                Ok(bytes) => bytes.to_vec(),
                                Err(e) => {
                                    error!("Failed to collect fetched range body: {}", e);
                                    return Err(Self::build_error_response(
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        "InternalError",
                                        "Failed to collect response body",
                                        None,
                                    ));
                                }
                            },
                            None => Vec::new(),
                        };

                        // Cache the fetched range asynchronously
                        let range_handler_clone = range_handler.clone();
                        let cache_key_clone = cache_key.to_string();
                        let start = cached_range.start;
                        let end = cached_range.end;
                        let ttl = config.cache.get_ttl;
                        let data_clone = fetched_data.clone();
                        let s3_headers_clone = s3_response.headers.clone();
                        let s3_client_clone = s3_client.clone();

                        tokio::spawn(async move {
                            // Use the new method to create ObjectMetadata with all S3 response headers
                            // Note: extract_object_metadata_from_response already extracts total object size
                            // from Content-Range header, so we should NOT override content_length
                            let mut metadata = s3_client_clone
                                .extract_object_metadata_from_response(&s3_headers_clone);
                            metadata.upload_state = crate::cache_types::UploadState::Complete;
                            // cumulative_size tracks how much data we've cached, not total object size
                            metadata.cumulative_size = end - start + 1;

                            if let Err(e) = range_handler_clone
                                .store_range_new_storage(
                                    &cache_key_clone,
                                    start,
                                    end,
                                    &data_clone,
                                    metadata,
                                    ttl,
                                )
                                .await
                            {
                                warn!("Failed to cache recovered range {}-{}: {}", start, end, e);
                            } else {
                                debug!("Cached recovered range {}-{}", start, end);
                            }
                        });

                        fetched_data
                    }
                }
            };

            // Identity optimization: Check if requested range exactly matches cached range (Requirement 3.1)
            // This avoids unnecessary slice calculations when ranges match exactly
            let sliced_data = if range_spec.start == cached_range.start
                && range_spec.end == cached_range.end
            {
                // Exact match - no slicing needed (Requirement 3.1, 2.1)
                debug!(
                    "Identity optimization: requested range exactly matches cached range, no slicing needed, cache_key={}, range={}-{} ({}bytes)",
                    cache_key, range_spec.start, range_spec.end, data.len()
                );
                data
            } else {
                // Ranges don't match exactly - calculate slice parameters
                // Slice the data to match the exact requested range
                // The cached range might be larger than the requested range
                // Requirements 1.2, 1.5, 2.1, 2.2, 2.3
                let slice_start = (range_spec.start - cached_range.start) as usize;
                let slice_end = slice_start + (range_spec.end - range_spec.start + 1) as usize;

                if slice_start != 0 || slice_end != data.len() {
                    // Slicing is needed - log detailed information (Requirement 2.1, 2.2, 2.3)
                    debug!(
                    "Slicing cached range: cache_key={}, cached_range={}-{} ({}bytes), requested_range={}-{} ({}bytes), slice_offset={}, slice_length={}, returning {} bytes",
                    cache_key,
                    cached_range.start, cached_range.end, data.len(),
                    range_spec.start, range_spec.end, range_spec.end - range_spec.start + 1,
                    slice_start, slice_end - slice_start, slice_end - slice_start
                );

                    // Validate slice bounds before extracting
                    if slice_end > data.len() {
                        error!(
                        "Slice bounds error: slice_end={} exceeds data.len()={}, cache_key={}, cached_range={}-{}, requested_range={}-{}",
                        slice_end, data.len(), cache_key, cached_range.start, cached_range.end, range_spec.start, range_spec.end
                    );
                        return Err(Self::build_error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "InternalError",
                            "Failed to slice cached range data.",
                            None,
                        ));
                    }

                    data[slice_start..slice_end].to_vec()
                } else {
                    // This case should not occur since we check for exact match above
                    // But keep it as a safety fallback
                    warn!(
                    "Unexpected: slice calculation resulted in no slicing, but ranges didn't match exactly, cache_key={}, cached_range={}-{}, requested_range={}-{}",
                    cache_key, cached_range.start, cached_range.end, range_spec.start, range_spec.end
                );
                    data
                }
            };

            // Validate sliced data size matches expected size (Requirement 1.1)
            let expected_size = (range_spec.end - range_spec.start + 1) as usize;
            if sliced_data.len() != expected_size {
                error!(
                    "Sliced data size mismatch: expected {} bytes, got {} bytes, cache_key={}, cached_range={}-{}, requested_range={}-{}",
                    expected_size, sliced_data.len(), cache_key, cached_range.start, cached_range.end, range_spec.start, range_spec.end
                );
                return Err(Self::build_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    "Sliced data size validation failed.",
                    None,
                ));
            }

            // Create simple metrics for single range (100% cache efficiency)
            let data_len = sliced_data.len() as u64;
            (sliced_data, Some((data_len, 0u64, 1usize, 100.0f64)))
        } else {
            // Multiple cached ranges need to be merged
            debug!(
                "Merging {} cached ranges for response",
                overlap.cached_ranges.len()
            );

            // Log each cached range being merged (Requirement 2.1, 2.2, 3.5)
            debug!(
                "Multiple range merge details: cache_key={}, requested_range={}-{}, num_cached_ranges={}",
                cache_key, range_spec.start, range_spec.end, overlap.cached_ranges.len()
            );
            for (i, cached_range) in overlap.cached_ranges.iter().enumerate() {
                debug!(
                    "Cached range {} for merge: start={}, end={}, size={} bytes, etag={}",
                    i,
                    cached_range.start,
                    cached_range.end,
                    cached_range.end - cached_range.start + 1,
                    cached_range.etag
                );
            }

            // Call merge_range_segments with no fetched ranges (all data is cached)
            match range_handler
                .merge_range_segments(
                    cache_key,
                    range_spec,
                    &overlap.cached_ranges,
                    &[], // No fetched ranges - all data is from cache
                )
                .await
            {
                Ok(merge_result) => {
                    // Validate merged data size
                    let expected_size = range_spec.end - range_spec.start + 1;
                    if merge_result.data.len() as u64 != expected_size {
                        error!(
                            "Merge validation failed: expected {} bytes, got {} bytes",
                            expected_size,
                            merge_result.data.len()
                        );
                        return Err(Self::build_error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "InternalError",
                            "Range merge validation failed.",
                            None,
                        ));
                    }

                    // Update is_ram_hit based on merge result
                    is_ram_hit = merge_result.ram_hit;

                    (
                        merge_result.data,
                        Some((
                            merge_result.bytes_from_cache,
                            merge_result.bytes_from_s3,
                            merge_result.segments_merged,
                            merge_result.cache_efficiency,
                        )),
                    )
                }
                Err(e) => {
                    error!("Failed to merge cached ranges: {}", e);
                    return Err(Self::build_error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        "Failed to merge cached ranges.",
                        None,
                    ));
                }
            }
        };

        Ok((range_data, merge_metrics, is_ram_hit))
    }

    /// Forward conditional request to S3 with proper cache management based on response
    /// Requirements: 1.2, 1.3, 2.2, 2.3, 3.2, 3.3, 3.4, 4.2, 4.3, 4.4, 6.1, 6.2, 6.3
    async fn forward_conditional_request_to_s3(
        method: Method,
        uri: hyper::Uri,
        host: String,
        mut headers: HashMap<String, String>,
        cache_key: String,
        cache_manager: Arc<CacheManager>,
        s3_client: Arc<S3Client>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        debug!("Forwarding conditional {} request to S3: {}", method, uri);

        // Inject proxy identification Referer header if conditions are met
        let auth_header_owned: Option<String> = headers.get("authorization")
            .or_else(|| headers.get("Authorization")).cloned()
            ;
        maybe_add_referer(&mut headers, proxy_referer, auth_header_owned.as_deref());

        // Build S3 request context
        let context = build_s3_request_context(
            method.clone(),
            uri.clone(),
            headers.clone(),
            None, // GET/HEAD requests have no body
            host.clone(),
        );

        match s3_client.forward_request(context).await {
            Ok(s3_response) => {
                let status = s3_response.status;

                // Requirement 8.2: Log S3 response status and cache actions taken
                info!(
                    "S3 response for conditional request: status={} uri={}",
                    status, uri
                );

                // Handle cache management based on S3 response status
                match status {
                    StatusCode::OK => {
                        // S3 returns 200: Return to client, invalidate old cache, cache new data
                        // Requirements: 1.3, 2.3, 3.2, 4.2, 6.1

                        // Requirement 8.3: Log cache invalidation when S3 returns new data
                        info!(
                            "S3 returned 200 OK for conditional request - invalidating old cache and caching new data: cache_key={} reason=s3_returned_new_data",
                            cache_key
                        );

                        // Invalidate old cached data
                        if let Err(e) = cache_manager.invalidate_cache(&cache_key).await {
                            warn!("Failed to invalidate cache for conditional request: cache_key={}, error={}", cache_key, e);
                        } else {
                            // Requirement 8.3: Log successful cache invalidation
                            info!("Cache invalidated successfully for conditional request: cache_key={} reason=s3_returned_new_data", cache_key);
                        }

                        // Extract metadata from S3 response for caching
                        let metadata =
                            s3_client.extract_metadata_from_response(&s3_response.headers);
                        let response_headers = s3_response.headers.clone();

                        // Cache new data based on method
                        if method == Method::HEAD {
                            // Cache HEAD response using unified method
                            if let Err(e) = cache_manager
                                .store_head_cache_entry_unified(
                                    &cache_key,
                                    response_headers.clone(),
                                    metadata,
                                )
                                .await
                            {
                                warn!("Failed to cache HEAD response for conditional request: cache_key={}, error={}", cache_key, e);
                            } else {
                                info!("Cached new HEAD data for conditional request: cache_key={} action=cache_new_head_data", cache_key);
                            }
                        } else if let Some(body) = &s3_response.body {
                            // Cache GET response body
                            match body {
                                crate::s3_client::S3ResponseBody::Buffered(bytes) => {
                                    if let Err(e) = Self::cache_response_appropriately(
                                        &cache_manager,
                                        &cache_key,
                                        &uri,
                                        bytes,
                                        &response_headers,
                                        &metadata,
                                    )
                                    .await
                                    {
                                        warn!("Failed to cache response for conditional request: cache_key={}, error={}", cache_key, e);
                                    }
                                }
                                crate::s3_client::S3ResponseBody::Streaming(_) => {
                                    // For streaming responses, we'll return the stream and cache in background
                                    // This is handled in the response conversion below
                                    info!("Streaming response for conditional request - will cache in background: cache_key={} action=cache_streaming_data", cache_key);
                                }
                            }
                        }

                        // Return response to client
                        Self::convert_s3_response_to_http(s3_response)
                    }
                    StatusCode::NOT_MODIFIED => {
                        // S3 returns 304: Return to client, refresh TTL
                        // Requirements: 1.2, 2.2, 3.3, 4.3, 6.2

                        // Requirement 8.4: Log TTL refresh when S3 returns 304
                        info!(
                            "S3 returned 304 Not Modified for conditional request - refreshing cache TTL: cache_key={} action=refresh_ttl",
                            cache_key
                        );

                        // Refresh cache TTL
                        if let Err(e) = cache_manager.refresh_cache_ttl(&cache_key).await {
                            warn!("Failed to refresh cache TTL for conditional request: cache_key={}, error={}", cache_key, e);
                        } else {
                            // Requirement 8.4: Log successful TTL refresh
                            info!("Cache TTL refreshed successfully for conditional request: cache_key={} action=ttl_refreshed", cache_key);
                        }

                        // Return 304 response to client
                        Self::convert_s3_response_to_http(s3_response)
                    }
                    StatusCode::PRECONDITION_FAILED => {
                        // S3 returns 412: Return to client, do NOT invalidate cache
                        // Requirements: 3.4, 4.4, 6.3

                        // Requirement 8.2: Log S3 response and cache action (no action in this case)
                        info!(
                            "S3 returned 412 Precondition Failed for conditional request - keeping cache unchanged: cache_key={} action=no_cache_change",
                            cache_key
                        );

                        // Do NOT invalidate cache - this is the key fix
                        // The cache remains valid since the precondition failed

                        // Return 412 response to client
                        Self::convert_s3_response_to_http(s3_response)
                    }
                    _ => {
                        // Other status codes: Return to client without cache modification
                        // Requirement 8.2: Log S3 response and cache action
                        info!(
                            "S3 returned status {} for conditional request - no cache modification: cache_key={} action=no_cache_change",
                            status, cache_key
                        );
                        Self::convert_s3_response_to_http(s3_response)
                    }
                }
            }
            Err(e) => {
                error!("Failed to forward conditional request to S3: {}", e);
                Ok(Self::build_error_response(
                    StatusCode::BAD_GATEWAY,
                    "BadGateway",
                    "Failed to forward request to S3",
                    None,
                ))
            }
        }
    }

    /// Forward GET/HEAD request to S3 without caching (for non-cacheable operations)
    /// Requirements: 1.4, 1.5, 2.3, 2.4, 3.3, 3.4, 4.3, 4.4, 5.3, 5.4, 6.9, 6.10
    async fn forward_get_head_to_s3_without_caching(
        method: Method,
        uri: hyper::Uri,
        host: String,
        mut headers: HashMap<String, String>,
        s3_client: Arc<S3Client>,
        operation_type: Option<&str>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        if let Some(op_type) = operation_type {
            debug!(
                "Forwarding {} ({}) request to S3 without caching: {}",
                method, op_type, uri
            );
        } else {
            debug!(
                "Forwarding {} request to S3 without caching: {}",
                method, uri
            );
        }

        // Inject proxy identification Referer header if conditions are met
        let auth_header_owned: Option<String> = headers.get("authorization")
            .or_else(|| headers.get("Authorization")).cloned()
            ;
        maybe_add_referer(&mut headers, proxy_referer, auth_header_owned.as_deref());

        // Build S3 request context with operation type
        let context = build_s3_request_context_with_operation(
            method.clone(),
            uri.clone(),
            headers.clone(),
            None, // GET/HEAD requests have no body
            host.clone(),
            operation_type.map(|s| s.to_string()),
        );

        match s3_client.forward_request(context).await {
            Ok(s3_response) => {
                debug!(
                    "Successfully received response from S3: {}",
                    s3_response.status
                );

                // Return S3 response directly without caching - Requirements 1.4, 2.3, 3.3, 4.3, 5.3, 6.9
                // Error responses are passed through without modification - Requirements 1.5, 2.4, 3.4, 4.4, 5.4, 6.10
                Self::convert_s3_response_to_http(s3_response)
            }
            Err(e) => {
                error!("Failed to forward request to S3: {}", e);
                Ok(Self::build_error_response(
                    StatusCode::BAD_GATEWAY,
                    "BadGateway",
                    "Failed to forward request to S3",
                    None,
                ))
            }
        }
    }

    /// Helper function to cache response based on whether it's a part-number request or regular GET
    /// For part requests, also completes the active part fetch tracking to allow waiting requests to proceed.
    async fn cache_response_appropriately(
        cache_manager: &Arc<CacheManager>,
        cache_key: &str,
        uri: &hyper::Uri,
        bytes: &[u8],
        response_headers: &HashMap<String, String>,
        metadata: &CacheMetadata,
    ) -> std::result::Result<(), String> {
        // Parse query parameters to check for partNumber
        let query_params: HashMap<String, String> = uri
            .query()
            .unwrap_or("")
            .split('&')
            .filter_map(|pair| {
                let mut parts = pair.split('=');
                match (parts.next(), parts.next()) {
                    (Some(key), Some(value)) => Some((key.to_string(), value.to_string())),
                    _ => None,
                }
            })
            .collect();

        if let Some(part_number_str) = query_params.get("partNumber") {
            if let Ok(part_number) = part_number_str.parse::<u32>() {
                // This is a part-number response - cache it as a part
                let content_range = response_headers
                    .get("content-range")
                    .or_else(|| response_headers.get("Content-Range"))
                    .cloned()
                    .unwrap_or_else(|| {
                        let part_size = bytes.len() as u64;
                        format!("bytes 0-{}/{}", part_size - 1, part_size)
                    });

                cache_manager
                    .store_part_as_range(
                        cache_key,
                        part_number,
                        &content_range,
                        response_headers,
                        bytes,
                    )
                    .await
                    .map_err(|e| e.to_string())?;

                debug!("Cached part response: cache_key={}, part_number={}, size={} action=cache_part_data", cache_key, part_number, bytes.len());
            } else {
                return Err(format!("Invalid part number: {}", part_number_str));
            }
        } else {
            // Regular GET response - cache as full object
            cache_manager
                .store_response_with_headers(
                    cache_key,
                    bytes,
                    response_headers.clone(),
                    metadata.clone(),
                )
                .await
                .map_err(|e| e.to_string())?;

            info!(
                "Cached GET response: cache_key={}, size={} action=cache_get_data",
                cache_key,
                bytes.len()
            );
        }

        Ok(())
    }

    /// Forward GET/HEAD request to S3 with download coordination (InFlightTracker).
    ///
    /// When download coordination is enabled, this method coordinates concurrent requests
    /// for the same uncached resource:
    /// - First request becomes the "fetcher" and performs the S3 fetch
    /// - Subsequent requests become "waiters" and wait for the fetcher to complete
    /// - After completion, waiters serve from cache or fall back to their own S3 fetch
    ///
    /// Requirements: 15.1, 15.2, 15.4, 17.1, 17.2, 17.3, 17.4, 18.4
    async fn forward_get_head_with_coordination(
        method: Method,
        uri: hyper::Uri,
        host: String,
        headers: HashMap<String, String>,
        cache_key: String,
        cache_manager: Arc<CacheManager>,
        s3_client: Arc<S3Client>,
        inflight_tracker: Arc<InFlightTracker>,
        range_handler: Arc<RangeHandler>,
        config: Arc<Config>,
        coordination_enabled: bool,
        wait_timeout: std::time::Duration,
        metrics_manager: Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        // If coordination is disabled, go directly to S3
        // Requirement 18.4
        if !coordination_enabled {
            cache_manager.update_statistics(false, 0, false);
            cache_manager.record_bucket_cache_access(&cache_key, false).await;
            return Self::forward_get_head_to_s3_and_cache(
                method,
                uri,
                host,
                headers,
                cache_key,
                cache_manager,
                s3_client,
                proxy_referer,
            )
            .await;
        }

        // Create flight key for full object GET
        // Requirement 15.4: Full-object GET and range GET are independent flight keys
        let flight_key = InFlightTracker::make_full_key(&cache_key);

        match inflight_tracker.try_register(&flight_key) {
            FetchRole::Fetcher(guard) => {
                // We are the fetcher - perform the S3 fetch
                // Requirement 15.1: First cache-miss registers as fetcher
                debug!(
                    "Download coordination: fetcher for flight_key={}",
                    flight_key
                );

                // Fetcher always goes to S3 — record cache miss
                cache_manager.update_statistics(false, 0, false);
                cache_manager.record_bucket_cache_access(&cache_key, false).await;

                let response = Self::forward_get_head_to_s3_and_cache(
                    method,
                    uri,
                    host,
                    headers,
                    cache_key,
                    cache_manager,
                    s3_client,
                    proxy_referer,
                )
                .await;

                // Notify waiters of completion and record metrics
                // Requirements 16.1, 16.3, 19.1
                match &response {
                    Ok(resp) if resp.status().is_success() => {
                        guard.complete_success();
                        // Record fetcher success metric
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_fetcher_success().await;
                        }
                    }
                    Ok(resp) => {
                        guard.complete_error(format!("S3 returned status {}", resp.status()));
                        // Record fetcher error metric
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_fetcher_error().await;
                        }
                    }
                    Err(_) => {
                        // Infallible error type, but handle for completeness
                        guard.complete_success();
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_fetcher_success().await;
                        }
                    }
                }

                response
            }
            FetchRole::Waiter(mut rx) => {
                // We are a waiter - wait for the fetcher to complete
                // Requirement 15.2: Subsequent cache-miss returns waiter
                debug!(
                    "Download coordination: waiter for flight_key={}",
                    flight_key
                );

                // Record wait start for metrics
                // Requirement 19.1
                if let Some(ref mm) = metrics_manager {
                    mm.read().await.record_coalesce_wait().await;
                }
                let wait_start = std::time::Instant::now();

                // Wait with timeout for the fetcher to complete
                // Requirement 17.3: Waiter timeout falls back to own S3 fetch
                match tokio::time::timeout(wait_timeout, rx.recv()).await {
                    Ok(Ok(Ok(()))) => {
                        // Record wait duration and cache hit metrics
                        // Requirements 19.2, 19.5
                        if let Some(ref mm) = metrics_manager {
                            let mm_guard = mm.read().await;
                            mm_guard.record_coalesce_wait_duration(wait_start.elapsed()).await;
                            mm_guard.record_coalesce_cache_hit().await;
                        }

                        // Fetcher completed successfully - try to serve from cache
                        // Requirement 17.1: Waiter serves from cache after success
                        debug!(
                            "Download coordination: fetcher completed, attempting cache lookup for {}",
                            cache_key
                        );

                        // Try cache first — the fetcher should have cached the data
                        Self::serve_from_cache_or_s3(
                            method,
                            uri,
                            host,
                            headers,
                            cache_key,
                            cache_manager,
                            range_handler,
                            s3_client,
                            config,
                            proxy_referer,
                        )
                        .await
                    }
                    Ok(Ok(Err(error))) => {
                        // Record wait duration (no cache hit since fetcher failed)
                        // Requirement 19.5
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_wait_duration(wait_start.elapsed()).await;
                        }

                        // Fetcher completed with error - fall back to own S3 fetch
                        // Requirement 17.2: Waiter falls back on fetcher error
                        debug!(
                            "Download coordination: fetcher error ({}), falling back to S3 for {}",
                            error, cache_key
                        );
                        Self::forward_get_head_to_s3_and_cache(
                            method,
                            uri,
                            host,
                            headers,
                            cache_key,
                            cache_manager,
                            s3_client,
                            proxy_referer,
                        )
                        .await
                    }
                    Ok(Err(_recv_error)) => {
                        // Record wait duration (no cache hit since channel closed)
                        // Requirement 19.5
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_wait_duration(wait_start.elapsed()).await;
                        }

                        // Channel closed (fetcher dropped without completing) - fall back to S3
                        // Requirement 20.2: Waiters detect channel closure and fall back
                        debug!(
                            "Download coordination: channel closed, falling back to S3 for {}",
                            cache_key
                        );
                        Self::forward_get_head_to_s3_and_cache(
                            method,
                            uri,
                            host,
                            headers,
                            cache_key,
                            cache_manager,
                            s3_client,
                            proxy_referer,
                        )
                        .await
                    }
                    Err(_timeout) => {
                        // Record wait duration and timeout metric
                        // Requirements 19.3, 19.5
                        if let Some(ref mm) = metrics_manager {
                            let mm_guard = mm.read().await;
                            mm_guard.record_coalesce_wait_duration(wait_start.elapsed()).await;
                            mm_guard.record_coalesce_timeout().await;
                        }

                        // Timeout waiting for fetcher - fall back to own S3 fetch
                        // Requirement 17.3: Waiter timeout falls back
                        debug!(
                            "Download coordination: timeout waiting for fetcher, falling back to S3 for {}",
                            cache_key
                        );
                        Self::forward_get_head_to_s3_and_cache(
                            method,
                            uri,
                            host,
                            headers,
                            cache_key,
                            cache_manager,
                            s3_client,
                            proxy_referer,
                        )
                        .await
                    }
                }
            }
        }
    }

    /// Forward part-number GET request with download coordination.
    /// Uses InFlightTracker with part-specific flight keys to coalesce concurrent
    /// requests for the same part of the same object.
    /// Requirements: 12.4, 15.3, 17.1-17.4
    async fn forward_part_with_coordination(
        method: Method,
        uri: hyper::Uri,
        host: String,
        headers: HashMap<String, String>,
        cache_key: String,
        part_number: u32,
        cache_manager: Arc<CacheManager>,
        s3_client: Arc<S3Client>,
        inflight_tracker: Arc<InFlightTracker>,
        coordination_enabled: bool,
        wait_timeout: std::time::Duration,
        metrics_manager: Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        // If coordination is disabled, go directly to S3
        if !coordination_enabled {
            return Self::forward_get_head_to_s3_and_cache(
                method,
                uri,
                host,
                headers,
                cache_key,
                cache_manager,
                s3_client,
                proxy_referer,
            )
            .await;
        }

        // Create flight key for part-number request
        // Requirement 15.3: Part-number requests use part key for independent tracking
        let flight_key = InFlightTracker::make_part_key(&cache_key, part_number);

        match inflight_tracker.try_register(&flight_key) {
            FetchRole::Fetcher(guard) => {
                debug!(
                    "Download coordination: part fetcher for flight_key={}",
                    flight_key
                );

                let response = Self::forward_get_head_to_s3_and_cache(
                    method,
                    uri,
                    host,
                    headers,
                    cache_key,
                    cache_manager,
                    s3_client,
                    proxy_referer,
                )
                .await;

                match &response {
                    Ok(resp) if resp.status().is_success() => {
                        guard.complete_success();
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_fetcher_success().await;
                        }
                    }
                    Ok(resp) => {
                        guard.complete_error(format!("S3 returned status {}", resp.status()));
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_fetcher_error().await;
                        }
                    }
                    Err(_) => {
                        guard.complete_success();
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_fetcher_success().await;
                        }
                    }
                }

                response
            }
            FetchRole::Waiter(mut rx) => {
                debug!(
                    "Download coordination: part waiter for flight_key={}",
                    flight_key
                );

                if let Some(ref mm) = metrics_manager {
                    mm.read().await.record_coalesce_wait().await;
                }
                let wait_start = std::time::Instant::now();

                match tokio::time::timeout(wait_timeout, rx.recv()).await {
                    Ok(Ok(Ok(()))) => {
                        if let Some(ref mm) = metrics_manager {
                            let mm_guard = mm.read().await;
                            mm_guard.record_coalesce_wait_duration(wait_start.elapsed()).await;
                            mm_guard.record_coalesce_cache_hit().await;
                        }

                        debug!(
                            "Download coordination: part fetcher completed, re-entering cache lookup for {}:part{}",
                            cache_key, part_number
                        );

                        // Try cache first — the fetcher should have cached the part
                        match cache_manager.lookup_part(&cache_key, part_number).await {
                            Ok(Some(cached_part)) => {
                                debug!(
                                    "Coalescing waiter: serving part {} from cache for {}",
                                    part_number, cache_key
                                );
                                cache_manager.update_statistics(true, cached_part.data.len() as u64, false);
                                cache_manager.record_bucket_cache_access(&cache_key, true).await;
                                Self::serve_cached_part_response(cached_part, method, uri.path()).await
                            }
                            _ => {
                                // Cache miss — fall back to S3
                                debug!(
                                    "Coalescing waiter: part {} cache miss for {}, falling back to S3",
                                    part_number, cache_key
                                );
                                cache_manager.update_statistics(false, 0, false);
                                cache_manager.record_bucket_cache_access(&cache_key, false).await;
                                Self::forward_get_head_to_s3_and_cache(
                                    method, uri, host, headers, cache_key, cache_manager, s3_client, proxy_referer,
                                )
                                .await
                            }
                        }
                    }
                    Ok(Ok(Err(error))) => {
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_wait_duration(wait_start.elapsed()).await;
                        }
                        debug!(
                            "Download coordination: part fetcher error ({}), falling back for {}:part{}",
                            error, cache_key, part_number
                        );
                        Self::forward_get_head_to_s3_and_cache(
                            method, uri, host, headers, cache_key, cache_manager, s3_client, proxy_referer,
                        )
                        .await
                    }
                    Ok(Err(_recv_error)) => {
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_wait_duration(wait_start.elapsed()).await;
                        }
                        debug!(
                            "Download coordination: part channel closed, falling back for {}:part{}",
                            cache_key, part_number
                        );
                        Self::forward_get_head_to_s3_and_cache(
                            method, uri, host, headers, cache_key, cache_manager, s3_client, proxy_referer,
                        )
                        .await
                    }
                    Err(_timeout) => {
                        if let Some(ref mm) = metrics_manager {
                            let mm_guard = mm.read().await;
                            mm_guard.record_coalesce_wait_duration(wait_start.elapsed()).await;
                            mm_guard.record_coalesce_timeout().await;
                        }
                        debug!(
                            "Download coordination: part waiter timeout, falling back for {}:part{}",
                            cache_key, part_number
                        );
                        Self::forward_get_head_to_s3_and_cache(
                            method, uri, host, headers, cache_key, cache_manager, s3_client, proxy_referer,
                        )
                        .await
                    }
                }
            }
        }
    }

    /// Forward range request with download coordination.
    /// Uses InFlightTracker with range-specific flight keys to coalesce concurrent
    /// requests for the same byte range of the same object.
    /// Requirements: 12.3, 15.3, 17.1-17.4
    #[allow(clippy::too_many_arguments)]
    async fn forward_range_with_coordination(
        method: Method,
        uri: hyper::Uri,
        host: String,
        headers: HashMap<String, String>,
        cache_key: String,
        range_spec: RangeSpec,
        overlap: crate::range_handler::RangeOverlap,
        cache_manager: Arc<CacheManager>,
        range_handler: Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        config: Arc<Config>,
        is_signed: bool,
        preloaded_metadata: Option<&crate::cache_types::NewCacheMetadata>,
        inflight_tracker: Arc<InFlightTracker>,
        metrics_manager: Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        // If coordination is disabled, forward directly
        if !config.cache.download_coordination.enabled {
            if is_signed {
                return Self::forward_signed_range_request(
                    method, uri, host, headers, cache_key, range_spec, overlap,
                    cache_manager, range_handler, s3_client, config, proxy_referer,
                )
                .await;
            } else {
                return Self::forward_range_request_to_s3(
                    method, uri, host, headers, cache_key, range_spec, overlap,
                    cache_manager, range_handler, s3_client, config, preloaded_metadata, proxy_referer,
                )
                .await;
            }
        }

        // Create flight key for range request
        // Requirement 15.3: Use exact byte range for independent tracking
        let flight_key = InFlightTracker::make_range_key(&cache_key, range_spec.start, range_spec.end);
        let wait_timeout = config.cache.download_coordination.wait_timeout();

        match inflight_tracker.try_register(&flight_key) {
            FetchRole::Fetcher(guard) => {
                debug!(
                    "Download coordination: range fetcher for flight_key={}",
                    flight_key
                );

                let response = if is_signed {
                    Self::forward_signed_range_request(
                        method, uri, host, headers, cache_key, range_spec, overlap,
                        cache_manager, range_handler, s3_client, config, proxy_referer,
                    )
                    .await
                } else {
                    Self::forward_range_request_to_s3(
                        method, uri, host, headers, cache_key, range_spec, overlap,
                        cache_manager, range_handler, s3_client, config, preloaded_metadata, proxy_referer,
                    )
                    .await
                };

                match &response {
                    Ok(resp) if resp.status().is_success() || resp.status() == StatusCode::PARTIAL_CONTENT => {
                        guard.complete_success();
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_fetcher_success().await;
                        }
                    }
                    Ok(resp) => {
                        guard.complete_error(format!("S3 returned status {}", resp.status()));
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_fetcher_error().await;
                        }
                    }
                    Err(_) => {
                        guard.complete_success();
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_fetcher_success().await;
                        }
                    }
                }

                response
            }
            FetchRole::Waiter(mut rx) => {
                debug!(
                    "Download coordination: range waiter for flight_key={}",
                    flight_key
                );

                if let Some(ref mm) = metrics_manager {
                    mm.read().await.record_coalesce_wait().await;
                }
                let wait_start = std::time::Instant::now();

                match tokio::time::timeout(wait_timeout, rx.recv()).await {
                    Ok(Ok(Ok(()))) => {
                        if let Some(ref mm) = metrics_manager {
                            let mm_guard = mm.read().await;
                            mm_guard.record_coalesce_wait_duration(wait_start.elapsed()).await;
                            mm_guard.record_coalesce_cache_hit().await;
                        }

                        debug!(
                            "Download coordination: range fetcher completed, re-entering for {}:{}-{}",
                            cache_key, range_spec.start, range_spec.end
                        );

                        // Recompute overlap to reflect data the fetcher just cached.
                        // This prevents the waiter from re-fetching from S3 and double-counting
                        // size when the stale overlap still shows missing_ranges.
                        let overlap_to_use = match range_handler
                            .find_cached_ranges(&cache_key, &range_spec, None, preloaded_metadata)
                            .await
                        {
                            Ok(fresh) => {
                                debug!(
                                    "Download coordination: recomputed overlap for {}:{}-{}, can_serve_from_cache={}, cached={}, missing={}",
                                    cache_key, range_spec.start, range_spec.end,
                                    fresh.can_serve_from_cache, fresh.cached_ranges.len(), fresh.missing_ranges.len()
                                );
                                fresh
                            }
                            Err(e) => {
                                debug!(
                                    "Download coordination: failed to recompute overlap ({}), using original for {}:{}-{}",
                                    e, cache_key, range_spec.start, range_spec.end
                                );
                                overlap.clone()
                            }
                        };

                        // Re-enter the forwarding path with fresh overlap
                        if is_signed {
                            Self::forward_signed_range_request(
                                method, uri, host, headers, cache_key, range_spec, overlap_to_use,
                                cache_manager, range_handler, s3_client, config, proxy_referer,
                            )
                            .await
                        } else {
                            Self::forward_range_request_to_s3(
                                method, uri, host, headers, cache_key, range_spec, overlap_to_use,
                                cache_manager, range_handler, s3_client, config, preloaded_metadata, proxy_referer,
                            )
                            .await
                        }
                    }
                    Ok(Ok(Err(error))) => {
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_wait_duration(wait_start.elapsed()).await;
                        }
                        debug!(
                            "Download coordination: range fetcher error ({}), falling back for {}:{}-{}",
                            error, cache_key, range_spec.start, range_spec.end
                        );
                        if is_signed {
                            Self::forward_signed_range_request(
                                method, uri, host, headers, cache_key, range_spec, overlap,
                                cache_manager, range_handler, s3_client, config, proxy_referer,
                            )
                            .await
                        } else {
                            Self::forward_range_request_to_s3(
                                method, uri, host, headers, cache_key, range_spec, overlap,
                                cache_manager, range_handler, s3_client, config, preloaded_metadata, proxy_referer,
                            )
                            .await
                        }
                    }
                    Ok(Err(_recv_error)) => {
                        if let Some(ref mm) = metrics_manager {
                            mm.read().await.record_coalesce_wait_duration(wait_start.elapsed()).await;
                        }
                        debug!(
                            "Download coordination: range channel closed, falling back for {}:{}-{}",
                            cache_key, range_spec.start, range_spec.end
                        );
                        if is_signed {
                            Self::forward_signed_range_request(
                                method, uri, host, headers, cache_key, range_spec, overlap,
                                cache_manager, range_handler, s3_client, config, proxy_referer,
                            )
                            .await
                        } else {
                            Self::forward_range_request_to_s3(
                                method, uri, host, headers, cache_key, range_spec, overlap,
                                cache_manager, range_handler, s3_client, config, preloaded_metadata, proxy_referer,
                            )
                            .await
                        }
                    }
                    Err(_timeout) => {
                        if let Some(ref mm) = metrics_manager {
                            let mm_guard = mm.read().await;
                            mm_guard.record_coalesce_wait_duration(wait_start.elapsed()).await;
                            mm_guard.record_coalesce_timeout().await;
                        }
                        debug!(
                            "Download coordination: range waiter timeout, falling back for {}:{}-{}",
                            cache_key, range_spec.start, range_spec.end
                        );
                        if is_signed {
                            Self::forward_signed_range_request(
                                method, uri, host, headers, cache_key, range_spec, overlap,
                                cache_manager, range_handler, s3_client, config, proxy_referer,
                            )
                            .await
                        } else {
                            Self::forward_range_request_to_s3(
                                method, uri, host, headers, cache_key, range_spec, overlap,
                                cache_manager, range_handler, s3_client, config, preloaded_metadata, proxy_referer,
                            )
                            .await
                        }
                    }
                }
            }
        }
    }

    /// Try to serve a full-object GET from cache. If cache miss, forward to S3 and cache.
    /// Used by coalescing waiters after the fetcher completes — the data should be cached,
    /// so this avoids a redundant S3 fetch in the common case.
    async fn serve_from_cache_or_s3(
        method: Method,
        uri: hyper::Uri,
        host: String,
        headers: HashMap<String, String>,
        cache_key: String,
        cache_manager: Arc<CacheManager>,
        range_handler: Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        config: Arc<Config>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        // Try cache first — the fetcher should have cached the data
        let preloaded_metadata = match cache_manager.get_metadata_cached(&cache_key).await {
            Ok(m) => m,
            Err(_) => None,
        };

        if let Some(ref metadata) = preloaded_metadata {
            let total_size = metadata.object_metadata.content_length;
            if total_size > 0 {
                let full_range = crate::range_handler::RangeSpec { start: 0, end: total_size - 1 };
                if let Ok(overlap) = range_handler
                    .find_cached_ranges(&cache_key, &full_range, None, preloaded_metadata.as_ref())
                    .await
                {
                    if overlap.can_serve_from_cache {
                        debug!(
                            "Coalescing waiter: serving full object from cache for {}",
                            cache_key
                        );
                        cache_manager.update_statistics(true, total_size, false);
                        cache_manager.record_bucket_cache_access(&cache_key, true).await;

                        let header_map: hyper::header::HeaderMap = headers
                            .iter()
                            .filter_map(|(k, v)| {
                                k.parse::<hyper::header::HeaderName>()
                                    .ok()
                                    .zip(v.parse::<hyper::header::HeaderValue>().ok())
                            })
                            .collect();

                        return Self::serve_full_object_from_cache(
                            method,
                            &full_range,
                            &overlap,
                            &cache_key,
                            cache_manager,
                            range_handler,
                            s3_client,
                            &host,
                            uri.path(),
                            &header_map,
                            config,
                        )
                        .await;
                    }
                }
            }
        }

        // Cache miss — fall back to S3 (fetcher may not have finished caching yet)
        debug!(
            "Coalescing waiter: cache miss for {}, falling back to S3",
            cache_key
        );
        cache_manager.update_statistics(false, 0, false);
        cache_manager.record_bucket_cache_access(&cache_key, false).await;
        Self::forward_get_head_to_s3_and_cache(
            method, uri, host, headers, cache_key, cache_manager, s3_client, proxy_referer,
        )
        .await
    }

    /// Forward GET/HEAD request to S3 and cache the response with streaming support
    /// Requirement 2.8: When object size is NOT known, forward full GET to S3 and cache response
    ///
    /// For streaming responses (> 1MB), uses TeeStream to simultaneously stream to client
    /// and cache in background. For buffered responses (< 1MB), caches synchronously.
    async fn forward_get_head_to_s3_and_cache(
        method: Method,
        uri: hyper::Uri,
        host: String,
        mut headers: HashMap<String, String>,
        cache_key: String,
        cache_manager: Arc<CacheManager>,
        s3_client: Arc<S3Client>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        if method == Method::GET {
            debug!(
                operation = "GET",
                cache_result = "MISS",
                path = uri.path(),
                "Cache operation completed"
            );
        }
        debug!("Forwarding {} request to S3: {}", method, uri);

        // Inject proxy identification Referer header if conditions are met
        let auth_header_owned: Option<String> = headers.get("authorization")
            .or_else(|| headers.get("Authorization")).cloned()
            ;
        maybe_add_referer(&mut headers, proxy_referer, auth_header_owned.as_deref());

        // Build S3 request context
        let context = build_s3_request_context(
            method.clone(),
            uri.clone(),
            headers.clone(),
            None, // GET/HEAD requests have no body
            host.clone(),
        );

        match s3_client.forward_request(context).await {
            Ok(s3_response) => {
                debug!(
                    "Successfully received response from S3: {}",
                    s3_response.status
                );

                // Extract metadata from S3 response
                let metadata = s3_client.extract_metadata_from_response(&s3_response.headers);
                let response_headers = s3_response.headers.clone();
                let status = s3_response.status;

                // HEAD requests: cache headers only, no body
                if method == Method::HEAD {
                    if status.is_success() {
                        if let Err(e) = cache_manager
                            .store_head_cache_entry_unified(
                                &cache_key,
                                response_headers.clone(),
                                metadata.clone(),
                            )
                            .await
                        {
                            warn!("Failed to cache HEAD response for key {}: {}", cache_key, e);
                        } else {
                            debug!(
                                "Successfully cached HEAD response (unified) for key: {}",
                                cache_key
                            );
                        }
                    }

                    let mut response_builder = Response::builder().status(status);
                    for (key, value) in response_headers {
                        response_builder = response_builder.header(&key, &value);
                    }
                    return Ok(response_builder
                        .body(
                            Full::new(Bytes::new())
                                .map_err(|never| match never {})
                                .boxed(),
                        )
                        .unwrap());
                }

                // GET requests: handle streaming vs buffered
                match s3_response.body {
                    Some(S3ResponseBody::Streaming(incoming)) => {
                        // Convert Incoming to a Stream of Frames using BodyExt
                        let frame_stream = futures::stream::unfold(incoming, |mut body| {
                            Box::pin(async move {
                                match body.frame().await {
                                    Some(Ok(frame)) => Some((Ok(frame), body)),
                                    Some(Err(e)) => Some((Err(e), body)),
                                    None => None,
                                }
                            })
                        });

                        if status.is_success() {
                            // Streaming success response - use TeeStream to stream to client while caching in background
                            debug!("Streaming GET response to client while caching in background: cache_key={}", cache_key);

                            // Create channel for cache data
                            let (cache_tx, mut cache_rx) = mpsc::channel::<Bytes>(100);

                            // Spawn background task to collect and cache data
                            let cache_key_clone = cache_key.clone();
                            let cache_manager_clone = Arc::clone(&cache_manager);
                            let headers_for_cache = response_headers.clone();
                            let metadata_for_cache = metadata.clone();
                            let uri_clone = uri.clone();

                            tokio::spawn(async move {
                                let mut accumulated = Vec::new();
                                while let Some(chunk) = cache_rx.recv().await {
                                    accumulated.extend_from_slice(&chunk);
                                }

                                if !accumulated.is_empty() {
                                    let body_size = accumulated.len() as u64;
                                    debug!(
                                        "Caching streamed GET response: cache_key={}, content_length={} bytes",
                                        cache_key_clone, body_size
                                    );

                                    if let Err(e) = Self::cache_response_appropriately(
                                        &cache_manager_clone,
                                        &cache_key_clone,
                                        &uri_clone,
                                        &accumulated,
                                        &headers_for_cache,
                                        &metadata_for_cache,
                                    )
                                    .await
                                    {
                                        warn!(
                                            "Failed to cache streamed response: cache_key={}, error={}",
                                            cache_key_clone, e
                                        );
                                    } else {
                                        debug!(
                                            "Cached streamed response: cache_key={}, size={} bytes",
                                            cache_key_clone, body_size
                                        );
                                    }
                                }
                            });

                            // Wrap with TeeStream to send data to both client and cache
                            let tee_stream = TeeStream::new(frame_stream, cache_tx);

                            // Build streaming response
                            let mut response_builder = Response::builder().status(status);
                            for (key, value) in response_headers {
                                response_builder = response_builder.header(&key, &value);
                            }

                            Ok(response_builder
                                .body(BoxBody::new(StreamBody::new(tee_stream)))
                                .unwrap())
                        } else {
                            // Non-success streaming response (error from S3) — forward without caching
                            debug!(
                                "Forwarding non-success streaming response without caching: cache_key={}, status={}",
                                cache_key, status
                            );

                            let mut response_builder = Response::builder().status(status);
                            for (key, value) in response_headers {
                                response_builder = response_builder.header(&key, &value);
                            }

                            Ok(response_builder
                                .body(BoxBody::new(StreamBody::new(frame_stream)))
                                .unwrap())
                        }
                    }
                    Some(S3ResponseBody::Buffered(bytes)) => {
                        // Buffered response (small) - cache synchronously
                        if status.is_success() {
                            let body_size = bytes.len() as u64;
                            debug!("GET response has body of size: {} bytes", body_size);

                            debug!(
                                "Caching buffered GET response: cache_key={}, content_length={} bytes",
                                cache_key, body_size
                            );

                            if let Err(e) = Self::cache_response_appropriately(
                                &cache_manager,
                                &cache_key,
                                &uri,
                                &bytes[..],
                                &response_headers,
                                &metadata,
                            )
                            .await
                            {
                                warn!(
                                    "Failed to cache buffered response: cache_key={}, error={}",
                                    cache_key, e
                                );
                            } else {
                                debug!(
                                    "Cached buffered response: cache_key={}, size={} bytes",
                                    cache_key, body_size
                                );
                            }
                        }

                        let mut response_builder = Response::builder().status(status);
                        for (key, value) in response_headers {
                            response_builder = response_builder.header(&key, &value);
                        }
                        Ok(response_builder
                            .body(Full::new(bytes).map_err(|never| match never {}).boxed())
                            .unwrap())
                    }
                    None => {
                        // No body
                        warn!(
                            "GET response has no body, cannot cache for key: {}",
                            cache_key
                        );
                        let mut response_builder = Response::builder().status(status);
                        for (key, value) in response_headers {
                            response_builder = response_builder.header(&key, &value);
                        }
                        Ok(response_builder
                            .body(
                                Full::new(Bytes::new())
                                    .map_err(|never| match never {})
                                    .boxed(),
                            )
                            .unwrap())
                    }
                }
            }
            Err(e) => {
                error!("Failed to forward request to S3: {}", e);

                Ok(Self::build_error_response(
                    StatusCode::BAD_GATEWAY,
                    "BadGateway",
                    "Failed to forward request to S3",
                    None,
                ))
            }
        }
    }

    /// Forward a signed range request to S3 with selective caching
    ///
    /// This function handles range requests where the Range header is included
    /// in the AWS SigV4 signature. It forwards the entire original range to S3
    /// (preserving the signature) while selectively caching only the missing
    /// portions during streaming.
    ///
    /// # Requirements
    /// - Requirement 2.1: Forward entire original Range header to S3
    /// - Requirement 2.2: Preserve all request headers exactly as received
    /// - Requirement 2.3: Do not attempt to fetch missing ranges separately
    /// - Requirement 2.4: Stream response to client
    /// - Requirement 3.1, 3.2, 3.3: Selectively cache only missing portions
    #[allow(clippy::too_many_arguments)]
    async fn forward_signed_range_request(
        method: Method,
        uri: hyper::Uri,
        host: String,
        mut client_headers: HashMap<String, String>,
        cache_key: String,
        range_spec: RangeSpec,
        overlap: crate::range_handler::RangeOverlap,
        _cache_manager: Arc<CacheManager>,
        range_handler: Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        config: Arc<Config>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        info!(
            "Forwarding signed range request to S3: range={}-{} missing_ranges={} cache_key={}",
            range_spec.start,
            range_spec.end,
            overlap.missing_ranges.len(),
            cache_key
        );

        // Inject proxy identification Referer header if conditions are met
        let auth_header_owned: Option<String> = client_headers.get("authorization")
            .or_else(|| client_headers.get("Authorization")).cloned();
        maybe_add_referer(&mut client_headers, proxy_referer, auth_header_owned.as_deref());

        // Build S3 request context with original headers (Requirement 2.2)
        // All headers are preserved exactly as received to maintain signature validity
        let context = crate::s3_client::build_s3_request_context(
            method.clone(),
            uri.clone(),
            client_headers.clone(),
            None, // No body for GET requests
            host.clone(),
        );

        // Forward request to S3 with retry on transient failures (Requirement 2.1, 2.3)
        const MAX_RETRIES: u32 = 2;
        let mut last_error = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                // Brief delay before retry (100ms * attempt)
                tokio::time::sleep(tokio::time::Duration::from_millis(100 * attempt as u64)).await;
                debug!(
                    "Retrying S3 request: cache_key={}, attempt={}/{}",
                    cache_key,
                    attempt + 1,
                    MAX_RETRIES + 1
                );

                // Rebuild context for retry (connection may have been reset)
                let retry_context = crate::s3_client::build_s3_request_context(
                    method.clone(),
                    uri.clone(),
                    client_headers.clone(),
                    None,
                    host.clone(),
                );

                match s3_client.forward_request(retry_context).await {
                    Ok(s3_response) => {
                        return Self::handle_signed_range_s3_response(
                            s3_response,
                            cache_key,
                            range_spec,
                            overlap,
                            range_handler,
                            s3_client,
                            config,
                        )
                        .await;
                    }
                    Err(e) => {
                        warn!(
                            "S3 request retry {} failed: cache_key={}, error={}",
                            attempt + 1,
                            cache_key,
                            e
                        );
                        last_error = Some(e);
                    }
                }
            } else {
                // First attempt
                match s3_client.forward_request(context.clone()).await {
                    Ok(s3_response) => {
                        return Self::handle_signed_range_s3_response(
                            s3_response,
                            cache_key,
                            range_spec,
                            overlap,
                            range_handler,
                            s3_client,
                            config,
                        )
                        .await;
                    }
                    Err(e) => {
                        warn!(
                            "S3 request failed (will retry): cache_key={}, error={}",
                            cache_key, e
                        );
                        last_error = Some(e);
                    }
                }
            }
        }

        // All retries exhausted
        error!(
            "Failed to forward signed range request to S3 after {} attempts: cache_key={}, error={:?}",
            MAX_RETRIES + 1, cache_key, last_error
        );
        Ok(Self::build_error_response(
            StatusCode::BAD_GATEWAY,
            "BadGateway",
            "Failed to fetch from S3",
            None,
        ))
    }

    /// Handle S3 response for signed range request (extracted for retry logic)
    #[allow(clippy::too_many_arguments)]
    async fn handle_signed_range_s3_response(
        s3_response: crate::s3_client::S3Response,
        cache_key: String,
        range_spec: RangeSpec,
        overlap: crate::range_handler::RangeOverlap,
        range_handler: Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        config: Arc<Config>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        let status = s3_response.status;

        // Handle successful range response (206 Partial Content)
        if status == StatusCode::PARTIAL_CONTENT || status == StatusCode::OK {
            // Extract ETag for caching
            let etag = s3_response
                .headers
                .get("etag")
                .or_else(|| s3_response.headers.get("ETag"))
                .cloned();

            debug!(
                "S3 returned {} for signed range request: cache_key={}, etag={:?}",
                status, cache_key, etag
            );

            // Handle the response body based on type
            match s3_response.body {
                Some(S3ResponseBody::Streaming(incoming)) => {
                    // Streaming response - cache while streaming to client (Requirement 2.4)
                    // Cache the entire response as a single range

                    if !overlap.missing_ranges.is_empty() && etag.is_some() {
                        // Clone the Arc<Mutex<DiskCacheManager>> for use in spawned task
                        let disk_cache = Arc::clone(range_handler.get_disk_cache_manager());
                        let cache_manager = Arc::clone(range_handler.get_cache_manager());
                        let cache_key_clone = cache_key.clone();
                        let range_spec_clone = range_spec.clone();
                        let _etag_clone = etag.clone().unwrap();
                        let ttl = config.cache.get_ttl;

                        // Extract complete object metadata from S3 response headers to preserve all headers
                        let object_metadata =
                            s3_client.extract_object_metadata_from_response(&s3_response.headers);

                        // Create channel for cache data
                        let (cache_tx, mut cache_rx) = mpsc::channel::<Bytes>(100);

                        // Spawn background task to collect and cache data
                        tokio::spawn(async move {
                            let mut data = Vec::new();
                            while let Some(chunk) = cache_rx.recv().await {
                                data.extend_from_slice(&chunk);
                            }

                            // Write to cache as a range
                            if !data.is_empty() {
                                // Check capacity and evict if needed before caching
                                if let Err(e) =
                                    cache_manager.evict_if_needed(data.len() as u64).await
                                {
                                    warn!("Eviction failed before caching range: {}", e);
                                }

                                // Use the extracted object metadata to preserve all S3 response headers

                                let mut disk_cache_guard = disk_cache.write().await;
                                let resolved = cache_manager.resolve_settings(&cache_key_clone).await;
                                if let Err(e) = disk_cache_guard
                                    .store_range(
                                        &cache_key_clone,
                                        range_spec_clone.start,
                                        range_spec_clone.end,
                                        &data,
                                        object_metadata,
                                        ttl,
                                        resolved.compression_enabled,
                                    )
                                    .await
                                {
                                    warn!(
                                        "Failed to cache signed range response: cache_key={}, error={}",
                                        cache_key_clone, e
                                    );
                                } else {
                                    debug!(
                                        "Cached signed range response: cache_key={}, range={}-{}, size={}",
                                                cache_key_clone, range_spec_clone.start, range_spec_clone.end, data.len()
                                            );
                                }
                            }
                        });

                        // Convert Incoming to a Stream of Frames using BodyExt
                        let frame_stream = futures::stream::unfold(incoming, |mut body| {
                            Box::pin(async move {
                                match body.frame().await {
                                    Some(Ok(frame)) => Some((Ok(frame), body)),
                                    Some(Err(e)) => Some((Err(e), body)),
                                    None => None,
                                }
                            })
                        });

                        // Wrap with TeeStream to send data to both client and cache
                        let tee_stream = TeeStream::new(frame_stream, cache_tx);

                        // Build streaming response
                        let mut response_builder = Response::builder().status(status);
                        for (key, value) in &s3_response.headers {
                            // Skip checksum headers since they apply to the full object, not the range
                            let key_lower = key.to_lowercase();
                            if !matches!(
                                key_lower.as_str(),
                                "x-amz-checksum-crc32"
                                    | "x-amz-checksum-crc32c"
                                    | "x-amz-checksum-sha1"
                                    | "x-amz-checksum-sha256"
                                    | "x-amz-checksum-crc64nvme"
                                    | "x-amz-checksum-type"
                                    | "content-md5"
                            ) {
                                response_builder =
                                    response_builder.header(key.as_str(), value.as_str());
                            }
                        }

                        return Ok(response_builder
                            .body(BoxBody::new(StreamBody::new(tee_stream)))
                            .unwrap());
                    }

                    // No missing ranges or no etag - just stream without caching
                    let frame_stream = futures::stream::unfold(incoming, |mut body| {
                        Box::pin(async move {
                            match body.frame().await {
                                Some(Ok(frame)) => Some((Ok(frame), body)),
                                Some(Err(e)) => Some((Err(e), body)),
                                None => None,
                            }
                        })
                    });

                    let mut response_builder = Response::builder().status(status);
                    for (key, value) in &s3_response.headers {
                        // Skip checksum headers since they apply to the full object, not the range
                        let key_lower = key.to_lowercase();
                        if !matches!(
                            key_lower.as_str(),
                            "x-amz-checksum-crc32"
                                | "x-amz-checksum-crc32c"
                                | "x-amz-checksum-sha1"
                                | "x-amz-checksum-sha256"
                                | "x-amz-checksum-crc64nvme"
                                | "x-amz-checksum-type"
                                | "content-md5"
                        ) {
                            response_builder =
                                response_builder.header(key.as_str(), value.as_str());
                        }
                    }

                    return Ok(response_builder
                        .body(BoxBody::new(StreamBody::new(frame_stream)))
                        .unwrap());
                }
                Some(S3ResponseBody::Buffered(bytes)) => {
                    // Buffered response - cache asynchronously to avoid blocking client
                    if !overlap.missing_ranges.is_empty() && etag.is_some() {
                        let disk_cache = Arc::clone(range_handler.get_disk_cache_manager());
                        let cache_manager = Arc::clone(range_handler.get_cache_manager());
                        let cache_key_clone = cache_key.clone();
                        let range_spec_clone = range_spec.clone();
                        let ttl = config.cache.get_ttl;
                        let bytes_clone = bytes.clone();

                        // Extract complete object metadata from S3 response headers to preserve all headers
                        let object_metadata =
                            s3_client.extract_object_metadata_from_response(&s3_response.headers);

                        // Spawn background task to cache data without blocking client
                        tokio::spawn(async move {
                            // Check capacity and evict if needed before caching
                            if let Err(e) = cache_manager
                                .evict_if_needed(bytes_clone.len() as u64)
                                .await
                            {
                                warn!("Eviction failed before caching range: {}", e);
                            }

                            let mut disk_cache_guard = disk_cache.write().await;
                            let resolved = cache_manager.resolve_settings(&cache_key_clone).await;
                            if let Err(e) = disk_cache_guard
                                .store_range(
                                    &cache_key_clone,
                                    range_spec_clone.start,
                                    range_spec_clone.end,
                                    &bytes_clone,
                                    object_metadata,
                                    ttl,
                                    resolved.compression_enabled,
                                )
                                .await
                            {
                                warn!(
                                    "Failed to cache signed range response: cache_key={}, error={}",
                                    cache_key_clone, e
                                );
                            } else {
                                debug!(
                                            "Cached signed range response: cache_key={}, range={}-{}, size={}",
                                            cache_key_clone, range_spec_clone.start, range_spec_clone.end, bytes_clone.len()
                                        );
                            }
                        });
                    }

                    let mut response_builder = Response::builder().status(status);
                    for (key, value) in &s3_response.headers {
                        // Skip checksum headers since they apply to the full object, not the range
                        let key_lower = key.to_lowercase();
                        if !matches!(
                            key_lower.as_str(),
                            "x-amz-checksum-crc32"
                                | "x-amz-checksum-crc32c"
                                | "x-amz-checksum-sha1"
                                | "x-amz-checksum-sha256"
                                | "x-amz-checksum-crc64nvme"
                                | "x-amz-checksum-type"
                                | "content-md5"
                        ) {
                            response_builder =
                                response_builder.header(key.as_str(), value.as_str());
                        }
                    }

                    return Ok(response_builder
                        .body(Full::new(bytes).map_err(|never| match never {}).boxed())
                        .unwrap());
                }
                None => {
                    // No body in response
                    let mut response_builder = Response::builder().status(status);
                    for (key, value) in &s3_response.headers {
                        // Skip checksum headers since they apply to the full object, not the range
                        let key_lower = key.to_lowercase();
                        if !matches!(
                            key_lower.as_str(),
                            "x-amz-checksum-crc32"
                                | "x-amz-checksum-crc32c"
                                | "x-amz-checksum-sha1"
                                | "x-amz-checksum-sha256"
                                | "x-amz-checksum-crc64nvme"
                                | "x-amz-checksum-type"
                                | "content-md5"
                        ) {
                            response_builder =
                                response_builder.header(key.as_str(), value.as_str());
                        }
                    }
                    return Ok(response_builder
                        .body(
                            Full::new(Bytes::new())
                                .map_err(|never| match never {})
                                .boxed(),
                        )
                        .unwrap());
                }
            }
        }

        // Forward error responses without caching (Requirement 8.4)
        warn!(
            "S3 returned error for signed range request: cache_key={}, status={}",
            cache_key, status
        );

        Self::convert_s3_response_to_http(s3_response)
    }

    /// Forward range request to S3 and merge with cached data
    async fn forward_range_request_to_s3(
        method: Method,
        uri: hyper::Uri,
        host: String,
        mut headers: HashMap<String, String>,
        cache_key: String,
        range_spec: RangeSpec,
        overlap: crate::range_handler::RangeOverlap,
        cache_manager: Arc<CacheManager>,
        range_handler: Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        config: Arc<Config>,
        preloaded_metadata: Option<&crate::cache_types::NewCacheMetadata>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        // Check if all requested bytes are cached (fully cached case) - Requirement 1.3
        if overlap.missing_ranges.is_empty() {
            debug!("All requested bytes are cached, serving entirely from cache without S3 fetch");
            let header_map: HeaderMap = headers
                .iter()
                .filter_map(|(k, v)| {
                    k.parse::<hyper::header::HeaderName>()
                        .ok()
                        .and_then(|name| {
                            v.parse::<hyper::header::HeaderValue>()
                                .ok()
                                .map(|val| (name, val))
                        })
                })
                .collect();
            return Self::serve_range_from_cache(
                method,
                &range_spec,
                &overlap,
                &cache_key,
                cache_manager,
                range_handler,
                s3_client,
                &host,
                &uri.to_string(),
                &header_map,
                config,
                preloaded_metadata,
            )
            .await;
        }

        // Complete cache miss - use streaming to avoid buffering large responses
        // Inject proxy identification Referer header if conditions are met
        let auth_header_owned: Option<String> = headers.get("authorization")
            .or_else(|| headers.get("Authorization")).cloned()
            ;
        maybe_add_referer(&mut headers, proxy_referer, auth_header_owned.as_deref());

        // This prevents AWS SDK throughput failures for large files
        if overlap.cached_ranges.is_empty() {
            debug!(
                operation = "GET",
                cache_result = "MISS",
                cache_type = "range_request",
                range_start = range_spec.start,
                range_end = range_spec.end,
                cache_key = %cache_key,
                "Cache operation completed"
            );

            // Stream directly from S3 while caching in background
            // This ensures bytes flow to client immediately, satisfying throughput requirements
            return Self::stream_range_from_s3_with_caching(
                method,
                uri,
                host,
                headers,
                cache_key,
                range_spec,
                range_handler,
                s3_client,
                config,
            )
            .await;
        }

        // Partially cached case - consolidate missing ranges - Requirement 1.2
        // Note: Partial cache hits still require buffering to merge cached + fetched data
        debug!(
            "Partially cached request: {} cached ranges, {} missing ranges",
            overlap.cached_ranges.len(),
            overlap.missing_ranges.len()
        );

        // Consolidate missing ranges to minimize S3 requests using configured gap threshold
        let gap_threshold = config.cache.range_merge_gap_threshold;
        debug!("Using range merge gap threshold: {} bytes", gap_threshold);
        let consolidated_ranges =
            range_handler.consolidate_missing_ranges(overlap.missing_ranges.clone(), gap_threshold);

        debug!(
            "Cache miss for {} range {}-{}, fetching from S3",
            cache_key, range_spec.start, range_spec.end
        );

        // Fetch only consolidated missing ranges from S3 in parallel - Requirements 1.1, 1.4
        debug!(
            "Fetching {} consolidated missing ranges from S3",
            consolidated_ranges.len()
        );

        match range_handler
            .fetch_missing_ranges(
                &cache_key,
                &consolidated_ranges,
                &s3_client,
                &host,
                &uri,
                &headers,
            )
            .await
        {
            Ok(fetched_ranges) => {
                debug!(
                    "Successfully fetched {} ranges from S3",
                    fetched_ranges.len()
                );

                // Cache fetched ranges for future requests - Requirement 1.4
                for (fetched_spec, fetched_data, response_headers) in &fetched_ranges {
                    // Use the new method to create ObjectMetadata with all S3 response headers
                    // Note: extract_object_metadata_from_response already extracts total object size
                    // from Content-Range header, so we should NOT override content_length
                    let mut object_metadata =
                        s3_client.extract_object_metadata_from_response(response_headers);
                    object_metadata.upload_state = crate::cache_types::UploadState::Complete;
                    // cumulative_size tracks how much data we've cached, not total object size
                    object_metadata.cumulative_size = fetched_spec.end - fetched_spec.start + 1;

                    // Spawn async cache write to avoid blocking response
                    let range_handler_clone = range_handler.clone();
                    let cache_key_clone = cache_key.clone();
                    let start = fetched_spec.start;
                    let end = fetched_spec.end;
                    let data_clone = fetched_data.to_vec();
                    let ttl = config.cache.get_ttl;
                    tokio::spawn(async move {
                        if let Err(e) = range_handler_clone
                            .store_range_new_storage(
                                &cache_key_clone,
                                start,
                                end,
                                &data_clone,
                                object_metadata,
                                ttl,
                            )
                            .await
                        {
                            warn!("Failed to cache fetched range {}-{}: {}", start, end, e);
                        } else {
                            debug!("Cached fetched range {}-{}", start, end);
                        }
                    });
                }

                // Merge cached and fetched ranges with comprehensive error handling - Requirements 1.5, 2.1, 2.5, 8.2
                debug!(
                    "Merging {} cached ranges with {} fetched ranges",
                    overlap.cached_ranges.len(),
                    fetched_ranges.len()
                );

                // Use merge_ranges_with_fallback for robust error handling
                match range_handler
                    .merge_ranges_with_fallback(
                        &cache_key,
                        &range_spec,
                        &overlap.cached_ranges,
                        &fetched_ranges,
                        &s3_client,
                        &host,
                        &uri,
                        &headers,
                    )
                    .await
                {
                    Ok(merge_result) => {
                        // Build 206 Partial Content response
                        let content_length = merge_result.data.len() as u64;

                        // Get total object size from cached metadata for correct Content-Range header
                        let total_object_size = match cache_manager
                            .get_metadata_from_disk(&cache_key)
                            .await
                        {
                            Ok(Some(metadata)) => {
                                debug!("Using total object size from cache metadata in merge: {} bytes", metadata.object_metadata.content_length);
                                metadata.object_metadata.content_length
                            }
                            _ => {
                                // Fallback: use range size (incorrect but prevents errors)
                                warn!("Could not determine total object size for Content-Range header in merge scenario, using range size as fallback");
                                range_spec.end - range_spec.start + 1
                            }
                        };

                        let mut response_builder = Response::builder()
                            .status(StatusCode::PARTIAL_CONTENT)
                            .header("content-length", content_length.to_string())
                            .header(
                                "content-range",
                                range_handler
                                    .build_content_range_header(&range_spec, total_object_size),
                            )
                            .header("accept-ranges", "bytes");

                        // Add cache metadata headers if available
                        if !overlap.cached_ranges.is_empty() {
                            let cached_range = &overlap.cached_ranges[0];
                            if !cached_range.etag.is_empty() {
                                response_builder =
                                    response_builder.header("etag", &cached_range.etag);
                            }
                            if !cached_range.last_modified.is_empty() {
                                response_builder = response_builder
                                    .header("last-modified", &cached_range.last_modified);
                            }
                        }

                        // For HEAD requests, don't include body
                        let response_body = if method == Method::HEAD {
                            Full::new(Bytes::new())
                                .map_err(|never| match never {})
                                .boxed()
                        } else {
                            Full::new(Bytes::from(merge_result.data))
                                .map_err(|never| match never {})
                                .boxed()
                        };

                        Ok(response_builder.body(response_body).unwrap())
                    }
                    Err(e) => {
                        // This should rarely happen since merge_ranges_with_fallback handles most errors
                        // But if the fallback S3 fetch also fails, we return an error response
                        error!("Range merge and fallback both failed: {}", e);
                        Ok(Self::build_error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "InternalError",
                            "Failed to serve range request.",
                            None,
                        ))
                    }
                }
            }
            Err(e) => {
                error!(
                    "Failed to fetch missing ranges from S3: {}, falling back to complete S3 fetch",
                    e
                );
                // Fall back to complete S3 fetch - Requirement 2.5
                Self::fetch_complete_range_from_s3(
                    method,
                    uri,
                    host,
                    headers,
                    cache_key,
                    range_spec,
                    cache_manager,
                    range_handler,
                    s3_client,
                    config,
                )
                .await
            }
        }
    }

    /// Stream range request directly from S3 to client while caching in background
    ///
    /// This function is used for complete cache misses to avoid buffering large responses.
    /// It streams bytes directly to the client as they arrive from S3, preventing
    /// AWS SDK throughput failures for large files.
    ///
    /// The TeeStream wrapper sends data to both:
    /// 1. The client (immediately, as bytes arrive)
    /// 2. A background task that accumulates and caches the data
    async fn stream_range_from_s3_with_caching(
        method: Method,
        uri: hyper::Uri,
        host: String,
        headers: HashMap<String, String>,
        cache_key: String,
        range_spec: RangeSpec,
        range_handler: Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        config: Arc<Config>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        debug!(
            "Streaming range {}-{} from S3 with background caching: cache_key={}",
            range_spec.start, range_spec.end, cache_key
        );

        // Build S3 request context with range header and streaming enabled
        let mut s3_headers = headers.clone();
        // Remove any existing Range header (case-insensitive) to avoid duplicates
        s3_headers.retain(|k, _| k.to_lowercase() != "range");
        // Insert new Range header with proper capitalization
        s3_headers.insert(
            "Range".to_string(),
            format!("bytes={}-{}", range_spec.start, range_spec.end),
        );

        let mut context =
            build_s3_request_context(method.clone(), uri.clone(), s3_headers, None, host.clone());
        context.allow_streaming = true; // Enable streaming for immediate byte flow

        match s3_client.forward_request(context).await {
            Ok(s3_response) => {
                debug!(
                    "Received S3 response for streaming: status={}",
                    s3_response.status
                );

                if s3_response.status == StatusCode::PARTIAL_CONTENT {
                    // Use the streaming with caching function
                    Self::convert_s3_response_to_http_with_caching(
                        s3_response,
                        cache_key,
                        range_spec,
                        range_handler,
                        s3_client,
                        config,
                    )
                    .await
                } else if s3_response.status == StatusCode::OK {
                    // S3 returned full object instead of partial content
                    // This can happen if the range covers the entire object
                    debug!("S3 returned 200 OK instead of 206, streaming full response");
                    Self::convert_s3_response_to_http_with_caching(
                        s3_response,
                        cache_key,
                        range_spec,
                        range_handler,
                        s3_client,
                        config,
                    )
                    .await
                } else if s3_response.status == StatusCode::PRECONDITION_FAILED {
                    warn!("S3 returned 412 Precondition Failed, returning to client without invalidating cache");

                    Ok(Self::build_error_response(
                        StatusCode::PRECONDITION_FAILED,
                        "PreconditionFailed",
                        "Precondition failed",
                        None,
                    ))
                } else {
                    // Forward the S3 error response
                    Self::convert_s3_response_to_http(s3_response)
                }
            }
            Err(e) => {
                error!("Failed to stream range request from S3: {}", e);
                Ok(Self::build_error_response(
                    StatusCode::BAD_GATEWAY,
                    "BadGateway",
                    "Failed to forward range request to S3",
                    None,
                ))
            }
        }
    }

    /// Fetch complete range from S3 as fallback when merge fails
    async fn fetch_complete_range_from_s3(
        method: Method,
        uri: hyper::Uri,
        host: String,
        headers: HashMap<String, String>,
        cache_key: String,
        range_spec: RangeSpec,
        _cache_manager: Arc<CacheManager>,
        range_handler: Arc<RangeHandler>,
        s3_client: Arc<S3Client>,
        config: Arc<Config>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        debug!(
            "Fetching complete range {}-{} from S3 as fallback",
            range_spec.start, range_spec.end
        );

        // Build S3 request context with range header
        let mut s3_headers = headers.clone();
        // Remove any existing Range header (case-insensitive) to avoid duplicates
        s3_headers.retain(|k, _| k.to_lowercase() != "range");
        // Insert new Range header with proper capitalization
        s3_headers.insert(
            "Range".to_string(),
            format!("bytes={}-{}", range_spec.start, range_spec.end),
        );

        let context = build_s3_request_context(method.clone(), uri, s3_headers, None, host);

        match s3_client.forward_request(context).await {
            Ok(s3_response) => {
                debug!(
                    "Successfully received range response from S3: {}",
                    s3_response.status
                );

                if s3_response.status == StatusCode::PARTIAL_CONTENT {
                    // Use the new streaming with caching function
                    Self::convert_s3_response_to_http_with_caching(
                        s3_response,
                        cache_key.clone(),
                        range_spec.clone(),
                        range_handler.clone(),
                        s3_client.clone(),
                        config.clone(),
                    )
                    .await
                } else if s3_response.status == StatusCode::PRECONDITION_FAILED {
                    // Handle 412 response by returning to client without invalidating cache - Requirements 3.2, 4.2, 5.4
                    warn!("S3 returned 412 Precondition Failed, returning to client without invalidating cache");

                    Ok(Self::build_error_response(
                        StatusCode::PRECONDITION_FAILED,
                        "PreconditionFailed",
                        "Precondition failed",
                        None,
                    ))
                } else {
                    // Forward the S3 error response
                    Self::convert_s3_response_to_http(s3_response)
                }
            }
            Err(e) => {
                error!("Failed to forward range request to S3: {}", e);
                Ok(Self::build_error_response(
                    StatusCode::BAD_GATEWAY,
                    "BadGateway",
                    "Failed to forward range request to S3",
                    None,
                ))
            }
        }
    }

    /// Forward AWS SigV4 signed request without modification to preserve signature
    async fn forward_signed_request(
        req: Request<hyper::body::Incoming>,
        host: String,
        s3_client: Arc<S3Client>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        debug!("Forwarding signed request without modification to preserve AWS SigV4 signature");

        Self::forward_signed_put_request_impl(req, host, s3_client, proxy_referer).await
    }

    /// Implementation for forwarding signed requests
    async fn forward_signed_put_request_impl(
        req: Request<hyper::body::Incoming>,
        host: String,
        s3_client: Arc<S3Client>,
        proxy_referer: &Option<String>,
    ) -> std::result::Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        // Get connection pool to resolve DNS and get target IP
        let pool = s3_client.get_connection_pool();
        let mut pool_manager = pool.lock().await;

        // Resolve DNS to get target IP
        match pool_manager.get_connection(&host, None).await {
            Ok(connection) => {
                let target_ip = connection.ip_address;
                drop(pool_manager); // Release lock

                // Create TLS connector
                let mut root_store = rustls::RootCertStore::empty();

                // Load system root certificates
                match rustls_native_certs::load_native_certs() {
                    Ok(certs) => {
                        for cert in certs {
                            if let Err(e) = root_store.add(cert) {
                                warn!("Failed to add cert: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to load native certs: {}", e);
                        return Ok(Self::build_error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "InternalError",
                            "Failed to load TLS certificates",
                            None,
                        ));
                    }
                }

                let tls_config = rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth();

                let tls_connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));

                // Forward the signed request using raw HTTP forwarding
                match crate::signed_request_proxy::forward_signed_request(
                    req,
                    &host,
                    target_ip,
                    &tls_connector,
                    proxy_referer.as_deref(),
                )
                .await
                {
                    Ok(response) => {
                        debug!("Successfully forwarded signed request");
                        Ok(response)
                    }
                    Err(e) => {
                        error!("Failed to forward signed request: {}", e);
                        Ok(Self::build_error_response(
                            StatusCode::BAD_GATEWAY,
                            "BadGateway",
                            "Failed to forward signed request to S3",
                            None,
                        ))
                    }
                }
            }
            Err(e) => {
                error!("Failed to get connection for signed request: {}", e);
                Ok(Self::build_error_response(
                    StatusCode::BAD_GATEWAY,
                    "BadGateway",
                    "Failed to resolve S3 endpoint",
                    None,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_conditional_headers_if_match() {
        let mut headers = HashMap::new();
        headers.insert("if-match".to_string(), "\"etag123\"".to_string());

        assert!(HttpProxy::has_conditional_headers(&headers));
    }

    #[test]
    fn test_has_conditional_headers_if_none_match() {
        let mut headers = HashMap::new();
        headers.insert("if-none-match".to_string(), "\"etag123\"".to_string());

        assert!(HttpProxy::has_conditional_headers(&headers));
    }

    #[test]
    fn test_has_conditional_headers_if_modified_since() {
        let mut headers = HashMap::new();
        headers.insert(
            "if-modified-since".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        );

        assert!(HttpProxy::has_conditional_headers(&headers));
    }

    #[test]
    fn test_has_conditional_headers_if_unmodified_since() {
        let mut headers = HashMap::new();
        headers.insert(
            "if-unmodified-since".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        );

        assert!(HttpProxy::has_conditional_headers(&headers));
    }

    #[test]
    fn test_has_conditional_headers_multiple() {
        let mut headers = HashMap::new();
        headers.insert("if-match".to_string(), "\"etag123\"".to_string());
        headers.insert(
            "if-modified-since".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        );

        assert!(HttpProxy::has_conditional_headers(&headers));
    }

    #[test]
    fn test_has_conditional_headers_none() {
        let mut headers = HashMap::new();
        headers.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 ...".to_string(),
        );
        headers.insert("content-type".to_string(), "application/json".to_string());

        assert!(!HttpProxy::has_conditional_headers(&headers));
    }

    #[test]
    fn test_has_conditional_headers_empty() {
        let headers = HashMap::new();

        assert!(!HttpProxy::has_conditional_headers(&headers));
    }

    #[test]
    fn test_should_bypass_cache_list_operations() {
        let mut query_params = HashMap::new();

        // Test list-type parameter (ListObjects)
        query_params.insert("list-type".to_string(), "2".to_string());
        let (should_bypass, op_type, reason) =
            HttpProxy::should_bypass_cache("/bucket/", &query_params);
        assert!(should_bypass);
        assert_eq!(op_type, Some("ListObjects".to_string()));
        assert_eq!(
            reason,
            Some("list operation - always fetch fresh data".to_string())
        );

        // Test delimiter parameter (ListObjects)
        query_params.clear();
        query_params.insert("delimiter".to_string(), "/".to_string());
        let (should_bypass, op_type, _reason) =
            HttpProxy::should_bypass_cache("/bucket/", &query_params);
        assert!(should_bypass);
        assert_eq!(op_type, Some("ListObjects".to_string()));

        // Test versions parameter (ListObjectVersions)
        query_params.clear();
        query_params.insert("versions".to_string(), "".to_string());
        let (should_bypass, op_type, _reason) =
            HttpProxy::should_bypass_cache("/bucket/", &query_params);
        assert!(should_bypass);
        assert_eq!(op_type, Some("ListObjectVersions".to_string()));

        // Test uploads parameter (ListMultipartUploads)
        query_params.clear();
        query_params.insert("uploads".to_string(), "".to_string());
        let (should_bypass, op_type, _reason) =
            HttpProxy::should_bypass_cache("/bucket/", &query_params);
        assert!(should_bypass);
        assert_eq!(op_type, Some("ListMultipartUploads".to_string()));
    }

    #[test]
    fn test_should_bypass_cache_root_path() {
        let query_params = HashMap::new();

        // Test root path (ListBuckets)
        let (should_bypass, op_type, reason) = HttpProxy::should_bypass_cache("/", &query_params);
        assert!(should_bypass);
        assert_eq!(op_type, Some("ListBuckets".to_string()));
        assert_eq!(
            reason,
            Some("list operation - always fetch fresh data".to_string())
        );
    }

    #[test]
    fn test_should_bypass_cache_metadata_operations() {
        let mut query_params = HashMap::new();

        // Test acl parameter
        query_params.insert("acl".to_string(), "".to_string());
        let (should_bypass, op_type, _) =
            HttpProxy::should_bypass_cache("/bucket/object", &query_params);
        assert!(should_bypass);
        assert_eq!(op_type, Some("GetObjectAcl".to_string()));

        // Test tagging parameter
        query_params.clear();
        query_params.insert("tagging".to_string(), "".to_string());
        let (should_bypass, op_type, _) =
            HttpProxy::should_bypass_cache("/bucket/object", &query_params);
        assert!(should_bypass);
        assert_eq!(op_type, Some("GetObjectTagging".to_string()));

        // Test attributes parameter
        query_params.clear();
        query_params.insert("attributes".to_string(), "".to_string());
        let (should_bypass, op_type, _) =
            HttpProxy::should_bypass_cache("/bucket/object", &query_params);
        assert!(should_bypass);
        assert_eq!(op_type, Some("GetObjectAttributes".to_string()));
    }

    #[test]
    fn test_should_not_bypass_cache_part_number() {
        let mut query_params = HashMap::new();

        // Test partNumber parameter - should NOT bypass cache anymore
        query_params.insert("partNumber".to_string(), "1".to_string());
        let (should_bypass, op_type, reason) =
            HttpProxy::should_bypass_cache("/bucket/object", &query_params);
        assert!(!should_bypass);
        assert_eq!(op_type, None);
        assert_eq!(reason, None);
    }

    #[test]
    fn test_should_not_bypass_cache_get_object() {
        let query_params = HashMap::new();

        // Test regular GetObject (no query parameters)
        let (should_bypass, op_type, reason) =
            HttpProxy::should_bypass_cache("/bucket/object", &query_params);
        assert!(!should_bypass);
        assert_eq!(op_type, None);
        assert_eq!(reason, None);
    }

    #[test]
    fn test_should_not_bypass_cache_version_id_only() {
        let mut query_params = HashMap::new();

        // versionId alone does not trigger bypass in should_bypass_cache() —
        // versionId bypass is handled earlier in handle_get_head_request()
        query_params.insert("versionId".to_string(), "abc123".to_string());
        let (should_bypass, op_type, reason) =
            HttpProxy::should_bypass_cache("/bucket/object", &query_params);
        assert!(!should_bypass);
        assert_eq!(op_type, None);
        assert_eq!(reason, None);
    }

    #[test]
    fn test_head_object_always_cacheable() {
        let mut query_params = HashMap::new();

        // HeadObject requests should always be cached, even with query parameters
        // that would trigger bypass for GET requests
        query_params.insert("list-type".to_string(), "2".to_string());
        let (should_bypass, _, _) =
            HttpProxy::should_bypass_cache("/bucket/object.txt", &query_params);
        assert!(should_bypass); // The function returns true for GET

        // However, in handle_get_head_request, HEAD requests to objects are always cached
        // Only HEAD to root path "/" (HeadBucket/ListBuckets) bypasses cache

        // Test that root path detection works
        query_params.clear();
        let (should_bypass, op_type, _) = HttpProxy::should_bypass_cache("/", &query_params);
        assert!(should_bypass);
        assert_eq!(op_type, Some("ListBuckets".to_string()));
    }

    #[test]
    fn test_is_get_object_part_valid() {
        let mut query_params = HashMap::new();

        // Test valid GET request with partNumber
        query_params.insert("partNumber".to_string(), "1".to_string());
        let result = HttpProxy::is_get_object_part(&Method::GET, &query_params);
        assert_eq!(result, Some(1));

        // Test valid GET request with larger part number
        query_params.clear();
        query_params.insert("partNumber".to_string(), "42".to_string());
        let result = HttpProxy::is_get_object_part(&Method::GET, &query_params);
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_is_get_object_part_invalid_method() {
        let mut query_params = HashMap::new();
        query_params.insert("partNumber".to_string(), "1".to_string());

        // Test non-GET methods
        let result = HttpProxy::is_get_object_part(&Method::HEAD, &query_params);
        assert_eq!(result, None);

        let result = HttpProxy::is_get_object_part(&Method::PUT, &query_params);
        assert_eq!(result, None);

        let result = HttpProxy::is_get_object_part(&Method::POST, &query_params);
        assert_eq!(result, None);
    }

    #[test]
    fn test_is_get_object_part_upload_verification() {
        let mut query_params = HashMap::new();

        // Test GET request with both partNumber and uploadId (upload verification)
        query_params.insert("partNumber".to_string(), "1".to_string());
        query_params.insert("uploadId".to_string(), "abc123".to_string());
        let result = HttpProxy::is_get_object_part(&Method::GET, &query_params);
        assert_eq!(result, None);
    }

    #[test]
    fn test_is_get_object_part_invalid_part_numbers() {
        let mut query_params = HashMap::new();

        // Test zero part number
        query_params.insert("partNumber".to_string(), "0".to_string());
        let result = HttpProxy::is_get_object_part(&Method::GET, &query_params);
        assert_eq!(result, None);

        // Test negative part number
        query_params.clear();
        query_params.insert("partNumber".to_string(), "-1".to_string());
        let result = HttpProxy::is_get_object_part(&Method::GET, &query_params);
        assert_eq!(result, None);

        // Test non-numeric part number
        query_params.clear();
        query_params.insert("partNumber".to_string(), "abc".to_string());
        let result = HttpProxy::is_get_object_part(&Method::GET, &query_params);
        assert_eq!(result, None);

        // Test empty part number
        query_params.clear();
        query_params.insert("partNumber".to_string(), "".to_string());
        let result = HttpProxy::is_get_object_part(&Method::GET, &query_params);
        assert_eq!(result, None);
    }

    #[test]
    fn test_is_get_object_part_no_part_number() {
        let query_params = HashMap::new();

        // Test GET request without partNumber parameter
        let result = HttpProxy::is_get_object_part(&Method::GET, &query_params);
        assert_eq!(result, None);
    }

    #[test]
    fn test_presigned_url_expiration_detection() {
        use crate::presigned_url::parse_presigned_url;

        // Test expired presigned URL
        let mut query_params = HashMap::new();
        query_params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        query_params.insert("X-Amz-Date".to_string(), "20240115T120000Z".to_string()); // Past date
        query_params.insert("X-Amz-Expires".to_string(), "3600".to_string());
        query_params.insert("X-Amz-Signature".to_string(), "abc123".to_string());

        let presigned_info = parse_presigned_url(&query_params);
        assert!(presigned_info.is_some());
        assert!(presigned_info.unwrap().is_expired());

        // Test valid presigned URL (use a date far in the future)
        let mut query_params = HashMap::new();
        query_params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        query_params.insert("X-Amz-Date".to_string(), "20300115T120000Z".to_string()); // Far future date
        query_params.insert("X-Amz-Expires".to_string(), "3600".to_string());
        query_params.insert("X-Amz-Signature".to_string(), "abc123".to_string());

        let presigned_info = parse_presigned_url(&query_params);
        assert!(presigned_info.is_some());
        assert!(!presigned_info.unwrap().is_expired());

        // Test non-presigned URL
        let mut query_params = HashMap::new();
        query_params.insert("versionId".to_string(), "abc123".to_string());

        let presigned_info = parse_presigned_url(&query_params);
        assert!(presigned_info.is_none());
    }

    #[test]
    fn test_parse_content_range_valid() {
        // Basic valid case
        let result = parse_content_range("bytes 0-999/5000");
        assert_eq!(result, Some((0, 999, 5000)));

        // Large values (multipart part sizes)
        let result = parse_content_range("bytes 10485760-15728639/24117248");
        assert_eq!(result, Some((10485760, 15728639, 24117248)));

        // Single byte range
        let result = parse_content_range("bytes 0-0/1");
        assert_eq!(result, Some((0, 0, 1)));

        // Last byte of file
        let result = parse_content_range("bytes 4999-4999/5000");
        assert_eq!(result, Some((4999, 4999, 5000)));
    }

    #[test]
    fn test_parse_content_range_with_whitespace() {
        // Leading/trailing whitespace
        let result = parse_content_range("  bytes 0-999/5000  ");
        assert_eq!(result, Some((0, 999, 5000)));
    }

    #[test]
    fn test_parse_content_range_invalid_format() {
        // Missing "bytes " prefix
        let result = parse_content_range("0-999/5000");
        assert_eq!(result, None);

        // Wrong prefix
        let result = parse_content_range("octets 0-999/5000");
        assert_eq!(result, None);

        // Missing slash
        let result = parse_content_range("bytes 0-999");
        assert_eq!(result, None);

        // Missing dash
        let result = parse_content_range("bytes 0999/5000");
        assert_eq!(result, None);

        // Empty string
        let result = parse_content_range("");
        assert_eq!(result, None);

        // Just "bytes"
        let result = parse_content_range("bytes");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_content_range_invalid_numbers() {
        // Non-numeric start
        let result = parse_content_range("bytes abc-999/5000");
        assert_eq!(result, None);

        // Non-numeric end
        let result = parse_content_range("bytes 0-xyz/5000");
        assert_eq!(result, None);

        // Non-numeric total
        let result = parse_content_range("bytes 0-999/total");
        assert_eq!(result, None);

        // Negative numbers (parsed as non-numeric due to u64)
        let result = parse_content_range("bytes -1-999/5000");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_content_range_invalid_ranges() {
        // Start > end
        let result = parse_content_range("bytes 1000-999/5000");
        assert_eq!(result, None);

        // End >= total
        let result = parse_content_range("bytes 0-5000/5000");
        assert_eq!(result, None);

        // End > total
        let result = parse_content_range("bytes 0-6000/5000");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_content_range_unknown_total() {
        // Unknown total (asterisk) - we need total for part caching
        let result = parse_content_range("bytes 0-999/*");
        assert_eq!(result, None);
    }

    // Property-based tests using quickcheck
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: correct-get-part-behaviour, Property 3: Content-Range Parsing Extracts Correct Size**
    ///
    /// *For any* valid Content-Range header string `bytes {start}-{end}/{total}`,
    /// the parsed size equals `end - start + 1`.
    ///
    /// **Validates: Requirement 3.1**
    #[quickcheck]
    fn prop_content_range_parsing_extracts_correct_size(
        start: u32,
        range_size: u16,
        extra_total: u16,
    ) -> TestResult {
        // Use u32 for start to avoid overflow issues while still testing large values
        // Use u16 for range_size to keep ranges reasonable (1 to 65535 bytes)
        // Use u16 for extra_total to add padding beyond end

        // Filter: range_size must be at least 1 (valid range has at least 1 byte)
        if range_size == 0 {
            return TestResult::discard();
        }

        let start = start as u64;
        let range_size = range_size as u64;
        let extra_total = extra_total as u64;

        // Calculate end and total ensuring validity constraints:
        // - start <= end (guaranteed by range_size >= 1)
        // - end < total (guaranteed by extra_total >= 0, so total = end + 1 + extra_total)
        let end = start + range_size - 1;
        let total = end + 1 + extra_total; // total > end always

        // Construct the Content-Range header string
        let header = format!("bytes {}-{}/{}", start, end, total);

        // Parse the header
        let result = parse_content_range(&header);

        // Verify parsing succeeds and returns correct values
        match result {
            Some((parsed_start, parsed_end, parsed_total)) => {
                // Verify the parsed values match what we constructed
                if parsed_start != start {
                    return TestResult::failed();
                }
                if parsed_end != end {
                    return TestResult::failed();
                }
                if parsed_total != total {
                    return TestResult::failed();
                }

                // Verify the key property: size = end - start + 1
                let expected_size = range_size;
                let actual_size = parsed_end - parsed_start + 1;
                if actual_size != expected_size {
                    return TestResult::failed();
                }

                TestResult::passed()
            }
            None => {
                // Parsing should succeed for valid inputs
                TestResult::failed()
            }
        }
    }

    // ---- maybe_add_referer tests ----

    /// Test 6.1: maybe_add_referer adds Referer header when all conditions are met:
    /// - proxy_referer is Some
    /// - no existing Referer header
    /// - referer not in SignedHeaders (or no auth header)
    ///
    /// **Validates: Requirements 1.1, 1.2, 1.3**
    #[test]
    fn test_maybe_add_referer_adds_header_when_conditions_met() {
        let mut headers = HashMap::new();
        headers.insert("host".to_string(), "my-bucket.s3.amazonaws.com".to_string());

        let proxy_referer = Some("s3-hybrid-cache/1.0.0 (test-host)".to_string());

        // No auth header — always safe to add
        maybe_add_referer(&mut headers, &proxy_referer, None);

        assert_eq!(
            headers.get("Referer").unwrap(),
            "s3-hybrid-cache/1.0.0 (test-host)"
        );
    }

    /// Also verify it works with an auth header that does NOT include referer in SignedHeaders.
    ///
    /// **Validates: Requirements 1.1, 1.2, 1.3**
    #[test]
    fn test_maybe_add_referer_adds_header_with_auth_not_signing_referer() {
        let mut headers = HashMap::new();
        headers.insert("host".to_string(), "my-bucket.s3.amazonaws.com".to_string());

        let proxy_referer = Some("s3-hybrid-cache/1.0.0 (test-host)".to_string());
        let auth = "AWS4-HMAC-SHA256 Credential=AKID/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abcdef1234567890";

        maybe_add_referer(&mut headers, &proxy_referer, Some(auth));

        assert_eq!(
            headers.get("Referer").unwrap(),
            "s3-hybrid-cache/1.0.0 (test-host)"
        );
    }

    /// Test 6.2: maybe_add_referer skips when Referer header is already present.
    ///
    /// **Validates: Requirement 1.4**
    #[test]
    fn test_maybe_add_referer_skips_when_referer_already_present() {
        let mut headers = HashMap::new();
        headers.insert("host".to_string(), "my-bucket.s3.amazonaws.com".to_string());
        headers.insert("Referer".to_string(), "https://example.com".to_string());

        let proxy_referer = Some("s3-hybrid-cache/1.0.0 (test-host)".to_string());

        maybe_add_referer(&mut headers, &proxy_referer, None);

        // Original value preserved
        assert_eq!(headers.get("Referer").unwrap(), "https://example.com");
    }

    /// Also verify case-insensitive matching of existing Referer key.
    ///
    /// **Validates: Requirement 1.4**
    #[test]
    fn test_maybe_add_referer_skips_when_referer_present_lowercase() {
        let mut headers = HashMap::new();
        headers.insert("referer".to_string(), "https://example.com".to_string());

        let proxy_referer = Some("s3-hybrid-cache/1.0.0 (test-host)".to_string());

        maybe_add_referer(&mut headers, &proxy_referer, None);

        // Original value preserved, no second entry
        assert_eq!(headers.len(), 1);
        assert_eq!(headers.get("referer").unwrap(), "https://example.com");
    }

    /// Test 6.3: maybe_add_referer skips when "referer" is in SignedHeaders.
    ///
    /// **Validates: Requirements 3.1, 3.4**
    #[test]
    fn test_maybe_add_referer_skips_when_referer_in_signed_headers() {
        let mut headers = HashMap::new();
        headers.insert("host".to_string(), "my-bucket.s3.amazonaws.com".to_string());

        let proxy_referer = Some("s3-hybrid-cache/1.0.0 (test-host)".to_string());
        let auth = "AWS4-HMAC-SHA256 Credential=AKID/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;referer;x-amz-content-sha256;x-amz-date, Signature=abcdef1234567890";

        maybe_add_referer(&mut headers, &proxy_referer, Some(auth));

        // Referer must NOT be added because it's in SignedHeaders
        assert!(headers.get("Referer").is_none());
    }

    /// Test 6.4: maybe_add_referer skips when proxy_referer is None (feature disabled).
    ///
    /// **Validates: Requirement 2.3**
    #[test]
    fn test_maybe_add_referer_skips_when_disabled() {
        let mut headers = HashMap::new();
        headers.insert("host".to_string(), "my-bucket.s3.amazonaws.com".to_string());

        let proxy_referer: Option<String> = None;

        maybe_add_referer(&mut headers, &proxy_referer, None);

        assert!(headers.get("Referer").is_none());
    }

    /// Test 6.5: Header format matches `s3-hybrid-cache/{version} ({hostname})`.
    /// Uses env!("CARGO_PKG_VERSION") to verify the version component.
    ///
    /// **Validates: Requirements 1.2, 1.3**
    #[test]
    fn test_maybe_add_referer_header_format() {
        let mut headers = HashMap::new();

        let hostname = "ip-172-31-34-221.us-west-2.compute.internal";
        let version = env!("CARGO_PKG_VERSION");
        let expected = format!("s3-hybrid-cache/{} ({})", version, hostname);
        let proxy_referer = Some(expected.clone());

        maybe_add_referer(&mut headers, &proxy_referer, None);

        let actual = headers.get("Referer").expect("Referer header should be present");
        assert_eq!(actual, &expected);

        // Verify the format structure: starts with "s3-hybrid-cache/", contains version, ends with "(hostname)"
        assert!(actual.starts_with("s3-hybrid-cache/"));
        assert!(actual.contains(version));
        assert!(actual.ends_with(&format!("({})", hostname)));
    }
}
