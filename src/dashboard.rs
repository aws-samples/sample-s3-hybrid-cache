//! Dashboard Module
//!
//! Provides a web-based dashboard interface for monitoring S3 Hybrid Cache performance and viewing application logs.

use crate::cache::CacheManager;
use crate::config::DashboardConfig;
use crate::logging::LoggerManager;
use crate::metrics::MetricsManager;
use crate::shutdown::ShutdownSignal;
use crate::{ProxyError, Result};
use chrono::{DateTime, Utc};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// Connection guard to automatically decrement connection counter when dropped
struct ConnectionGuard {
    counter: Arc<std::sync::atomic::AtomicUsize>,
}

impl ConnectionGuard {
    fn new(counter: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        Self { counter }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.counter
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Dashboard server for web-based monitoring interface
#[derive(Clone)]
pub struct DashboardServer {
    config: Arc<DashboardConfig>,
    logger_manager: Option<Arc<Mutex<LoggerManager>>>,
    static_handler: Arc<StaticFileHandler>,
    api_handler: Arc<ApiHandler>,
}

impl DashboardServer {
    /// Create new dashboard server
    pub fn new(config: Arc<DashboardConfig>, app_log_dir: PathBuf) -> Self {
        let log_reader = Arc::new(LogReader::new(
            app_log_dir,
            gethostname::gethostname().to_string_lossy().to_string(),
        ));

        let static_handler = Arc::new(StaticFileHandler::new());
        let api_handler = Arc::new(ApiHandler::new(log_reader, config.clone()));

        Self {
            config,
            logger_manager: None,
            static_handler,
            api_handler,
        }
    }

    /// Set cache manager reference
    pub async fn set_cache_manager(&self, cache_manager: Arc<CacheManager>) {
        self.api_handler.set_cache_manager(cache_manager).await;
    }

    /// Set metrics manager reference
    pub async fn set_metrics_manager(&self, metrics_manager: Arc<RwLock<MetricsManager>>) {
        self.api_handler.set_metrics_manager(metrics_manager).await;
    }

    /// Set logger manager reference
    pub fn set_logger_manager(&mut self, logger_manager: Arc<Mutex<LoggerManager>>) {
        self.logger_manager = Some(logger_manager);
    }

    /// Start the dashboard server
    pub async fn start(&self, mut shutdown_signal: ShutdownSignal) -> Result<()> {
        let addr = SocketAddr::new(
            self.config.bind_address.parse().map_err(|e| {
                ProxyError::ConfigError(format!("Invalid dashboard bind address: {}", e))
            })?,
            self.config.port,
        );

        let listener = TcpListener::bind(addr).await.map_err(|e| {
            ProxyError::IoError(format!(
                "Failed to bind dashboard server to {}: {}",
                addr, e
            ))
        })?;

        info!("Dashboard server listening on {}", addr);
        info!(
            "Dashboard configuration: cache_refresh={}s, logs_refresh={}s, max_log_entries={}",
            self.config.cache_stats_refresh_interval.as_secs(),
            self.config.logs_refresh_interval.as_secs(),
            self.config.max_log_entries
        );

        // Track active connections for graceful shutdown
        let active_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_connections = 50; // Limit concurrent connections to prevent resource exhaustion

        loop {
            tokio::select! {
                // Handle incoming connections
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, remote_addr)) => {
                            // Check connection limit
                            let current_connections = active_connections.load(std::sync::atomic::Ordering::Relaxed);
                            if current_connections >= max_connections {
                                warn!("Dashboard connection limit reached ({}), rejecting connection from {}",
                                       max_connections, remote_addr);
                                // Close the stream immediately
                                drop(stream);
                                continue;
                            }

                            debug!("Dashboard accepting connection from {}", remote_addr);

                            let io = TokioIo::new(stream);
                            let server_clone = self.clone_for_request();
                            let connections_counter = active_connections.clone();

                            // Increment connection counter
                            connections_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            tokio::spawn(async move {
                                let _connection_guard = ConnectionGuard::new(connections_counter);

                                let service = service_fn(move |req| {
                                    let server = server_clone.clone();
                                    async move {
                                        // Add timeout to prevent hanging connections
                                        match tokio::time::timeout(
                                            std::time::Duration::from_secs(30),
                                            server.handle_request(req)
                                        ).await {
                                            Ok(result) => result,
                                            Err(_) => {
                                                error!("Dashboard request timed out after 30 seconds");
                                                Ok(Response::builder()
                                                    .status(StatusCode::REQUEST_TIMEOUT)
                                                    .header("Content-Type", "text/plain")
                                                    .body("Request timeout".to_string())
                                                    .unwrap_or_else(|_| Response::new("Timeout".to_string())))
                                            }
                                        }
                                    }
                                });

                                // Configure HTTP connection with timeouts
                                let mut builder = http1::Builder::new();
                                builder.keep_alive(true);
                                builder.max_buf_size(64 * 1024); // 64KB max buffer

                                if let Err(e) = builder.serve_connection(io, service).await {
                                    // Only log non-graceful shutdown errors
                                    if !e.to_string().contains("connection closed") {
                                        error!("Error serving dashboard connection: {}", e);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept dashboard connection: {}", e);
                            // Add small delay to prevent tight error loops
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                }
                // Handle shutdown signal
                _ = shutdown_signal.wait_for_shutdown() => {
                    info!("Dashboard server received shutdown signal");
                    break;
                }
            }
        }

        // Wait for active connections to finish (with timeout)
        let shutdown_timeout = std::time::Duration::from_secs(5);
        let start_time = std::time::Instant::now();

        while active_connections.load(std::sync::atomic::Ordering::Relaxed) > 0
            && start_time.elapsed() < shutdown_timeout
        {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let remaining_connections = active_connections.load(std::sync::atomic::Ordering::Relaxed);
        if remaining_connections > 0 {
            info!(
                "Dashboard server shutdown with {} active connections remaining",
                remaining_connections
            );
        } else {
            info!("Dashboard server shutdown complete - all connections closed gracefully");
        }

        Ok(())
    }

    /// Clone server state for request handling
    fn clone_for_request(&self) -> DashboardServerClone {
        DashboardServerClone {
            static_handler: self.static_handler.clone(),
            api_handler: self.api_handler.clone(),
        }
    }
}

/// Cloneable version of dashboard server for request handling
#[derive(Clone)]
struct DashboardServerClone {
    static_handler: Arc<StaticFileHandler>,
    api_handler: Arc<ApiHandler>,
}

impl DashboardServerClone {
    async fn handle_request(
        &self,
        req: Request<hyper::body::Incoming>,
    ) -> Result<Response<String>> {
        let method = req.method();
        let uri = req.uri();
        let path = uri.path();

        debug!("Dashboard request: {} {}", method, path);

        // Only allow GET requests
        if method != Method::GET {
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .header("Allow", "GET")
                .body("Method Not Allowed".to_string())
                .map_err(|e| ProxyError::HttpError(format!("Failed to build response: {}", e)))?);
        }

        match path {
            // API endpoints
            "/api/cache-stats" => self.api_handler.get_cache_stats().await,
            "/api/bucket-stats" => self.api_handler.get_bucket_stats().await,
            "/api/system-info" => self.api_handler.get_system_info().await,
            "/api/logs" => {
                let params = parse_log_query_params(uri);
                // Validate parameters and return 400 for invalid ones
                if let Some(validation_error) = validate_log_params(&params) {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Content-Type", "application/json")
                        .header("Cache-Control", "no-cache")
                        .body(format!(
                            r#"{{"error": "Invalid parameters", "code": 400, "message": "{}"}}"#,
                            validation_error.replace('"', "\\\"")
                        ))
                        .map_err(|e| {
                            error!("Failed to build validation error response: {}", e);
                            ProxyError::HttpError(format!("Failed to build error response: {}", e))
                        })?);
                }
                self.api_handler.get_logs(params).await
            }
            // Static files
            "/" | "/index.html" => {
                self.static_handler
                    .handle_static_request("/index.html")
                    .await
            }
            "/style.css" => {
                self.static_handler
                    .handle_static_request("/style.css")
                    .await
            }
            "/script.js" => {
                self.static_handler
                    .handle_static_request("/script.js")
                    .await
            }
            // 404 for everything else
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("Content-Type", "text/plain")
                .body("Not Found".to_string())
                .map_err(|e| {
                    ProxyError::HttpError(format!("Failed to build 404 response: {}", e))
                })?),
        }
    }
}

/// Static file handler for embedded HTML, CSS, and JavaScript
pub struct StaticFileHandler {
    // Static files are embedded in the binary
}

impl StaticFileHandler {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn handle_static_request(&self, path: &str) -> Result<Response<String>> {
        let (content, content_type) = match path {
            "/index.html" => (self.get_index_html(), "text/html"),
            "/style.css" => (self.get_style_css(), "text/css"),
            "/script.js" => (self.get_script_js(), "application/javascript"),
            _ => {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("Content-Type", "text/plain")
                    .body("Static file not found".to_string())
                    .map_err(|e| {
                        ProxyError::HttpError(format!("Failed to build 404 response: {}", e))
                    })?);
            }
        };

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", content_type)
            .header("Cache-Control", "no-cache")
            .body(content)
            .map_err(|e| {
                ProxyError::HttpError(format!("Failed to build static file response: {}", e))
            })?)
    }

    pub fn get_index_html(&self) -> String {
        // Minimal HTML dashboard interface
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>S3 Hybrid Cache</title>
    <link rel="stylesheet" href="/style.css">
</head>
<body>
    <header>
        <h1>S3 Hybrid Cache</h1>
        <div id="system-info">
            <span id="hostname">Loading...</span> | 
            <span id="version">Loading...</span> | 
            <span id="uptime">Loading...</span>
        </div>
    </header>
    
    <nav>
        <button id="cache-stats-tab" class="tab-button active" onclick="showTab('cache-stats')">Cache Stats</button>
        <button id="logs-tab" class="tab-button" onclick="showTab('logs')">Application Logs</button>
    </nav>
    
    <main>
        <div id="cache-stats" class="tab-content active">
            <h2>Cache Statistics</h2>
            <div id="cache-stats-content">Loading...</div>
            <div id="bucket-stats-section" style="display:none;">
                <h2>Per-Bucket Cache Settings</h2>
                <div class="bucket-stats-controls">
                    <input type="text" id="bucket-filter" placeholder="Filter by bucket name..." />
                    <span id="bucket-stats-toggle"></span>
                </div>
                <div class="bucket-stats-table">
                    <table id="bucket-stats-table">
                        <thead>
                            <tr>
                                <th class="sortable" data-sort="bucket">Bucket <span class="sort-arrow"></span></th>
                                <th class="sortable" data-sort="hits">Hits <span class="sort-arrow"></span></th>
                                <th class="sortable" data-sort="misses">Misses <span class="sort-arrow"></span></th>
                                <th class="sortable" data-sort="hitRate">Hit Rate <span class="sort-arrow"></span></th>
                                <th class="sortable" data-sort="getTtl">GET TTL <span class="sort-arrow"></span></th>
                                <th class="sortable" data-sort="readCache">Read Cache <span class="sort-arrow"></span></th>
                                <th class="sortable" data-sort="writeCache">Write Cache <span class="sort-arrow"></span></th>
                                <th class="sortable" data-sort="ramCache">RAM Cache <span class="sort-arrow"></span></th>
                            </tr>
                        </thead>
                        <tbody id="bucket-stats-body"></tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <div id="logs" class="tab-content">
            <h2>Application Logs</h2>
            <div class="logs-controls">
                <label for="log-level-filter">Level:</label>
                <select id="log-level-filter">
                    <option value="">All</option>
                    <option value="ERROR">ERROR</option>
                    <option value="WARN">WARN</option>
                    <option value="INFO">INFO</option>
                    <option value="DEBUG">DEBUG</option>
                </select>
                
                <label for="log-text-filter">Filter:</label>
                <input type="text" id="log-text-filter" placeholder="Search logs..." style="width: 200px;">
                
                <label for="log-limit">Entries:</label>
                <select id="log-limit">
                    <option value="50">50</option>
                    <option value="100" selected>100</option>
                    <option value="200">200</option>
                    <option value="500">500</option>
                </select>
                
                <button id="refresh-logs" onclick="refreshLogs()">Refresh</button>
            </div>
            <div id="logs-content">Loading...</div>
        </div>
    </main>
    
    <script src="/script.js"></script>
</body>
</html>"#.to_string()
    }

    pub fn get_style_css(&self) -> String {
        // Minimal, responsive CSS for dashboard layout
        r#"* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    line-height: 1.6;
    color: #333;
    background-color: #f5f5f5;
}

header {
    background: #2c3e50;
    color: white;
    padding: 1rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
}

header h1 {
    font-size: 1.5rem;
}

#system-info {
    font-size: 0.9rem;
    opacity: 0.9;
}

nav {
    background: #34495e;
    padding: 0;
    display: flex;
}

.tab-button {
    background: none;
    border: none;
    color: white;
    padding: 1rem 2rem;
    cursor: pointer;
    transition: background-color 0.2s;
}

.tab-button:hover {
    background-color: rgba(255, 255, 255, 0.1);
}

.tab-button.active {
    background-color: #3498db;
}

main {
    padding: 2rem;
    max-width: 1200px;
    margin: 0 auto;
}

.tab-content {
    display: none;
}

.tab-content.active {
    display: block;
}

.tab-content h2 {
    margin-bottom: 1rem;
    color: #2c3e50;
}

.stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 1rem;
    margin-bottom: 2rem;
}

.stat-card {
    background: white;
    padding: 1.5rem;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.stat-card h3 {
    color: #2c3e50;
    margin-bottom: 0.25rem;
    font-size: 1.1rem;
}

.stat-subtitle {
    color: #7f8c8d;
    font-size: 0.8rem;
    margin-bottom: 1rem;
}

.stat-item {
    display: flex;
    justify-content: space-between;
    margin-bottom: 0.5rem;
}

.stat-item span[title] {
    cursor: help;
    border-bottom: 1px dotted #95a5a6;
}

.stat-item.warning {
    background-color: #fff3cd;
    padding: 0.25rem 0.5rem;
    border-radius: 4px;
    margin-left: -0.5rem;
    margin-right: -0.5rem;
}

.stat-value {
    font-weight: bold;
    color: #27ae60;
}

.stat-value.warning-value {
    color: #e67e22;
}

.logs-controls {
    background: white;
    padding: 1rem;
    border-radius: 8px;
    margin-bottom: 1rem;
    display: flex;
    gap: 1rem;
    align-items: center;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.logs-controls label {
    font-weight: bold;
}

.logs-controls select, .logs-controls button, .logs-controls input[type="text"] {
    padding: 0.5rem;
    border: 1px solid #ddd;
    border-radius: 4px;
}

.logs-controls input[type="text"] {
    min-width: 150px;
}

.logs-controls input[type="text"]:focus {
    outline: none;
    border-color: #3498db;
    box-shadow: 0 0 0 2px rgba(52, 152, 219, 0.2);
}

.logs-controls button {
    background: #3498db;
    color: white;
    cursor: pointer;
}

.logs-controls button:hover {
    background: #2980b9;
}

.logs-table {
    background: white;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.logs-table table {
    width: 100%;
    border-collapse: collapse;
}

.logs-table th {
    background: #34495e;
    color: white;
    padding: 1rem;
    text-align: left;
}

.logs-table td {
    padding: 0.75rem 1rem;
    border-bottom: 1px solid #eee;
}

.logs-table tr:hover {
    background-color: #f8f9fa;
}

.log-level {
    padding: 0.25rem 0.5rem;
    border-radius: 4px;
    font-size: 0.8rem;
    font-weight: bold;
}

.log-level.ERROR {
    background: #e74c3c;
    color: white;
}

.log-level.WARN {
    background: #f39c12;
    color: white;
}

.log-level.INFO {
    background: #3498db;
    color: white;
}

.log-level.DEBUG {
    background: #95a5a6;
    color: white;
}

.loading {
    text-align: center;
    padding: 2rem;
    color: #7f8c8d;
}

.error {
    background: #e74c3c;
    color: white;
    padding: 1rem;
    border-radius: 4px;
    margin: 1rem 0;
}

#bucket-stats-section {
    margin-top: 2rem;
}

#bucket-stats-section h2 {
    margin-bottom: 1rem;
    color: #2c3e50;
}

.bucket-stats-controls {
    background: white;
    padding: 1rem;
    border-radius: 8px;
    margin-bottom: 1rem;
    display: flex;
    gap: 1rem;
    align-items: center;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.bucket-stats-controls input[type="text"] {
    padding: 0.5rem;
    border: 1px solid #ddd;
    border-radius: 4px;
    min-width: 200px;
}

.bucket-stats-controls input[type="text"]:focus {
    outline: none;
    border-color: #3498db;
    box-shadow: 0 0 0 2px rgba(52, 152, 219, 0.2);
}

#bucket-stats-toggle a {
    color: #3498db;
    cursor: pointer;
    text-decoration: none;
    font-size: 0.9rem;
}

#bucket-stats-toggle a:hover {
    text-decoration: underline;
}

.bucket-stats-table {
    background: white;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.bucket-stats-table table {
    width: 100%;
    border-collapse: collapse;
}

.bucket-stats-table th {
    background: #34495e;
    color: white;
    padding: 0.75rem 1rem;
    text-align: left;
    white-space: nowrap;
    user-select: none;
}

.bucket-stats-table th.sortable {
    cursor: pointer;
}

.bucket-stats-table th.sortable:hover {
    background: #3d566e;
}

.bucket-stats-table th .sort-arrow {
    font-size: 0.7rem;
    margin-left: 0.25rem;
}

.bucket-stats-table td {
    padding: 0.6rem 1rem;
    border-bottom: 1px solid #eee;
}

.bucket-stats-table tr.bucket-row {
    cursor: pointer;
}

.bucket-stats-table tr.bucket-row:hover {
    background-color: #f0f7ff;
}

.bucket-stats-table tr.detail-row {
    background-color: #f8f9fa;
}

.bucket-stats-table tr.detail-row td {
    padding: 1rem;
}

.detail-content {
    font-size: 0.9rem;
}

.detail-content h4 {
    margin: 0 0 0.5rem 0;
    color: #2c3e50;
    font-size: 0.95rem;
}

.detail-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
    gap: 0.4rem 1.5rem;
    margin-bottom: 0.75rem;
}

.detail-grid .detail-item {
    display: flex;
    justify-content: space-between;
    gap: 0.5rem;
}

.detail-grid .detail-label {
    color: #7f8c8d;
}

.detail-grid .detail-value {
    font-weight: bold;
}

.prefix-table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 0.5rem;
    font-size: 0.85rem;
}

.prefix-table th {
    background: #ecf0f1;
    color: #2c3e50;
    padding: 0.4rem 0.6rem;
    text-align: left;
    font-weight: 600;
}

.prefix-table td {
    padding: 0.4rem 0.6rem;
    border-bottom: 1px solid #eee;
}

.badge-on {
    color: #27ae60;
    font-weight: bold;
}

.badge-off {
    color: #e74c3c;
    font-weight: bold;
}

@media (max-width: 768px) {
    header {
        flex-direction: column;
        gap: 0.5rem;
    }
    
    nav {
        flex-direction: column;
    }
    
    .logs-controls {
        flex-direction: column;
        align-items: stretch;
    }
    
    .logs-table {
        overflow-x: auto;
    }

    .bucket-stats-controls {
        flex-direction: column;
        align-items: stretch;
    }

    .bucket-stats-table {
        overflow-x: auto;
    }
}"#
        .to_string()
    }

    pub fn get_script_js(&self) -> String {
        // JavaScript for dashboard functionality
        r#"let currentTab = 'cache-stats';
let cacheStatsInterval;
let logsInterval;
let cacheStatsRefreshMs = 5000;  // Default, overridden by server config
let logsRefreshMs = 10000;       // Default, overridden by server config

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    showTab('cache-stats');
    loadSystemInfo().then(() => startAutoRefresh());
});

function showTab(tabName) {
    // Hide all tab contents
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    
    // Remove active class from all tab buttons
    document.querySelectorAll('.tab-button').forEach(button => {
        button.classList.remove('active');
    });
    
    // Show selected tab content
    document.getElementById(tabName).classList.add('active');
    document.getElementById(tabName + '-tab').classList.add('active');
    
    currentTab = tabName;
    
    // Load content for the selected tab
    if (tabName === 'cache-stats') {
        loadCacheStats();
    } else if (tabName === 'logs') {
        loadLogs();
    }
}

function startAutoRefresh() {
    // Clear existing intervals
    if (cacheStatsInterval) clearInterval(cacheStatsInterval);
    if (logsInterval) clearInterval(logsInterval);
    
    // Auto-refresh cache stats using server-configured interval
    cacheStatsInterval = setInterval(() => {
        if (currentTab === 'cache-stats') {
            loadCacheStats();
        }
        // Always refresh system info (uptime) regardless of tab
        loadSystemInfo();
    }, cacheStatsRefreshMs);
    
    // Auto-refresh logs using server-configured interval
    logsInterval = setInterval(() => {
        if (currentTab === 'logs') {
            loadLogs();
        }
    }, logsRefreshMs);
}

async function loadSystemInfo() {
    try {
        const response = await fetch('/api/system-info');
        const data = await response.json();
        
        document.getElementById('hostname').textContent = 'Host: ' + (data.hostname || 'Unknown');
        document.getElementById('version').textContent = 'Version: ' + (data.version || 'Unknown');
        
        if (data.uptime_seconds) {
            const uptime = formatUptime(data.uptime_seconds);
            document.getElementById('uptime').textContent = 'Uptime: ' + uptime;
        }

        // Apply server-configured refresh intervals (only on first load)
        if (data.cache_stats_refresh_ms && data.cache_stats_refresh_ms > 0) {
            cacheStatsRefreshMs = data.cache_stats_refresh_ms;
        }
        if (data.logs_refresh_ms && data.logs_refresh_ms > 0) {
            logsRefreshMs = data.logs_refresh_ms;
        }
    } catch (error) {
        console.error('Failed to load system info:', error);
        // Fallback to cache stats API for system info
        try {
            const response = await fetch('/api/cache-stats');
            const data = await response.json();
            
            document.getElementById('hostname').textContent = 'Host: ' + (data.hostname || 'Unknown');
            document.getElementById('version').textContent = 'Version: ' + (data.version || 'Unknown');
            
            if (data.overall && data.overall.uptime_seconds) {
                const uptime = formatUptime(data.overall.uptime_seconds);
                document.getElementById('uptime').textContent = 'Uptime: ' + uptime;
            }
        } catch (fallbackError) {
            console.error('Failed to load system info from fallback:', fallbackError);
        }
    }
}

async function loadCacheStats() {
    const content = document.getElementById('cache-stats-content');
    
    try {
        const response = await fetch('/api/cache-stats');
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        content.innerHTML = renderCacheStats(data);
    } catch (error) {
        content.innerHTML = `<div class="error">Failed to load cache statistics: ${error.message}</div>`;
    }
    
    loadBucketStats();
}

async function loadLogs() {
    const content = document.getElementById('logs-content');
    const levelFilter = document.getElementById('log-level-filter').value;
    const textFilter = document.getElementById('log-text-filter').value.trim();
    const limit = document.getElementById('log-limit').value;
    
    try {
        let url = '/api/logs?limit=' + limit;
        if (levelFilter) {
            url += '&level=' + levelFilter;
        }
        if (textFilter) {
            url += '&text=' + encodeURIComponent(textFilter);
        }
        
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        // Store data for rendering
        window.lastLogsData = data;
        content.innerHTML = renderLogs(data);
    } catch (error) {
        content.innerHTML = `<div class="error">Failed to load logs: ${error.message}</div>`;
    }
}

function refreshLogs() {
    loadLogs();
}

function renderCacheStats(data) {
    if (!data) {
        return '<div class="loading">No cache statistics available</div>';
    }
    
    // Build stale handle errors display (only show if > 0)
    const staleHandleDisplay = (data.metadata_cache?.stale_handle_errors || 0) > 0 
        ? `<div class="stat-item warning">
               <span title="File handle errors when reading metadata from shared storage (NFS/EFS). Indicates stale NFS file handles that required retry.">Stale Handle Errors:</span>
               <span class="stat-value warning-value">${formatNumber(data.metadata_cache?.stale_handle_errors || 0)}</span>
           </div>`
        : '';
    
    // Build disk revalidated display (only show if > 0)
    const staleRefreshes = data.metadata_cache?.stale_refreshes || 0;
    const totalMetadataLookups = (data.metadata_cache?.hits || 0) + (data.metadata_cache?.misses || 0) + staleRefreshes;
    const diskRevalidatedPct = totalMetadataLookups > 0 ? (staleRefreshes / totalMetadataLookups * 100).toFixed(1) : '0.0';
    const staleRefreshesDisplay = staleRefreshes > 0 
        ? `<div class="stat-item">
               <span title="Percentage of metadata lookups where the cached entry exceeded the refresh interval and was re-read from disk.">Disk Revalidated:</span>
               <span class="stat-value">${diskRevalidatedPct}%</span>
           </div>`
        : '';
    
    // Build last consolidation display (only show if available)
    const lastConsolidationDisplay = data.last_consolidation 
        ? `<div class="stat-item">
               <span title="When the journal consolidator last processed pending journal entries and updated cache size tracking.">Last Consolidation:</span>
               <span class="stat-value">${formatTimestamp(data.last_consolidation)}</span>
           </div>`
        : '';
    
    return `
        <div class="stats-grid">
            <div class="stat-card">
                <h3>RAM Cache: Object Ranges</h3>
                <p class="stat-subtitle">Object range data (GET responses)</p>
                <div class="stat-item">
                    <span title="Percentage of RAM cache lookups that found data. Higher is better.">Hit Rate:</span>
                    <span class="stat-value">${(data.ram_cache?.hit_rate || 0).toFixed(1)}%</span>
                </div>
                <div class="stat-item">
                    <span title="Number of GET requests served from RAM cache without disk I/O.">Hits:</span>
                    <span class="stat-value">${formatNumber(data.ram_cache?.hits || 0)}</span>
                </div>
                <div class="stat-item">
                    <span title="Number of GET requests not found in RAM cache, falling through to disk.">Misses:</span>
                    <span class="stat-value">${formatNumber(data.ram_cache?.misses || 0)}</span>
                </div>
                <div class="stat-item">
                    <span title="Current RAM cache usage vs configured maximum size.">Size:</span>
                    <span class="stat-value">${data.ram_cache?.size_human || '0 B'}${data.ram_cache?.max_size_human ? ' / ' + data.ram_cache.max_size_human : ''}</span>
                </div>
                <div class="stat-item">
                    <span title="Number of entries removed from RAM cache to make room for new data.">Evictions:</span>
                    <span class="stat-value">${formatNumber(data.ram_cache?.evictions || 0)}</span>
                </div>
            </div>
            
            <div class="stat-card">
                <h3>RAM Metadata Cache</h3>
                <p class="stat-subtitle">In-memory cache for .meta objects (HEAD + GET)</p>
                <div class="stat-item">
                    <span title="Percentage of metadata lookups served from the in-memory metadata cache.">Hit Rate:</span>
                    <span class="stat-value">${(data.metadata_cache?.hit_rate || 0).toFixed(1)}%</span>
                </div>
                <div class="stat-item">
                    <span title="Number of metadata lookups served from the in-memory cache.">Hits:</span>
                    <span class="stat-value">${formatNumber(data.metadata_cache?.hits || 0)}</span>
                </div>
                <div class="stat-item">
                    <span title="Number of metadata lookups that required reading from disk.">Misses:</span>
                    <span class="stat-value">${formatNumber(data.metadata_cache?.misses || 0)}</span>
                </div>
                <div class="stat-item">
                    <span title="Number of metadata entries currently held in RAM vs configured maximum.">Cached Entries:</span>
                    <span class="stat-value">${formatNumber(data.metadata_cache?.entries || 0)} / ${formatNumber(data.metadata_cache?.max_entries || 0)}</span>
                </div>
                <div class="stat-item">
                    <span title="Number of metadata entries evicted from RAM cache when at capacity (LRU).">Evictions:</span>
                    <span class="stat-value">${formatNumber(data.metadata_cache?.evictions || 0)}</span>
                </div>
                ${staleRefreshesDisplay}
                ${staleHandleDisplay}
            </div>
            
            <div class="stat-card">
                <h3>Disk Cache: Object Ranges</h3>
                <p class="stat-subtitle">GET and PUT data on disk</p>
                <div class="stat-item">
                    <span title="Percentage of GET requests served from disk cache.">Hit Rate:</span>
                    <span class="stat-value">${(data.disk_cache?.hit_rate || 0).toFixed(1)}%</span>
                </div>
                <div class="stat-item">
                    <span title="Number of GET requests served from cached range data on disk.">Hits:</span>
                    <span class="stat-value">${formatNumber(data.disk_cache?.hits || 0)}</span>
                </div>
                <div class="stat-item">
                    <span title="Number of GET requests forwarded to S3 (not found in cache).">Misses:</span>
                    <span class="stat-value">${formatNumber(data.disk_cache?.misses || 0)}</span>
                </div>
                <div class="stat-item">
                    <span title="Total disk cache usage (read + write cache) vs configured maximum.">Total Disk Size:</span>
                    <span class="stat-value">${data.disk_cache?.size_human || '0 B'}${data.disk_cache?.max_size_human ? ' / ' + data.disk_cache.max_size_human : ''}</span>
                </div>
                <div class="stat-item">
                    <span title="Disk space used by MPUs in progress and PUT objects not yet read via GET. Subset of total disk size, capped at write_cache_percent of total.">Write Cache Size:</span>
                    <span class="stat-value">${data.write_cache?.size_human || '0 B'} / ${data.write_cache?.max_size_human || '0 B'}</span>
                </div>
                <div class="stat-item">
                    <span title="Number of cached ranges removed during eviction to free disk space.">Evictions:</span>
                    <span class="stat-value">${formatNumber(data.disk_cache?.evictions || 0)}</span>
                </div>
            </div>
            
            <div class="stat-card">
                <h3>Overall Statistics</h3>
                <div class="stat-item">
                    <span title="Total GET and HEAD cache lookups (read cache requests + metadata cache requests).">Cache Requests (GET + HEAD):</span>
                    <span class="stat-value">${formatNumber((data.overall?.total_requests || 0) + (data.metadata_cache?.hits || 0) + (data.metadata_cache?.misses || 0))}</span>
                </div>
                <div class="stat-item">
                    <span title="Total bytes served from cache instead of fetching from S3.">S3 Transfer Saved:</span>
                    <span class="stat-value">${data.overall?.s3_transfer_saved_human || '0 B'}</span>
                </div>
                <div class="stat-item">
                    <span title="Time since the proxy process started.">Uptime:</span>
                    <span class="stat-value">${formatUptime(data.overall?.uptime_seconds || 0)}</span>
                </div>
                ${lastConsolidationDisplay}
            </div>
        </div>
    `;
}

function renderLogs(data) {
    if (!data || !data.entries || data.entries.length === 0) {
        const textFilter = document.getElementById('log-text-filter').value.trim();
        if (textFilter) {
            return '<div class="loading">No log entries match the filter "' + escapeHtml(textFilter) + '"</div>';
        }
        return '<div class="loading">No log entries available</div>';
    }
    
    const rows = data.entries.map(entry => `
        <tr>
            <td>${formatTimestamp(entry.timestamp)}</td>
            <td><span class="log-level ${entry.level}">${entry.level}</span></td>
            <td>${entry.target || ''}</td>
            <td>${escapeHtml(entry.message)}</td>
        </tr>
    `).join('');
    
    const textFilter = document.getElementById('log-text-filter').value.trim();
    const filterNote = textFilter ? ` (matching "${escapeHtml(textFilter)}")` : '';
    
    return `
        <div class="logs-table">
            <table>
                <thead>
                    <tr>
                        <th>Timestamp</th>
                        <th>Level</th>
                        <th>Target</th>
                        <th>Message</th>
                    </tr>
                </thead>
                <tbody>
                    ${rows}
                </tbody>
            </table>
        </div>
        <p style="margin-top: 1rem; color: #7f8c8d;">
            Showing ${data.entries.length} entries${filterNote}${data.has_more ? ' (more available)' : ''}
        </p>
    `;
}

function formatNumber(num) {
    return num.toLocaleString();
}

function formatUptime(seconds) {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    
    if (days > 0) {
        return `${days}d ${hours}h ${minutes}m`;
    } else if (hours > 0) {
        return `${hours}h ${minutes}m`;
    } else {
        return `${minutes}m`;
    }
}

function formatTimestamp(timestamp) {
    // Handle SystemTime object format from Rust (secs_since_epoch, nanos_since_epoch)
    if (timestamp && typeof timestamp === 'object' && timestamp.secs_since_epoch) {
        const date = new Date(timestamp.secs_since_epoch * 1000);
        return date.toLocaleString();
    }
    // Handle Unix timestamp in milliseconds
    const date = new Date(timestamp);
    return date.toLocaleString();
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Event listeners for log controls
document.getElementById('log-level-filter').addEventListener('change', loadLogs);
document.getElementById('log-limit').addEventListener('change', loadLogs);

// Text filter with debounce - fetches from server with filter applied
let textFilterTimeout;
document.getElementById('log-text-filter').addEventListener('input', function() {
    clearTimeout(textFilterTimeout);
    textFilterTimeout = setTimeout(loadLogs, 300);
});

// === Bucket Stats Table ===
let bucketStatsData = [];
let bucketSortCol = 'hits';
let bucketSortAsc = false;
let bucketShowAll = false;
const BUCKET_PAGE_SIZE = 20;

async function loadBucketStats() {
    try {
        const response = await fetch('/api/bucket-stats');
        if (!response.ok) return;
        const data = await response.json();
        const section = document.getElementById('bucket-stats-section');
        if (!data.buckets || data.buckets.length === 0) {
            section.style.display = 'none';
            return;
        }
        section.style.display = '';
        bucketStatsData = data.buckets;
        renderBucketStats();
    } catch (e) {
        console.error('Failed to load bucket stats:', e);
    }
}

function renderBucketStats() {
    const filter = (document.getElementById('bucket-filter').value || '').toLowerCase();
    let rows = bucketStatsData;
    if (filter) {
        rows = rows.filter(b => b.bucket.toLowerCase().includes(filter));
    }
    // Sort
    rows = rows.slice().sort((a, b) => {
        let va, vb;
        switch (bucketSortCol) {
            case 'bucket': va = a.bucket.toLowerCase(); vb = b.bucket.toLowerCase(); break;
            case 'hits': va = a.hit_count; vb = b.hit_count; break;
            case 'misses': va = a.miss_count; vb = b.miss_count; break;
            case 'hitRate': va = a.hit_rate; vb = b.hit_rate; break;
            case 'getTtl': va = a.settings.get_ttl; vb = b.settings.get_ttl; break;
            case 'readCache': va = a.settings.read_cache_enabled ? 1 : 0; vb = b.settings.read_cache_enabled ? 1 : 0; break;
            case 'writeCache': va = a.settings.write_cache_enabled ? 1 : 0; vb = b.settings.write_cache_enabled ? 1 : 0; break;
            case 'ramCache': va = a.settings.ram_cache_eligible ? 1 : 0; vb = b.settings.ram_cache_eligible ? 1 : 0; break;
            default: va = a.hit_count; vb = b.hit_count;
        }
        if (va < vb) return bucketSortAsc ? -1 : 1;
        if (va > vb) return bucketSortAsc ? 1 : -1;
        return 0;
    });

    const total = rows.length;
    const display = bucketShowAll ? rows : rows.slice(0, BUCKET_PAGE_SIZE);

    // Update toggle
    const toggle = document.getElementById('bucket-stats-toggle');
    if (total > BUCKET_PAGE_SIZE) {
        if (bucketShowAll) {
            toggle.innerHTML = '<a onclick="bucketShowAll=false;renderBucketStats();">Show top ' + BUCKET_PAGE_SIZE + '</a>';
        } else {
            toggle.innerHTML = '<a onclick="bucketShowAll=true;renderBucketStats();">Show all ' + total + ' buckets</a>';
        }
    } else {
        toggle.innerHTML = '';
    }

    // Update sort arrows
    document.querySelectorAll('#bucket-stats-table th.sortable').forEach(th => {
        const arrow = th.querySelector('.sort-arrow');
        if (th.dataset.sort === bucketSortCol) {
            arrow.textContent = bucketSortAsc ? '\u25B2' : '\u25BC';
        } else {
            arrow.textContent = '';
        }
    });

    // Render rows
    const tbody = document.getElementById('bucket-stats-body');
    tbody.innerHTML = '';
    display.forEach((b, idx) => {
        const tr = document.createElement('tr');
        tr.className = 'bucket-row';
        tr.innerHTML =
            '<td>' + escapeHtml(b.bucket) + '</td>' +
            '<td>' + formatNumber(b.hit_count) + '</td>' +
            '<td>' + formatNumber(b.miss_count) + '</td>' +
            '<td>' + (b.hit_rate * 100).toFixed(1) + '%</td>' +
            '<td>' + escapeHtml(b.settings.get_ttl) + '</td>' +
            '<td class="' + (b.settings.read_cache_enabled ? 'badge-on' : 'badge-off') + '">' + (b.settings.read_cache_enabled ? 'On' : 'Off') + '</td>' +
            '<td class="' + (b.settings.write_cache_enabled ? 'badge-on' : 'badge-off') + '">' + (b.settings.write_cache_enabled ? 'On' : 'Off') + '</td>' +
            '<td class="' + (b.settings.ram_cache_eligible ? 'badge-on' : 'badge-off') + '">' + (b.settings.ram_cache_eligible ? 'On' : 'Off') + '</td>';
        tr.addEventListener('click', function() { toggleBucketDetail(this, b); });
        tbody.appendChild(tr);
    });
}

function toggleBucketDetail(row, b) {
    const next = row.nextElementSibling;
    if (next && next.classList.contains('detail-row')) {
        next.remove();
        return;
    }
    // Remove any other open detail rows
    document.querySelectorAll('#bucket-stats-body tr.detail-row').forEach(r => r.remove());

    const detail = document.createElement('tr');
    detail.className = 'detail-row';
    const td = document.createElement('td');
    td.colSpan = 8;

    const s = b.settings;
    let html = '<div class="detail-content">';
    html += '<h4>Resolved Settings</h4>';
    html += '<div class="detail-grid">';
    html += detailItem('GET TTL', s.get_ttl);
    html += detailItem('HEAD TTL', s.head_ttl);
    html += detailItem('PUT TTL', s.put_ttl);
    html += detailItem('Read Cache', s.read_cache_enabled ? 'On' : 'Off');
    html += detailItem('Write Cache', s.write_cache_enabled ? 'On' : 'Off');
    html += detailItem('Compression', s.compression_enabled ? 'On' : 'Off');
    html += detailItem('RAM Cache', s.ram_cache_eligible ? 'On' : 'Off');
    html += detailItem('Prefix Overrides', String(s.prefix_override_count));
    html += '</div>';

    if (s.prefix_overrides && s.prefix_overrides.length > 0) {
        html += '<h4>Prefix Overrides</h4>';
        html += '<table class="prefix-table"><thead><tr>';
        html += '<th>Prefix</th><th>GET TTL</th><th>HEAD TTL</th><th>PUT TTL</th><th>Read</th><th>Write</th><th>Compression</th><th>RAM</th>';
        html += '</tr></thead><tbody>';
        s.prefix_overrides.forEach(po => {
            html += '<tr>';
            html += '<td>' + escapeHtml(po.prefix) + '</td>';
            html += '<td>' + (po.get_ttl != null ? escapeHtml(po.get_ttl) : '-') + '</td>';
            html += '<td>' + (po.head_ttl != null ? escapeHtml(po.head_ttl) : '-') + '</td>';
            html += '<td>' + (po.put_ttl != null ? escapeHtml(po.put_ttl) : '-') + '</td>';
            html += '<td>' + (po.read_cache_enabled != null ? (po.read_cache_enabled ? 'On' : 'Off') : '-') + '</td>';
            html += '<td>' + (po.write_cache_enabled != null ? (po.write_cache_enabled ? 'On' : 'Off') : '-') + '</td>';
            html += '<td>' + (po.compression_enabled != null ? (po.compression_enabled ? 'On' : 'Off') : '-') + '</td>';
            html += '<td>' + (po.ram_cache_eligible != null ? (po.ram_cache_eligible ? 'On' : 'Off') : '-') + '</td>';
            html += '</tr>';
        });
        html += '</tbody></table>';
    }
    html += '</div>';
    td.innerHTML = html;
    detail.appendChild(td);
    row.after(detail);
}

function detailItem(label, value) {
    return '<div class="detail-item"><span class="detail-label">' + label + '</span><span class="detail-value">' + escapeHtml(value) + '</span></div>';
}

// Sort handler
document.querySelectorAll('#bucket-stats-table th.sortable').forEach(th => {
    th.addEventListener('click', function() {
        const col = this.dataset.sort;
        if (bucketSortCol === col) {
            bucketSortAsc = !bucketSortAsc;
        } else {
            bucketSortCol = col;
            bucketSortAsc = col === 'bucket';
        }
        renderBucketStats();
    });
});

// Bucket filter with debounce
let bucketFilterTimeout;
document.getElementById('bucket-filter').addEventListener('input', function() {
    clearTimeout(bucketFilterTimeout);
    bucketFilterTimeout = setTimeout(renderBucketStats, 200);
});"#.to_string()
    }
}

/// API handler for JSON endpoints
pub struct ApiHandler {
    cache_manager: Arc<RwLock<Option<Arc<CacheManager>>>>,
    metrics_manager: Arc<RwLock<Option<Arc<RwLock<MetricsManager>>>>>,
    log_reader: Arc<LogReader>,
    // Cache for frequently accessed data to optimize response times
    cached_stats: Arc<RwLock<Option<(SystemTime, CacheStatsResponse)>>>,
    stats_cache_duration: std::time::Duration,
    /// Dashboard config for refresh intervals
    config: Arc<DashboardConfig>,
}

impl ApiHandler {
    pub fn new(log_reader: Arc<LogReader>, config: Arc<DashboardConfig>) -> Self {
        Self {
            cache_manager: Arc::new(RwLock::new(None)),
            metrics_manager: Arc::new(RwLock::new(None)),
            log_reader,
            cached_stats: Arc::new(RwLock::new(None)),
            stats_cache_duration: std::time::Duration::from_secs(2), // Cache stats for 2 seconds
            config,
        }
    }

    pub async fn set_cache_manager(&self, cache_manager: Arc<CacheManager>) {
        let mut cm = self.cache_manager.write().await;
        *cm = Some(cache_manager);
    }

    pub async fn set_metrics_manager(&self, metrics_manager: Arc<RwLock<MetricsManager>>) {
        let mut mm = self.metrics_manager.write().await;
        *mm = Some(metrics_manager);
    }

    pub async fn get_cache_stats(&self) -> Result<Response<String>> {
        debug!("Dashboard API: cache stats requested");

        // Check if we have a cached response that's still valid
        {
            let cached = self.cached_stats.read().await;
            if let Some((cached_time, cached_response)) = cached.as_ref() {
                if let Ok(elapsed) = SystemTime::now().duration_since(*cached_time) {
                    if elapsed < self.stats_cache_duration {
                        // Return cached response
                        let body = serde_json::to_string(cached_response).map_err(|e| {
                            error!("Failed to serialize cached cache stats: {}", e);
                            ProxyError::SerializationError(format!(
                                "Failed to serialize cached stats: {}",
                                e
                            ))
                        })?;

                        return Ok(Response::builder()
                            .status(StatusCode::OK)
                            .header("Content-Type", "application/json")
                            .header("Cache-Control", "no-cache")
                            .header("X-Cache", "HIT")
                            .body(body)
                            .map_err(|e| {
                                error!("Failed to build cached response: {}", e);
                                ProxyError::HttpError(format!(
                                    "Failed to build cached response: {}",
                                    e
                                ))
                            })?);
                    }
                }
            }
        }

        // Generate fresh stats
        let cache_manager_guard = match self.cache_manager.read().await {
            guard => guard,
        };
        let metrics_manager_guard = match self.metrics_manager.read().await {
            guard => guard,
        };

        let stats = if let Some(cache_manager) = cache_manager_guard.as_ref() {
            debug!("Cache manager is available, getting cache statistics");
            // Get cache statistics with updated sizes
            let cache_stats = cache_manager.get_cache_size_stats().await.map_err(|e| {
                error!("Failed to get cache size stats: {}", e);
                e
            })?;
            debug!(
                "Cache size stats: read_cache_size={}, write_cache_size={}, ram_cache_size={}",
                cache_stats.read_cache_size,
                cache_stats.write_cache_size,
                cache_stats.ram_cache_size
            );

            let ram_stats = cache_manager.get_ram_cache_stats();
            debug!("RAM cache stats available: {}", ram_stats.is_some());

            // Get metadata cache stats
            let metadata_cache = cache_manager.get_metadata_cache();
            let metadata_stats = metadata_cache.metrics();
            let metadata_entries = metadata_cache.len().await;
            let metadata_max_entries = metadata_cache.config().max_entries;

            // Calculate overall request count with overflow protection
            let total_requests = cache_stats
                .cache_hits
                .saturating_add(cache_stats.cache_misses);

            // Use RAM cache stats if available
            let (ram_hit_rate, ram_hits, ram_misses, ram_evictions, ram_max_size) =
                if let Some(ref ram_stats) = ram_stats {
                    (
                        ram_stats.hit_rate * 100.0,
                        ram_stats.hit_count,
                        ram_stats.miss_count,
                        ram_stats.eviction_count,
                        ram_stats.max_size,
                    )
                } else {
                    // Fallback to the simplified rate from cache_stats (already in percentage)
                    (cache_stats.ram_cache_hit_rate * 100.0, 0, 0, 0, 0)
                };

            // Calculate disk cache statistics
            // Disk hits = total GET hits minus RAM hits (requests served from disk, not RAM).
            // Disk misses = total GET misses (every overall miss is also a disk miss).
            // Note: ram_misses cannot be used for subtraction here because RAM misses
            // include both "miss RAM, hit disk" and "miss RAM, miss disk" cases,
            // making ram_misses >= get_misses and causing saturating_sub to yield 0.
            let disk_only_get_hits = cache_stats.get_hits.saturating_sub(ram_hits);
            let disk_only_get_misses = cache_stats.get_misses;

            // Get uptime from metrics manager if available
            let uptime_seconds = if let Some(metrics_manager) = metrics_manager_guard.as_ref() {
                debug!("Metrics manager is available, getting uptime");
                let fresh_metrics = metrics_manager.read().await.collect_metrics().await;
                debug!(
                    "Fresh metrics collected, uptime: {}",
                    fresh_metrics.uptime_seconds
                );

                // Use minimum of 1 second for uptime (0 is normal at startup)
                fresh_metrics.uptime_seconds.max(1)
            } else {
                warn!("Metrics manager not available in dashboard, this indicates a configuration issue");
                // Fallback: return 0 to indicate the issue
                0
            };

            // Calculate correct disk cache size (read cache only, write cache is separate)
            // Note: disk_cache now shows only read cache, write_cache shows write cache separately
            let disk_cache_size = cache_stats.read_cache_size;
            let max_cache_size = cache_stats.max_cache_size_limit;

            // Get write cache size and calculate max write cache size
            // Write cache max is write_cache_percent of total cache (default 10%)
            let write_cache_size = cache_stats.write_cache_size;
            let write_cache_max_size =
                (max_cache_size as f64 * cache_stats.max_write_cache_percent as f64 / 100.0) as u64;
            let write_cache_utilization = if write_cache_max_size > 0 {
                (write_cache_size as f64 / write_cache_max_size as f64 * 100.0) as f32
            } else {
                0.0
            };

            // Get last consolidation timestamp from consolidator if available
            let last_consolidation =
                if let Some(consolidator) = cache_manager.get_journal_consolidator().await {
                    let size_state = consolidator.get_size_state().await;
                    // Only include if consolidation has actually run (not UNIX_EPOCH)
                    if size_state.last_consolidation > std::time::UNIX_EPOCH {
                        Some(size_state.last_consolidation)
                    } else {
                        None
                    }
                } else {
                    None
                };

            CacheStatsResponse {
                timestamp: SystemTime::now(),
                hostname: gethostname::gethostname().to_string_lossy().to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                ram_cache: CacheStats {
                    hit_rate: ram_hit_rate,
                    hits: ram_hits,
                    misses: ram_misses,
                    size_bytes: cache_stats.ram_cache_size,
                    size_human: format_bytes(cache_stats.ram_cache_size),
                    max_size_bytes: if ram_max_size > 0 {
                        Some(ram_max_size)
                    } else {
                        None
                    },
                    max_size_human: if ram_max_size > 0 {
                        Some(format_bytes(ram_max_size))
                    } else {
                        None
                    },
                    evictions: ram_evictions,
                },
                metadata_cache: MetadataCacheStats {
                    hit_rate: if metadata_stats.hits + metadata_stats.misses > 0 {
                        (metadata_stats.hits as f64
                            / (metadata_stats.hits + metadata_stats.misses) as f64
                            * 100.0) as f32
                    } else {
                        0.0
                    },
                    hits: metadata_stats.hits,
                    misses: metadata_stats.misses,
                    entries: metadata_entries as u64,
                    max_entries: metadata_max_entries as u64,
                    evictions: metadata_stats.evictions,
                    stale_refreshes: metadata_stats.stale_refreshes,
                    stale_handle_errors: metadata_stats.stale_handle_errors,
                },
                disk_cache: CacheStats {
                    hit_rate: {
                        let disk_total = disk_only_get_hits.saturating_add(disk_only_get_misses);
                        if disk_total > 0 {
                            (disk_only_get_hits as f64 / disk_total as f64 * 100.0) as f32
                        } else {
                            0.0
                        }
                    },
                    hits: disk_only_get_hits,
                    misses: disk_only_get_misses,
                    size_bytes: disk_cache_size,
                    size_human: format_bytes(disk_cache_size),
                    max_size_bytes: if max_cache_size > 0 {
                        Some(max_cache_size)
                    } else {
                        None
                    },
                    max_size_human: if max_cache_size > 0 {
                        Some(format_bytes(max_cache_size))
                    } else {
                        None
                    },
                    evictions: cache_stats.evicted_entries,
                },
                write_cache: WriteCacheStats {
                    size_bytes: write_cache_size,
                    size_human: format_bytes(write_cache_size),
                    max_size_bytes: write_cache_max_size,
                    max_size_human: format_bytes(write_cache_max_size),
                    utilization_percent: write_cache_utilization,
                    evicted_uploads: cache_stats.incomplete_uploads_evicted,
                },
                overall: OverallStats {
                    total_requests,
                    uptime_seconds,
                    s3_transfer_saved_bytes: cache_stats.bytes_served_from_cache,
                    s3_transfer_saved_human: format_bytes(cache_stats.bytes_served_from_cache),
                },
                last_consolidation,
            }
        } else {
            // Return service unavailable if cache manager is not available
            warn!("Dashboard API: cache manager not available, returning service unavailable");
            return Ok(Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header("Content-Type", "application/json")
                .header("Cache-Control", "no-cache")
                .header("Retry-After", "30")
                .body(r#"{"error": "Cache manager not available", "code": 503, "message": "The cache manager is currently unavailable. Please try again later."}"#.to_string())
                .map_err(|e| {
                    error!("Failed to build service unavailable response: {}", e);
                    ProxyError::HttpError(format!("Failed to build error response: {}", e))
                })?);
        };

        // Cache the response with error handling
        match self.cached_stats.write().await {
            mut cached => {
                *cached = Some((SystemTime::now(), stats.clone()));
            }
        }

        let body = serde_json::to_string(&stats).map_err(|e| {
            error!("Failed to serialize cache stats: {}", e);
            ProxyError::SerializationError(format!("Failed to serialize cache stats: {}", e))
        })?;

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .header("Cache-Control", "no-cache")
            .header("X-Cache", "MISS")
            .body(body)
            .map_err(|e| {
                error!("Failed to build cache stats response: {}", e);
                ProxyError::HttpError(format!("Failed to build cache stats response: {}", e))
            })?)
    }

    pub async fn get_system_info(&self) -> Result<Response<String>> {
        debug!("Dashboard API: system info requested");

        let start_time = std::time::Instant::now();

        // Get uptime from metrics manager if available with error handling
        let uptime_seconds =
            if let Some(metrics_manager) = self.metrics_manager.read().await.as_ref() {
                match metrics_manager.read().await.get_cached_metrics().await {
                    Some(cached_metrics) => cached_metrics.uptime_seconds,
                    None => {
                        debug!("No cached metrics available for system info, using system time");
                        SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs()
                    }
                }
            } else {
                debug!("Metrics manager not available for system info, using system time");
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            };

        let system_info = SystemInfoResponse {
            timestamp: SystemTime::now(),
            hostname: gethostname::gethostname().to_string_lossy().to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds,
            status: "running".to_string(),
            cache_stats_refresh_ms: self.config.cache_stats_refresh_interval.as_millis() as u64,
            logs_refresh_ms: self.config.logs_refresh_interval.as_millis() as u64,
        };

        let body = serde_json::to_string(&system_info).map_err(|e| {
            error!("Failed to serialize system info: {}", e);
            ProxyError::SerializationError(format!("Failed to serialize system info: {}", e))
        })?;

        let elapsed = start_time.elapsed();
        debug!("System info API response generated in {:?}", elapsed);

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .header("Cache-Control", "no-cache")
            .header("X-Response-Time", &format!("{}ms", elapsed.as_millis()))
            .body(body)
            .map_err(|e| {
                error!("Failed to build system info response: {}", e);
                ProxyError::HttpError(format!("Failed to build system info response: {}", e))
            })?)
    }

    pub async fn get_logs(&self, params: LogQueryParams) -> Result<Response<String>> {
        debug!(
            "Dashboard API: logs requested with limit={:?}, level_filter={:?}",
            params.limit, params.level_filter
        );

        // Optimize log reading with early limits and efficient processing
        let start_time = std::time::Instant::now();

        // Read logs with comprehensive error handling
        let entries = match self.log_reader.read_recent_logs(params).await {
            Ok(entries) => entries,
            Err(e) => {
                error!("Failed to read log files: {}", e);
                // Return 500 with error details
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .header("Cache-Control", "no-cache")
                    .body(format!(
                        r#"{{"error": "Failed to read log files", "code": 500, "message": "{}"}}"#,
                        e.to_string().replace('"', "\\\"")
                    ))
                    .map_err(|e| {
                        error!("Failed to build error response: {}", e);
                        ProxyError::HttpError(format!("Failed to build error response: {}", e))
                    })?);
            }
        };

        let response = LogsResponse {
            entries: entries.clone(),
            total_count: entries.len(),
            has_more: false, // Simplified for now
        };

        let body = serde_json::to_string(&response).map_err(|e| {
            error!("Failed to serialize logs response: {}", e);
            ProxyError::SerializationError(format!("Failed to serialize logs: {}", e))
        })?;

        let elapsed = start_time.elapsed();
        debug!("Log API response generated in {:?}", elapsed);

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .header("Cache-Control", "no-cache")
            .header("X-Response-Time", &format!("{}ms", elapsed.as_millis()))
            .body(body)
            .map_err(|e| {
                error!("Failed to build logs response: {}", e);
                ProxyError::HttpError(format!("Failed to build logs response: {}", e))
            })?)
    }

    /// Return per-bucket cache hit/miss stats and resolved settings summary.
    pub async fn get_bucket_stats(&self) -> Result<Response<String>> {
        debug!("Dashboard API: bucket stats requested");

        let cache_manager_guard = self.cache_manager.read().await;
        let metrics_manager_guard = self.metrics_manager.read().await;

        let cache_manager = match cache_manager_guard.as_ref() {
            Some(cm) => cm,
            None => {
                return Ok(Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header("Content-Type", "application/json")
                    .header("Cache-Control", "no-cache")
                    .body(r#"{"error": "Cache manager not available", "code": 503}"#.to_string())
                    .map_err(|e| ProxyError::HttpError(format!("Failed to build response: {}", e)))?);
            }
        };

        let bsm = cache_manager.get_bucket_settings_manager();
        let bucket_names = bsm.buckets_with_settings().await;

        // Get per-bucket cache stats from metrics manager
        let bucket_cache_stats = if let Some(mm) = metrics_manager_guard.as_ref() {
            mm.read().await.get_bucket_cache_stats().await
        } else {
            std::collections::HashMap::new()
        };

        let mut entries = Vec::with_capacity(bucket_names.len());
        for bucket in &bucket_names {
            // Resolve bucket-level settings (empty path = bucket level, no prefix match)
            let resolved = bsm.resolve(bucket, "").await;
            let prefix_overrides_raw = bsm.get_prefix_overrides(bucket).await;

            let prefix_overrides: Vec<PrefixOverrideSummary> = prefix_overrides_raw
                .iter()
                .map(|po| PrefixOverrideSummary {
                    prefix: po.prefix.clone(),
                    get_ttl: po.get_ttl.map(crate::bucket_settings::format_duration),
                    head_ttl: po.head_ttl.map(crate::bucket_settings::format_duration),
                    put_ttl: po.put_ttl.map(crate::bucket_settings::format_duration),
                    read_cache_enabled: po.read_cache_enabled,
                    write_cache_enabled: po.write_cache_enabled,
                    compression_enabled: po.compression_enabled,
                    ram_cache_eligible: po.ram_cache_eligible,
                })
                .collect();
            let prefix_count = prefix_overrides.len();

            let stats = bucket_cache_stats.get(bucket.as_str());
            let hit_count = stats.map_or(0, |s| s.hit_count);
            let miss_count = stats.map_or(0, |s| s.miss_count);
            let total = hit_count + miss_count;
            let hit_rate = if total > 0 {
                hit_count as f64 / total as f64
            } else {
                0.0
            };

            entries.push(BucketStatsEntry {
                bucket: bucket.clone(),
                hit_count,
                miss_count,
                hit_rate,
                settings: ResolvedSettingsSummary {
                    get_ttl: crate::bucket_settings::format_duration(resolved.get_ttl),
                    head_ttl: crate::bucket_settings::format_duration(resolved.head_ttl),
                    put_ttl: crate::bucket_settings::format_duration(resolved.put_ttl),
                    read_cache_enabled: resolved.read_cache_enabled,
                    write_cache_enabled: resolved.write_cache_enabled,
                    compression_enabled: resolved.compression_enabled,
                    ram_cache_eligible: resolved.ram_cache_eligible,
                    prefix_override_count: prefix_count,
                    prefix_overrides,
                },
            });
        }

        let response = BucketStatsResponse { buckets: entries };
        let body = serde_json::to_string(&response).map_err(|e| {
            error!("Failed to serialize bucket stats: {}", e);
            ProxyError::SerializationError(format!("Failed to serialize bucket stats: {}", e))
        })?;

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .header("Cache-Control", "no-cache")
            .body(body)
            .map_err(|e| {
                error!("Failed to build bucket stats response: {}", e);
                ProxyError::HttpError(format!("Failed to build bucket stats response: {}", e))
            })?)
    }
}

/// Log reader for application logs
pub struct LogReader {
    app_log_dir: PathBuf,
    hostname: String,
}

impl LogReader {
    pub fn new(app_log_dir: PathBuf, hostname: String) -> Self {
        Self {
            app_log_dir,
            hostname,
        }
    }

    pub async fn read_recent_logs(&self, params: LogQueryParams) -> Result<Vec<LogEntry>> {
        let limit = params.limit.unwrap_or(100);
        debug!(
            "Reading logs with params: limit={}, level_filter={:?}",
            limit, params.level_filter
        );

        let mut entries = Vec::new();

        // Get the host-specific log directory
        let host_log_dir = self.app_log_dir.join(&self.hostname);

        if !host_log_dir.exists() {
            debug!("Log directory does not exist: {:?}", host_log_dir);
            return Ok(entries);
        }

        // Check if directory is readable
        if let Err(e) = tokio::fs::metadata(&host_log_dir).await {
            warn!("Cannot access log directory {:?}: {}", host_log_dir, e);
            return Err(ProxyError::IoError(format!(
                "Cannot access log directory {:?}: {}",
                host_log_dir, e
            )));
        }

        // Find the most recent log files with error handling
        let log_files = match self.find_recent_log_files(&host_log_dir) {
            Ok(files) => files,
            Err(e) => {
                error!("Failed to find log files in {:?}: {}", host_log_dir, e);
                return Err(e);
            }
        };

        if log_files.is_empty() {
            debug!("No log files found in {:?}", host_log_dir);
            return Ok(entries);
        }

        for log_file in log_files {
            if entries.len() >= limit {
                break;
            }

            match self
                .read_log_file(&log_file, &params, limit - entries.len())
                .await
            {
                Ok(file_entries) => {
                    entries.extend(file_entries);
                }
                Err(e) => {
                    // Log the error but continue with other files
                    warn!("Failed to read log file {:?}: {}", log_file, e);
                    continue;
                }
            }
        }

        // Sort by timestamp (most recent first) and limit
        entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        entries.truncate(limit);

        Ok(entries)
    }

    /// Find recent log files, sorted by modification time (newest first)
    fn find_recent_log_files(&self, log_dir: &PathBuf) -> Result<Vec<PathBuf>> {
        let mut log_files = Vec::new();

        let entries = std::fs::read_dir(log_dir).map_err(|e| {
            ProxyError::IoError(format!("Failed to read log directory {:?}: {}", log_dir, e))
        })?;

        for entry in entries {
            match entry {
                Ok(entry) => {
                    let path = entry.path();

                    // Check if it's a file and has the right extension/name
                    if let Ok(metadata) = path.metadata() {
                        if metadata.is_file() {
                            let is_log_file = path.extension().map_or(false, |ext| ext == "log")
                                || path.file_name().map_or(false, |name| {
                                    name.to_string_lossy().contains("s3-proxy.log")
                                });

                            if is_log_file {
                                log_files.push(path);
                            }
                        }
                    } else {
                        // Skip files we can't read metadata for
                        debug!("Skipping file with unreadable metadata: {:?}", path);
                    }
                }
                Err(e) => {
                    // Log but continue with other entries
                    debug!("Skipping directory entry due to error: {}", e);
                }
            }
        }

        // Sort by modification time (newest first) with error handling
        log_files.sort_by(|a, b| {
            let a_time = a
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::UNIX_EPOCH);
            let b_time = b
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::UNIX_EPOCH);
            b_time.cmp(&a_time)
        });

        Ok(log_files)
    }

    /// Read log entries from a single log file
    async fn read_log_file(
        &self,
        file_path: &PathBuf,
        params: &LogQueryParams,
        max_entries: usize,
    ) -> Result<Vec<LogEntry>> {
        let mut entries = Vec::new();

        // For performance, limit the amount of file we read
        let max_file_size = 1024 * 1024; // 1MB max per file
        let metadata = tokio::fs::metadata(file_path).await.map_err(|e| {
            ProxyError::IoError(format!("Failed to get metadata for {:?}: {}", file_path, e))
        })?;

        let file_size = metadata.len() as usize;
        let read_size = std::cmp::min(file_size, max_file_size);

        // Read from the end of the file for recent entries
        let mut file = tokio::fs::File::open(file_path).await.map_err(|e| {
            ProxyError::IoError(format!("Failed to open log file {:?}: {}", file_path, e))
        })?;

        let start_pos = if file_size > read_size {
            file_size - read_size
        } else {
            0
        };

        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        file.seek(std::io::SeekFrom::Start(start_pos as u64))
            .await
            .map_err(|e| {
                ProxyError::IoError(format!("Failed to seek in log file {:?}: {}", file_path, e))
            })?;

        let mut buffer = vec![0u8; read_size];
        let bytes_read = file.read(&mut buffer).await.map_err(|e| {
            ProxyError::IoError(format!("Failed to read log file {:?}: {}", file_path, e))
        })?;

        let content = String::from_utf8_lossy(&buffer[..bytes_read]);

        // Process lines in reverse order to get most recent entries first
        let lines: Vec<&str> = content.lines().collect();
        for line in lines.iter().rev() {
            if entries.len() >= max_entries {
                break;
            }

            if let Some(entry) = self.parse_log_line(line) {
                // Apply level filter if specified
                if let Some(ref level_filter) = params.level_filter {
                    if entry.level != *level_filter {
                        continue;
                    }
                }

                // Apply time filter if specified
                if let Some(since) = params.since {
                    if entry.timestamp < since {
                        continue;
                    }
                }

                // Apply text filter if specified (case-insensitive search in message and target)
                if let Some(ref text_filter) = params.text_filter {
                    let text_lower = text_filter.to_lowercase();
                    let message_lower = entry.message.to_lowercase();
                    let target_lower = entry.target.to_lowercase();
                    if !message_lower.contains(&text_lower) && !target_lower.contains(&text_lower) {
                        continue;
                    }
                }

                entries.push(entry);
            }
        }

        Ok(entries)
    }

    /// Parse a single log line in tracing format
    fn parse_log_line(&self, line: &str) -> Option<LogEntry> {
        // Tracing format has inconsistent spacing:
        // ERROR: 2026-01-23T09:05:11.370707948+00:00 ERROR ThreadId(07) ... (single space)
        // INFO/WARN/DEBUG: 2026-01-23T15:24:13.161401787+00:00  INFO ThreadId(05) ... (double space)

        if line.trim().is_empty() {
            return None;
        }

        // Find the timestamp end (first space after timestamp)
        let timestamp_end = line.find(' ')?;
        let timestamp_str = &line[..timestamp_end];

        // Parse timestamp
        let timestamp = match chrono::DateTime::parse_from_rfc3339(timestamp_str) {
            Ok(dt) => dt.with_timezone(&chrono::Utc),
            Err(_) => {
                // Try parsing without timezone
                match chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.f") {
                    Ok(ndt) => {
                        chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc)
                    }
                    Err(_) => return None,
                }
            }
        };

        // Get the rest after timestamp, trimming any leading spaces (handles both single and double space)
        let rest = line[timestamp_end..].trim_start();

        // Split on spaces to get level and remaining parts
        let rest_parts: Vec<&str> = rest.splitn(3, ' ').collect();
        if rest_parts.len() < 3 {
            return None;
        }

        // Parse level
        let level = rest_parts[0].to_string();

        // Skip ThreadId part and get target and message
        let after_thread = rest_parts[2]; // This should be "s3_proxy::cache_size_tracker: src/cache_size_tracker.rs:1250: Cache validation: scanning 5 metadata files using shared validator"

        // Split on the first colon to separate target from location:message
        let (target, location_and_message) = if let Some(colon_pos) = after_thread.find(": ") {
            let target_part = after_thread[..colon_pos].trim();
            let rest = after_thread[colon_pos + 2..].trim(); // Skip ": "
            (target_part.to_string(), rest)
        } else {
            ("unknown".to_string(), after_thread)
        };

        // Split location from message (location format: src/file.rs:line:)
        // Look for the pattern ": " after a file location to separate location from message
        let message = if let Some(location_end) = location_and_message.find(": ") {
            // Check if this looks like a file location (contains .rs: followed by number)
            let before_colon = &location_and_message[..location_end];
            if before_colon.contains(".rs:")
                && before_colon
                    .chars()
                    .last()
                    .map_or(false, |c| c.is_ascii_digit())
            {
                location_and_message[location_end + 2..].to_string()
            } else {
                location_and_message.to_string()
            }
        } else {
            location_and_message.to_string()
        };

        // Extract structured fields from the message (simplified)
        let mut fields = HashMap::new();

        // Look for key=value patterns in the message
        if let Ok(key_value_regex) = regex::Regex::new(r"(\w+)=([^\s,]+)") {
            for cap in key_value_regex.captures_iter(&message) {
                if let (Some(key), Some(value)) = (cap.get(1), cap.get(2)) {
                    fields.insert(key.as_str().to_string(), value.as_str().to_string());
                }
            }
        }

        Some(LogEntry {
            timestamp,
            level,
            target,
            message,
            fields,
        })
    }
}

/// Cache statistics response structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStatsResponse {
    pub timestamp: SystemTime,
    pub hostname: String,
    pub version: String,
    pub ram_cache: CacheStats,
    pub metadata_cache: MetadataCacheStats,
    pub disk_cache: CacheStats,       // Read cache (object cache)
    pub write_cache: WriteCacheStats, // Write cache (mpus_in_progress)
    pub overall: OverallStats,
    /// Timestamp of last consolidation cycle
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_consolidation: Option<SystemTime>,
}

/// Write cache statistics (MPUs in progress and PUT objects not yet read)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteCacheStats {
    pub size_bytes: u64,
    pub size_human: String,
    pub max_size_bytes: u64, // Write cache limit (% of total cache)
    pub max_size_human: String,
    pub utilization_percent: f32,
    pub evicted_uploads: u64, // Number of incomplete uploads evicted
}

/// Metadata cache statistics (RAM cache for NewCacheMetadata objects)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataCacheStats {
    pub hit_rate: f32,
    pub hits: u64,
    pub misses: u64,
    pub entries: u64,
    pub max_entries: u64,
    pub evictions: u64,
    pub stale_refreshes: u64,
    pub stale_handle_errors: u64,
}

/// Individual cache statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    pub hit_rate: f32,
    pub hits: u64,
    pub misses: u64,
    pub size_bytes: u64,
    pub size_human: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_size_human: Option<String>,
    pub evictions: u64,
}

/// Overall system statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverallStats {
    pub total_requests: u64,
    pub uptime_seconds: u64,
    pub s3_transfer_saved_bytes: u64,
    pub s3_transfer_saved_human: String,
}

/// System information response structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfoResponse {
    pub timestamp: SystemTime,
    pub hostname: String,
    pub version: String,
    pub uptime_seconds: u64,
    pub status: String,
    /// Cache stats refresh interval in milliseconds (from config)
    pub cache_stats_refresh_ms: u64,
    /// Logs refresh interval in milliseconds (from config)
    pub logs_refresh_ms: u64,
}

/// Log entry structure
#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub target: String,
    pub message: String,
    pub fields: HashMap<String, String>,
}

/// Log query parameters
#[derive(Debug)]
pub struct LogQueryParams {
    pub limit: Option<usize>,
    pub level_filter: Option<String>,
    pub text_filter: Option<String>,
    pub since: Option<DateTime<Utc>>,
}

/// Logs response structure
#[derive(Debug, Serialize)]
pub struct LogsResponse {
    pub entries: Vec<LogEntry>,
    pub total_count: usize,
    pub has_more: bool,
}

/// Per-bucket cache stats response for `/api/bucket-stats`
#[derive(Debug, Serialize)]
pub struct BucketStatsResponse {
    pub buckets: Vec<BucketStatsEntry>,
}

/// Per-bucket cache stats entry
#[derive(Debug, Serialize)]
pub struct BucketStatsEntry {
    pub bucket: String,
    pub hit_count: u64,
    pub miss_count: u64,
    pub hit_rate: f64,
    pub settings: ResolvedSettingsSummary,
}

/// Summary of resolved settings for a bucket (bucket-level, no prefix)
#[derive(Debug, Serialize)]
pub struct ResolvedSettingsSummary {
    pub get_ttl: String,
    pub head_ttl: String,
    pub put_ttl: String,
    pub read_cache_enabled: bool,
    pub write_cache_enabled: bool,
    pub compression_enabled: bool,
    pub ram_cache_eligible: bool,
    pub prefix_override_count: usize,
    pub prefix_overrides: Vec<PrefixOverrideSummary>,
}

/// Summary of a prefix override for the dashboard
#[derive(Debug, Serialize)]
pub struct PrefixOverrideSummary {
    pub prefix: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_ttl: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_ttl: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub put_ttl: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_cache_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_cache_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ram_cache_eligible: Option<bool>,
}

/// Validate log query parameters
pub fn validate_log_params(params: &LogQueryParams) -> Option<String> {
    // Check limit bounds
    if let Some(limit) = params.limit {
        if limit == 0 {
            return Some("Limit must be greater than 0".to_string());
        }
        if limit > 10000 {
            return Some("Limit cannot exceed 10000".to_string());
        }
    }

    // Check log level validity
    if let Some(ref level) = params.level_filter {
        match level.as_str() {
            "ERROR" | "WARN" | "INFO" | "DEBUG" | "TRACE" => {
                // Valid log level
            }
            _ => {
                return Some(format!(
                    "Invalid log level '{}'. Must be one of: ERROR, WARN, INFO, DEBUG, TRACE",
                    level
                ));
            }
        }
    }

    // Check timestamp validity (when implemented)
    if params.since.is_some() {
        return Some("Since parameter is not yet implemented".to_string());
    }

    None // No validation errors
}

/// Parse and validate log query parameters from URI
pub fn parse_log_query_params(uri: &Uri) -> LogQueryParams {
    let mut limit = None;
    let mut level_filter = None;
    let mut text_filter = None;
    let since = None; // Not implemented yet

    if let Some(query) = uri.query() {
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                // URL decode the value
                let decoded_value = urlencoding::decode(value).unwrap_or_else(|_| {
                    warn!("Failed to URL decode parameter value: {}", value);
                    std::borrow::Cow::Borrowed(value)
                });

                match key {
                    "limit" => {
                        match decoded_value.parse::<usize>() {
                            Ok(l) => {
                                // Validate limit bounds (1 to 10000)
                                if l >= 1 && l <= 10000 {
                                    limit = Some(l);
                                } else {
                                    warn!("Invalid limit parameter: {} (must be 1-10000)", l);
                                }
                            }
                            Err(_) => {
                                warn!("Invalid limit parameter format: {}", decoded_value);
                            }
                        }
                    }
                    "level" => {
                        let level_str = decoded_value.trim().to_uppercase();
                        // Validate log level and sanitize input
                        if !level_str.is_empty() {
                            // Additional sanitization: only allow alphanumeric characters
                            if level_str.chars().all(|c| c.is_ascii_alphanumeric()) {
                                match level_str.as_str() {
                                    "ERROR" | "WARN" | "INFO" | "DEBUG" | "TRACE" => {
                                        level_filter = Some(level_str);
                                    }
                                    _ => {
                                        warn!("Invalid log level parameter: {} (must be ERROR, WARN, INFO, DEBUG, or TRACE)", level_str);
                                    }
                                }
                            } else {
                                warn!(
                                    "Log level parameter contains invalid characters: {}",
                                    level_str
                                );
                            }
                        }
                    }
                    "text" => {
                        let text_str = decoded_value.trim().to_string();
                        if !text_str.is_empty() && text_str.len() <= 200 {
                            text_filter = Some(text_str);
                        } else if text_str.len() > 200 {
                            warn!("Text filter too long: {} chars (max 200)", text_str.len());
                        }
                    }
                    "since" => {
                        // Since parameter reserved for future use
                        debug!("Since parameter not yet implemented: {}", decoded_value);
                    }
                    _ => {
                        // Log unknown parameters but don't fail
                        debug!("Unknown query parameter: {}={}", key, decoded_value);
                    }
                }
            } else {
                // Handle malformed parameter pairs
                warn!("Malformed query parameter: {}", pair);
            }
        }
    }

    LogQueryParams {
        limit,
        level_filter,
        text_filter,
        since,
    }
}

/// Format bytes into human-readable format using binary units (IEC standard)
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    const THRESHOLD: f64 = 1024.0;

    if bytes == 0 {
        return "0 B".to_string();
    }

    let bytes_f = bytes as f64;
    let unit_index = (bytes_f.log10() / THRESHOLD.log10()).floor() as usize;
    let unit_index = unit_index.min(UNITS.len() - 1);

    let value = bytes_f / THRESHOLD.powi(unit_index as i32);

    if value >= 100.0 {
        format!("{:.0} {}", value, UNITS[unit_index])
    } else if value >= 10.0 {
        format!("{:.1} {}", value, UNITS[unit_index])
    } else {
        format!("{:.2} {}", value, UNITS[unit_index])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(1536), "1.50 KiB");
        assert_eq!(format_bytes(1048576), "1.00 MiB");
        assert_eq!(format_bytes(1073741824), "1.00 GiB");
    }

    #[test]
    fn test_parse_log_query_params() {
        let uri: Uri = "/api/logs?limit=50&level=ERROR".parse().unwrap();
        let params = parse_log_query_params(&uri);

        assert_eq!(params.limit, Some(50));
        assert_eq!(params.level_filter, Some("ERROR".to_string()));
    }
}
