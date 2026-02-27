//! Logging Module
//!
//! Provides comprehensive logging for both S3-compatible access logs and
//! application logs with proper formatting, partitioning, and host identification.

use crate::{ProxyError, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Access log entry in S3-compatible format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessLogEntry {
    pub bucket_owner: String,
    pub bucket: String,
    pub time: DateTime<Utc>,
    pub remote_ip: String,
    pub requester: String,
    pub request_id: String,
    pub operation: String,
    pub key: String,
    pub request_uri: String,
    pub http_status: u16,
    pub error_code: Option<String>,
    pub bytes_sent: u64,
    pub object_size: Option<u64>,
    pub total_time: u64,
    pub turn_around_time: u64,
    pub referer: Option<String>,
    pub user_agent: Option<String>,
    pub host_id: String,
    pub signature_version: Option<String>,
    pub cipher_suite: Option<String>,
    pub authentication_type: Option<String>,
    pub host_header: Option<String>,
    pub tls_version: Option<String>,
    pub access_point_arn: Option<String>,
    pub acl_required: Option<String>,
    pub source_region: Option<String>,
}

/// Buffered access log writer that batches log entries in RAM before flushing to disk.
///
/// This reduces disk I/O on shared filesystems (NFS/EFS) by buffering entries and
/// flushing periodically (every 5 seconds) or when the buffer reaches max capacity.
pub struct AccessLogBuffer {
    /// Buffer holding pending log entries
    buffer: Arc<Mutex<Vec<AccessLogEntry>>>,
    /// Directory for access log files
    log_dir: PathBuf,
    /// Hostname for log file naming
    hostname: String,
    /// Interval between automatic flushes
    flush_interval: Duration,
    /// Maximum entries before forced flush
    max_buffer_size: usize,
    /// Timestamp of last flush
    last_flush: Arc<RwLock<Instant>>,
    /// Flag to prevent concurrent flushes
    flush_in_progress: AtomicBool,
}

/// Result of flushing the access log buffer to disk
#[derive(Debug, Clone)]
pub struct AccessLogFlushResult {
    /// Number of entries that were flushed
    pub entries_flushed: usize,
    /// Whether the flush was skipped (no pending entries)
    pub skipped: bool,
    /// Whether the flush was skipped because another flush is in progress
    pub already_in_progress: bool,
}

impl Default for AccessLogFlushResult {
    fn default() -> Self {
        Self {
            entries_flushed: 0,
            skipped: false,
            already_in_progress: false,
        }
    }
}

/// Default flush interval for access log buffer (5 seconds)
fn default_access_log_flush_interval() -> Duration {
    Duration::from_secs(5)
}

/// Default maximum buffer size for access log buffer (1000 entries)
fn default_access_log_buffer_size() -> usize {
    1000
}

/// RAII guard to ensure flush_in_progress is cleared
struct AccessLogFlushGuard<'a> {
    flag: &'a AtomicBool,
}

impl<'a> Drop for AccessLogFlushGuard<'a> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}

impl AccessLogBuffer {
    /// Create a new AccessLogBuffer
    ///
    /// # Arguments
    /// * `log_dir` - Directory for access log files
    /// * `hostname` - Hostname for log file naming
    /// * `flush_interval` - Interval between automatic flushes (default: 5s)
    /// * `max_buffer_size` - Maximum entries before forced flush (default: 1000)
    pub fn new(
        log_dir: PathBuf,
        hostname: String,
        flush_interval: Option<Duration>,
        max_buffer_size: Option<usize>,
    ) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
            log_dir,
            hostname,
            flush_interval: flush_interval.unwrap_or_else(default_access_log_flush_interval),
            max_buffer_size: max_buffer_size.unwrap_or_else(default_access_log_buffer_size),
            last_flush: Arc::new(RwLock::new(Instant::now())),
            flush_in_progress: AtomicBool::new(false),
        }
    }

    /// Add entry to buffer (non-blocking)
    ///
    /// This method adds an entry to the buffer and checks if a flush is needed.
    /// The flush check is non-blocking - if a flush is already in progress,
    /// the entry is simply added to the buffer.
    ///
    /// # Arguments
    /// * `entry` - The access log entry to add
    pub async fn log(&self, entry: AccessLogEntry) -> Result<()> {
        // Add entry to buffer
        {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.push(entry);
        }

        // Check if flush is needed
        self.maybe_flush().await?;

        Ok(())
    }

    /// Check if flush needed and perform if so
    ///
    /// Flushes the buffer if:
    /// - The flush interval has elapsed, OR
    /// - The buffer has reached max capacity
    async fn maybe_flush(&self) -> Result<()> {
        let should_flush = {
            // Check buffer size
            let buffer_len = self.buffer.lock().unwrap().len();
            if buffer_len >= self.max_buffer_size {
                true
            } else {
                // Check time since last flush
                if let Ok(last_flush) = self.last_flush.try_read() {
                    last_flush.elapsed() >= self.flush_interval
                } else {
                    false
                }
            }
        };

        if should_flush {
            // Attempt flush - ignore errors here, they'll be logged
            let _ = self.flush().await;
        }

        Ok(())
    }

    /// Flush buffer to disk
    ///
    /// This method writes all buffered entries to the appropriate date-partitioned
    /// log file. It uses atomic operations to prevent concurrent flushes.
    ///
    /// # Returns
    /// * `Ok(AccessLogFlushResult)` - Information about what was flushed
    /// * `Err` - If the flush failed (entries are retained for retry)
    pub async fn flush(&self) -> Result<AccessLogFlushResult> {
        // Check if another flush is already in progress
        if self
            .flush_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            debug!("Access log flush already in progress, skipping");
            return Ok(AccessLogFlushResult {
                already_in_progress: true,
                ..Default::default()
            });
        }

        // Ensure we clear the flush_in_progress flag when done
        let _guard = AccessLogFlushGuard {
            flag: &self.flush_in_progress,
        };

        // Take all entries from the buffer
        let entries: Vec<AccessLogEntry> = {
            let mut buffer = self.buffer.lock().unwrap();
            std::mem::take(&mut *buffer)
        };

        // If no entries to flush, just update the timestamp and return
        if entries.is_empty() {
            let mut last_flush = self.last_flush.write().await;
            *last_flush = Instant::now();
            return Ok(AccessLogFlushResult {
                skipped: true,
                ..Default::default()
            });
        }

        let entries_count = entries.len();

        // Group entries by date for proper partitioning
        let mut entries_by_date: std::collections::HashMap<String, Vec<&AccessLogEntry>> =
            std::collections::HashMap::new();

        for entry in &entries {
            let date_key = entry.time.format("%Y/%m/%d").to_string();
            entries_by_date.entry(date_key).or_default().push(entry);
        }

        // Write entries to files
        for (date_path, date_entries) in entries_by_date {
            if let Err(e) = self.write_entries_to_file(&date_path, &date_entries).await {
                // On failure, restore entries to buffer for retry
                warn!("Failed to write access log entries: {}", e);
                let mut buffer = self.buffer.lock().unwrap();
                for entry in entries {
                    buffer.push(entry);
                }
                return Err(e);
            }
        }

        // Update the last flush timestamp
        let mut last_flush = self.last_flush.write().await;
        *last_flush = Instant::now();

        debug!("Flushed {} access log entries to disk", entries_count);

        Ok(AccessLogFlushResult {
            entries_flushed: entries_count,
            skipped: false,
            already_in_progress: false,
        })
    }

    /// Write entries to a date-partitioned file
    async fn write_entries_to_file(
        &self,
        date_path: &str,
        entries: &[&AccessLogEntry],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Create date-partitioned directory: /logs/access/YYYY/MM/DD/
        let log_dir = self.log_dir.join(date_path);
        tokio::fs::create_dir_all(&log_dir).await.map_err(|e| {
            ProxyError::IoError(format!("Failed to create access log directory: {}", e))
        })?;

        // Create filename with timestamp and hostname
        let timestamp = entries[0].time;
        let filename = format!(
            "{}-{}",
            timestamp.format("%Y-%m-%d-%H-%M-%S"),
            self.hostname
        );
        let log_file_path = log_dir.join(&filename);

        // Format all entries
        let mut content = String::new();
        for entry in entries {
            content.push_str(&Self::format_entry(entry));
            content.push('\n');
        }

        // Write using atomic temp+rename for durability
        let temp_path = log_file_path.with_extension("tmp");

        // If the target file exists, append to it instead of replacing
        if log_file_path.exists() {
            // Append to existing file
            use tokio::io::AsyncWriteExt;
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_file_path)
                .await
                .map_err(|e| {
                    ProxyError::IoError(format!("Failed to open access log file: {}", e))
                })?;

            file.write_all(content.as_bytes())
                .await
                .map_err(|e| ProxyError::IoError(format!("Failed to write access log: {}", e)))?;

            file.flush()
                .await
                .map_err(|e| ProxyError::IoError(format!("Failed to flush access log: {}", e)))?;
        } else {
            // Write to temp file and rename for atomicity
            tokio::fs::write(&temp_path, content.as_bytes())
                .await
                .map_err(|e| {
                    ProxyError::IoError(format!("Failed to write temp access log: {}", e))
                })?;

            tokio::fs::rename(&temp_path, &log_file_path)
                .await
                .map_err(|e| ProxyError::IoError(format!("Failed to rename access log: {}", e)))?;
        }

        debug!("Wrote {} entries to {:?}", entries.len(), log_file_path);
        Ok(())
    }

    /// Force flush (for shutdown)
    ///
    /// This method is similar to `flush()` but is intended for use during
    /// graceful shutdown. It will wait briefly for any in-progress flush to
    /// complete before performing its own flush.
    ///
    /// # Returns
    /// * `Ok(AccessLogFlushResult)` - Information about what was flushed
    /// * `Err` - If the flush failed
    pub async fn force_flush(&self) -> Result<AccessLogFlushResult> {
        // Wait briefly for any in-progress flush to complete
        for _ in 0..10 {
            if !self.flush_in_progress.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Now perform the flush
        self.flush().await
    }

    /// Format access log entry in S3-compatible format
    ///
    /// This is the same format as the existing `AccessLogWriter::format_entry()`
    fn format_entry(entry: &AccessLogEntry) -> String {
        // S3 Server Access Log format (version_id field removed)
        format!(
            "{} {} [{}] {} {} {} {} \"{}\" {} {} {} {} {} {} \"{}\" \"{}\" {} {} {} {} {} {} {} {} {}",
            entry.bucket_owner,
            entry.bucket,
            entry.time.format("%d/%b/%Y:%H:%M:%S %z"),
            entry.remote_ip,
            entry.requester,
            entry.request_id,
            entry.operation,
            entry.request_uri,
            entry.http_status,
            entry.error_code.as_deref().unwrap_or("-"),
            entry.bytes_sent,
            entry.object_size.map_or("-".to_string(), |s| s.to_string()),
            entry.total_time,
            entry.turn_around_time,
            entry.referer.as_deref().unwrap_or("-"),
            entry.user_agent.as_deref().unwrap_or("-"),
            entry.host_id,
            entry.signature_version.as_deref().unwrap_or("-"),
            entry.cipher_suite.as_deref().unwrap_or("-"),
            entry.authentication_type.as_deref().unwrap_or("-"),
            entry.host_header.as_deref().unwrap_or("-"),
            entry.tls_version.as_deref().unwrap_or("-"),
            entry.access_point_arn.as_deref().unwrap_or("-"),
            entry.acl_required.as_deref().unwrap_or("-"),
            entry.source_region.as_deref().unwrap_or("-"),
        )
    }

    /// Get the number of entries currently in the buffer
    ///
    /// This is useful for reporting entries lost during shutdown if flush fails.
    pub fn pending_entries_count(&self) -> usize {
        self.buffer.lock().unwrap().len()
    }

    /// Get the number of entries currently in the buffer (for testing)
    #[cfg(test)]
    pub fn buffer_len(&self) -> usize {
        self.pending_entries_count()
    }

    /// Check if a flush is needed based on the flush interval (for testing)
    #[cfg(test)]
    pub fn should_flush(&self) -> bool {
        let buffer_len = self.buffer.lock().unwrap().len();
        if buffer_len >= self.max_buffer_size {
            return true;
        }
        if let Ok(last_flush) = self.last_flush.try_read() {
            last_flush.elapsed() >= self.flush_interval
        } else {
            false
        }
    }
}

/// Logging configuration
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    pub access_log_dir: PathBuf,
    pub app_log_dir: PathBuf,
    pub access_log_enabled: bool,
    pub access_log_mode: AccessLogMode,
    pub hostname: String,
    pub log_level: String,
    /// Flush interval for access log buffer (default: 5s)
    pub access_log_flush_interval: Duration,
    /// Maximum entries in access log buffer before forced flush (default: 1000)
    pub access_log_buffer_size: usize,
}

/// Access logging mode
#[derive(Debug, Clone, PartialEq, Default)]
pub enum AccessLogMode {
    #[default]
    All, // Log all requests
    CachedOnly, // Log only requests served from cache
}

/// Logger manager for handling all logging operations
pub struct LoggerManager {
    pub config: LoggingConfig,
    access_log_buffer: Option<AccessLogBuffer>, // Changed from AccessLogWriter
}

impl LoggerManager {
    /// Create a new logger manager
    pub fn new(config: LoggingConfig) -> Self {
        Self {
            config,
            access_log_buffer: None,
        }
    }

    /// Create a new logger manager from config module types
    pub fn from_config(config: crate::config::LoggingConfig, hostname: String) -> Self {
        let logging_config = LoggingConfig {
            access_log_dir: config.access_log_dir,
            app_log_dir: config.app_log_dir,
            access_log_enabled: config.access_log_enabled,
            access_log_mode: match config.access_log_mode {
                crate::config::AccessLogMode::All => AccessLogMode::All,
                crate::config::AccessLogMode::CachedOnly => AccessLogMode::CachedOnly,
            },
            hostname,
            log_level: config.log_level,
            access_log_flush_interval: config.access_log_flush_interval,
            access_log_buffer_size: config.access_log_buffer_size,
        };

        Self::new(logging_config)
    }

    /// Initialize the logging system
    pub fn initialize(&mut self) -> Result<()> {
        // Initialize application logging with tracing
        self.setup_application_logging()?;

        // Initialize access logging if enabled
        if self.config.access_log_enabled {
            // Task 4.3: Pass config values to AccessLogBuffer::new()
            self.access_log_buffer = Some(AccessLogBuffer::new(
                self.config.access_log_dir.clone(),
                self.config.hostname.clone(),
                Some(self.config.access_log_flush_interval),
                Some(self.config.access_log_buffer_size),
            ));
        }

        info!("Logging system initialized");
        Ok(())
    }

    /// Setup application logging with tracing
    fn setup_application_logging(&self) -> Result<()> {
        // Create host-specific app log directory
        let host_log_dir = self.config.app_log_dir.join(&self.config.hostname);
        std::fs::create_dir_all(&host_log_dir).map_err(|e| {
            ProxyError::IoError(format!("Failed to create app log directory: {}", e))
        })?;

        // Create rolling file appender for application logs with daily rotation
        let file_appender =
            RollingFileAppender::new(Rotation::DAILY, host_log_dir.clone(), "s3-proxy.log");

        // Setup tracing subscriber with both console and file output
        // Use .compact() for consistent formatting across all log levels
        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(file_appender)
            .with_ansi(false)
            .with_target(true)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .with_level(true)
            .with_timer(tracing_subscriber::fmt::time::ChronoUtc::rfc_3339())
            .compact(); // Use compact format for consistency

        let console_layer = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stdout)
            .with_ansi(true)
            .with_target(false)
            .with_level(true)
            .with_timer(tracing_subscriber::fmt::time::ChronoUtc::rfc_3339())
            .compact(); // Use compact format for consistency

        // Try to set global subscriber, but don't fail if already set (for tests)
        // Use config log_level, but allow RUST_LOG env var to override
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(&self.config.log_level));

        let result = tracing_subscriber::registry()
            .with(env_filter)
            .with(file_layer)
            .with(console_layer)
            .try_init();

        match result {
            Ok(_) => {
                info!(
                    "Application logging initialized for host: {}",
                    self.config.hostname
                );
                info!("Application logs will be written to: {:?}", host_log_dir);
            }
            Err(_) => {
                // Already initialized, likely in tests - this is fine
                debug!("Tracing subscriber already initialized, skipping");
            }
        }

        Ok(())
    }

    /// Perform log rotation and cleanup
    pub fn rotate_logs(&self) -> Result<()> {
        let host_log_dir = self.config.app_log_dir.join(&self.config.hostname);

        // Clean up old log files (keep last 30 days)
        self.cleanup_old_logs(&host_log_dir, 30)?;

        // Clean up old access logs if access logging is enabled
        if self.config.access_log_enabled {
            self.cleanup_old_access_logs(30)?;
        }

        info!("Log rotation and cleanup completed");
        Ok(())
    }

    /// Clean up old application log files
    fn cleanup_old_logs(&self, log_dir: &PathBuf, keep_days: u32) -> Result<()> {
        let cutoff_time = std::time::SystemTime::now()
            - std::time::Duration::from_secs(keep_days as u64 * 24 * 3600);

        if let Ok(entries) = std::fs::read_dir(log_dir) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if let Ok(modified) = metadata.modified() {
                        if modified < cutoff_time {
                            if let Err(e) = std::fs::remove_file(entry.path()) {
                                warn!("Failed to remove old log file {:?}: {}", entry.path(), e);
                            } else {
                                debug!("Removed old log file: {:?}", entry.path());
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Clean up old access log files
    fn cleanup_old_access_logs(&self, keep_days: u32) -> Result<()> {
        let cutoff_time = std::time::SystemTime::now()
            - std::time::Duration::from_secs(keep_days as u64 * 24 * 3600);

        // Walk through the date-partitioned directory structure
        self.cleanup_directory_recursive(&self.config.access_log_dir, cutoff_time)?;

        Ok(())
    }

    /// Recursively clean up directories and files older than cutoff time
    fn cleanup_directory_recursive(
        &self,
        dir: &PathBuf,
        cutoff_time: std::time::SystemTime,
    ) -> Result<()> {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();

                if path.is_dir() {
                    // Recursively clean subdirectories
                    self.cleanup_directory_recursive(&path, cutoff_time)?;

                    // Remove empty directories
                    if let Ok(mut dir_entries) = std::fs::read_dir(&path) {
                        if dir_entries.next().is_none() {
                            if let Err(e) = std::fs::remove_dir(&path) {
                                debug!("Failed to remove empty directory {:?}: {}", path, e);
                            } else {
                                debug!("Removed empty directory: {:?}", path);
                            }
                        }
                    }
                } else if let Ok(metadata) = entry.metadata() {
                    if let Ok(modified) = metadata.modified() {
                        if modified < cutoff_time {
                            if let Err(e) = std::fs::remove_file(&path) {
                                warn!("Failed to remove old access log file {:?}: {}", path, e);
                            } else {
                                debug!("Removed old access log file: {:?}", path);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Log an access entry
    pub async fn log_access(&self, entry: AccessLogEntry, served_from_cache: bool) -> Result<()> {
        if let Some(buffer) = &self.access_log_buffer {
            // Check if we should log this entry based on mode
            let should_log = match self.config.access_log_mode {
                AccessLogMode::All => true,
                AccessLogMode::CachedOnly => served_from_cache,
            };

            if should_log {
                buffer.log(entry).await?;
            }
        }
        Ok(())
    }

    /// Force flush the access log buffer (for shutdown)
    ///
    /// This method should be called during graceful shutdown to ensure
    /// all buffered access log entries are written to disk.
    pub async fn force_flush(&self) -> Result<AccessLogFlushResult> {
        if let Some(buffer) = &self.access_log_buffer {
            buffer.force_flush().await
        } else {
            Ok(AccessLogFlushResult {
                skipped: true,
                ..Default::default()
            })
        }
    }

    /// Create an access log entry from HTTP request/response data
    pub fn create_access_log_entry(
        &self,
        method: &str,
        remote_ip: String,
        request_uri: String,
        http_status: u16,
        bytes_sent: u64,
        object_size: Option<u64>,
        total_time: u64,
        turn_around_time: u64,
        user_agent: Option<String>,
        referer: Option<String>,
        host_header: Option<String>,
        error_code: Option<String>,
    ) -> AccessLogEntry {
        // Extract bucket and key from request URI
        let (bucket, key) = self.parse_s3_uri(&request_uri);

        // Determine operation type based on HTTP method and URI
        let operation = match method {
            "HEAD" => "REST.HEAD.OBJECT".to_string(),
            "GET" => "REST.GET.OBJECT".to_string(),
            "PUT" => "REST.PUT.OBJECT".to_string(),
            "DELETE" => "REST.DELETE.OBJECT".to_string(),
            _ => format!("REST.{}.OBJECT", method),
        };

        AccessLogEntry {
            bucket_owner: bucket.clone(),
            bucket,
            time: Utc::now(),
            remote_ip,
            requester: "-".to_string(), // Anonymous access
            request_id: uuid::Uuid::new_v4().to_string(),
            operation,
            key,
            request_uri,
            http_status,
            error_code,
            bytes_sent,
            object_size,
            total_time,
            turn_around_time,
            referer,
            user_agent,
            host_id: self.config.hostname.clone(),
            signature_version: None,
            cipher_suite: None,
            authentication_type: Some("Anonymous".to_string()),
            host_header,
            tls_version: None,
            access_point_arn: None,
            acl_required: None,
            source_region: None,
        }
    }

    /// Parse S3 URI to extract bucket and key
    pub fn parse_s3_uri(&self, uri: &str) -> (String, String) {
        // Remove query parameters
        let path = uri.split('?').next().unwrap_or(uri);

        // Remove leading slash
        let path = path.strip_prefix('/').unwrap_or(path);

        // Split into bucket and key
        if let Some(slash_pos) = path.find('/') {
            let bucket = path[..slash_pos].to_string();
            let key = path[slash_pos + 1..].to_string();
            (bucket, key)
        } else {
            // Just bucket, no key
            (path.to_string(), "-".to_string())
        }
    }

    /// Get the number of pending entries in the access log buffer
    ///
    /// This is useful for reporting entries lost during shutdown if flush fails.
    /// Returns 0 if access logging is disabled.
    pub fn pending_entries_count(&self) -> usize {
        if let Some(buffer) = &self.access_log_buffer {
            buffer.pending_entries_count()
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::TempDir;

    /// Helper function to create a test AccessLogEntry
    fn create_test_entry(bucket: &str, key: &str) -> AccessLogEntry {
        AccessLogEntry {
            bucket_owner: bucket.to_string(),
            bucket: bucket.to_string(),
            time: Utc::now(),
            remote_ip: "192.168.1.1".to_string(),
            requester: "-".to_string(),
            request_id: format!("test-request-{}", uuid::Uuid::new_v4()),
            operation: "REST.GET.OBJECT".to_string(),
            key: key.to_string(),
            request_uri: format!("/{}/{}", bucket, key),
            http_status: 200,
            error_code: None,
            bytes_sent: 1024,
            object_size: Some(1024),
            total_time: 100,
            turn_around_time: 50,
            referer: None,
            user_agent: Some("test-agent".to_string()),
            host_id: "test-host".to_string(),
            signature_version: None,
            cipher_suite: None,
            authentication_type: Some("Anonymous".to_string()),
            host_header: Some(format!("{}.s3.amazonaws.com", bucket)),
            tls_version: None,
            access_point_arn: None,
            acl_required: None,
            source_region: None,
        }
    }

    // =========================================================================
    // Unit tests for AccessLogBuffer
    // =========================================================================

    #[tokio::test]
    async fn test_access_log_buffer_entry_buffering() {
        // Test that log() adds entries to the buffer
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        // Create buffer with large flush interval and buffer size to prevent auto-flush
        let buffer = AccessLogBuffer::new(
            log_dir,
            "test-host".to_string(),
            Some(Duration::from_secs(3600)), // 1 hour - won't trigger time-based flush
            Some(1000),                      // Large buffer size
        );

        // Initially buffer should be empty
        assert_eq!(buffer.buffer_len(), 0);

        // Add first entry
        let entry1 = create_test_entry("bucket1", "key1");
        buffer.log(entry1).await.unwrap();
        assert_eq!(buffer.buffer_len(), 1);

        // Add second entry
        let entry2 = create_test_entry("bucket2", "key2");
        buffer.log(entry2).await.unwrap();
        assert_eq!(buffer.buffer_len(), 2);

        // Add third entry
        let entry3 = create_test_entry("bucket3", "key3");
        buffer.log(entry3).await.unwrap();
        assert_eq!(buffer.buffer_len(), 3);
    }

    #[tokio::test]
    async fn test_access_log_buffer_flush_writes_correct_format() {
        // Test that flush writes entries in S3-compatible format
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        let buffer = AccessLogBuffer::new(
            log_dir.clone(),
            "test-host".to_string(),
            Some(Duration::from_secs(3600)),
            Some(1000),
        );

        // Add an entry with known values
        let entry = AccessLogEntry {
            bucket_owner: "test-bucket".to_string(),
            bucket: "test-bucket".to_string(),
            time: Utc::now(),
            remote_ip: "10.0.0.1".to_string(),
            requester: "-".to_string(),
            request_id: "req-12345".to_string(),
            operation: "REST.GET.OBJECT".to_string(),
            key: "path/to/file.txt".to_string(),
            request_uri: "/test-bucket/path/to/file.txt".to_string(),
            http_status: 200,
            error_code: None,
            bytes_sent: 2048,
            object_size: Some(2048),
            total_time: 150,
            turn_around_time: 75,
            referer: Some("https://example.com".to_string()),
            user_agent: Some("TestAgent/1.0".to_string()),
            host_id: "test-host".to_string(),
            signature_version: Some("SigV4".to_string()),
            cipher_suite: None,
            authentication_type: Some("AuthHeader".to_string()),
            host_header: Some("test-bucket.s3.amazonaws.com".to_string()),
            tls_version: Some("TLSv1.2".to_string()),
            access_point_arn: None,
            acl_required: None,
            source_region: None,
        };

        buffer.log(entry.clone()).await.unwrap();

        // Force flush
        let result = buffer.force_flush().await.unwrap();
        assert_eq!(result.entries_flushed, 1);
        assert!(!result.skipped);

        // Find and read the log file
        let date_path = Utc::now().format("%Y/%m/%d").to_string();
        let log_subdir = log_dir.join(&date_path);

        // Read the log file content
        let entries: Vec<_> = std::fs::read_dir(&log_subdir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 1, "Should have exactly one log file");

        let log_content = std::fs::read_to_string(entries[0].path()).unwrap();

        // Verify S3-compatible format fields are present
        assert!(
            log_content.contains("test-bucket"),
            "Should contain bucket name"
        );
        assert!(log_content.contains("10.0.0.1"), "Should contain remote IP");
        assert!(
            log_content.contains("req-12345"),
            "Should contain request ID"
        );
        assert!(
            log_content.contains("REST.GET.OBJECT"),
            "Should contain operation"
        );
        assert!(
            log_content.contains("/test-bucket/path/to/file.txt"),
            "Should contain request URI"
        );
        assert!(log_content.contains("200"), "Should contain HTTP status");
        assert!(log_content.contains("2048"), "Should contain bytes sent");
        assert!(
            log_content.contains("TestAgent/1.0"),
            "Should contain user agent"
        );
        assert!(
            log_content.contains("https://example.com"),
            "Should contain referer"
        );
    }

    #[tokio::test]
    async fn test_access_log_buffer_size_limit_triggers_flush() {
        // Test that buffer size limit triggers automatic flush
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        // Create buffer with small max size (3 entries) to trigger flush
        let buffer = AccessLogBuffer::new(
            log_dir.clone(),
            "test-host".to_string(),
            Some(Duration::from_secs(3600)), // Long interval - won't trigger time-based flush
            Some(3),                         // Small buffer size - will trigger size-based flush
        );

        // Add entries up to the limit
        for i in 0..3 {
            let entry = create_test_entry("bucket", &format!("key{}", i));
            buffer.log(entry).await.unwrap();
        }

        // After adding 3 entries (reaching max_buffer_size), flush should have been triggered
        // Buffer should be empty after flush
        assert_eq!(
            buffer.buffer_len(),
            0,
            "Buffer should be empty after size-triggered flush"
        );

        // Verify log file was created
        let date_path = Utc::now().format("%Y/%m/%d").to_string();
        let log_subdir = log_dir.join(&date_path);
        assert!(
            log_subdir.exists(),
            "Log directory should exist after flush"
        );

        let entries: Vec<_> = std::fs::read_dir(&log_subdir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            !entries.is_empty(),
            "Should have at least one log file after flush"
        );
    }

    #[tokio::test]
    async fn test_access_log_buffer_flush_timing_logic() {
        // Test should_flush based on interval
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        // Create buffer with very short flush interval (100ms)
        let buffer = AccessLogBuffer::new(
            log_dir,
            "test-host".to_string(),
            Some(Duration::from_millis(100)), // Short interval for testing
            Some(1000),                       // Large buffer size
        );

        // Add an entry
        let entry = create_test_entry("bucket", "key");
        buffer.log(entry).await.unwrap();

        // Initially should_flush might be false (just created)
        // But after waiting for the interval, it should be true

        // Wait for flush interval to elapse
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Now should_flush should return true due to elapsed time
        assert!(
            buffer.should_flush(),
            "should_flush should return true after interval elapsed"
        );

        // Perform flush
        let result = buffer.flush().await.unwrap();
        assert_eq!(result.entries_flushed, 1);

        // After flush, should_flush should return false (no entries and timer reset)
        assert!(
            !buffer.should_flush(),
            "should_flush should return false after flush with empty buffer"
        );
    }

    #[tokio::test]
    async fn test_access_log_buffer_flush_empty_buffer() {
        // Test that flushing an empty buffer is handled correctly
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        let buffer = AccessLogBuffer::new(
            log_dir,
            "test-host".to_string(),
            Some(Duration::from_secs(5)),
            Some(1000),
        );

        // Flush empty buffer
        let result = buffer.flush().await.unwrap();
        assert!(result.skipped, "Flushing empty buffer should be skipped");
        assert_eq!(result.entries_flushed, 0);
    }

    #[tokio::test]
    async fn test_access_log_buffer_force_flush() {
        // Test force_flush for shutdown scenarios
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        let buffer = AccessLogBuffer::new(
            log_dir.clone(),
            "test-host".to_string(),
            Some(Duration::from_secs(3600)), // Long interval
            Some(1000),                      // Large buffer
        );

        // Add entries
        for i in 0..5 {
            let entry = create_test_entry("bucket", &format!("key{}", i));
            buffer.log(entry).await.unwrap();
        }

        assert_eq!(buffer.buffer_len(), 5);

        // Force flush should write all entries
        let result = buffer.force_flush().await.unwrap();
        assert_eq!(result.entries_flushed, 5);
        assert!(!result.skipped);
        assert_eq!(
            buffer.buffer_len(),
            0,
            "Buffer should be empty after force_flush"
        );
    }

    #[tokio::test]
    async fn test_access_log_buffer_date_partitioning() {
        // Test that entries are written to date-partitioned directories
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        let buffer = AccessLogBuffer::new(
            log_dir.clone(),
            "test-host".to_string(),
            Some(Duration::from_secs(3600)),
            Some(1000),
        );

        // Add entry
        let entry = create_test_entry("bucket", "key");
        let entry_time = entry.time;
        buffer.log(entry).await.unwrap();

        // Flush
        buffer.force_flush().await.unwrap();

        // Verify date-partitioned directory structure: YYYY/MM/DD/
        let expected_path = log_dir
            .join(entry_time.format("%Y").to_string())
            .join(entry_time.format("%m").to_string())
            .join(entry_time.format("%d").to_string());

        assert!(
            expected_path.exists(),
            "Date-partitioned directory should exist: {:?}",
            expected_path
        );
    }

    #[tokio::test]
    async fn test_access_log_buffer_multiple_flushes() {
        // Test multiple flush cycles
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        let buffer = AccessLogBuffer::new(
            log_dir.clone(),
            "test-host".to_string(),
            Some(Duration::from_secs(3600)),
            Some(1000),
        );

        // First batch
        for i in 0..3 {
            let entry = create_test_entry("bucket", &format!("key-batch1-{}", i));
            buffer.log(entry).await.unwrap();
        }
        let result1 = buffer.force_flush().await.unwrap();
        assert_eq!(result1.entries_flushed, 3);

        // Second batch
        for i in 0..2 {
            let entry = create_test_entry("bucket", &format!("key-batch2-{}", i));
            buffer.log(entry).await.unwrap();
        }
        let result2 = buffer.force_flush().await.unwrap();
        assert_eq!(result2.entries_flushed, 2);

        // Verify total entries in log file
        let date_path = Utc::now().format("%Y/%m/%d").to_string();
        let log_subdir = log_dir.join(&date_path);

        let log_files: Vec<_> = std::fs::read_dir(&log_subdir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();

        // Count total lines across all log files
        let mut total_lines = 0;
        for file in &log_files {
            let content = std::fs::read_to_string(file.path()).unwrap();
            total_lines += content.lines().count();
        }
        assert_eq!(
            total_lines, 5,
            "Should have 5 total log entries across all files"
        );
    }

    #[tokio::test]
    async fn test_access_log_buffer_concurrent_flush_prevention() {
        // Test that concurrent flushes are prevented
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        let buffer = Arc::new(AccessLogBuffer::new(
            log_dir,
            "test-host".to_string(),
            Some(Duration::from_secs(3600)),
            Some(1000),
        ));

        // Add entries
        for i in 0..10 {
            let entry = create_test_entry("bucket", &format!("key{}", i));
            buffer.log(entry).await.unwrap();
        }

        // Spawn multiple concurrent flush attempts
        let buffer1 = buffer.clone();
        let buffer2 = buffer.clone();

        let (result1, result2) = tokio::join!(buffer1.flush(), buffer2.flush());

        // One should succeed with entries, the other should report already_in_progress or succeed
        let r1 = result1.unwrap();
        let r2 = result2.unwrap();

        // At least one should have flushed entries or reported already_in_progress
        let total_flushed = r1.entries_flushed + r2.entries_flushed;
        let any_in_progress = r1.already_in_progress || r2.already_in_progress;

        // Either all entries were flushed by one call, or one was blocked
        assert!(
            total_flushed == 10 || any_in_progress,
            "Either all entries should be flushed or one flush should be blocked"
        );
    }
}
