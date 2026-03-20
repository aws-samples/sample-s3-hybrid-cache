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

/// Tracks the current log file being appended to within a rotation window
struct CurrentLogFile {
    /// Path to the current log file
    path: PathBuf,
    /// Date partition key ("YYYY/MM/DD") this file belongs to
    date_key: String,
    /// When this file was first created
    created_at: Instant,
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
    /// Current log file being appended to (per rotation window)
    current_file: Arc<RwLock<Option<CurrentLogFile>>>,
    /// Rotation interval for access log files
    file_rotation_interval: Duration,
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

/// Result of a log cleanup cycle
#[derive(Debug, Clone, Default)]
pub struct LogCleanupResult {
    /// Number of access log files deleted
    pub access_files_deleted: u32,
    /// Number of application log files deleted
    pub app_files_deleted: u32,
    /// Number of errors encountered during cleanup
    pub errors: u32,
}

/// Default flush interval for access log buffer (5 seconds)
fn default_access_log_flush_interval() -> Duration {
    Duration::from_secs(5)
}

/// Default maximum buffer size for access log buffer (1000 entries)
fn default_access_log_buffer_size() -> usize {
    1000
}

/// Default file rotation interval for access log files (5 minutes)
fn default_access_log_file_rotation_interval() -> Duration {
    Duration::from_secs(5 * 60)
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
    /// * `file_rotation_interval` - Time window for appending to same file (default: 5m)
    pub fn new(
        log_dir: PathBuf,
        hostname: String,
        flush_interval: Option<Duration>,
        max_buffer_size: Option<usize>,
        file_rotation_interval: Option<Duration>,
    ) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
            log_dir,
            hostname,
            flush_interval: flush_interval.unwrap_or_else(default_access_log_flush_interval),
            max_buffer_size: max_buffer_size.unwrap_or_else(default_access_log_buffer_size),
            last_flush: Arc::new(RwLock::new(Instant::now())),
            flush_in_progress: AtomicBool::new(false),
            current_file: Arc::new(RwLock::new(None)),
            file_rotation_interval: file_rotation_interval
                .unwrap_or_else(default_access_log_file_rotation_interval),
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

    /// Write entries to a date-partitioned file, reusing the current file within the rotation window
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

        // Format all entries
        let mut content = String::new();
        for entry in entries {
            content.push_str(&Self::format_entry(entry));
            content.push('\n');
        }

        // Determine whether to append to current file or create a new one
        let now = Instant::now();
        let should_append = {
            let current = self.current_file.read().await;
            if let Some(ref cf) = *current {
                cf.date_key == date_path
                    && cf.created_at + self.file_rotation_interval > now
                    && cf.path.exists()
            } else {
                false
            }
        };

        if should_append {
            // Append to existing file within the rotation window
            let log_file_path = {
                let current = self.current_file.read().await;
                current.as_ref().unwrap().path.clone()
            };

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

            debug!("Appended {} entries to {:?}", entries.len(), log_file_path);
        } else {
            // Create a new file named {first_entry_timestamp}-{hostname}
            let timestamp = entries[0].time;
            let filename = format!(
                "{}-{}",
                timestamp.format("%Y-%m-%d-%H-%M-%S"),
                self.hostname
            );
            let log_file_path = log_dir.join(&filename);

            // Write using atomic temp+rename for durability
            let temp_path = log_file_path.with_extension("tmp");

            tokio::fs::write(&temp_path, content.as_bytes())
                .await
                .map_err(|e| {
                    ProxyError::IoError(format!("Failed to write temp access log: {}", e))
                })?;

            tokio::fs::rename(&temp_path, &log_file_path)
                .await
                .map_err(|e| ProxyError::IoError(format!("Failed to rename access log: {}", e)))?;

            // Update current_file tracking
            let mut current = self.current_file.write().await;
            *current = Some(CurrentLogFile {
                path: log_file_path.clone(),
                date_key: date_path.to_string(),
                created_at: now,
            });

            debug!("Created new log file {:?} with {} entries", log_file_path, entries.len());
        }

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
    /// Days to retain access log files (default: 30, range: 1-365)
    pub access_log_retention_days: u32,
    /// Days to retain application log files (default: 30, range: 1-365)
    pub app_log_retention_days: u32,
    /// Interval between log cleanup cycles (default: 24h, range: 1h-7d)
    pub log_cleanup_interval: Duration,
    /// Time window for appending to same access log file (default: 5m, range: 1m-60m)
    pub access_log_file_rotation_interval: Duration,
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
            access_log_retention_days: config.access_log_retention_days,
            app_log_retention_days: config.app_log_retention_days,
            log_cleanup_interval: config.log_cleanup_interval,
            access_log_file_rotation_interval: config.access_log_file_rotation_interval,
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
                Some(self.config.access_log_file_rotation_interval),
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
            .unwrap_or_else(|_| EnvFilter::new(&format!("{},trust_dns_proto=error", self.config.log_level)));

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
    pub fn rotate_logs(&self, access_log_retention_days: u32, app_log_retention_days: u32) -> Result<LogCleanupResult> {
        let mut result = LogCleanupResult::default();
        let host_log_dir = self.config.app_log_dir.join(&self.config.hostname);

        // Clean up old application log files
        let (deleted, errors) = self.cleanup_old_logs(&host_log_dir, app_log_retention_days)?;
        result.app_files_deleted = deleted;
        result.errors += errors;

        // Clean up old access logs if access logging is enabled
        if self.config.access_log_enabled {
            let (deleted, errors) = self.cleanup_old_access_logs(access_log_retention_days)?;
            result.access_files_deleted = deleted;
            result.errors += errors;
        }

        info!("Log rotation and cleanup completed");
        Ok(result)
    }

    /// Clean up old application log files
    /// Returns (files_deleted, errors)
    fn cleanup_old_logs(&self, log_dir: &PathBuf, keep_days: u32) -> Result<(u32, u32)> {
        let cutoff_time = std::time::SystemTime::now()
            - std::time::Duration::from_secs(keep_days as u64 * 24 * 3600);

        let mut deleted = 0u32;
        let mut errors = 0u32;

        if let Ok(entries) = std::fs::read_dir(log_dir) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if let Ok(modified) = metadata.modified() {
                        if modified < cutoff_time {
                            if let Err(e) = std::fs::remove_file(entry.path()) {
                                warn!("Failed to remove old log file {:?}: {}", entry.path(), e);
                                errors += 1;
                            } else {
                                debug!("Removed old log file: {:?}", entry.path());
                                deleted += 1;
                            }
                        }
                    }
                }
            }
        }

        Ok((deleted, errors))
    }

    /// Clean up old access log files
    /// Returns (files_deleted, errors)
    fn cleanup_old_access_logs(&self, keep_days: u32) -> Result<(u32, u32)> {
        let cutoff_time = std::time::SystemTime::now()
            - std::time::Duration::from_secs(keep_days as u64 * 24 * 3600);

        let mut deleted = 0u32;
        let mut errors = 0u32;

        // Walk through the date-partitioned directory structure
        self.cleanup_directory_recursive(&self.config.access_log_dir, cutoff_time, &mut deleted, &mut errors)?;

        Ok((deleted, errors))
    }

    /// Recursively clean up directories and files older than cutoff time
    fn cleanup_directory_recursive(
        &self,
        dir: &PathBuf,
        cutoff_time: std::time::SystemTime,
        deleted: &mut u32,
        errors: &mut u32,
    ) -> Result<()> {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();

                if path.is_dir() {
                    // Recursively clean subdirectories
                    self.cleanup_directory_recursive(&path, cutoff_time, deleted, errors)?;

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
                                *errors += 1;
                            } else {
                                debug!("Removed old access log file: {:?}", path);
                                *deleted += 1;
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
            None,                            // Default file rotation interval
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
            None,
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
            None,                            // Default file rotation interval
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
            None,                             // Default file rotation interval
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
            None,
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
            None,                            // Default file rotation interval
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
            None,
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
            None,
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
            None,
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

    // =========================================================================
    // Unit tests for CurrentLogFile state transitions
    // Requirements: 9.5, 9.6, 9.7, 9.8
    // =========================================================================

    #[tokio::test]
    async fn test_no_current_file_creates_new_file() {
        // When no current file exists, the first flush should create a new file
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        let buffer = AccessLogBuffer::new(
            log_dir.clone(),
            "test-host".to_string(),
            Some(Duration::from_secs(3600)),
            Some(10000),
            Some(Duration::from_secs(300)),
        );

        // Verify current_file starts as None
        assert!(buffer.current_file.read().await.is_none());

        // Log an entry and flush
        let entry = create_test_entry("test-bucket", "test-key");
        buffer.log(entry).await.unwrap();
        let result = buffer.force_flush().await.unwrap();
        assert_eq!(result.entries_flushed, 1);

        // current_file should now be set
        let current = buffer.current_file.read().await;
        assert!(current.is_some(), "current_file should be set after first flush");
        let cf = current.as_ref().unwrap();
        assert!(cf.path.exists(), "created file should exist on disk");
        assert_eq!(cf.date_key, Utc::now().format("%Y/%m/%d").to_string());

        // Exactly one file in the date directory
        let date_path = Utc::now().format("%Y/%m/%d").to_string();
        let files: Vec<_> = std::fs::read_dir(log_dir.join(&date_path))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();
        assert_eq!(files.len(), 1, "First flush should create exactly one file");
    }

    #[tokio::test]
    async fn test_within_rotation_window_appends_to_same_file() {
        // When flushing within the rotation window, entries append to the same file
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        let buffer = AccessLogBuffer::new(
            log_dir.clone(),
            "test-host".to_string(),
            Some(Duration::from_secs(3600)),
            Some(10000),
            Some(Duration::from_secs(300)), // 5 minute rotation window
        );

        // First flush — creates a new file
        let entry1 = create_test_entry("test-bucket", "key-1");
        buffer.log(entry1).await.unwrap();
        buffer.force_flush().await.unwrap();

        let first_path = buffer.current_file.read().await.as_ref().unwrap().path.clone();

        // Second flush — should append to the same file (within rotation window)
        let entry2 = create_test_entry("test-bucket", "key-2");
        buffer.log(entry2).await.unwrap();
        buffer.force_flush().await.unwrap();

        let second_path = buffer.current_file.read().await.as_ref().unwrap().path.clone();
        assert_eq!(first_path, second_path, "Should reuse the same file within rotation window");

        // Only one file should exist
        let date_path = Utc::now().format("%Y/%m/%d").to_string();
        let files: Vec<_> = std::fs::read_dir(log_dir.join(&date_path))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();
        assert_eq!(files.len(), 1, "Should have exactly 1 file (appended, not rotated)");

        // File should contain both entries (2 lines)
        let content = std::fs::read_to_string(&first_path).unwrap();
        assert_eq!(content.lines().count(), 2, "File should contain 2 entries");
    }

    #[tokio::test]
    async fn test_expired_rotation_window_creates_new_file() {
        // When the rotation window has expired, the next flush creates a new file
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        // Use a very short rotation interval (100ms) so we can test expiry quickly
        let buffer = AccessLogBuffer::new(
            log_dir.clone(),
            "test-host".to_string(),
            Some(Duration::from_secs(3600)),
            Some(10000),
            Some(Duration::from_millis(100)),
        );

        // First flush — creates file
        let entry1 = create_test_entry("test-bucket", "key-1");
        buffer.log(entry1).await.unwrap();
        buffer.force_flush().await.unwrap();

        let first_path = buffer.current_file.read().await.as_ref().unwrap().path.clone();

        // Sleep past the rotation interval AND past the 1-second timestamp boundary
        // so the new file gets a distinct name (filenames use second-level precision)
        tokio::time::sleep(Duration::from_millis(1200)).await;

        // Second flush — rotation window expired, should create a new file
        let entry2 = create_test_entry("test-bucket", "key-2");
        buffer.log(entry2).await.unwrap();
        buffer.force_flush().await.unwrap();

        let second_path = buffer.current_file.read().await.as_ref().unwrap().path.clone();
        assert_ne!(first_path, second_path, "Should create a new file after rotation window expires");

        // Two files should exist
        let date_path = Utc::now().format("%Y/%m/%d").to_string();
        let files: Vec<_> = std::fs::read_dir(log_dir.join(&date_path))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();
        assert_eq!(files.len(), 2, "Should have 2 files after rotation");

        // Each file should have exactly 1 entry
        for file in &files {
            let content = std::fs::read_to_string(file.path()).unwrap();
            assert_eq!(content.lines().count(), 1, "Each rotated file should have 1 entry");
        }
    }

    #[tokio::test]
    async fn test_date_partition_change_creates_new_file() {
        // When the date_key changes, a new file is created even within the rotation window.
        // Since entries use Utc::now(), we test this indirectly by verifying the date_key
        // check in write_entries_to_file: manually set current_file to a different date_key
        // and confirm the next flush creates a new file (not an append).
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        let buffer = AccessLogBuffer::new(
            log_dir.clone(),
            "test-host".to_string(),
            Some(Duration::from_secs(3600)),
            Some(10000),
            Some(Duration::from_secs(300)), // 5 minute rotation window
        );

        // First flush — creates a file for today's date
        let entry1 = create_test_entry("test-bucket", "key-1");
        buffer.log(entry1).await.unwrap();
        buffer.force_flush().await.unwrap();

        let first_path = buffer.current_file.read().await.as_ref().unwrap().path.clone();

        // Verify the file has 1 entry
        let content_before = std::fs::read_to_string(&first_path).unwrap();
        assert_eq!(content_before.lines().count(), 1, "First file should have 1 entry");

        // Manually override current_file's date_key to simulate a date partition mismatch.
        // This mimics what happens at midnight when the date rolls over: the current_file
        // was created for yesterday but new entries belong to today.
        {
            let mut current = buffer.current_file.write().await;
            if let Some(ref mut cf) = *current {
                cf.date_key = "1999/12/31".to_string();
            }
        }

        // Next flush — date_key won't match today's entries, so a new file is created
        // (not appended to the old one)
        let entry2 = create_test_entry("test-bucket", "key-2");
        buffer.log(entry2).await.unwrap();
        buffer.force_flush().await.unwrap();

        let second_path = buffer.current_file.read().await.as_ref().unwrap().path.clone();

        // current_file should now point to today's date
        let current = buffer.current_file.read().await;
        let cf = current.as_ref().unwrap();
        assert_eq!(
            cf.date_key,
            Utc::now().format("%Y/%m/%d").to_string(),
            "current_file date_key should match today after date partition change"
        );

        // The key assertion: the date_key mismatch caused a NEW file to be created
        // (via atomic write), not an append to the old file. The old file should still
        // have exactly 1 entry (it was not appended to).
        let content_after = std::fs::read_to_string(&first_path).unwrap();
        assert_eq!(
            content_after.lines().count(), 1,
            "Original file should still have 1 entry (not appended to after date change)"
        );

        // The new file should have exactly 1 entry
        let new_content = std::fs::read_to_string(&second_path).unwrap();
        assert_eq!(
            new_content.lines().count(), 1,
            "New file created after date partition change should have 1 entry"
        );
    }

    // =========================================================================
    // Property-based tests for log lifecycle
    // =========================================================================

    // Feature: log-lifecycle, Property 7: File rotation window consolidation
    // **Validates: Requirements 9.5, 9.6, 9.7**
    //
    // For any sequence of access log flushes with known timestamps and a given
    // file_rotation_interval, the number of distinct files created should equal
    // the number of rotation windows spanned. All entries flushed within the same
    // rotation window should be written to the same file. A flush after the rotation
    // interval has elapsed since the current file's creation should create a new file.
    #[tokio::test]
    async fn prop_file_rotation_window_consolidation() {
        use quickcheck::{Gen, Arbitrary};

        // Run multiple random iterations to get property-based coverage.
        // Each iteration sleeps ~1.2s between batches, so we limit iterations
        // and batch count to keep total runtime reasonable (~30s).
        let mut gen = Gen::new(100);
        for _ in 0..20 {
            // Generate random batch count [1, 3] and flushes per batch [1, 4]
            let batch_count = (u8::arbitrary(&mut gen) % 3) as usize + 1;
            let mut flushes_per_batch: Vec<usize> = Vec::new();
            let mut entries_per_flush: Vec<Vec<usize>> = Vec::new();

            for _ in 0..batch_count {
                let num_flushes = (u8::arbitrary(&mut gen) % 4) as usize + 1;
                let mut flush_entries = Vec::new();
                for _ in 0..num_flushes {
                    let num_entries = (u8::arbitrary(&mut gen) % 3) as usize + 1; // 1-3 entries per flush
                    flush_entries.push(num_entries);
                }
                flushes_per_batch.push(num_flushes);
                entries_per_flush.push(flush_entries);
            }

            let temp_dir = TempDir::new().unwrap();
            let log_dir = temp_dir.path().to_path_buf();

            // Use a rotation interval of 500ms. This is short enough for fast tests
            // but we sleep >1s between batches to ensure both the rotation window
            // expires AND the entry timestamp (second-level precision) changes,
            // producing distinct filenames.
            let rotation_interval = Duration::from_millis(500);

            let buffer = AccessLogBuffer::new(
                log_dir.clone(),
                "test-host".to_string(),
                Some(Duration::from_secs(3600)), // Long flush interval — we flush manually
                Some(10000),                     // Large buffer size
                Some(rotation_interval),
            );

            let mut total_entries = 0usize;

            for (batch_idx, flush_entries) in entries_per_flush.iter().enumerate() {
                // If not the first batch, sleep past both the rotation interval AND
                // the 1-second timestamp boundary so the new file gets a distinct name.
                if batch_idx > 0 {
                    tokio::time::sleep(Duration::from_millis(1200)).await;
                }

                // Perform all flushes in this batch quickly (within the rotation window)
                for &num_entries in flush_entries {
                    for _ in 0..num_entries {
                        let entry = create_test_entry("test-bucket", "test-key");
                        buffer.log(entry).await.unwrap();
                        total_entries += 1;
                    }
                    let result = buffer.force_flush().await.unwrap();
                    assert!(!result.skipped, "Flush should not be skipped when entries exist");
                }
            }

            // Count files in the date-partitioned directory
            let date_path = chrono::Utc::now().format("%Y/%m/%d").to_string();
            let log_subdir = log_dir.join(&date_path);

            let files: Vec<_> = std::fs::read_dir(&log_subdir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_file())
                .collect();

            // File count should equal batch count: flushes within the same rotation
            // window append to the same file, and sleeping past the interval creates a new one
            assert_eq!(
                files.len(),
                batch_count,
                "Expected {} files (one per rotation window batch), got {}. \
                 Batches: {:?}, entries_per_flush: {:?}",
                batch_count,
                files.len(),
                flushes_per_batch,
                entries_per_flush,
            );

            // Total lines across all files should equal total entries
            let mut total_lines = 0usize;
            for file in &files {
                let content = std::fs::read_to_string(file.path()).unwrap();
                total_lines += content.lines().count();
            }
            assert_eq!(
                total_lines, total_entries,
                "Expected {} total log lines, got {}",
                total_entries, total_lines,
            );
        }
    }
}

#[cfg(test)]
mod log_lifecycle_property_tests {
    use super::*;
    use filetime::{set_file_mtime, FileTime};
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;
    use tempfile::TempDir;

    // Feature: log-lifecycle, Property 4: Cleanup deletes exactly the expired files
    // **Validates: Requirements 4.4, 4.5, 5.1, 5.2, 8.1, 8.2**
    //
    // For any set of files with random modification times and any retention period in [1, 365] days,
    // after running cleanup:
    //   (a) no file older than the retention period should remain
    //   (b) all files newer than the retention period should be preserved
    //   (c) application log cleanup only touches files within the current hostname's directory
    //   (d) access log cleanup traverses the entire date-partitioned tree
    #[quickcheck]
    fn prop_cleanup_deletes_exactly_expired_files(
        file_ages_days: Vec<u16>,
        retention_raw: u16,
        other_host_ages: Vec<u16>,
    ) -> TestResult {
        // Need at least one file to test
        if file_ages_days.is_empty() {
            return TestResult::discard();
        }
        // Cap file count to keep tests fast
        if file_ages_days.len() > 20 || other_host_ages.len() > 10 {
            return TestResult::discard();
        }

        // Map retention to valid range [1, 365]
        let retention_days = (retention_raw % 365) as u32 + 1;

        let temp_dir = TempDir::new().unwrap();
        let access_log_dir = temp_dir.path().join("access_logs");
        let app_log_dir = temp_dir.path().join("app_logs");
        let hostname = "test-host-prop4";

        // Create directory structure
        let host_app_dir = app_log_dir.join(hostname);
        std::fs::create_dir_all(&host_app_dir).unwrap();
        let other_host_dir = app_log_dir.join("other-host");
        std::fs::create_dir_all(&other_host_dir).unwrap();

        // Create access log date-partitioned dir
        let access_date_dir = access_log_dir.join("2024/01/15");
        std::fs::create_dir_all(&access_date_dir).unwrap();

        let now = std::time::SystemTime::now();

        // --- Create app log files for our hostname ---
        let mut expected_app_surviving = 0u32;
        let mut expected_app_deleted = 0u32;
        for (i, &age_raw) in file_ages_days.iter().enumerate() {
            let age_days = age_raw % 730; // up to ~2 years
            let path = host_app_dir.join(format!("s3-proxy.log.{}", i));
            std::fs::write(&path, format!("app log content {}", i)).unwrap();

            let mtime_secs = now
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                - (age_days as i64 * 86400);
            set_file_mtime(&path, FileTime::from_unix_time(mtime_secs, 0)).unwrap();

            if age_days < retention_days as u16 {
                expected_app_surviving += 1;
            } else {
                expected_app_deleted += 1;
            }
        }

        // --- Create app log files for OTHER hostname (should NOT be touched) ---
        let other_host_file_count = other_host_ages.len();
        for (i, &age_raw) in other_host_ages.iter().enumerate() {
            let age_days = age_raw % 730;
            let path = other_host_dir.join(format!("s3-proxy.log.{}", i));
            std::fs::write(&path, format!("other host log {}", i)).unwrap();

            let mtime_secs = now
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                - (age_days as i64 * 86400);
            set_file_mtime(&path, FileTime::from_unix_time(mtime_secs, 0)).unwrap();
        }

        // --- Create access log files in date-partitioned tree ---
        let mut expected_access_surviving = 0u32;
        let mut expected_access_deleted = 0u32;
        for (i, &age_raw) in file_ages_days.iter().enumerate() {
            let age_days = age_raw % 730;
            let path = access_date_dir.join(format!("2024-01-15-00-00-00-access-{}", i));
            std::fs::write(&path, format!("access log content {}", i)).unwrap();

            let mtime_secs = now
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                - (age_days as i64 * 86400);
            set_file_mtime(&path, FileTime::from_unix_time(mtime_secs, 0)).unwrap();

            if age_days < retention_days as u16 {
                expected_access_surviving += 1;
            } else {
                expected_access_deleted += 1;
            }
        }

        // Create LoggerManager with access logging enabled
        let config = LoggingConfig {
            access_log_dir: access_log_dir.clone(),
            app_log_dir: app_log_dir.clone(),
            access_log_enabled: true,
            access_log_mode: AccessLogMode::All,
            hostname: hostname.to_string(),
            log_level: "warn".to_string(),
            access_log_flush_interval: Duration::from_secs(5),
            access_log_buffer_size: 1000,
            access_log_retention_days: retention_days,
            app_log_retention_days: retention_days,
            log_cleanup_interval: Duration::from_secs(86400),
            access_log_file_rotation_interval: Duration::from_secs(300),
        };
        let manager = LoggerManager::new(config);

        // Run cleanup
        let result = manager.rotate_logs(retention_days, retention_days).unwrap();

        // (a) & (b): Check app log files — only non-expired files for our hostname remain
        let remaining_app_files: Vec<_> = std::fs::read_dir(&host_app_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();

        if remaining_app_files.len() as u32 != expected_app_surviving {
            return TestResult::error(format!(
                "App log: expected {} surviving files, got {}",
                expected_app_surviving,
                remaining_app_files.len()
            ));
        }

        if result.app_files_deleted != expected_app_deleted {
            return TestResult::error(format!(
                "App log: expected {} deleted, result reports {}",
                expected_app_deleted, result.app_files_deleted
            ));
        }

        // (c): Other hostname's files must ALL still exist (hostname-scoped cleanup)
        let remaining_other_files: Vec<_> = std::fs::read_dir(&other_host_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();

        if remaining_other_files.len() != other_host_file_count {
            return TestResult::error(format!(
                "Other host: expected {} files untouched, got {}",
                other_host_file_count,
                remaining_other_files.len()
            ));
        }

        // (d): Check access log files — traverses entire tree
        // Count remaining files recursively in access_log_dir
        fn count_files_recursive(dir: &std::path::Path) -> u32 {
            let mut count = 0;
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        count += count_files_recursive(&path);
                    } else {
                        count += 1;
                    }
                }
            }
            count
        }

        let remaining_access_files = count_files_recursive(&access_log_dir);
        if remaining_access_files != expected_access_surviving {
            return TestResult::error(format!(
                "Access log: expected {} surviving files, got {}",
                expected_access_surviving, remaining_access_files
            ));
        }

        if result.access_files_deleted != expected_access_deleted {
            return TestResult::error(format!(
                "Access log: expected {} deleted, result reports {}",
                expected_access_deleted, result.access_files_deleted
            ));
        }

        TestResult::passed()
    }

    // Feature: log-lifecycle, Property 5: Empty directories are removed after cleanup
    // **Validates: Requirements 4.6**
    //
    // For any date-partitioned directory tree where all files in a day/month/year directory
    // have been deleted by cleanup, the empty day, month, and year directories should be removed.
    // Directories that still contain files or non-empty subdirectories should be preserved.
    #[quickcheck]
    fn prop_empty_directories_removed_after_cleanup(
        // Each element is (month 1-12, day 1-28, file_count 1-3, all_expired: bool)
        partitions_raw: Vec<(u8, u8, u8, bool)>,
        retention_raw: u16,
    ) -> TestResult {
        // Need at least one partition
        if partitions_raw.is_empty() {
            return TestResult::discard();
        }
        // Cap partition count to keep tests fast
        if partitions_raw.len() > 12 {
            return TestResult::discard();
        }

        // Map retention to valid range [1, 365]
        let retention_days = (retention_raw % 365) as u32 + 1;

        let temp_dir = TempDir::new().unwrap();
        let access_log_dir = temp_dir.path().join("access_logs");
        let app_log_dir = temp_dir.path().join("app_logs");
        let hostname = "test-host-prop5";

        // Create app log dir (needed by LoggerManager)
        let host_app_dir = app_log_dir.join(hostname);
        std::fs::create_dir_all(&host_app_dir).unwrap();

        let now = std::time::SystemTime::now();
        let now_secs = now
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Track which (year, month, day) partitions should survive
        // A partition survives if it has at least one non-expired file
        let mut surviving_day_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Deduplicate partitions by (month, day) to avoid conflicts
        let mut seen_partitions: std::collections::HashSet<(u8, u8)> = std::collections::HashSet::new();

        let year = "2024";
        for &(month_raw, day_raw, file_count_raw, all_expired) in &partitions_raw {
            let month = (month_raw % 12) + 1; // 1-12
            let day = (day_raw % 28) + 1; // 1-28

            // Skip duplicate partitions
            if !seen_partitions.insert((month, day)) {
                continue;
            }

            let file_count = ((file_count_raw % 3) + 1) as usize; // 1-3 files

            let date_dir = access_log_dir.join(format!("{}/{:02}/{:02}", year, month, day));
            std::fs::create_dir_all(&date_dir).unwrap();

            for i in 0..file_count {
                let path = date_dir.join(format!("2024-{:02}-{:02}-00-00-00-file-{}", month, day, i));
                std::fs::write(&path, format!("access log content {}", i)).unwrap();

                let mtime_secs = if all_expired {
                    // Set mtime well beyond retention period
                    now_secs - ((retention_days as i64 + 30) * 86400)
                } else {
                    // Set mtime to recent (within retention)
                    now_secs - 3600 // 1 hour ago
                };
                set_file_mtime(&path, FileTime::from_unix_time(mtime_secs, 0)).unwrap();
            }

            if !all_expired {
                surviving_day_dirs.insert(format!("{}/{:02}/{:02}", year, month, day));
            }
        }

        // Create LoggerManager
        let config = LoggingConfig {
            access_log_dir: access_log_dir.clone(),
            app_log_dir: app_log_dir.clone(),
            access_log_enabled: true,
            access_log_mode: AccessLogMode::All,
            hostname: hostname.to_string(),
            log_level: "warn".to_string(),
            access_log_flush_interval: Duration::from_secs(5),
            access_log_buffer_size: 1000,
            access_log_retention_days: retention_days,
            app_log_retention_days: retention_days,
            log_cleanup_interval: Duration::from_secs(86400),
            access_log_file_rotation_interval: Duration::from_secs(300),
        };
        let manager = LoggerManager::new(config);

        // Run cleanup
        manager.rotate_logs(retention_days, retention_days).unwrap();

        // Verify: surviving day directories still exist
        for day_path in &surviving_day_dirs {
            let full_path = access_log_dir.join(day_path);
            if !full_path.exists() {
                return TestResult::error(format!(
                    "Non-empty day dir should be preserved but was removed: {}",
                    day_path
                ));
            }
        }

        // Verify: no empty directories remain anywhere in the access log tree
        fn find_empty_dirs(dir: &std::path::Path, results: &mut Vec<std::path::PathBuf>) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                let entries: Vec<_> = entries.filter_map(|e| e.ok()).collect();
                if entries.is_empty() {
                    results.push(dir.to_path_buf());
                    return;
                }
                for entry in &entries {
                    let path = entry.path();
                    if path.is_dir() {
                        find_empty_dirs(&path, results);
                    }
                }
            }
        }

        let mut empty_dirs = Vec::new();
        if access_log_dir.exists() {
            find_empty_dirs(&access_log_dir, &mut empty_dirs);
        }

        // The access_log_dir root itself may be empty if all partitions were expired — that's fine,
        // cleanup_directory_recursive doesn't remove the root dir. But any subdirectory that is
        // empty should have been removed.
        let empty_subdirs: Vec<_> = empty_dirs
            .iter()
            .filter(|d| *d != &access_log_dir)
            .collect();

        if !empty_subdirs.is_empty() {
            return TestResult::error(format!(
                "Empty subdirectories should have been removed: {:?}",
                empty_subdirs
            ));
        }

        // Verify: parent directories of surviving partitions exist (month and year)
        for day_path in &surviving_day_dirs {
            let parts: Vec<&str> = day_path.split('/').collect();
            // Check year dir
            let year_dir = access_log_dir.join(parts[0]);
            if !year_dir.exists() {
                return TestResult::error(format!(
                    "Year dir should be preserved for surviving partition: {}",
                    parts[0]
                ));
            }
            // Check month dir
            let month_dir = access_log_dir.join(format!("{}/{}", parts[0], parts[1]));
            if !month_dir.exists() {
                return TestResult::error(format!(
                    "Month dir should be preserved for surviving partition: {}/{}",
                    parts[0], parts[1]
                ));
            }
        }

        TestResult::passed()
    }

    // Feature: log-lifecycle, Property 6: Cleanup continues after I/O errors
    // **Validates: Requirements 6.3**
    //
    // For any set of files where some files are inaccessible (permission denied, etc.),
    // cleanup should still delete all accessible expired files and report the error count.
    // The number of successfully deleted files plus the error count should equal the total
    // number of expired files.
    #[quickcheck]
    fn prop_cleanup_continues_after_io_errors(
        deletable_count_raw: u8,
        undeletable_count_raw: u8,
    ) -> TestResult {
        // Map to reasonable counts: at least 1 deletable and 1 undeletable
        let deletable_count = (deletable_count_raw % 10) as usize + 1;
        let undeletable_count = (undeletable_count_raw % 10) as usize + 1;

        let temp_dir = TempDir::new().unwrap();
        let access_log_dir = temp_dir.path().join("access_logs");
        let app_log_dir = temp_dir.path().join("app_logs");
        let hostname = "test-host-prop6";

        // Create app log dir (needed by LoggerManager)
        let host_app_dir = app_log_dir.join(hostname);
        std::fs::create_dir_all(&host_app_dir).unwrap();

        let now = std::time::SystemTime::now();
        let now_secs = now
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // All files are expired (well beyond any retention period)
        let expired_mtime_secs = now_secs - (400 * 86400); // 400 days ago
        let retention_days = 1u32;

        // --- Create deletable expired files in a normal (writable) subdirectory ---
        let writable_dir = access_log_dir.join("2023/01/01");
        std::fs::create_dir_all(&writable_dir).unwrap();

        for i in 0..deletable_count {
            let path = writable_dir.join(format!("2023-01-01-00-00-00-deletable-{}", i));
            std::fs::write(&path, format!("deletable content {}", i)).unwrap();
            set_file_mtime(&path, FileTime::from_unix_time(expired_mtime_secs, 0)).unwrap();
        }

        // --- Create undeletable expired files in a read-only subdirectory ---
        // Making the parent directory read-only prevents file deletion on Unix
        let readonly_dir = access_log_dir.join("2023/02/01");
        std::fs::create_dir_all(&readonly_dir).unwrap();

        for i in 0..undeletable_count {
            let path = readonly_dir.join(format!("2023-02-01-00-00-00-undeletable-{}", i));
            std::fs::write(&path, format!("undeletable content {}", i)).unwrap();
            set_file_mtime(&path, FileTime::from_unix_time(expired_mtime_secs, 0)).unwrap();
        }

        // Make the subdirectory read-only (removes write permission)
        // This prevents deletion of files inside it on Unix
        let mut perms = std::fs::metadata(&readonly_dir).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        {
            perms.set_readonly(true);
        }
        std::fs::set_permissions(&readonly_dir, perms).unwrap();

        // Create LoggerManager
        let config = LoggingConfig {
            access_log_dir: access_log_dir.clone(),
            app_log_dir: app_log_dir.clone(),
            access_log_enabled: true,
            access_log_mode: AccessLogMode::All,
            hostname: hostname.to_string(),
            log_level: "warn".to_string(),
            access_log_flush_interval: Duration::from_secs(5),
            access_log_buffer_size: 1000,
            access_log_retention_days: retention_days,
            app_log_retention_days: retention_days,
            log_cleanup_interval: Duration::from_secs(86400),
            access_log_file_rotation_interval: Duration::from_secs(300),
        };
        let manager = LoggerManager::new(config);

        // Run cleanup — this should NOT panic or abort
        let result = manager.rotate_logs(retention_days, retention_days).unwrap();

        // Restore permissions so tempfile can clean up the directory
        let mut perms = std::fs::metadata(&readonly_dir).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        {
            perms.set_readonly(false);
        }
        std::fs::set_permissions(&readonly_dir, perms).unwrap();

        // Assert: all deletable expired files were removed
        let remaining_writable: Vec<_> = std::fs::read_dir(&writable_dir)
            .ok()
            .map(|entries| entries.filter_map(|e| e.ok()).filter(|e| e.path().is_file()).collect())
            .unwrap_or_default();

        if !remaining_writable.is_empty() {
            return TestResult::error(format!(
                "Expected all {} deletable files removed, but {} remain",
                deletable_count,
                remaining_writable.len()
            ));
        }

        // Assert: undeletable files still exist
        let remaining_readonly: Vec<_> = std::fs::read_dir(&readonly_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();

        if remaining_readonly.len() != undeletable_count {
            return TestResult::error(format!(
                "Expected {} undeletable files to remain, but {} remain",
                undeletable_count,
                remaining_readonly.len()
            ));
        }

        // Assert: deleted count matches deletable files
        if result.access_files_deleted != deletable_count as u32 {
            return TestResult::error(format!(
                "Expected {} access files deleted, got {}",
                deletable_count, result.access_files_deleted
            ));
        }

        // Assert: error count matches undeletable files
        if result.errors != undeletable_count as u32 {
            return TestResult::error(format!(
                "Expected {} errors, got {}",
                undeletable_count, result.errors
            ));
        }

        // Assert: deleted + errors = total expired files
        let total_expired = (deletable_count + undeletable_count) as u32;
        if result.access_files_deleted + result.errors != total_expired {
            return TestResult::error(format!(
                "deleted ({}) + errors ({}) != total expired ({})",
                result.access_files_deleted, result.errors, total_expired
            ));
        }

        TestResult::passed()
    }


}
