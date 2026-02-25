//! Cache Size Tracking Module
//!
//! Provides validation scan logic for multi-instance deployments with shared disk cache.
//! Size tracking is handled by the JournalConsolidator - this module delegates size queries
//! to the consolidator and retains only validation scan logic.
//!
//! Note: The CacheSizeTracker no longer maintains its own size state. All size queries
//! are delegated to the JournalConsolidator which is the single source of truth for cache size.

use crate::journal_consolidator::JournalConsolidator;
use crate::{ProxyError, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Configuration for cache size tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSizeConfig {
    /// Interval between checkpoints (default: 300s = 5 minutes)
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval: Duration,

    /// Time of day for daily validation scan in 24-hour format "HH:MM" (default: "00:00" = midnight local time)
    /// Examples: "00:00" (midnight), "03:30" (3:30 AM), "14:00" (2:00 PM)
    /// Fixed 1-hour jitter is automatically applied to prevent thundering herd
    #[serde(default = "default_validation_time_of_day")]
    pub validation_time_of_day: String,

    /// Enable validation scans (default: true)
    #[serde(default = "default_validation_enabled")]
    pub validation_enabled: bool,

    /// TTL for incomplete multipart uploads before eviction (default: 1 day)
    #[serde(default = "default_incomplete_upload_ttl")]
    pub incomplete_upload_ttl: Duration,
}

fn default_checkpoint_interval() -> Duration {
    Duration::from_secs(30) // 30 seconds for near-realtime cross-instance consolidation
}

fn default_validation_time_of_day() -> String {
    "00:00".to_string() // Midnight local time
}

fn default_validation_enabled() -> bool {
    true
}

fn default_incomplete_upload_ttl() -> Duration {
    Duration::from_secs(86400) // 1 day
}

impl Default for CacheSizeConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: default_checkpoint_interval(),
            validation_time_of_day: default_validation_time_of_day(),
            validation_enabled: default_validation_enabled(),
            incomplete_upload_ttl: default_incomplete_upload_ttl(),
        }
    }
}

// NOTE: Checkpoint struct has been removed as part of Task 12.
// Size tracking is now handled by JournalConsolidator which uses SizeState in size_state.json.

/// Validation metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationMetadata {
    /// Last validation timestamp
    #[serde(with = "systemtime_serde")]
    pub last_validation: SystemTime,

    /// Scanned size from validation
    pub scanned_size: u64,

    /// Tracked size at validation time
    pub tracked_size: u64,

    /// Drift in bytes (scanned - tracked)
    pub drift_bytes: i64,

    /// Scan duration in milliseconds
    pub scan_duration_ms: u64,

    /// Number of metadata files scanned
    pub metadata_files_scanned: u64,

    /// Number of expired GET cache entries deleted (active expiration)
    #[serde(default)]
    pub cache_entries_expired: u64,

    /// Number of GET cache entries skipped (actively being used)
    #[serde(default)]
    pub cache_entries_skipped: u64,

    /// Number of GET cache expiration errors encountered
    #[serde(default)]
    pub cache_expiration_errors: u64,

    /// Whether active GET cache expiration was enabled during this validation
    #[serde(default)]
    pub active_expiration_enabled: bool,

    /// Write cache size scanned during validation
    /// Requirement 6.3: Track write cache size separately
    #[serde(default)]
    pub write_cache_size: u64,

    /// Number of write cache entries expired during validation
    #[serde(default)]
    pub write_cache_expired: u64,

    /// Number of incomplete uploads evicted during validation
    #[serde(default)]
    pub incomplete_uploads_evicted: u64,
}

/// Result of scanning a single cache file
#[derive(Debug, Clone)]
struct ScanFileResult {
    /// Size in bytes (for metadata files)
    size_bytes: u64,
    /// Whether GET cache entry was expired and deleted (active expiration)
    cache_expired: bool,
    /// Whether GET cache entry was skipped (actively being used)
    cache_skipped: bool,
    /// Whether GET cache expiration encountered an error
    cache_error: bool,
}

/// Format bytes in human-readable units (KiB, MiB, GiB, TiB)
fn format_bytes_human(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    const TIB: u64 = GIB * 1024;

    if bytes >= TIB {
        format!("{:.1} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format duration in human-readable units (ms, s, Xm Ys, Xh Ym)
fn format_duration_human(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let millis = duration.as_millis();

    if total_secs >= 3600 {
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        format!("{}h {}m", hours, mins)
    } else if total_secs >= 60 {
        let mins = total_secs / 60;
        let secs = total_secs % 60;
        format!("{}m {}s", mins, secs)
    } else if total_secs > 0 {
        format!("{}s", total_secs)
    } else {
        format!("{}ms", millis)
    }
}

/// Cache size metrics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSizeMetrics {
    /// Current tracked size in bytes (total)
    pub current_size: u64,

    /// Current write cache size in bytes
    /// Requirement 6.3: Track write cache size separately
    pub write_cache_size: u64,

    /// Last checkpoint timestamp
    #[serde(with = "systemtime_serde")]
    pub last_checkpoint: SystemTime,

    /// Last validation timestamp
    #[serde(with = "option_systemtime_serde")]
    pub last_validation: Option<SystemTime>,

    /// Last validation drift in bytes
    pub last_validation_drift: Option<i64>,

    /// Number of checkpoints written
    pub checkpoint_count: u64,

    /// Current delta log size in bytes
    pub delta_log_size: u64,
}

/// Cache size tracker for multi-instance deployments
///
/// Size tracking is handled by the JournalConsolidator - this struct delegates size queries
/// to the consolidator and retains only validation scan logic. The consolidator is the
/// single source of truth for cache size, calculating size deltas from journal entries.
pub struct CacheSizeTracker {
    // Configuration
    config: CacheSizeConfig,
    cache_dir: PathBuf,
    actively_remove_cached_data: bool,

    // Reference to JournalConsolidator for size queries (Task 12.2)
    // The consolidator is the single source of truth for cache size
    consolidator: Arc<JournalConsolidator>,

    // Validation tracking
    last_validation: Mutex<Instant>,

    // File paths for validation
    validation_path: PathBuf,
    validation_lock_path: PathBuf,

    // Background task handles
    validation_task: Mutex<Option<JoinHandle<()>>>,

    // Weak reference to cache manager for GET cache expiration
    cache_manager: Mutex<Option<Weak<crate::cache::CacheManager>>>,
}

impl CacheSizeTracker {
    /// Create new tracker with reference to JournalConsolidator
    ///
    /// The consolidator handles all size tracking - this tracker only provides
    /// validation scan logic and delegates size queries to the consolidator.
    pub async fn new(
        cache_dir: PathBuf,
        config: CacheSizeConfig,
        actively_remove_cached_data: bool,
        consolidator: Arc<JournalConsolidator>,
    ) -> Result<Self> {
        // Create size tracking directory
        let size_tracking_dir = cache_dir.join("size_tracking");
        if !size_tracking_dir.exists() {
            std::fs::create_dir_all(&size_tracking_dir).map_err(|e| {
                ProxyError::CacheError(format!("Failed to create size tracking directory: {}", e))
            })?;
            info!("Created size tracking directory: {:?}", size_tracking_dir);
        }

        // Set up file paths for validation
        let validation_path = size_tracking_dir.join("validation.json");
        let validation_lock_path = size_tracking_dir.join("validation.lock");

        // Check if size state exists (for determining if immediate validation is needed)
        let size_state_path = size_tracking_dir.join("size_state.json");
        let size_state_missing = !size_state_path.exists();

        let tracker = Self {
            config: config.clone(),
            cache_dir,
            actively_remove_cached_data,
            consolidator,
            last_validation: Mutex::new(if size_state_missing {
                // Force immediate validation if no size state exists
                Instant::now() - std::time::Duration::from_secs(86400 * 365)
            } else {
                Instant::now()
            }),
            validation_path,
            validation_lock_path,
            validation_task: Mutex::new(None),
            cache_manager: Mutex::new(None),
        };

        // Note: Size will be loaded from disk on first access
        // We can't call async get_size() here in the constructor
        info!(
            "Cache size tracker initialized: validation_time={}{}",
            config.validation_time_of_day,
            if size_state_missing {
                ", immediate_validation=true"
            } else {
                ""
            }
        );

        Ok(tracker)
    }

    // NOTE: update_size() and update_write_cache_size() methods have been removed.
    // Size tracking is now handled by JournalConsolidator through journal entries.
    // See requirements.md section 5.3: "update_size() method on CacheSizeTracker is removed"

    /// Get current write cache size (delegates to consolidator - Task 12.4)
    /// Requirement 6.3: Track write cache size separately
    pub async fn get_write_cache_size(&self) -> u64 {
        self.consolidator.get_write_cache_size().await
    }

    // NOTE: set_write_cache_size() has been removed - consolidator handles all size state.

    // NOTE: update_size_sync() method has been removed.
    // Size tracking is now handled by JournalConsolidator through journal entries.
    // Tests should use the JournalConsolidator API instead.

    /// Get current tracked size (delegates to consolidator - Task 12.3)
    pub async fn get_size(&self) -> u64 {
        self.consolidator.get_current_size().await
    }

    /// Get validation path for testing
    pub fn get_validation_path(&self) -> &std::path::Path {
        &self.validation_path
    }

    /// Set cache manager reference for GET cache expiration
    pub fn set_cache_manager(&self, cache_manager: Weak<crate::cache::CacheManager>) {
        *self.cache_manager.lock().unwrap() = Some(cache_manager);
    }

    /// Get actively_remove_cached_data flag
    pub fn is_active_expiration_enabled(&self) -> bool {
        self.actively_remove_cached_data
    }

    /// Get metrics for monitoring
    ///
    /// Note: Checkpoint-related metrics have been removed. Size tracking is now handled
    /// by the JournalConsolidator which exposes metrics via get_size_state().
    pub async fn get_metrics(&self) -> CacheSizeMetrics {
        // Read validation metadata if it exists
        let (last_validation_time, last_validation_drift) =
            match self.read_validation_metadata().await {
                Ok(metadata) => (Some(metadata.last_validation), Some(metadata.drift_bytes)),
                Err(_) => (None, None),
            };

        CacheSizeMetrics {
            current_size: self.get_size().await,
            write_cache_size: self.get_write_cache_size().await,
            last_checkpoint: SystemTime::now(), // Deprecated - consolidator handles persistence
            last_validation: last_validation_time,
            last_validation_drift,
            checkpoint_count: 0, // Deprecated - consolidator handles persistence
            delta_log_size: 0,   // Deprecated - delta files no longer used
        }
    }

    /// Shutdown and flush pending state
    ///
    /// Note: Checkpoint writing has been removed. The JournalConsolidator handles
    /// final size state persistence during shutdown.
    pub async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down cache size tracker");

        // Stop background validation task
        if let Some(handle) = self.validation_task.lock().unwrap().take() {
            handle.abort();
        }

        // Note: Checkpoint writing removed - JournalConsolidator handles final persist
        // via run_consolidation_cycle() during shutdown

        info!("Cache size tracker shutdown complete");
        Ok(())
    }

    // NOTE: flush_delta_log() method has been removed as part of Task 11.
    // Delta files are no longer used - size tracking is handled by JournalConsolidator.

    // NOTE: recover() method has been removed as part of Task 12.6.
    // Size state recovery is now handled by JournalConsolidator.initialize().
    // The consolidator loads size from size_state.json on startup and is the
    // single source of truth for cache size.

    // NOTE: The following methods have been removed as part of Task 11 (Remove Checkpoint Background Task):
    // - read_all_per_instance_delta_files() - delta files no longer used
    // - read_delta_log_with_write_cache() - delta files no longer used
    // - read_checkpoint() - checkpoint.json replaced by size_state.json
    // - read_delta_log() - delta files no longer used
    // - write_checkpoint() - consolidator handles persistence via size_state.json
    // - archive_and_truncate_all_delta_files() - delta files no longer used
    // - cleanup_old_delta_archives() - delta files no longer used
    // - cleanup_stale_delta_files() - delta files no longer used
    //
    // Size tracking is now handled by JournalConsolidator which persists to size_state.json
    // after each consolidation cycle (every 5 seconds).

    /// Read validation metadata
    pub async fn read_validation_metadata(&self) -> Result<ValidationMetadata> {
        let content = tokio::fs::read_to_string(&self.validation_path)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to read validation metadata: {}", e))
            })?;

        let metadata: ValidationMetadata = serde_json::from_str(&content).map_err(|e| {
            ProxyError::CacheError(format!("Failed to parse validation metadata: {}", e))
        })?;

        Ok(metadata)
    }

    /// Write validation metadata
    pub async fn write_validation_metadata(
        &self,
        scanned_size: u64,
        tracked_size: u64,
        drift: i64,
        duration: Duration,
        files_scanned: u64,
        cache_expired: u64,
        cache_skipped: u64,
        cache_errors: u64,
    ) -> Result<()> {
        self.write_validation_metadata_with_write_cache(
            scanned_size,
            tracked_size,
            drift,
            duration,
            files_scanned,
            cache_expired,
            cache_skipped,
            cache_errors,
            0, // write_cache_size
            0, // write_cache_expired
            0, // incomplete_uploads_evicted
        )
        .await
    }

    /// Write validation metadata with write cache information
    /// Requirement 6.3: Track write cache size separately
    pub async fn write_validation_metadata_with_write_cache(
        &self,
        scanned_size: u64,
        tracked_size: u64,
        drift: i64,
        duration: Duration,
        files_scanned: u64,
        cache_expired: u64,
        cache_skipped: u64,
        cache_errors: u64,
        write_cache_size: u64,
        write_cache_expired: u64,
        incomplete_uploads_evicted: u64,
    ) -> Result<()> {
        let metadata = ValidationMetadata {
            last_validation: SystemTime::now(),
            scanned_size,
            tracked_size,
            drift_bytes: drift,
            scan_duration_ms: duration.as_millis() as u64,
            metadata_files_scanned: files_scanned,
            cache_entries_expired: cache_expired,
            cache_entries_skipped: cache_skipped,
            cache_expiration_errors: cache_errors,
            active_expiration_enabled: self.actively_remove_cached_data,
            write_cache_size,
            write_cache_expired,
            incomplete_uploads_evicted,
        };

        let json = serde_json::to_string_pretty(&metadata).map_err(|e| {
            ProxyError::CacheError(format!("Failed to serialize validation metadata: {}", e))
        })?;

        tokio::fs::write(&self.validation_path, json)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to write validation metadata: {}", e))
            })?;

        // Update last validation time
        *self.last_validation.lock().unwrap() = Instant::now();

        Ok(())
    }

    // NOTE: start_checkpoint_task() and checkpoint_loop() have been removed as part of Task 11.
    // Size tracking and persistence is now handled by JournalConsolidator which:
    // - Runs consolidation every 5 seconds
    // - Persists size_state.json after each cycle
    // - Triggers eviction when cache exceeds capacity
    // See requirements.md section 5.4: "Checkpoint background task is removed"

    /// Start background validation task
    pub fn start_validation_task(self: &std::sync::Arc<Self>) {
        if !self.config.validation_enabled {
            info!("Validation disabled, not starting validation task");
            return;
        }

        let tracker = Arc::clone(self);

        let handle = tokio::spawn(async move {
            tracker.validation_scheduler().await;
        });

        *self.validation_task.lock().unwrap() = Some(handle);
    }

    /// Validation scheduler - runs once per day at configured time with fixed 1-hour jitter
    async fn validation_scheduler(&self) {
        loop {
            // Calculate next scheduled validation time
            let next_validation_time = self.calculate_next_validation_time().await;

            // Calculate sleep duration until next validation
            let now = SystemTime::now();
            let sleep_duration = next_validation_time
                .duration_since(now)
                .unwrap_or(Duration::ZERO);

            // Format timestamp for logging
            let next_time_chrono: chrono::DateTime<chrono::Local> = next_validation_time.into();
            info!(
                "Cache validation: next run {} (in {})",
                next_time_chrono.format("%Y-%m-%d %H:%M"),
                format_duration_human(sleep_duration)
            );

            // Sleep until scheduled time
            tokio::time::sleep(sleep_duration).await;

            // Attempt validation
            if let Err(e) = self.perform_validation().await {
                error!("Validation failed: {}", e);
            }

            // Loop to schedule next day's validation
        }
    }

    /// Calculate next validation time based on configured time of day with fixed 1-hour jitter
    pub async fn calculate_next_validation_time(&self) -> SystemTime {
        use chrono::{Duration as ChronoDuration, Local, Timelike};
        use rand::Rng;

        // Check if validation metadata exists - if not, run immediately
        if self.read_validation_metadata().await.is_err() {
            info!("No validation metadata found, scheduling immediate validation");
            return SystemTime::now();
        }

        // Parse configured time of day (e.g., "00:00" for midnight)
        let (target_hour, target_minute) =
            self.parse_time_of_day(&self.config.validation_time_of_day);

        // Get current local time
        let now = Local::now();

        // Calculate next occurrence of target time
        let mut next_time = now
            .with_hour(target_hour)
            .unwrap()
            .with_minute(target_minute)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();

        // If target time has already passed today, schedule for tomorrow
        if next_time <= now {
            next_time = next_time + ChronoDuration::days(1);
        }

        // Check if validation already ran in the last 23 hours (leave 1 hour buffer for jitter)
        if let Ok(metadata) = self.read_validation_metadata().await {
            let elapsed = SystemTime::now()
                .duration_since(metadata.last_validation)
                .unwrap_or(Duration::MAX);

            // Only skip if validation ran less than 23 hours ago
            if elapsed < Duration::from_secs(82800) {
                // 23 hours
                // If we're already past today's target time, next_time is already tomorrow
                // Don't add another day
                debug!(
                    "Validation ran {} ago, next run at {}",
                    format_duration_human(elapsed),
                    next_time
                );
            }
        }

        // Add fixed 1-hour jitter to prevent thundering herd
        let jitter = {
            let mut rng = rand::thread_rng();
            Duration::from_secs(rng.gen_range(0..=3600))
        };

        // Convert to SystemTime and add jitter
        let next_system_time: SystemTime = next_time.into();
        next_system_time + jitter
    }

    /// Parse time of day string in "HH:MM" format
    fn parse_time_of_day(&self, time_str: &str) -> (u32, u32) {
        // Parse "HH:MM" format
        let parts: Vec<&str> = time_str.split(':').collect();
        if parts.len() != 2 {
            warn!("Invalid time format '{}', using midnight", time_str);
            return (0, 0);
        }

        let hour = parts[0].parse::<u32>().unwrap_or(0).min(23);
        let minute = parts[1].parse::<u32>().unwrap_or(0).min(59);

        (hour, minute)
    }

    /// Clean up incomplete multipart uploads older than TTL
    ///
    /// Scans mpus_in_progress/ directory for uploads that have exceeded
    /// the incomplete_upload_ttl and removes them.
    ///
    /// # Requirements
    /// - Requirement 5.3, 5.4: Incomplete uploads should always be cleaned up
    ///
    /// # Returns
    /// Number of bytes freed
    async fn cleanup_incomplete_uploads(&self) -> Result<u64> {
        let mpus_dir = self.cache_dir.join("mpus_in_progress");

        if !mpus_dir.exists() {
            debug!("No mpus_in_progress directory, nothing to clean up");
            return Ok(0);
        }

        let now = SystemTime::now();
        let incomplete_upload_ttl = self.config.incomplete_upload_ttl;
        let mut total_freed: u64 = 0;
        let mut evicted_count: u64 = 0;

        // Read directory entries
        let mut entries = match tokio::fs::read_dir(&mpus_dir).await {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to read mpus_in_progress directory: {}", e);
                return Ok(0);
            }
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let upload_dir = entry.path();

            if !upload_dir.is_dir() {
                continue;
            }

            let upload_meta_path = upload_dir.join("upload.meta");

            // Check age based on file mtime
            let age = if upload_meta_path.exists() {
                match tokio::fs::metadata(&upload_meta_path).await {
                    Ok(metadata) => match metadata.modified() {
                        Ok(modified) => now.duration_since(modified).unwrap_or_default(),
                        Err(_) => Duration::from_secs(0),
                    },
                    Err(_) => Duration::from_secs(0),
                }
            } else {
                // No metadata file, check directory mtime
                match tokio::fs::metadata(&upload_dir).await {
                    Ok(metadata) => match metadata.modified() {
                        Ok(modified) => now.duration_since(modified).unwrap_or_default(),
                        Err(_) => Duration::from_secs(0),
                    },
                    Err(_) => Duration::from_secs(0),
                }
            };

            if age > incomplete_upload_ttl {
                // Parts are stored inside the upload directory, so just track directory size
                // and remove the whole directory
                let mut dir_size: u64 = 0;

                let mut dir_entries = match tokio::fs::read_dir(&upload_dir).await {
                    Ok(entries) => entries,
                    Err(_) => continue,
                };

                while let Ok(Some(dir_entry)) = dir_entries.next_entry().await {
                    let path = dir_entry.path();
                    if let Ok(metadata) = tokio::fs::metadata(&path).await {
                        dir_size += metadata.len();
                    }
                }

                total_freed += dir_size;

                if let Err(e) = tokio::fs::remove_dir_all(&upload_dir).await {
                    warn!("Failed to remove upload directory {:?}: {}", upload_dir, e);
                } else {
                    evicted_count += 1;
                    info!(
                        "Evicted incomplete upload during validation: dir={:?}, age={:?}",
                        upload_dir, age
                    );
                }
            }
        }

        if evicted_count > 0 {
            info!(
                "Incomplete upload cleanup: evicted={} uploads, freed={} bytes",
                evicted_count, total_freed
            );
        }

        Ok(total_freed)
    }

    /// Perform validation scan
    async fn perform_validation(&self) -> Result<()> {
        // Try to acquire global lock
        let _lock = match self.try_acquire_validation_lock().await {
            Ok(lock) => lock,
            Err(e) => {
                info!("Another instance is validating, skipping: {}", e);
                return Ok(());
            }
        };

        let start = Instant::now();

        // Always run incomplete upload cleanup during daily validation
        // Requirement 5.3, 5.4: Incomplete uploads should always be cleaned up
        let incomplete_uploads_freed = self.cleanup_incomplete_uploads().await.unwrap_or(0);
        if incomplete_uploads_freed > 0 {
            info!(
                "Incomplete upload cleanup during validation: freed {} bytes",
                format_bytes_human(incomplete_uploads_freed)
            );
        }

        // Scan metadata files using shared validator (returns size, cache_expired, cache_skipped, cache_errors)
        let (scanned_size, cache_expired, cache_skipped, cache_errors) =
            self.scan_metadata_with_shared_validator().await?;
        let tracked_size = self.get_size().await; // Delegate to consolidator (Task 12.3)
        let drift = scanned_size as i64 - tracked_size as i64;

        let duration = start.elapsed();
        let drift_sign = if drift >= 0 { "+" } else { "" };
        info!(
            "Cache validation: {} scanned, drift {}{}, expired {} GET, {}",
            format_bytes_human(scanned_size),
            drift_sign,
            format_bytes_human(drift.unsigned_abs()),
            cache_expired,
            format_duration_human(duration)
        );

        // Always reconcile to scanned size after validation
        // The validation scan is expensive (once per day), so we trust its result
        if drift != 0 {
            debug!(
                "Reconciling tracked size to scanned size: {} bytes drift",
                drift
            );
            // Update the consolidator's size state (Task 12 - consolidator is source of truth)
            self.consolidator
                .update_size_from_validation(scanned_size, None)
                .await;
        }

        // Write validation metadata
        let files_scanned = 0; // Will be updated when we implement scanning
        self.write_validation_metadata(
            scanned_size,
            tracked_size,
            drift,
            duration,
            files_scanned,
            cache_expired,
            cache_skipped,
            cache_errors,
        )
        .await?;

        Ok(())
    }

    /// Try to acquire validation lock
    pub async fn try_acquire_validation_lock(&self) -> Result<ValidationLock> {
        use fs2::FileExt;
        use std::fs::OpenOptions;

        // Ensure parent directory exists
        if let Some(parent) = self.validation_lock_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ProxyError::CacheError(format!("Failed to create validation lock directory: {}", e))
            })?;
        }

        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.validation_lock_path)
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to open validation lock file: {}", e))
            })?;

        // Try to acquire exclusive lock with timeout
        lock_file.try_lock_exclusive().map_err(|e| {
            ProxyError::CacheError(format!("Failed to acquire validation lock: {}", e))
        })?;

        debug!("Acquired validation lock");
        Ok(ValidationLock { file: lock_file })
    }

    /// Scan metadata files using shared validator (replaces redundant parallel scanning)
    ///
    /// This method now uses the shared CacheValidator to avoid duplicating scanning logic.
    /// The coordinated initialization handles the initial scan, and this method is used
    /// only for periodic validation scans.
    async fn scan_metadata_with_shared_validator(&self) -> Result<(u64, u64, u64, u64)> {
        use rayon::iter::ParallelBridge;
        use rayon::prelude::*;
        use std::sync::atomic::{AtomicU64, Ordering};
        use walkdir::WalkDir;

        let now = std::time::SystemTime::now();
        let metadata_dir = self.cache_dir.join("metadata");

        if !metadata_dir.exists() {
            return Ok((0, 0, 0, 0));
        }

        // Atomic counters for lock-free accumulation from parallel workers
        let total_size = AtomicU64::new(0);
        let cache_expired = AtomicU64::new(0);
        let cache_skipped = AtomicU64::new(0);
        let cache_errors = AtomicU64::new(0);
        let files_processed = AtomicU64::new(0);

        // Stream WalkDir directly into rayon's parallel iterator via par_bridge().
        // WalkDir yields entries lazily — no Vec<PathBuf> allocation.
        // par_bridge() feeds entries to rayon's work-stealing thread pool as they're discovered.
        // Memory: O(rayon_threads) instead of O(total_files).
        WalkDir::new(&metadata_dir)
            .follow_links(false)
            .into_iter()
            .filter_map(|entry| match entry {
                Ok(e) if e.path().extension().map_or(false, |ext| ext == "meta") => {
                    Some(e.into_path())
                }
                Ok(_) => None,
                Err(e) => {
                    warn!("Directory walk error during validation: {}", e);
                    None
                }
            })
            .par_bridge()
            .for_each(|path| {
                let result = self.scan_metadata_file(&path, now);
                total_size.fetch_add(result.size_bytes, Ordering::Relaxed);
                if result.cache_expired {
                    cache_expired.fetch_add(1, Ordering::Relaxed);
                }
                if result.cache_skipped {
                    cache_skipped.fetch_add(1, Ordering::Relaxed);
                }
                if result.cache_error {
                    cache_errors.fetch_add(1, Ordering::Relaxed);
                }
                let count = files_processed.fetch_add(1, Ordering::Relaxed) + 1;
                if count % 100_000 == 0 {
                    info!("Cache validation progress: {} files processed", count);
                }
            });

        let total = files_processed.load(Ordering::Relaxed);
        info!("Cache validation: processed {} metadata files", total);

        Ok((
            total_size.load(Ordering::Relaxed),
            cache_expired.load(Ordering::Relaxed),
            cache_skipped.load(Ordering::Relaxed),
            cache_errors.load(Ordering::Relaxed),
        ))
    }

    /// Scan metadata file and optionally delete if expired (GET cache expiration)
    fn scan_metadata_file(&self, path: &PathBuf, now: SystemTime) -> ScanFileResult {
        use crate::cache_types::NewCacheMetadata;

        // Read and parse metadata
        let content = match std::fs::read(path) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to read metadata file {:?}: {}", path, e);
                return ScanFileResult {
                    size_bytes: 0,
                    cache_expired: false,
                    cache_skipped: false,
                    cache_error: true,
                };
            }
        };

        let metadata: NewCacheMetadata = match serde_json::from_slice(&content) {
            Ok(m) => m,
            Err(e) => {
                warn!("Failed to parse metadata file {:?}: {}, removing invalid file and associated data", path, e);

                // Remove the unparseable metadata file
                if let Err(remove_err) = std::fs::remove_file(path) {
                    warn!(
                        "Failed to remove invalid metadata file {:?}: {}",
                        path, remove_err
                    );
                } else {
                    info!(
                        "Removed invalid metadata file during validation: {:?}",
                        path
                    );
                }

                return ScanFileResult {
                    size_bytes: 0,
                    cache_expired: false,
                    cache_skipped: false,
                    cache_error: true,
                };
            }
        };

        // Calculate total size from all ranges
        let total_size: u64 = metadata.ranges.iter().map(|r| r.compressed_size).sum();

        // Check if write cache entry is expired (Requirements 5.3, 5.4)
        // Write cache expiration is always checked, regardless of actively_remove_cached_data
        // because incomplete uploads should always be cleaned up
        if metadata.object_metadata.is_write_cached
            && metadata.object_metadata.is_write_cache_expired()
        {
            info!(
                "Write cache entry expired during validation: {}",
                metadata.cache_key
            );

            // Get cache manager reference to delete the entry
            let cache_manager = match self.cache_manager.lock().unwrap().as_ref() {
                Some(weak_ref) => match weak_ref.upgrade() {
                    Some(cm) => cm,
                    None => {
                        warn!("Cache manager reference is no longer valid for write cache cleanup");
                        return ScanFileResult {
                            size_bytes: total_size,
                            cache_expired: false,
                            cache_skipped: false,
                            cache_error: true,
                        };
                    }
                },
                None => {
                    // Cache manager not set, skip expiration
                    return ScanFileResult {
                        size_bytes: total_size,
                        cache_expired: false,
                        cache_skipped: false,
                        cache_error: false,
                    };
                }
            };

            // Delete the expired write cache entry
            let delete_result = tokio::runtime::Handle::try_current()
                .ok()
                .and_then(|handle| {
                    handle.block_on(async {
                        cache_manager
                            .check_and_invalidate_expired_write_cache(&metadata.cache_key)
                            .await
                            .ok()
                    })
                });

            match delete_result {
                Some(true) => {
                    debug!("Deleted expired write cache entry: {}", metadata.cache_key);
                    return ScanFileResult {
                        size_bytes: 0,
                        cache_expired: true,
                        cache_skipped: false,
                        cache_error: false,
                    };
                }
                Some(false) => {
                    // Entry was not expired or not write-cached (shouldn't happen here)
                    debug!(
                        "Write cache entry not deleted (not expired?): {}",
                        metadata.cache_key
                    );
                }
                None => {
                    warn!(
                        "Failed to delete expired write cache entry: {}",
                        metadata.cache_key
                    );
                    return ScanFileResult {
                        size_bytes: total_size,
                        cache_expired: false,
                        cache_skipped: false,
                        cache_error: true,
                    };
                }
            }
        }

        // Check if GET cache active expiration is enabled and entry is expired
        if self.actively_remove_cached_data && now > metadata.expires_at {
            debug!(
                "GET cache entry expired during validation: {}",
                metadata.cache_key
            );

            // Get cache manager reference to check if entry is active
            let cache_manager = match self.cache_manager.lock().unwrap().as_ref() {
                Some(weak_ref) => match weak_ref.upgrade() {
                    Some(cm) => cm,
                    None => {
                        warn!("Cache manager reference is no longer valid");
                        return ScanFileResult {
                            size_bytes: total_size,
                            cache_expired: false,
                            cache_skipped: false,
                            cache_error: true,
                        };
                    }
                },
                None => {
                    // Cache manager not set, skip expiration
                    return ScanFileResult {
                        size_bytes: total_size,
                        cache_expired: false,
                        cache_skipped: false,
                        cache_error: false,
                    };
                }
            };

            // Check if entry is actively being used (blocking call in parallel context)
            // This is safe because we're in a rayon parallel iterator
            let is_active = tokio::runtime::Handle::try_current()
                .ok()
                .and_then(|handle| {
                    handle.block_on(async {
                        cache_manager
                            .is_cache_entry_active(&metadata.cache_key)
                            .await
                            .ok()
                    })
                })
                .unwrap_or(true); // If we can't check, assume active to be safe

            if is_active {
                debug!(
                    "Skipping deletion of {} - actively being used",
                    metadata.cache_key
                );
                return ScanFileResult {
                    size_bytes: total_size,
                    cache_expired: false,
                    cache_skipped: true,
                    cache_error: false,
                };
            }

            // Safe to delete - entry is expired and not actively being used
            let delete_result = tokio::runtime::Handle::try_current()
                .ok()
                .and_then(|handle| {
                    handle.block_on(async {
                        cache_manager
                            .invalidate_cache(&metadata.cache_key)
                            .await
                            .ok()
                    })
                });

            match delete_result {
                Some(_) => {
                    debug!("Deleted expired GET cache entry: {}", metadata.cache_key);
                    // Don't count size since we deleted it
                    return ScanFileResult {
                        size_bytes: 0,
                        cache_expired: true,
                        cache_skipped: false,
                        cache_error: false,
                    };
                }
                None => {
                    warn!(
                        "Failed to delete expired GET cache entry: {}",
                        metadata.cache_key
                    );
                    return ScanFileResult {
                        size_bytes: total_size,
                        cache_expired: false,
                        cache_skipped: false,
                        cache_error: true,
                    };
                }
            }
        }

        // Entry not expired or active expiration disabled
        ScanFileResult {
            size_bytes: total_size,
            cache_expired: false,
            cache_skipped: false,
            cache_error: false,
        }
    }
}

/// RAII guard for validation lock
pub struct ValidationLock {
    file: std::fs::File,
}

impl Drop for ValidationLock {
    fn drop(&mut self) {
        #[allow(unused_imports)]
        use fs2::FileExt;
        let _ = self.file.unlock();
        debug!("Released validation lock");
    }
}

// Custom serde serialization for SystemTime
mod systemtime_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::{SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(time: &SystemTime, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let duration = time
            .duration_since(UNIX_EPOCH)
            .map_err(serde::ser::Error::custom)?;
        duration.as_secs().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + std::time::Duration::from_secs(secs))
    }
}

mod option_systemtime_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::{SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(
        time: &Option<SystemTime>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match time {
            Some(t) => {
                let duration = t
                    .duration_since(UNIX_EPOCH)
                    .map_err(serde::ser::Error::custom)?;
                Some(duration.as_secs()).serialize(serializer)
            }
            None => None::<u64>.serialize(serializer),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Option<SystemTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs_opt = Option::<u64>::deserialize(deserializer)?;
        Ok(secs_opt.map(|secs| UNIX_EPOCH + std::time::Duration::from_secs(secs)))
    }
}

// NOTE: get_instance_id() function has been removed as part of Task 11.
// It was only used by checkpoint/delta file handling which has been removed.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
    use crate::journal_manager::JournalManager;
    use crate::metadata_lock_manager::MetadataLockManager;
    use tempfile::TempDir;

    /// Helper to create a test tracker with a mock consolidator
    async fn create_test_tracker() -> (Arc<CacheSizeTracker>, Arc<JournalConsolidator>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();

        // Create required directories
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

        // Create mock dependencies for JournalConsolidator
        let journal_manager = Arc::new(JournalManager::new(
            cache_dir.clone(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            cache_dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidation_config = ConsolidationConfig::default();

        // Create the consolidator
        let consolidator = Arc::new(JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager,
            lock_manager,
            consolidation_config,
        ));

        // Initialize the consolidator
        consolidator.initialize().await.unwrap();

        let config = CacheSizeConfig::default();
        let tracker = Arc::new(
            CacheSizeTracker::new(cache_dir, config, false, consolidator.clone())
                .await
                .unwrap(),
        );
        (tracker, consolidator, temp_dir)
    }

    // NOTE: Tests for update_size() and update_size_sync() have been removed.
    // Size tracking is now handled by JournalConsolidator through journal entries.
    // See Task 10 in journal-based-size-tracking spec.

    #[tokio::test]
    async fn test_validation_metadata_persistence() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // Write validation metadata
        tracker
            .write_validation_metadata(
                1000000, // scanned_size
                1000500, // tracked_size
                -500,    // drift
                Duration::from_secs(120),
                50000, // files_scanned
                5,     // cache_expired
                1,     // cache_skipped
                0,     // cache_errors
            )
            .await
            .unwrap();

        // Read it back
        let metadata = tracker.read_validation_metadata().await.unwrap();

        assert_eq!(metadata.scanned_size, 1000000);
        assert_eq!(metadata.tracked_size, 1000500);
        assert_eq!(metadata.drift_bytes, -500);
        assert_eq!(metadata.scan_duration_ms, 120000);
        assert_eq!(metadata.metadata_files_scanned, 50000);
        assert_eq!(metadata.cache_entries_expired, 5);
        assert_eq!(metadata.cache_entries_skipped, 1);
        assert_eq!(metadata.cache_expiration_errors, 0);
        assert_eq!(metadata.active_expiration_enabled, false);
    }

    #[tokio::test]
    async fn test_metrics_collection() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // Get metrics - size starts at 0
        let metrics = tracker.get_metrics().await;

        assert_eq!(metrics.current_size, 0);
        assert_eq!(metrics.checkpoint_count, 0);
        assert!(metrics.last_validation.is_none()); // No validation yet
    }

    #[tokio::test]
    async fn test_actively_remove_cached_data_flag() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();

        // Create required directories
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

        // Create mock dependencies for JournalConsolidator
        let journal_manager = Arc::new(JournalManager::new(
            cache_dir.clone(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            cache_dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidation_config = ConsolidationConfig::default();
        let consolidator = Arc::new(JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager,
            lock_manager,
            consolidation_config,
        ));
        consolidator.initialize().await.unwrap();

        let config = CacheSizeConfig::default();

        // Create tracker with flag disabled
        let tracker_disabled = CacheSizeTracker::new(
            cache_dir.clone(),
            config.clone(),
            false,
            consolidator.clone(),
        )
        .await
        .unwrap();
        assert_eq!(tracker_disabled.actively_remove_cached_data, false);

        // Create tracker with flag enabled
        let tracker_enabled = CacheSizeTracker::new(cache_dir, config, true, consolidator)
            .await
            .unwrap();
        assert_eq!(tracker_enabled.actively_remove_cached_data, true);
    }

    #[tokio::test]
    async fn test_recovery_with_no_checkpoint() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // Should start at 0 (consolidator has no size state)
        assert_eq!(tracker.get_size().await, 0);
    }

    // NOTE: test_size_never_goes_negative and test_checkpoint_count_increments removed
    // as they relied on update_size() which has been removed.
    // Size tracking is now handled by JournalConsolidator.

    #[test]
    fn test_format_bytes_human() {
        assert_eq!(format_bytes_human(0), "0 B");
        assert_eq!(format_bytes_human(512), "512 B");
        assert_eq!(format_bytes_human(1024), "1.0 KiB");
        assert_eq!(format_bytes_human(1536), "1.5 KiB");
        assert_eq!(format_bytes_human(1024 * 1024), "1.0 MiB");
        assert_eq!(format_bytes_human(1024 * 1024 * 1024), "1.0 GiB");
        assert_eq!(format_bytes_human(1024 * 1024 * 1024 * 1024), "1.0 TiB");
        assert_eq!(format_bytes_human(655865624), "625.5 MiB");
    }

    #[test]
    fn test_format_duration_human() {
        assert_eq!(format_duration_human(Duration::from_millis(16)), "16ms");
        assert_eq!(format_duration_human(Duration::from_millis(500)), "500ms");
        assert_eq!(format_duration_human(Duration::from_secs(5)), "5s");
        assert_eq!(format_duration_human(Duration::from_secs(65)), "1m 5s");
        assert_eq!(format_duration_human(Duration::from_secs(3665)), "1h 1m");
        assert_eq!(
            format_duration_human(Duration::from_secs(173693)),
            "48h 14m"
        );
    }

    // ============================================================
    // Size State Recovery Tests (Task 11 & 12)
    // ============================================================
    // NOTE: Delta recovery tests have been removed as part of Task 11.
    // Checkpoint and delta file handling has been removed - size tracking
    // is now handled by JournalConsolidator via size_state.json.
    //
    // Task 12: CacheSizeTracker now delegates to JournalConsolidator for size queries.
    // These tests verify that the consolidator correctly loads size state and the
    // tracker correctly delegates to it.

    /// Test recovery from size_state.json via consolidator
    /// Verifies that the tracker correctly gets size from the consolidator's size state.
    #[tokio::test]
    async fn test_recovery_from_size_state_json() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();

        // Create required directories
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

        // Create a size_state.json file (as would be created by JournalConsolidator)
        let size_state = serde_json::json!({
            "total_size": 50000,
            "write_cache_size": 10000,
            "last_consolidation": 1706300000,
            "consolidation_count": 100,
            "last_updated_by": "test-instance:12345"
        });
        std::fs::write(
            cache_dir.join("size_tracking").join("size_state.json"),
            serde_json::to_string_pretty(&size_state).unwrap(),
        )
        .unwrap();

        // Create mock dependencies for JournalConsolidator
        let journal_manager = Arc::new(JournalManager::new(
            cache_dir.clone(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            cache_dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidation_config = ConsolidationConfig::default();
        let consolidator = Arc::new(JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager,
            lock_manager,
            consolidation_config,
        ));

        // Initialize consolidator - this loads size_state.json
        consolidator.initialize().await.unwrap();

        // Create tracker with consolidator reference
        let config = CacheSizeConfig::default();
        let tracker = CacheSizeTracker::new(cache_dir, config, false, consolidator)
            .await
            .unwrap();

        assert_eq!(
            tracker.get_size().await,
            50000,
            "Should get total size from consolidator"
        );
        assert_eq!(
            tracker.get_write_cache_size().await,
            10000,
            "Should get write cache size from consolidator"
        );
    }

    /// Test recovery with missing size_state.json
    /// Verifies that the tracker starts from zero when no size state file exists.
    #[tokio::test]
    async fn test_recovery_with_missing_size_state() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // Should start at 0 (consolidator has no size state file)
        assert_eq!(
            tracker.get_size().await,
            0,
            "Should start from zero when no size_state.json exists"
        );
        assert_eq!(
            tracker.get_write_cache_size().await,
            0,
            "Write cache should start from zero"
        );
    }

    /// Test recovery with malformed size_state.json
    /// Verifies that the consolidator handles invalid JSON gracefully.
    #[tokio::test]
    async fn test_recovery_with_malformed_size_state() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();

        // Create required directories
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

        // Create a malformed size_state.json
        std::fs::write(
            cache_dir.join("size_tracking").join("size_state.json"),
            "{ invalid json }",
        )
        .unwrap();

        // Create mock dependencies for JournalConsolidator
        let journal_manager = Arc::new(JournalManager::new(
            cache_dir.clone(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            cache_dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidation_config = ConsolidationConfig::default();
        let consolidator = Arc::new(JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager,
            lock_manager,
            consolidation_config,
        ));

        // Initialize consolidator - should handle malformed JSON gracefully
        consolidator.initialize().await.unwrap();

        // Create tracker with consolidator reference
        let config = CacheSizeConfig::default();
        let tracker = CacheSizeTracker::new(cache_dir, config, false, consolidator)
            .await
            .unwrap();

        assert_eq!(
            tracker.get_size().await,
            0,
            "Should start from zero when size_state.json is malformed"
        );
    }
}
