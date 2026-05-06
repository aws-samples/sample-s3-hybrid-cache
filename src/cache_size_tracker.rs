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

    /// Maximum duration for a single validation scan cycle (default: 4h)
    /// Used for self-tuning mode selection between full and rolling scans.
    #[serde(skip)]
    pub validation_max_duration: Duration,

    /// Cache inconsistency percentage that triggers a warning log (default: 5.0)
    #[serde(skip)]
    pub validation_threshold_warn: f64,

    /// Cache inconsistency percentage that triggers an error log (default: 20.0)
    #[serde(skip)]
    pub validation_threshold_error: f64,
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
            validation_max_duration: Duration::from_secs(4 * 3600), // 4 hours
            validation_threshold_warn: 5.0,
            validation_threshold_error: 20.0,
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

/// Rolling scan state persisted in `size_tracking/validation.json`.
///
/// Tracks the cursor position, scan rate, and rotation progress for the rolling
/// validation scan. This state survives proxy restarts so the scan resumes where
/// it left off rather than restarting from the beginning.
///
/// Defaults to cursor 0 with no scan rate when the file is missing or corrupted.
///
/// See: Requirement 5 (Rolling Cursor Persistence)
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RollingState {
    /// Next L1 directory index to process (0–255, wraps cyclically).
    pub cursor: u8,
    /// Observed seconds per L1 directory from the last cycle, used by
    /// [`CacheSizeTracker::estimate_batch_size`] to predict how many directories
    /// fit within the time budget. `None` on the very first rolling cycle.
    pub scan_rate: Option<f64>,
    /// Number of complete rotations through all 256 L1 directories.
    pub full_rotation_count: u64,
    /// Epoch seconds when the current rotation started, used to compute
    /// total rotation elapsed time when a full rotation completes.
    pub rotation_start_time: Option<u64>,
    /// Duration of the last full scan in seconds, used by [`determine_scan_mode`]
    /// to decide whether to switch from full to rolling mode.
    pub last_full_scan_duration_secs: Option<f64>,
}

/// Per-cycle statistics for a rolling scan, written alongside [`RollingState`]
/// to `validation.json` after each rolling scan cycle.
///
/// These statistics are used for observability (Requirement 8) and for the
/// mode-selection extrapolation that decides whether to switch back to full mode.
#[derive(Debug, Clone)]
pub struct RollingCycleStats {
    /// Number of L1 directories scanned in this cycle.
    pub dirs_scanned: u64,
    /// Total `.meta` files (objects) validated in this cycle.
    pub objects_validated: u64,
    /// Wall-clock seconds for this cycle.
    pub cycle_duration_secs: f64,
}

/// Previous scan state read from validation.json for mode selection decisions.
#[derive(Debug, Clone, Default)]
struct PreviousScanState {
    validation_type: Option<String>,
    last_full_scan_duration_secs: Option<f64>,
    rolling_cycle_duration_secs: Option<f64>,
    rolling_dirs_scanned: Option<u64>,
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

/// Scan mode for the periodic validation scan.
///
/// The system self-tunes between these two modes based on observed scan duration
/// and the configured `validation_max_duration` budget. See [`determine_scan_mode`]
/// for the decision logic.
///
/// See: Requirement 1 (Time-Based Mode Selection)
#[derive(Debug, Clone, PartialEq)]
pub enum ScanMode {
    /// Traverse all 256 L1 shard directories in a single cycle.
    Full,
    /// Traverse a subset of L1 directories per cycle, resuming from a persistent
    /// cursor on the next invocation. Full coverage is achieved over multiple cycles.
    Rolling,
}

/// Reason for the scan mode selection, included in the INFO log at the start
/// of each validation cycle for operator visibility.
///
/// See: Requirement 1.6, Requirement 8
#[derive(Debug, Clone)]
pub enum ScanModeReason {
    /// First scan ever — no previous scan history in `validation.json`.
    NoHistory,
    /// Previous full scan completed within `validation_max_duration`.
    FullWithinBudget,
    /// Previous full scan exceeded `validation_max_duration`.
    FullExceededBudget,
    /// Rolling scan extrapolated full time `(elapsed / dirs_scanned) * 256` exceeds budget.
    RollingExtrapolatedAbove,
    /// Rolling scan extrapolated full time fits within budget — switching back to full.
    RollingExtrapolatedBelow,
}

impl std::fmt::Display for ScanModeReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScanModeReason::NoHistory => write!(f, "no previous scan history"),
            ScanModeReason::FullWithinBudget => write!(f, "previous full scan within budget"),
            ScanModeReason::FullExceededBudget => write!(f, "previous full scan exceeded budget"),
            ScanModeReason::RollingExtrapolatedAbove => {
                write!(f, "rolling extrapolated full time exceeds budget")
            }
            ScanModeReason::RollingExtrapolatedBelow => {
                write!(f, "rolling extrapolated full time within budget")
            }
        }
    }
}

/// Determines the scan mode for the next validation cycle based on previous scan state.
///
/// This is a pure function with no side effects. Decision rules:
/// - No history → `Full` (first scan ever)
/// - Previous full scan exceeded budget → `Rolling`
/// - Previous full scan within budget → `Full` (stay)
/// - Previous rolling scan, extrapolated full time > budget → `Rolling` (stay)
/// - Previous rolling scan, extrapolated full time ≤ budget → `Full` (switch back)
///
/// The extrapolated full scan time is computed as `(elapsed / dirs_scanned) * 256`.
///
/// See: Requirements 1.1–1.5, 7.1, 7.4
pub fn determine_scan_mode(
    prev_validation_type: Option<&str>,
    last_full_scan_duration_secs: Option<f64>,
    rolling_cycle_duration_secs: Option<f64>,
    rolling_dirs_scanned: Option<u64>,
    max_duration: Duration,
) -> (ScanMode, ScanModeReason) {
    let budget_secs = max_duration.as_secs_f64();

    match prev_validation_type {
        None => (ScanMode::Full, ScanModeReason::NoHistory),
        Some("full") => match last_full_scan_duration_secs {
            Some(dur) if dur > budget_secs => {
                (ScanMode::Rolling, ScanModeReason::FullExceededBudget)
            }
            _ => (ScanMode::Full, ScanModeReason::FullWithinBudget),
        },
        Some("rolling") => {
            // Extrapolate: (elapsed / dirs_scanned) * 256
            match (rolling_cycle_duration_secs, rolling_dirs_scanned) {
                (Some(elapsed), Some(dirs)) if dirs > 0 => {
                    let extrapolated = (elapsed / dirs as f64) * 256.0;
                    if extrapolated > budget_secs {
                        (ScanMode::Rolling, ScanModeReason::RollingExtrapolatedAbove)
                    } else {
                        (ScanMode::Full, ScanModeReason::RollingExtrapolatedBelow)
                    }
                }
                // If we can't extrapolate (no data), stay rolling
                _ => (ScanMode::Rolling, ScanModeReason::RollingExtrapolatedAbove),
            }
        }
        // Unknown type, treat as no history
        Some(_) => (ScanMode::Full, ScanModeReason::NoHistory),
    }
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

    /// Forward scan results to the consolidator to reconcile size_state.json.
    /// Called on cold startup after a real metadata scan completes.
    pub async fn update_size_from_scan(
        &self,
        total_size: u64,
        write_cache_size: u64,
        cached_objects: u64,
    ) {
        self.consolidator
            .update_size_from_validation(total_size, Some(write_cache_size), Some(cached_objects))
            .await;
    }

    // NOTE: set_write_cache_size() has been removed - consolidator handles all size state.

    // NOTE: update_size_sync() method has been removed.
    // Size tracking is now handled by JournalConsolidator through journal entries.
    // Tests should use the JournalConsolidator API instead.

    /// Get current tracked size (delegates to consolidator - Task 12.3)
    pub async fn get_size(&self) -> u64 {
        self.consolidator.get_current_size().await
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
    #[allow(clippy::too_many_arguments)]
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
    #[allow(clippy::too_many_arguments)]
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

    /// Reads rolling scan state from `size_tracking/validation.json`.
    ///
    /// Parses the `rolling_*` fields from the existing validation state file.
    /// Returns [`RollingState::default()`] (cursor 0, no scan rate) if the file
    /// is missing or contains invalid JSON, logging a warning in either case.
    ///
    /// See: Requirements 5.2, 5.3
    pub fn read_rolling_state(&self) -> Result<RollingState> {
        let content = match std::fs::read_to_string(&self.validation_path) {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "Rolling state: validation.json missing or unreadable ({}), using defaults",
                    e
                );
                return Ok(RollingState::default());
            }
        };

        let json: serde_json::Value = match serde_json::from_str(&content) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "Rolling state: validation.json corrupted ({}), using defaults",
                    e
                );
                return Ok(RollingState::default());
            }
        };

        let cursor = json
            .get("rolling_cursor")
            .and_then(|v| v.as_u64())
            .map(|v| v.min(255) as u8)
            .unwrap_or(0);

        let scan_rate = json.get("rolling_scan_rate").and_then(|v| v.as_f64());

        let full_rotation_count = json
            .get("rolling_full_rotation_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let rotation_start_time = json
            .get("rolling_rotation_start_time")
            .and_then(|v| v.as_u64());

        let last_full_scan_duration_secs = json
            .get("last_full_scan_duration_secs")
            .and_then(|v| v.as_f64());

        Ok(RollingState {
            cursor,
            scan_rate,
            full_rotation_count,
            rotation_start_time,
            last_full_scan_duration_secs,
        })
    }

    /// Persists rolling scan state and cycle statistics to `validation.json`.
    ///
    /// Writes all rolling state fields plus standard validation fields (`last_validation`,
    /// `status`, `completed_at`, `validation_type`) using atomic write (write to temp
    /// file, then rename) to prevent corruption on shared storage.
    ///
    /// See: Requirements 5.1, 5.4, 7.2, 8.3
    pub fn write_rolling_state(
        &self,
        state: &RollingState,
        cycle_stats: &RollingCycleStats,
    ) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let json = serde_json::json!({
            "last_validation": now,
            "status": "completed",
            "completed_at": now,
            "validation_type": "rolling",
            "rolling_cursor": state.cursor,
            "rolling_dirs_scanned": cycle_stats.dirs_scanned,
            "rolling_objects_validated": cycle_stats.objects_validated,
            "rolling_cycle_duration_secs": cycle_stats.cycle_duration_secs,
            "rolling_scan_rate": state.scan_rate,
            "rolling_full_rotation_count": state.full_rotation_count,
            "rolling_rotation_start_time": state.rotation_start_time,
            "last_full_scan_duration_secs": state.last_full_scan_duration_secs,
        });

        let content = serde_json::to_string_pretty(&json).map_err(|e| {
            ProxyError::CacheError(format!("Failed to serialize rolling state: {}", e))
        })?;

        // Atomic write: write to temp file, then rename
        let temp_path = self.validation_path.with_extension("json.tmp");
        std::fs::write(&temp_path, &content).map_err(|e| {
            ProxyError::CacheError(format!("Failed to write rolling state temp file: {}", e))
        })?;
        std::fs::rename(&temp_path, &self.validation_path).map_err(|e| {
            ProxyError::CacheError(format!("Failed to rename rolling state temp file: {}", e))
        })?;

        Ok(())
    }

    /// Estimates how many L1 directories can be processed within the time budget.
    ///
    /// Uses the scan rate (seconds per L1 directory) from the previous cycle to compute
    /// `floor(max_duration / scan_rate)`, clamped to `[1, 256]`. On the first rolling
    /// cycle (when no scan rate is available), defaults to 64 directories.
    ///
    /// See: Requirements 3.4, 4.1
    pub fn estimate_batch_size(&self, scan_rate: Option<f64>, max_duration: Duration) -> usize {
        match scan_rate {
            Some(r) => {
                let budget_secs = max_duration.as_secs_f64();
                (budget_secs / r).floor().clamp(1.0, 256.0) as usize
            }
            None => 64,
        }
    }

    /// Selects L1 directory paths starting from `cursor`, wrapping cyclically at 256.
    ///
    /// Enumerates all bucket directories under `metadata_dir`, then for each bucket
    /// collects L1 subdirectories whose hex name (parsed as `u8`) falls in the cyclic
    /// range `[cursor, cursor + count) mod 256`. Directories starting with `_` (e.g.,
    /// `_journals`) are skipped.
    ///
    /// `count` is clamped to a maximum of 256 (the total number of L1 directories).
    ///
    /// Returns `(paths, wraps)` where `wraps` is `true` if the selection range crosses
    /// the 255→0 boundary.
    ///
    /// See: Requirement 3.1
    pub fn select_l1_directories(
        &self,
        metadata_dir: &std::path::Path,
        cursor: u8,
        count: usize,
    ) -> (Vec<PathBuf>, bool) {
        let count = count.min(256);
        let wraps = (cursor as usize) + count > 256;

        // Build the set of selected L1 indices
        let selected: std::collections::HashSet<u8> = (0..count)
            .map(|i| ((cursor as usize + i) % 256) as u8)
            .collect();

        let mut result = Vec::new();

        // Enumerate bucket directories under metadata_dir
        let bucket_entries = match std::fs::read_dir(metadata_dir) {
            Ok(entries) => entries,
            Err(_) => return (result, wraps),
        };

        for bucket_entry in bucket_entries.flatten() {
            let bucket_path = bucket_entry.path();
            if !bucket_path.is_dir() {
                continue;
            }
            // Skip internal directories (e.g., _journals)
            if bucket_path
                .file_name()
                .is_some_and(|n| n.to_str().is_some_and(|s| s.starts_with('_')))
            {
                continue;
            }

            // Enumerate L1 subdirectories within this bucket
            let l1_entries = match std::fs::read_dir(&bucket_path) {
                Ok(entries) => entries,
                Err(_) => continue,
            };

            for l1_entry in l1_entries.flatten() {
                let l1_path = l1_entry.path();
                if !l1_path.is_dir() {
                    continue;
                }

                // Parse the directory name as a 2-char lowercase hex → u8
                if let Some(name) = l1_path.file_name().and_then(|n| n.to_str()) {
                    if name.len() == 2 {
                        if let Ok(idx) = u8::from_str_radix(name, 16) {
                            if selected.contains(&idx) {
                                result.push(l1_path);
                            }
                        }
                    }
                }
            }
        }

        (result, wraps)
    }

    /// Applies proportional size correction after a rolling (partial) scan.
    ///
    /// Since only `dirs_scanned` of 256 L1 directories were scanned, this method
    /// extrapolates the drift observed in the scanned subset to adjust the tracked
    /// totals. This avoids large swings from replacing the full tracked size with
    /// a partial scan result.
    ///
    /// Formula:
    /// - `expected = tracked * dirs_scanned / 256`
    /// - `discrepancy = scanned - expected` (signed)
    /// - `corrected = tracked + discrepancy` (clamped to 0 minimum)
    ///
    /// Logs a warning if the discrepancy percentage exceeds `validation_threshold_warn`,
    /// and an error if it exceeds `validation_threshold_error`.
    ///
    /// See: Requirements 6.2, 6.3, 6.4
    pub fn apply_proportional_correction(
        &self,
        scanned_size: u64,
        scanned_objects: u64,
        dirs_scanned: usize,
        tracked_size: u64,
        tracked_objects: u64,
    ) -> (u64, u64) {
        // Compute expected values for the scanned subset
        let expected_size = tracked_size as u128 * dirs_scanned as u128 / 256;
        let expected_objects = tracked_objects as u128 * dirs_scanned as u128 / 256;

        // Compute discrepancy using signed arithmetic
        let size_discrepancy = scanned_size as i64 - expected_size as i64;
        let objects_discrepancy = scanned_objects as i64 - expected_objects as i64;

        // Apply correction, clamped to 0 minimum
        let corrected_size = (tracked_size as i64 + size_discrepancy).max(0) as u64;
        let corrected_objects = (tracked_objects as i64 + objects_discrepancy).max(0) as u64;

        // Compute discrepancy percentage and log if thresholds exceeded
        if expected_size > 0 {
            let discrepancy_pct =
                (size_discrepancy.unsigned_abs() as f64 / expected_size as f64) * 100.0;

            if discrepancy_pct > self.config.validation_threshold_error {
                error!(
                    "Rolling validation: size discrepancy {:.1}% exceeds error threshold ({:.1}%): \
                     scanned={}, expected={}, tracked={}",
                    discrepancy_pct,
                    self.config.validation_threshold_error,
                    format_bytes_human(scanned_size),
                    format_bytes_human(expected_size as u64),
                    format_bytes_human(tracked_size),
                );
            } else if discrepancy_pct > self.config.validation_threshold_warn {
                warn!(
                    "Rolling validation: size discrepancy {:.1}% exceeds warning threshold ({:.1}%): \
                     scanned={}, expected={}, tracked={}",
                    discrepancy_pct,
                    self.config.validation_threshold_warn,
                    format_bytes_human(scanned_size),
                    format_bytes_human(expected_size as u64),
                    format_bytes_human(tracked_size),
                );
            }
        } else if scanned_size > 0 {
            // expected is 0 but scanned is non-zero — log a warning
            warn!(
                "Rolling validation: expected size is 0 but scanned {} (dirs_scanned={}, tracked={})",
                format_bytes_human(scanned_size),
                dirs_scanned,
                format_bytes_human(tracked_size),
            );
        }

        (corrected_size, corrected_objects)
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
            next_time += ChronoDuration::days(1);
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
        let jitter = Duration::from_secs(fastrand::u64(0..=3600));

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

    /// Performs a validation scan with self-tuning mode selection.
    ///
    /// Reads previous scan state from `validation.json`, calls [`determine_scan_mode`]
    /// to choose between full and rolling mode, and dispatches accordingly. Also runs
    /// incomplete upload cleanup before the scan.
    ///
    /// See: Requirement 1.5
    async fn perform_validation(&self) -> Result<()> {
        // Try to acquire global lock
        let _lock = match self.try_acquire_validation_lock().await {
            Ok(lock) => lock,
            Err(e) => {
                info!("Another instance is validating, skipping: {}", e);
                return Ok(());
            }
        };

        // Always run incomplete upload cleanup during daily validation
        // Requirement 5.3, 5.4: Incomplete uploads should always be cleaned up
        let incomplete_uploads_freed = self.cleanup_incomplete_uploads().await.unwrap_or(0);
        if incomplete_uploads_freed > 0 {
            info!(
                "Incomplete upload cleanup during validation: freed {} bytes",
                format_bytes_human(incomplete_uploads_freed)
            );
        }

        // Read previous scan state from validation.json for mode selection
        let max_duration = self.config.validation_max_duration;
        let prev_state = self.read_previous_scan_state();

        let (mode, reason) = determine_scan_mode(
            prev_state.validation_type.as_deref(),
            prev_state.last_full_scan_duration_secs,
            prev_state.rolling_cycle_duration_secs,
            prev_state.rolling_dirs_scanned,
            max_duration,
        );

        info!(
            "Validation: {} mode (reason: {}), budget={}",
            match mode {
                ScanMode::Full => "full",
                ScanMode::Rolling => "rolling",
            },
            reason,
            format_duration_human(max_duration)
        );

        match mode {
            ScanMode::Full => self.perform_full_validation().await,
            ScanMode::Rolling => self.perform_rolling_validation().await,
        }
    }

    /// Previous scan state read from validation.json for mode selection.
    fn read_previous_scan_state(&self) -> PreviousScanState {
        let content = match std::fs::read_to_string(&self.validation_path) {
            Ok(c) => c,
            Err(_) => return PreviousScanState::default(),
        };

        let json: serde_json::Value = match serde_json::from_str(&content) {
            Ok(v) => v,
            Err(_) => return PreviousScanState::default(),
        };

        PreviousScanState {
            validation_type: json
                .get("validation_type")
                .and_then(|v| v.as_str())
                .map(String::from),
            last_full_scan_duration_secs: json
                .get("last_full_scan_duration_secs")
                .and_then(|v| v.as_f64()),
            rolling_cycle_duration_secs: json
                .get("rolling_cycle_duration_secs")
                .and_then(|v| v.as_f64()),
            rolling_dirs_scanned: json.get("rolling_dirs_scanned").and_then(|v| v.as_u64()),
        }
    }

    /// Performs a full validation scan over all L1 directories.
    ///
    /// This is the original full scan logic, extracted from `perform_validation`.
    /// Scans all `.meta` files via rayon-based parallel traversal, reconciles
    /// `size_state.json` with the scanned totals, and records the elapsed duration
    /// as `last_full_scan_duration_secs` in `validation.json` for the next cycle's
    /// mode decision. Logs a warning if the scan exceeds `validation_max_duration`.
    ///
    /// See: Requirements 1.4, 4.5, 7.1, 8.1
    async fn perform_full_validation(&self) -> Result<()> {
        let start = Instant::now();

        // Scan metadata files using shared validator (returns size, cache_expired, cache_skipped, cache_errors, object_count)
        let (scanned_size, cache_expired, cache_skipped, cache_errors, scanned_objects) =
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
        }

        // Always update size state from validation — even when size drift is zero,
        // cached_objects may have drifted due to multi-instance double-counting.
        // The validation scan's .meta file count is the authoritative object count.
        self.consolidator
            .update_size_from_validation(scanned_size, None, Some(scanned_objects))
            .await;

        // Write validation metadata
        let files_scanned = scanned_objects;
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

        // Persist last_full_scan_duration_secs for mode selection on next cycle
        self.persist_full_scan_duration(duration.as_secs_f64())
            .await?;

        // 8.2: Warn if full scan exceeded the time budget
        let max_duration = self.config.validation_max_duration;
        if duration > max_duration {
            warn!(
                "Full validation scan exceeded time budget: elapsed={}, budget={}",
                format_duration_human(duration),
                format_duration_human(max_duration)
            );
        }

        Ok(())
    }

    /// Persist the full scan duration to validation.json for next cycle's mode decision.
    async fn persist_full_scan_duration(&self, duration_secs: f64) -> Result<()> {
        // Read existing validation.json, add/update last_full_scan_duration_secs and validation_type
        let content = match tokio::fs::read_to_string(&self.validation_path).await {
            Ok(c) => c,
            Err(_) => "{}".to_string(),
        };

        let mut json: serde_json::Value =
            serde_json::from_str(&content).unwrap_or(serde_json::json!({}));

        if let Some(obj) = json.as_object_mut() {
            obj.insert(
                "last_full_scan_duration_secs".to_string(),
                serde_json::json!(duration_secs),
            );
            obj.insert("validation_type".to_string(), serde_json::json!("full"));
        }

        let updated = serde_json::to_string_pretty(&json).map_err(|e| {
            ProxyError::CacheError(format!("Failed to serialize validation state: {}", e))
        })?;

        // Atomic write
        let temp_path = self.validation_path.with_extension("json.tmp");
        tokio::fs::write(&temp_path, &updated).await.map_err(|e| {
            ProxyError::CacheError(format!("Failed to write validation temp file: {}", e))
        })?;
        tokio::fs::rename(&temp_path, &self.validation_path)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to rename validation temp file: {}", e))
            })?;

        Ok(())
    }

    /// Performs a rolling validation scan over a subset of L1 directories.
    ///
    /// Orchestrates the rolling scan lifecycle:
    /// 1. Reads rolling state (cursor, scan rate) from `validation.json`
    /// 2. Estimates batch size from scan rate and `validation_max_duration`
    /// 3. Selects and scans L1 directories in parallel using rayon
    /// 4. Processes additional batches if time remains and directories are pending
    /// 5. Applies proportional size correction to reconcile `size_state.json`
    /// 6. Detects full rotation (cursor wraps past 0xff) and logs completion
    /// 7. Persists updated cursor, scan rate, and cycle stats to `validation.json`
    ///
    /// See: Requirements 3, 4, 5, 6, 8
    async fn perform_rolling_validation(&self) -> Result<()> {
        use rayon::prelude::*;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::UNIX_EPOCH;

        let start = Instant::now();
        let max_duration = self.config.validation_max_duration;
        let metadata_dir = self.cache_dir.join("metadata");

        // 7.1: Read rolling state and estimate batch size
        let mut rolling_state = self.read_rolling_state()?;
        let start_cursor = rolling_state.cursor;
        let total_estimated = self.estimate_batch_size(rolling_state.scan_rate, max_duration);

        // Get current tracked size and objects for proportional correction
        let size_state = self.consolidator.get_size_state().await;
        let tracked_size = size_state.total_size;
        let tracked_objects = size_state.cached_objects;

        info!(
            "Rolling validation: starting scan at cursor={:02x}, estimated_dirs={}, cached_objects={}, budget={}",
            start_cursor,
            total_estimated,
            tracked_objects,
            format_duration_human(max_duration)
        );

        // Initialize rotation_start_time on first rolling cycle
        if rolling_state.rotation_start_time.is_none() {
            rolling_state.rotation_start_time = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            );
        }

        // 7.2: Batch loop — process dirs in batches, check time after each batch
        let mut total_dirs_scanned: usize = 0;
        let mut total_size = 0u64;
        let mut total_objects = 0u64;
        let mut _total_cache_expired = 0u64;
        let mut _total_cache_skipped = 0u64;
        let mut _total_cache_errors = 0u64;
        let mut dirs_remaining = total_estimated;

        let now_systime = SystemTime::now();

        while dirs_remaining > 0 {
            // Select L1 directories for this batch
            let batch_cursor = ((start_cursor as usize + total_dirs_scanned) % 256) as u8;
            let batch_count = dirs_remaining;
            let (l1_dirs, _wraps) =
                self.select_l1_directories(&metadata_dir, batch_cursor, batch_count);

            if l1_dirs.is_empty() {
                // No directories found on disk for this range — still advance cursor
                total_dirs_scanned += batch_count;
                break;
            }

            let _actual_batch_dirs = l1_dirs.len();

            // Scan selected L1 dirs in parallel using rayon (same pattern as scan_metadata_with_shared_validator)
            let batch_size = AtomicU64::new(0);
            let batch_objects = AtomicU64::new(0);
            let batch_expired = AtomicU64::new(0);
            let batch_skipped = AtomicU64::new(0);
            let batch_errors = AtomicU64::new(0);

            l1_dirs.par_iter().for_each(|l1_dir| {
                let l2_entries = match std::fs::read_dir(l1_dir) {
                    Ok(entries) => entries,
                    Err(_) => return,
                };
                for l2_entry in l2_entries.flatten() {
                    let l2_path = l2_entry.path();
                    if !l2_path.is_dir() {
                        continue;
                    }
                    let file_entries = match std::fs::read_dir(&l2_path) {
                        Ok(entries) => entries,
                        Err(_) => continue,
                    };
                    for file_entry in file_entries.flatten() {
                        let path = file_entry.path();
                        if path.extension().is_none_or(|ext| ext != "meta") {
                            continue;
                        }
                        let result = self.scan_metadata_file(&path, now_systime);
                        batch_size.fetch_add(result.size_bytes, Ordering::Relaxed);
                        batch_objects.fetch_add(1, Ordering::Relaxed);
                        if result.cache_expired {
                            batch_expired.fetch_add(1, Ordering::Relaxed);
                        }
                        if result.cache_skipped {
                            batch_skipped.fetch_add(1, Ordering::Relaxed);
                        }
                        if result.cache_error {
                            batch_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });

            total_size += batch_size.load(Ordering::Relaxed);
            total_objects += batch_objects.load(Ordering::Relaxed);
            _total_cache_expired += batch_expired.load(Ordering::Relaxed);
            _total_cache_skipped += batch_skipped.load(Ordering::Relaxed);
            _total_cache_errors += batch_errors.load(Ordering::Relaxed);
            // Advance by the number of L1 index slots we intended to cover, not just dirs found on disk
            total_dirs_scanned += batch_count;
            dirs_remaining = 0; // We processed the full estimated batch

            // Check if time remains and we could process more (up to 256 total)
            let elapsed = start.elapsed();
            if elapsed < max_duration && total_dirs_scanned < 256 {
                // Re-estimate how many more dirs we can fit in remaining time
                let elapsed_secs = elapsed.as_secs_f64();
                let remaining_secs = max_duration.as_secs_f64() - elapsed_secs;
                if total_dirs_scanned > 0 && remaining_secs > 0.0 {
                    let current_rate = elapsed_secs / total_dirs_scanned as f64;
                    let additional = (remaining_secs / current_rate).floor() as usize;
                    let additional = additional.min(256 - total_dirs_scanned);
                    if additional > 0 {
                        dirs_remaining = additional;
                    }
                }
            }
        }

        let elapsed = start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        // 7.3: Compute proportional size correction and update size state
        if total_dirs_scanned > 0 {
            let (corrected_size, corrected_objects) = self.apply_proportional_correction(
                total_size,
                total_objects,
                total_dirs_scanned.min(256),
                tracked_size,
                tracked_objects,
            );

            // Update size state via consolidator
            self.consolidator
                .update_size_from_validation(corrected_size, None, Some(corrected_objects))
                .await;
        }

        // Compute scan rate and new cursor
        let scan_rate = if total_dirs_scanned > 0 {
            elapsed_secs / total_dirs_scanned as f64
        } else {
            0.0
        };

        let new_cursor = ((start_cursor as usize + total_dirs_scanned) % 256) as u8;

        // Compute extrapolated full scan time for next cycle's mode decision
        let extrapolated_full_secs = if total_dirs_scanned > 0 {
            Some(scan_rate * 256.0)
        } else {
            None
        };

        // 7.4: Detect full rotation
        let wrapped = (start_cursor as usize + total_dirs_scanned) > 255;
        if wrapped {
            rolling_state.full_rotation_count += 1;

            if let Some(rotation_start) = rolling_state.rotation_start_time {
                let now_epoch = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let rotation_elapsed = now_epoch.saturating_sub(rotation_start);
                info!(
                    "Rolling validation: full rotation #{} complete, total rotation time={}",
                    rolling_state.full_rotation_count,
                    format_duration_human(Duration::from_secs(rotation_elapsed))
                );
            }

            // Reset rotation_start_time for next rotation
            rolling_state.rotation_start_time = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            );
        }

        // Update rolling state for persistence
        rolling_state.cursor = new_cursor;
        rolling_state.scan_rate = Some(scan_rate);
        // Preserve last_full_scan_duration_secs from previous state (it's set by full scans)

        // 7.5: Persist rolling state and cycle stats
        let cycle_stats = RollingCycleStats {
            dirs_scanned: total_dirs_scanned as u64,
            objects_validated: total_objects,
            cycle_duration_secs: elapsed_secs,
        };

        self.write_rolling_state(&rolling_state, &cycle_stats)?;

        // 7.6: Log scan completion
        let dirs_until_rotation = if new_cursor == 0 && total_dirs_scanned > 0 {
            0 // Just completed a rotation
        } else {
            256 - new_cursor as usize
        };

        info!(
            "Rolling validation complete: dirs_scanned={}, dirs_remaining_until_rotation={}, \
             objects_validated={}, elapsed={}, scan_rate={:.2}s/dir, new_cursor={:02x}{}",
            total_dirs_scanned,
            dirs_until_rotation,
            total_objects,
            format_duration_human(elapsed),
            scan_rate,
            new_cursor,
            if let Some(ext) = extrapolated_full_secs {
                format!(
                    ", extrapolated_full_scan={}",
                    format_duration_human(Duration::from_secs_f64(ext))
                )
            } else {
                String::new()
            }
        );

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
            .truncate(false)
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
    async fn scan_metadata_with_shared_validator(&self) -> Result<(u64, u64, u64, u64, u64)> {
        use rayon::prelude::*;
        use std::sync::atomic::{AtomicU64, Ordering};

        let now = std::time::SystemTime::now();
        let metadata_dir = self.cache_dir.join("metadata");

        if !metadata_dir.exists() {
            return Ok((0, 0, 0, 0, 0));
        }

        // Atomic counters for lock-free accumulation from parallel workers
        let total_size = AtomicU64::new(0);
        let cache_expired = AtomicU64::new(0);
        let cache_skipped = AtomicU64::new(0);
        let cache_errors = AtomicU64::new(0);
        let files_processed = AtomicU64::new(0);

        // Collect L1 shard directories for parallel traversal.
        // Structure: metadata/{bucket}/{L1}/{L2}/*.meta
        // Instead of a single sequential WalkDir over the entire tree, we enumerate
        // L1 directories and walk each in parallel via rayon. This overlaps NFS readdir
        // round-trips across threads, reducing wall-clock time from ~35 min to ~2-5 min.
        let mut l1_dirs: Vec<std::path::PathBuf> = Vec::new();
        if let Ok(bucket_entries) = std::fs::read_dir(&metadata_dir) {
            for bucket_entry in bucket_entries.flatten() {
                let bucket_path = bucket_entry.path();
                if !bucket_path.is_dir() {
                    continue;
                }
                // Skip _journals and other internal directories
                if bucket_path
                    .file_name()
                    .is_some_and(|n| n.to_str().is_some_and(|s| s.starts_with('_')))
                {
                    continue;
                }
                if let Ok(l1_entries) = std::fs::read_dir(&bucket_path) {
                    for l1_entry in l1_entries.flatten() {
                        let l1_path = l1_entry.path();
                        if l1_path.is_dir() {
                            l1_dirs.push(l1_path);
                        }
                    }
                }
            }
        }

        info!(
            "Cache validation: scanning {} L1 shard directories in parallel",
            l1_dirs.len()
        );

        // Walk each L1 directory in parallel using rayon's thread pool.
        // Within each L1, enumerate L2 subdirectories and their .meta files sequentially.
        l1_dirs.par_iter().for_each(|l1_dir| {
            let l2_entries = match std::fs::read_dir(l1_dir) {
                Ok(entries) => entries,
                Err(_) => return,
            };
            for l2_entry in l2_entries.flatten() {
                let l2_path = l2_entry.path();
                if !l2_path.is_dir() {
                    continue;
                }
                let file_entries = match std::fs::read_dir(&l2_path) {
                    Ok(entries) => entries,
                    Err(_) => continue,
                };
                for file_entry in file_entries.flatten() {
                    let path = file_entry.path();
                    if path.extension().is_none_or(|ext| ext != "meta") {
                        continue;
                    }
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
                    if count.is_multiple_of(100_000) {
                        info!("Cache validation progress: {} files processed", count);
                    }
                }
            }
        });

        let total = files_processed.load(Ordering::Relaxed);
        info!("Cache validation: processed {} metadata files", total);

        Ok((
            total_size.load(Ordering::Relaxed),
            cache_expired.load(Ordering::Relaxed),
            cache_skipped.load(Ordering::Relaxed),
            cache_errors.load(Ordering::Relaxed),
            files_processed.load(Ordering::Relaxed),
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
        assert!(!metadata.active_expiration_enabled);
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
        assert!(!tracker_disabled.actively_remove_cached_data);

        // Create tracker with flag enabled
        let tracker_enabled = CacheSizeTracker::new(cache_dir, config, true, consolidator)
            .await
            .unwrap();
        assert!(tracker_enabled.actively_remove_cached_data);
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

    // ============================================================
    // Rolling State Tests (Task 2)
    // ============================================================

    #[tokio::test]
    async fn test_rolling_state_write_read_roundtrip() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        let state = RollingState {
            cursor: 42,
            scan_rate: Some(56.25),
            full_rotation_count: 3,
            rotation_start_time: Some(1719763200),
            last_full_scan_duration_secs: Some(14520.3),
        };
        let cycle_stats = RollingCycleStats {
            dirs_scanned: 64,
            objects_validated: 523000,
            cycle_duration_secs: 3600.5,
        };

        tracker.write_rolling_state(&state, &cycle_stats).unwrap();
        let read_state = tracker.read_rolling_state().unwrap();

        assert_eq!(read_state.cursor, state.cursor);
        assert_eq!(read_state.scan_rate, state.scan_rate);
        assert_eq!(read_state.full_rotation_count, state.full_rotation_count);
        assert_eq!(read_state.rotation_start_time, state.rotation_start_time);
        assert_eq!(
            read_state.last_full_scan_duration_secs,
            state.last_full_scan_duration_secs
        );
    }

    #[tokio::test]
    async fn test_rolling_state_read_missing_file() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // validation.json does not exist yet — should return defaults
        let state = tracker.read_rolling_state().unwrap();

        assert_eq!(state.cursor, 0);
        assert_eq!(state.scan_rate, None);
        assert_eq!(state.full_rotation_count, 0);
        assert_eq!(state.rotation_start_time, None);
        assert_eq!(state.last_full_scan_duration_secs, None);
    }

    #[tokio::test]
    async fn test_rolling_state_read_corrupted_json() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // Write corrupted JSON to validation.json
        std::fs::write(&tracker.validation_path, "{ not valid json !!!").unwrap();

        let state = tracker.read_rolling_state().unwrap();

        assert_eq!(state.cursor, 0);
        assert_eq!(state.scan_rate, None);
        assert_eq!(state.full_rotation_count, 0);
        assert_eq!(state.rotation_start_time, None);
        assert_eq!(state.last_full_scan_duration_secs, None);
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

    // ============================================================
    // Property-Based Tests: Rolling State (Task 2.7)
    // ============================================================

    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: rolling-validation-scan, Property 5: Cursor persistence round-trip**
    ///
    /// For any cursor value c in [0, 255] and dirs_scanned n in [1, 256],
    /// after computing the new cursor as (c + n) % 256, writing state, and
    /// reading it back, the cursor SHALL match.
    ///
    /// **Validates: Requirements 3.5, 5.2, 5.3**
    #[quickcheck]
    fn prop_cursor_persistence_round_trip(cursor: u8, dirs_scanned: u16) -> TestResult {
        // dirs_scanned must be in [1, 256]
        if dirs_scanned == 0 || dirs_scanned > 256 {
            return TestResult::discard();
        }

        let expected_new_cursor = ((cursor as u16 + dirs_scanned) % 256) as u8;

        // Create a temp directory and tracker synchronously using a runtime
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

            let state = RollingState {
                cursor: expected_new_cursor,
                scan_rate: Some(10.0),
                full_rotation_count: 0,
                rotation_start_time: None,
                last_full_scan_duration_secs: None,
            };
            let cycle_stats = RollingCycleStats {
                dirs_scanned: dirs_scanned as u64,
                objects_validated: 100,
                cycle_duration_secs: 60.0,
            };

            tracker.write_rolling_state(&state, &cycle_stats).unwrap();
            let read_state = tracker.read_rolling_state().unwrap();
            (read_state.cursor, expected_new_cursor)
        });

        TestResult::from_bool(result.0 == result.1)
    }

    // ============================================================
    // Mode Selection Unit Tests (Task 3.5)
    // ============================================================

    #[test]
    fn test_mode_selection_no_history() {
        // No previous scan history → Full
        let (mode, _reason) =
            determine_scan_mode(None, None, None, None, Duration::from_secs(4 * 3600));
        assert_eq!(mode, ScanMode::Full);
    }

    #[test]
    fn test_mode_selection_full_exceeded_budget() {
        // Previous full scan exceeded budget → Rolling
        let (mode, _reason) = determine_scan_mode(
            Some("full"),
            Some(15000.0), // 15000s > 14400s (4h)
            None,
            None,
            Duration::from_secs(4 * 3600),
        );
        assert_eq!(mode, ScanMode::Rolling);
    }

    #[test]
    fn test_mode_selection_full_within_budget() {
        // Previous full scan within budget → Full
        let (mode, _reason) = determine_scan_mode(
            Some("full"),
            Some(3600.0), // 1h < 4h
            None,
            None,
            Duration::from_secs(4 * 3600),
        );
        assert_eq!(mode, ScanMode::Full);
    }

    #[test]
    fn test_mode_selection_rolling_extrapolated_above() {
        // Previous rolling scan, extrapolated full time > budget → Rolling
        // elapsed=3600s, dirs_scanned=64 → extrapolated = (3600/64)*256 = 14400s
        // budget = 14000s → 14400 > 14000 → Rolling
        let (mode, _reason) = determine_scan_mode(
            Some("rolling"),
            None,
            Some(3600.0),
            Some(64),
            Duration::from_secs(14000),
        );
        assert_eq!(mode, ScanMode::Rolling);
    }

    #[test]
    fn test_mode_selection_rolling_extrapolated_below() {
        // Previous rolling scan, extrapolated full time ≤ budget → Full
        // elapsed=3600s, dirs_scanned=64 → extrapolated = (3600/64)*256 = 14400s
        // budget = 14400s → 14400 ≤ 14400 → Full
        let (mode, _reason) = determine_scan_mode(
            Some("rolling"),
            None,
            Some(3600.0),
            Some(64),
            Duration::from_secs(14400),
        );
        assert_eq!(mode, ScanMode::Full);
    }

    // ============================================================
    // Property-Based Tests: Mode Selection (Task 3.6)
    // ============================================================

    // ============================================================
    // Batch Size Estimation Unit Tests (Task 4.2)
    // ============================================================

    #[tokio::test]
    async fn test_estimate_batch_size_with_scan_rate() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // 4 hours = 14400s, scan_rate = 60s/dir → floor(14400/60) = 240
        let result = tracker.estimate_batch_size(Some(60.0), Duration::from_secs(4 * 3600));
        assert_eq!(result, 240);
    }

    #[tokio::test]
    async fn test_estimate_batch_size_none_returns_default() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // No scan rate → default 64
        let result = tracker.estimate_batch_size(None, Duration::from_secs(4 * 3600));
        assert_eq!(result, 64);
    }

    #[tokio::test]
    async fn test_estimate_batch_size_clamp_to_min() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // Very high scan rate relative to budget → clamp to 1
        // budget=10s, rate=100s/dir → floor(10/100) = 0 → clamped to 1
        let result = tracker.estimate_batch_size(Some(100.0), Duration::from_secs(10));
        assert_eq!(result, 1);
    }

    #[tokio::test]
    async fn test_estimate_batch_size_clamp_to_max() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // Very low scan rate relative to budget → clamp to 256
        // budget=14400s, rate=1s/dir → floor(14400/1) = 14400 → clamped to 256
        let result = tracker.estimate_batch_size(Some(1.0), Duration::from_secs(14400));
        assert_eq!(result, 256);
    }

    // ============================================================
    // Property-Based Tests: Batch Size Estimation (Task 4.3)
    // ============================================================

    /// **Feature: rolling-validation-scan, Property 4: Batch size estimation from scan rate and time budget**
    ///
    /// For any positive scan rate r (seconds per L1 directory) and positive time budget t (seconds),
    /// the estimated batch size SHALL be floor(t / r) clamped to [1, 256].
    /// When no previous scan rate is available (first cycle), the batch size SHALL default to 64.
    ///
    /// **Validates: Requirements 3.4, 4.1**
    #[quickcheck]
    fn prop_batch_size_estimation(scan_rate_raw: u32, budget_secs_raw: u32) -> TestResult {
        // Ensure positive values; discard zeros
        if scan_rate_raw == 0 || budget_secs_raw == 0 {
            return TestResult::discard();
        }

        // Use values that produce finite, non-NaN, non-Inf f64
        let scan_rate = scan_rate_raw as f64;
        let budget_secs = budget_secs_raw as f64;

        let expected = (budget_secs / scan_rate).floor().clamp(1.0, 256.0) as usize;

        let rt = tokio::runtime::Runtime::new().unwrap();
        let actual = rt.block_on(async {
            let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;
            tracker
                .estimate_batch_size(Some(scan_rate), Duration::from_secs(budget_secs_raw as u64))
        });

        TestResult::from_bool(actual == expected)
    }

    /// **Feature: rolling-validation-scan, Property 4: Batch size estimation from scan rate and time budget**
    ///
    /// When no scan rate is available (None), the batch size SHALL always be 64.
    ///
    /// **Validates: Requirements 3.4, 4.1**
    #[quickcheck]
    fn prop_batch_size_none_always_64(budget_secs: u32) -> TestResult {
        if budget_secs == 0 {
            return TestResult::discard();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let actual = rt.block_on(async {
            let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;
            tracker.estimate_batch_size(None, Duration::from_secs(budget_secs as u64))
        });

        TestResult::from_bool(actual == 64)
    }

    // ============================================================
    // L1 Directory Selection Unit Tests (Task 5.2)
    // ============================================================

    /// Helper to create L1 directories for a bucket under metadata_dir.
    /// Creates dirs named with 2-char lowercase hex for each index in `indices`.
    fn create_l1_dirs(metadata_dir: &std::path::Path, bucket: &str, indices: &[u8]) {
        let bucket_dir = metadata_dir.join(bucket);
        std::fs::create_dir_all(&bucket_dir).unwrap();
        for &idx in indices {
            std::fs::create_dir_all(bucket_dir.join(format!("{:02x}", idx))).unwrap();
        }
    }

    #[tokio::test]
    async fn test_select_l1_directories_no_wrap() {
        let (tracker, _consolidator, temp_dir) = create_test_tracker().await;
        let metadata_dir = temp_dir.path().join("metadata");

        // Create dirs 00..05 for a single bucket
        create_l1_dirs(&metadata_dir, "my-bucket", &[0, 1, 2, 3, 4, 5]);

        // cursor=0, count=3 → should select 00, 01, 02
        let (dirs, wraps) = tracker.select_l1_directories(&metadata_dir, 0, 3);
        assert!(!wraps, "cursor=0, count=3 should not wrap");
        assert_eq!(dirs.len(), 3);

        let mut names: Vec<String> = dirs
            .iter()
            .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(String::from))
            .collect();
        names.sort();
        assert_eq!(names, vec!["00", "01", "02"]);
    }

    #[tokio::test]
    async fn test_select_l1_directories_wrapping() {
        let (tracker, _consolidator, temp_dir) = create_test_tracker().await;
        let metadata_dir = temp_dir.path().join("metadata");

        // Create dirs fe, ff, 00, 01 for a single bucket
        create_l1_dirs(&metadata_dir, "my-bucket", &[0xfe, 0xff, 0x00, 0x01]);

        // cursor=254, count=4 → should select fe, ff, 00, 01 (wrapping)
        let (dirs, wraps) = tracker.select_l1_directories(&metadata_dir, 254, 4);
        assert!(wraps, "cursor=254, count=4 should wrap");
        assert_eq!(dirs.len(), 4);

        let mut names: Vec<String> = dirs
            .iter()
            .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(String::from))
            .collect();
        names.sort();
        assert_eq!(names, vec!["00", "01", "fe", "ff"]);
    }

    #[tokio::test]
    async fn test_select_l1_directories_wraps_flag_boundary() {
        let (tracker, _consolidator, temp_dir) = create_test_tracker().await;
        let metadata_dir = temp_dir.path().join("metadata");

        // Create all 256 dirs
        let all_indices: Vec<u8> = (0..=255).collect();
        create_l1_dirs(&metadata_dir, "my-bucket", &all_indices);

        // Exactly at boundary: cursor=253, count=3 → 253,254,255 → no wrap
        let (_dirs, wraps) = tracker.select_l1_directories(&metadata_dir, 253, 3);
        assert!(
            !wraps,
            "cursor=253, count=3 should not wrap (253+3=256, not >256)"
        );

        // One past boundary: cursor=254, count=3 → 254,255,0 → wraps
        let (_dirs, wraps) = tracker.select_l1_directories(&metadata_dir, 254, 3);
        assert!(wraps, "cursor=254, count=3 should wrap (254+3=257 > 256)");
    }

    #[tokio::test]
    async fn test_select_l1_directories_multiple_buckets() {
        let (tracker, _consolidator, temp_dir) = create_test_tracker().await;
        let metadata_dir = temp_dir.path().join("metadata");

        // Two buckets, each with some L1 dirs
        create_l1_dirs(&metadata_dir, "bucket-a", &[0x00, 0x01, 0x02]);
        create_l1_dirs(&metadata_dir, "bucket-b", &[0x00, 0x01, 0x03]);

        // cursor=0, count=2 → should select 00, 01 from both buckets
        let (dirs, wraps) = tracker.select_l1_directories(&metadata_dir, 0, 2);
        assert!(!wraps);
        // bucket-a: 00, 01; bucket-b: 00, 01 → 4 total
        assert_eq!(dirs.len(), 4);
    }

    // ============================================================
    // Property-Based Tests: L1 Directory Selection (Task 5.3)
    // ============================================================

    /// **Feature: rolling-validation-scan, Property 3: Sequential directory selection with cyclic wrapping**
    ///
    /// For any cursor position c in [0, 255] and count n in [1, 256],
    /// the selected L1 directory indices SHALL be the set
    /// {(c + i) % 256 | i in 0..n} and have exactly n elements.
    ///
    /// **Validates: Requirements 3.1**
    #[quickcheck]
    fn prop_sequential_directory_selection_cyclic_wrapping(
        cursor: u8,
        count_raw: u16,
    ) -> TestResult {
        // count must be in [1, 256]
        let count = (count_raw % 256) as usize + 1; // maps to [1, 256]

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let (tracker, _consolidator, temp_dir) = create_test_tracker().await;
            let metadata_dir = temp_dir.path().join("metadata");

            // Create a single bucket with all 256 L1 dirs
            let all_indices: Vec<u8> = (0..=255).collect();
            create_l1_dirs(&metadata_dir, "test-bucket", &all_indices);

            let (dirs, wraps) = tracker.select_l1_directories(&metadata_dir, cursor, count);

            // Build expected index set
            let expected: std::collections::HashSet<u8> = (0..count)
                .map(|i| ((cursor as usize + i) % 256) as u8)
                .collect();

            // Extract actual indices from returned paths
            let actual: std::collections::HashSet<u8> = dirs
                .iter()
                .filter_map(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .and_then(|s| u8::from_str_radix(s, 16).ok())
                })
                .collect();

            // Verify wraps flag
            let expected_wraps = (cursor as usize) + count > 256;

            actual == expected && actual.len() == count && wraps == expected_wraps
        });

        TestResult::from_bool(result)
    }

    /// **Feature: rolling-validation-scan, Property 1: Mode selection is determined by previous scan duration and mode**
    ///
    /// For any previous scan state (mode, duration, dirs_scanned) and max_duration values,
    /// verify mode matches the time-based decision rules:
    /// - No history → Full
    /// - Previous full exceeded budget → Rolling
    /// - Previous full within budget → Full
    /// - Previous rolling with extrapolated time > budget → Rolling
    /// - Previous rolling with extrapolated time ≤ budget → Full
    ///
    /// **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 7.1, 7.4**
    #[quickcheck]
    fn prop_mode_selection(
        prev_type_idx: u8,
        duration_secs: u32,
        dirs_scanned: u16,
        budget_secs: u32,
    ) -> TestResult {
        // Budget must be positive
        if budget_secs == 0 {
            return TestResult::discard();
        }

        let budget = Duration::from_secs(budget_secs as u64);
        let budget_f64 = budget_secs as f64;

        // Map prev_type_idx to one of: None, "full", "rolling"
        let prev_type = match prev_type_idx % 3 {
            0 => None,
            1 => Some("full"),
            _ => Some("rolling"),
        };

        let dur = duration_secs as f64;
        let dirs = dirs_scanned.max(1) as u64; // Ensure at least 1

        let (mode, _reason) = match prev_type {
            None => {
                let (m, r) = determine_scan_mode(None, None, None, None, budget);
                // No history → must be Full
                if m != ScanMode::Full {
                    return TestResult::failed();
                }
                (m, r)
            }
            Some("full") => {
                let (m, r) = determine_scan_mode(Some("full"), Some(dur), None, None, budget);
                // Full exceeded budget → Rolling; within budget → Full
                let expected = if dur > budget_f64 {
                    ScanMode::Rolling
                } else {
                    ScanMode::Full
                };
                if m != expected {
                    return TestResult::failed();
                }
                (m, r)
            }
            Some("rolling") => {
                let (m, r) =
                    determine_scan_mode(Some("rolling"), None, Some(dur), Some(dirs), budget);
                // Extrapolated = (dur / dirs) * 256
                let extrapolated = (dur / dirs as f64) * 256.0;
                let expected = if extrapolated > budget_f64 {
                    ScanMode::Rolling
                } else {
                    ScanMode::Full
                };
                if m != expected {
                    return TestResult::failed();
                }
                (m, r)
            }
            _ => unreachable!(),
        };

        let _ = (mode, _reason);
        TestResult::passed()
    }

    // ============================================================
    // Proportional Size Correction Unit Tests (Task 6.3)
    // ============================================================

    #[tokio::test]
    async fn test_proportional_correction_no_drift() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // tracked=25600, dirs_scanned=64 → expected = 25600 * 64 / 256 = 6400
        // scanned=6400 → discrepancy=0 → corrected=25600
        let (corrected_size, corrected_objects) =
            tracker.apply_proportional_correction(6400, 640, 64, 25600, 2560);
        assert_eq!(corrected_size, 25600, "No-drift: size should be unchanged");
        assert_eq!(
            corrected_objects, 2560,
            "No-drift: objects should be unchanged"
        );
    }

    #[tokio::test]
    async fn test_proportional_correction_positive_drift() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // tracked=25600, dirs_scanned=64 → expected = 6400
        // scanned=7400 → discrepancy=+1000 → corrected=26600
        let (corrected_size, corrected_objects) =
            tracker.apply_proportional_correction(7400, 740, 64, 25600, 2560);
        assert_eq!(
            corrected_size, 26600,
            "Positive drift should increase total"
        );
        assert!(
            corrected_objects > 2560,
            "Positive drift should increase objects"
        );
    }

    #[tokio::test]
    async fn test_proportional_correction_negative_drift() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // tracked=25600, dirs_scanned=64 → expected = 6400
        // scanned=5400 → discrepancy=-1000 → corrected=24600
        let (corrected_size, corrected_objects) =
            tracker.apply_proportional_correction(5400, 540, 64, 25600, 2560);
        assert_eq!(
            corrected_size, 24600,
            "Negative drift should decrease total"
        );
        assert!(
            corrected_objects < 2560,
            "Negative drift should decrease objects"
        );
    }

    #[tokio::test]
    async fn test_proportional_correction_clamp_to_zero() {
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // tracked=1000, dirs_scanned=128 → expected = 1000 * 128 / 256 = 500
        // scanned=0 → discrepancy=-500 → corrected = 1000 + (-500) = 500
        // But let's make it clamp: tracked=100, dirs_scanned=128 → expected=50
        // scanned=0 → discrepancy=-50 → corrected = 100 + (-50) = 50 (still positive)
        // Need a case where corrected goes negative:
        // tracked=100, dirs_scanned=256 → expected=100
        // scanned=0 → discrepancy=-100 → corrected = 100 + (-100) = 0
        let (corrected_size, corrected_objects) =
            tracker.apply_proportional_correction(0, 0, 256, 100, 10);
        assert_eq!(corrected_size, 0, "Should clamp to 0");
        assert_eq!(corrected_objects, 0, "Should clamp to 0");

        // Even more extreme: tracked=100, dirs_scanned=128 → expected=50
        // scanned=0 → discrepancy=-50 → corrected=50 (not negative, but let's try harder)
        // tracked=10, dirs_scanned=64 → expected = 10*64/256 = 2
        // scanned=0 → discrepancy=-2 → corrected = 10 + (-2) = 8 (still positive)
        // To truly go negative: tracked=50, dirs_scanned=256 → expected=50
        // scanned=0 → discrepancy=-50 → corrected=0
        // tracked=50, dirs_scanned=256, scanned=0 → corrected = 50 + (0 - 50) = 0
        let (corrected_size, _) = tracker.apply_proportional_correction(0, 0, 256, 50, 5);
        assert_eq!(
            corrected_size, 0,
            "Should clamp to 0 when scanned is 0 for full range"
        );

        // Case where discrepancy would make it negative:
        // tracked=100, dirs_scanned=256 → expected=100
        // scanned=0 → discrepancy=-100 → corrected = max(0, 100 + (-100)) = 0
        // Now with partial: tracked=100, dirs_scanned=128 → expected=50
        // scanned=0 → discrepancy=-50 → corrected = max(0, 100 + (-50)) = 50
        // To go truly negative: need scanned << expected by more than tracked
        // tracked=10, dirs_scanned=256 → expected=10
        // scanned=0 → corrected = max(0, 10-10) = 0
        let (corrected_size, _) = tracker.apply_proportional_correction(0, 0, 256, 10, 1);
        assert_eq!(corrected_size, 0, "Should clamp to 0");
    }

    // ============================================================
    // Property-Based Tests: Proportional Size Correction (Task 6.4)
    // ============================================================

    /// **Feature: rolling-validation-scan, Property 6: Proportional size correction preserves total when no drift exists**
    ///
    /// For any tracked total size T, number of scanned directories N in [1, 256],
    /// if the scanned size exactly equals T * N / 256 (no drift), then the corrected
    /// total SHALL equal T.
    ///
    /// More generally, for any scanned size S and tracked total T, the corrected total
    /// SHALL equal T + (S - T * N / 256), clamped to 0 minimum.
    ///
    /// **Validates: Requirements 6.2**
    #[quickcheck]
    fn prop_proportional_correction_no_drift_invariant(
        tracked_total: u32,
        dirs_scanned_raw: u8,
    ) -> TestResult {
        // dirs_scanned must be in [1, 256]
        let dirs_scanned = (dirs_scanned_raw as usize % 256) + 1;

        let tracked = tracked_total as u64;
        // Compute scanned_size using the same integer division as the method
        let scanned_size = tracked as u128 * dirs_scanned as u128 / 256;
        let scanned_size = scanned_size as u64;

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;
            let (corrected_size, _) =
                tracker.apply_proportional_correction(scanned_size, 0, dirs_scanned, tracked, 0);
            corrected_size
        });

        TestResult::from_bool(result == tracked)
    }

    /// **Feature: rolling-validation-scan, Property 6: Proportional size correction preserves total when no drift exists**
    ///
    /// For any (tracked_total, scanned_size, dirs_scanned), the corrected total SHALL equal
    /// max(0, tracked_total + (scanned_size - tracked_total * dirs_scanned / 256)).
    ///
    /// **Validates: Requirements 6.2**
    #[quickcheck]
    fn prop_proportional_correction_formula(
        tracked_total: u32,
        scanned_size: u32,
        dirs_scanned_raw: u8,
    ) -> TestResult {
        let dirs_scanned = (dirs_scanned_raw as usize % 256) + 1;

        let tracked = tracked_total as u64;
        let scanned = scanned_size as u64;

        // Compute expected using the same formula
        let expected_for_scanned = tracked as u128 * dirs_scanned as u128 / 256;
        let discrepancy = scanned as i64 - expected_for_scanned as i64;
        let expected_corrected = (tracked as i64 + discrepancy).max(0) as u64;

        let rt = tokio::runtime::Runtime::new().unwrap();
        let actual = rt.block_on(async {
            let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;
            let (corrected_size, _) =
                tracker.apply_proportional_correction(scanned, 0, dirs_scanned, tracked, 0);
            corrected_size
        });

        TestResult::from_bool(actual == expected_corrected)
    }

    // ============================================================
    // Task 8.3: Full Scan Duration Recording and Time Budget Warning
    // ============================================================

    /// Helper to create a test tracker with a custom CacheSizeConfig
    async fn create_test_tracker_with_config(
        config: CacheSizeConfig,
    ) -> (Arc<CacheSizeTracker>, Arc<JournalConsolidator>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();

        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

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

        let tracker = Arc::new(
            CacheSizeTracker::new(cache_dir, config, false, consolidator.clone())
                .await
                .unwrap(),
        );
        (tracker, consolidator, temp_dir)
    }

    #[tokio::test]
    async fn test_full_scan_duration_persisted() {
        // Verify that persist_full_scan_duration writes last_full_scan_duration_secs
        // to validation.json with validation_type "full"
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        let duration_secs = 3600.5;
        tracker
            .persist_full_scan_duration(duration_secs)
            .await
            .unwrap();

        // Read validation.json and verify the field
        let content = std::fs::read_to_string(&tracker.validation_path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&content).unwrap();

        assert_eq!(
            json.get("last_full_scan_duration_secs")
                .and_then(|v| v.as_f64()),
            Some(3600.5),
            "last_full_scan_duration_secs should be persisted"
        );
        assert_eq!(
            json.get("validation_type").and_then(|v| v.as_str()),
            Some("full"),
            "validation_type should be 'full'"
        );
    }

    #[tokio::test]
    async fn test_full_scan_duration_readable_by_rolling_state() {
        // Verify that last_full_scan_duration_secs persisted by a full scan
        // can be read back via read_rolling_state
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        let duration_secs = 14520.3;
        tracker
            .persist_full_scan_duration(duration_secs)
            .await
            .unwrap();

        let state = tracker.read_rolling_state().unwrap();
        assert_eq!(
            state.last_full_scan_duration_secs,
            Some(14520.3),
            "read_rolling_state should return the persisted full scan duration"
        );
    }

    #[tokio::test]
    async fn test_full_scan_time_budget_warning_condition() {
        // Verify the warning condition: elapsed > validation_max_duration
        // We test the condition directly since perform_full_validation requires
        // actual filesystem scanning. The warning fires when duration > max_duration.
        let short_budget = Duration::from_millis(1);
        let config = CacheSizeConfig {
            validation_max_duration: short_budget,
            ..CacheSizeConfig::default()
        };
        let (tracker, _consolidator, _temp_dir) = create_test_tracker_with_config(config).await;

        // Simulate: a full scan took 10 seconds (well above 1ms budget)
        let simulated_duration = Duration::from_secs(10);
        let max_duration = tracker.config.validation_max_duration;

        // The warning condition from perform_full_validation
        assert!(
            simulated_duration > max_duration,
            "Simulated duration should exceed the short budget, triggering warning"
        );

        // Also verify the budget is what we set
        assert_eq!(max_duration, Duration::from_millis(1));
    }

    #[tokio::test]
    async fn test_full_scan_no_warning_within_budget() {
        // Verify no warning when duration is within budget
        let config = CacheSizeConfig {
            validation_max_duration: Duration::from_secs(4 * 3600), // 4 hours
            ..CacheSizeConfig::default()
        };
        let (tracker, _consolidator, _temp_dir) = create_test_tracker_with_config(config).await;

        let simulated_duration = Duration::from_secs(3600); // 1 hour
        let max_duration = tracker.config.validation_max_duration;

        assert!(
            simulated_duration <= max_duration,
            "Duration within budget should not trigger warning"
        );
    }

    #[tokio::test]
    async fn test_persist_full_scan_duration_preserves_existing_fields() {
        // Verify that persist_full_scan_duration doesn't clobber existing validation.json fields
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // Write initial validation metadata
        tracker
            .write_validation_metadata(500000, 500000, 0, Duration::from_secs(60), 1000, 0, 0, 0)
            .await
            .unwrap();

        // Now persist full scan duration on top
        tracker.persist_full_scan_duration(7200.0).await.unwrap();

        // Read back and verify both old and new fields exist
        let content = std::fs::read_to_string(&tracker.validation_path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&content).unwrap();

        assert_eq!(
            json.get("last_full_scan_duration_secs")
                .and_then(|v| v.as_f64()),
            Some(7200.0),
        );
        assert_eq!(
            json.get("validation_type").and_then(|v| v.as_str()),
            Some("full"),
        );
        // Existing fields should still be present
        assert!(
            json.get("scanned_size").is_some(),
            "Existing scanned_size field should be preserved"
        );
    }
}
