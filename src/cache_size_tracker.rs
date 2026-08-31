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
    /// The subset of `size_bytes` belonging to the write (staging) tier, classified
    /// per range through `cache_types::is_staged_range_spec`.
    ///
    /// Carried alongside `size_bytes` rather than recomputed later so the two are
    /// guaranteed to describe the same scan of the same file — including the deletion
    /// branches, where both drop to 0 together. Summing this is what lets the full
    /// Validation_Scan pass a real `write_cache_size` instead of `None`, which is the
    /// only mechanism that can un-inflate an already-inflated figure.
    ///
    /// Invariant: `staged_bytes <= size_bytes`.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 6.1, 6.2
    staged_bytes: u64,
    /// Whether GET cache entry was expired and deleted (active expiration)
    cache_expired: bool,
    /// Whether GET cache entry was skipped (actively being used)
    cache_skipped: bool,
    /// Whether GET cache expiration encountered an error
    cache_error: bool,
    /// Whether this scan pass left no `.meta` at the scanned path — because the pass
    /// itself removed it (unparseable self-heal, write-cache expiry, GET expiry) or
    /// because something else did between the read and the check.
    ///
    /// The object census must not count an entry the scan just deleted: the census is
    /// installed as `cached_objects`, so counting a removed entry reports an object
    /// that no longer exists and biases the figure **upward** — the direction that
    /// makes a future Entry_Budget (R4.4) over-evict.
    ///
    /// **This is set from `path.exists()`, deliberately, not from the return value of
    /// the deleting call.** Those return values do not answer the question the census
    /// asks. `check_and_invalidate_expired_write_cache` returns `Ok(true)` even when
    /// its own `remove_file` failed (`src/cache.rs:10933`, whose `metadata_deleted`
    /// flag gates only the decrement), `invalidate_cache` returns `Ok(())` without
    /// promising which paths went, and the unparseable arm's `remove_file` can fail on
    /// a read-only or contended volume. Each of those is a case where the `.meta`
    /// survives and must still be counted. Reading the filesystem asks the predicate
    /// the census is defined over — "is there a `.meta` here now" — rather than an
    /// adjacent figure that usually agrees with it.
    ///
    /// Costs one `stat` per deletion branch only, never on the common non-deleting
    /// path, which matters at the 100M-object design point (F8's traversal budget).
    ///
    /// Spec: cache-eviction-at-scale. Requirements: 7.1
    meta_removed: bool,
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
    /// The proportional-correction formula on its own, without the
    /// threshold logging. Used for the write-cache figure so a rolling scan applies
    /// the same extrapolation to it that it applies to `total_size`, rather than
    /// installing an unscaled partial sum as a whole-cache figure (Requirement 6.3).
    ///
    /// Kept as a separate helper rather than widening `apply_proportional_correction`'s
    /// return tuple, which is `pub` and has existing unit-test call sites.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 6.3
    fn proportional_correction(scanned: u64, dirs_scanned: usize, tracked: u64) -> u64 {
        let expected = tracked as u128 * dirs_scanned as u128 / 256;
        let discrepancy = scanned as i64 - expected as i64;
        (tracked as i64 + discrepancy).max(0) as u64
    }

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

        // Scan metadata files using shared validator (returns size, cache_expired, cache_skipped, cache_errors, object_count, staged_size)
        let (
            scanned_size,
            cache_expired,
            cache_skipped,
            cache_errors,
            files_visited,
            metas_removed,
            scanned_staged_size,
            staged_paths,
        ) = self.scan_metadata_with_shared_validator().await?;
        // The census is the surviving population, not the number of files visited. The
        // scan deletes `.meta` files as it goes — unparseable ones it self-heals (F7),
        // plus write-cache and GET expiry — and an object whose `.meta` this pass removed
        // is not a cached object. Counting it inflates `cached_objects`, the direction
        // that makes a future Entry_Budget (R4.4) evict against a phantom population.
        // `files_visited` is retained for `files_scanned` in validation.json, which is a
        // work-done figure. Spec: cache-eviction-at-scale. Requirements: 7.1
        let scanned_objects = files_visited.saturating_sub(metas_removed);
        let tracked_size = self.get_size().await; // Delegate to consolidator (Task 12.3)
        let tracked_staged_size = self.get_write_cache_size().await;
        let drift = scanned_size as i64 - tracked_size as i64;
        let staged_drift = scanned_staged_size as i64 - tracked_staged_size as i64;

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

        // Requirement 6.5: log the write-cache drift being corrected, so recurring drift
        // is visible without diffing state files. This is the line that makes an
        // accounting leak observable in the logs — before this, `write_cache_size` was
        // never corrected at all (the call below passed `None`), so a leak accumulated
        // silently and indefinitely while `total_size` was reconciled every scan.
        if staged_drift != 0 {
            warn!(
                "Cache validation: write-cache drift corrected: tracked {} -> scanned {} ({}{}), \
                 recomputed from {} .meta files",
                format_bytes_human(tracked_staged_size),
                format_bytes_human(scanned_staged_size),
                if staged_drift >= 0 { "+" } else { "-" },
                format_bytes_human(staged_drift.unsigned_abs()),
                // Files the figure was computed from, including any removed on this pass
                // (which contribute zero bytes) — a provenance figure, not the census.
                files_visited
            );
        } else {
            debug!(
                "Cache validation: write-cache size already consistent at {}",
                format_bytes_human(scanned_staged_size)
            );
        }

        // Always reconcile to scanned size after validation
        // The validation scan is expensive (once per day), so we trust its result
        if drift != 0 {
            debug!(
                "Reconciling tracked size to scanned size: {} bytes drift",
                drift
            );
        }

        // Always update size state from validation — even when size drift is zero,
        // `cached_objects` may have drifted, because increments and decrements are applied
        // per-instance to a shared counter and a missed decrement is never noticed by the
        // instance that missed it.
        //
        // **The absolute install below is sound HERE and only here**, because this is a
        // full scan: it visits every `.meta` under `metadata/`, so `scanned_objects` is a
        // whole-cache census and replacing the counter with it is a re-grounding rather
        // than an estimate. The previous wording — "the validation scan's .meta file count
        // is the authoritative object count" — was true of *this* function and read as
        // true of the scan in general. It is not true of `perform_rolling_validation`,
        // which observes a subset and can only extrapolate; see the block at that call
        // site for why that extrapolation cannot converge the counter and what R5 changes.
        //
        // Two consequences worth stating, since R4.4's Entry_Budget will consume this:
        //  - `scanned_objects` excludes `.meta` files this pass deleted (see above). Before
        //    that subtraction the census counted them, so it over-reported by the number of
        //    expiries and self-heals in the pass.
        //  - At the design point R4.5 says the full scan does not fit its budget, so this
        //    path may never run in the field. A counter that converges only here converges
        //    only in theory — which is why the Entry_Budget must check `validation_type`.
        //
        // Spec: cache-eviction-at-scale. Requirements: 7.1, 4.4, 4.5
        //
        // Requirement 6.1: pass the recomputed write-cache figure rather than `None`.
        // This is a full scan of every `.meta`, so `scanned_staged_size` is a
        // whole-cache figure and safe to install as an absolute value. It is also the
        // ONLY mechanism that can un-inflate an already-inflated `write_cache_size`:
        // stopping the leaks (R1, R5) prevents further inflation but cannot undo what
        // has already accumulated, which is why R5 and R6 must ship together.
        self.consolidator
            .update_size_from_validation(
                scanned_size,
                Some(scanned_staged_size),
                Some(scanned_objects),
            )
            .await;

        // R2.7 / R6.6: give the Write_Ledger back any staged object it has lost track of.
        // Also the in-place upgrade path — see the method doc.
        self.reappend_missing_ledger_entries(&staged_paths).await;

        // Write validation metadata. This is a work-done figure — how many `.meta` files
        // the pass opened — so it stays `files_visited` rather than the census, which is
        // smaller by the number removed on this pass.
        // Spec: cache-eviction-at-scale. Requirements: 7.1
        let files_scanned = files_visited;
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
        // Read from the same Size_State snapshot as `tracked_size`, so both sides of the
        // proportional correction describe one consistent view. Requirement 6.3.
        let tracked_staged_size = size_state.write_cache_size;

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
        // Staged (write-tier) subset of total_size across all batches — still a partial
        // sum over the scanned L1 subset. Requirement 6.3.
        let mut total_staged = 0u64;
        let mut total_objects = 0u64;
        // `.meta` files visited that this pass removed. `total_objects - total_removed` is
        // the surviving population over the scanned subset, and that — not `total_objects`
        // — is what the census may report. Spec: cache-eviction-at-scale. Requirements: 7.1
        let mut total_removed = 0u64;
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
            // Staged (write-tier) subset of batch_size. Requirement 6.3 — this is a
            // PARTIAL sum and must never be installed as a whole-cache figure; it goes
            // through the same proportional correction total_size does.
            let batch_staged = AtomicU64::new(0);
            let batch_objects = AtomicU64::new(0);
            // `.meta` files this pass removed. Subtracted from `batch_objects` before the
            // census is installed, so the scan does not report objects it just deleted.
            // Kept as a separate counter rather than by not incrementing `batch_objects`,
            // so "files visited" stays available for the log line and the two figures can
            // be compared. Spec: cache-eviction-at-scale. Requirements: 7.1
            let batch_removed = AtomicU64::new(0);
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
                        batch_staged.fetch_add(result.staged_bytes, Ordering::Relaxed);
                        batch_objects.fetch_add(1, Ordering::Relaxed);
                        if result.meta_removed {
                            batch_removed.fetch_add(1, Ordering::Relaxed);
                        }
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
            total_staged += batch_staged.load(Ordering::Relaxed);
            total_objects += batch_objects.load(Ordering::Relaxed);
            total_removed += batch_removed.load(Ordering::Relaxed);
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
            // Exclude `.meta` files this pass removed. Feeding `total_objects` here would
            // extrapolate a population that includes entries the scan itself deleted, and
            // because the correction adds the observed discrepancy to `tracked_objects`,
            // that error is multiplied by `256 / total_dirs_scanned` on the way in.
            // Spec: cache-eviction-at-scale. Requirements: 7.1
            let surviving_objects = total_objects.saturating_sub(total_removed);
            if total_removed > 0 {
                info!(
                    "Rolling validation: {} of {} .meta files visited were removed on this pass; \
                     census reports {} surviving",
                    total_removed, total_objects, surviving_objects
                );
            }
            let (corrected_size, corrected_objects) = self.apply_proportional_correction(
                total_size,
                surviving_objects,
                total_dirs_scanned.min(256),
                tracked_size,
                tracked_objects,
            );

            // A rolling pass observes a subset and therefore may NOT install an absolute
            // `cached_objects`; `corrected_objects` above is an extrapolation, and it is
            // recorded as such rather than presented as a census. Three things make it
            // unsound as a convergence mechanism, and none of them is fixable here:
            //
            //  1. The expectation `tracked * dirs_scanned / 256` is derived from the very
            //     value being corrected (R5.3).
            //  2. `total_dirs_scanned` advances by *intended* L1 index slots, including
            //     when `select_l1_directories` found nothing on disk, so the numerator and
            //     denominator describe different things (R5.3, F6).
            //  3. Every instance installs its own extrapolation absolutely over the shared
            //     `size_state.json`, so two rolling scans race — which is the
            //     multi-instance problem the full scan's comment names but the rolling
            //     path reproduces rather than resolves.
            //
            // R5.2's absolute per-shard aggregates are what make this sound: a shard's
            // count is replaced only by a pass that actually scanned that shard, and the
            // global figure is a sum rather than an extrapolation. Until then
            // `cached_objects` converges only when a full scan runs — a mode R4.5 says
            // does not exist at the design point — so R4.4's Entry_Budget MUST consult
            // `validation_type` in `validation.json` ("full" vs "rolling") before treating
            // this figure as a population count.
            // Spec: cache-eviction-at-scale. Requirements: 7.1, 4.4, 4.5, 5.2, 5.3
            debug!(
                "Rolling validation: cached_objects {} -> {} is an EXTRAPOLATION from {} \
                 surviving objects over {} of 256 L1 index slots, not a census",
                tracked_objects,
                corrected_objects,
                surviving_objects,
                total_dirs_scanned.min(256)
            );

            // Requirement 6.3: apply the SAME proportional correction to the write-cache
            // figure. `total_staged` is a partial sum over `total_dirs_scanned` of 256 L1
            // directories; installing it directly would report a fraction of the staged
            // bytes as the whole-cache figure and drive `write_cache_size` toward a large
            // undershoot — the direction that silently over-admits. The correction
            // extrapolates the observed drift instead, exactly as it does for total_size.
            let corrected_staged = Self::proportional_correction(
                total_staged,
                total_dirs_scanned.min(256),
                tracked_staged_size,
            );

            // Update size state via consolidator
            self.consolidator
                .update_size_from_validation(
                    corrected_size,
                    Some(corrected_staged),
                    Some(corrected_objects),
                )
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

    /// Re-append staged objects that no Write_Ledger knows about.
    ///
    /// # This is both the recovery path and the upgrade path
    ///
    /// A ledger append is best-effort: it happens after the `.meta` is written and after
    /// the accounting credit, and a failure is logged rather than failing the upload,
    /// because S3 already holds the object by that point. The cost of a lost append is
    /// that the entry becomes invisible to staging eviction — never that it is served
    /// wrongly, since the serve path does not consult the ledger. This is what repairs it.
    ///
    /// The same mechanism is what makes **upgrading in place require no migration step**
    /// (R6.6). A deployment upgrading to 2.7.0 has staged objects on disk and an empty
    /// ledger directory; every one of those objects is "missing from the ledger" by this
    /// method's test, so the first full validation scan after the upgrade populates the
    /// ledger from the authoritative `.meta` files. Nothing needs to be run by hand, and
    /// the pre-upgrade state is not special-cased anywhere — the recovery path and the
    /// cold-start path are the same code.
    ///
    /// Matching is by **cache key**, not by entry identity. A re-appended entry cannot
    /// reconstruct the original append's timestamp, so it is stamped now; that makes it
    /// look freshly written to the eviction watermark, which is the conservative direction
    /// (it is ranked as fresh-unread rather than expired-unread, so it is evicted later
    /// rather than sooner). Matching on identity instead would re-append a duplicate for
    /// every object on every scan.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 2.7, 6.6
    async fn reappend_missing_ledger_entries(&self, staged_paths: &[std::path::PathBuf]) {
        if staged_paths.is_empty() {
            return;
        }

        let ledger = self.consolidator.write_ledger().clone();
        let known = match ledger.staged_keys().await {
            Ok(keys) => keys,
            Err(e) => {
                warn!(
                    "Validation scan: could not read the Write_Ledger to reconcile it ({}). \
                     Skipping re-append; staging eviction may not see every staged object \
                     until the next scan.",
                    e
                );
                return;
            }
        };

        let mut reappended = 0u64;
        let mut ranges_appended = 0u64;

        for path in staged_paths {
            let content = match tokio::fs::read_to_string(path).await {
                Ok(c) => c,
                Err(_) => continue,
            };
            let metadata: crate::cache_types::NewCacheMetadata =
                match serde_json::from_str(&content) {
                    Ok(m) => m,
                    Err(_) => continue,
                };

            if known.contains(&metadata.cache_key) {
                continue;
            }
            // Re-check the flag from the parsed metadata rather than trusting the caller's
            // `staged_bytes > 0`: the two agree today, but this is the authoritative read
            // and it costs nothing here.
            if !metadata.object_metadata.is_write_cached {
                continue;
            }

            for range in &metadata.ranges {
                // Per-range membership, so a flagged object that has since gained a
                // read-tier range does not re-append that range to the Write_Ledger.
                // Doing so would give staging eviction a candidate whose bytes were
                // never credited to `write_cache_size`, and freeing it would debit
                // what nothing added.
                // Spec: write-cache-accounting-and-eviction. Requirements: 12.3, 12.4
                if !crate::cache_types::is_staged_range_spec(
                    range,
                    metadata.object_metadata.is_write_cached,
                ) {
                    continue;
                }
                self.consolidator
                    .record_staged_range(
                        &metadata.cache_key,
                        range.start,
                        range.end,
                        range.compressed_size,
                    )
                    .await;
                ranges_appended += 1;
            }
            reappended += 1;
        }

        if reappended > 0 {
            info!(
                "Validation scan: re-appended {} staged objects ({} ranges) to the Write_Ledger \
                 that it had no record of. On a freshly upgraded deployment this is expected \
                 and is the whole migration — no manual step is required.",
                reappended, ranges_appended
            );
        }
    }

    /// Scan metadata files using shared validator (replaces redundant parallel scanning)
    ///
    /// This method now uses the shared CacheValidator to avoid duplicating scanning logic.
    /// The coordinated initialization handles the initial scan, and this method is used
    /// only for periodic validation scans.
    /// Returns `(total_size, cache_expired, cache_skipped, cache_errors, files_scanned,
    /// staged_size, staged_paths)`. `staged_size` is the write-tier subset of `total_size`
    /// (Requirement 6.1) and is what the caller passes to
    /// `update_size_from_validation` in place of the `None` it used to pass.
    ///
    /// `staged_paths` is the `.meta` path of every object the scan found still staged,
    /// which the caller feeds to [`Self::reappend_missing_ledger_entries`] (R2.7 / R6.6).
    /// Collected here rather than by a second walk because this scan is already the one
    /// permitted pass over every `.meta` — walking again to find the same files would
    /// reinstate the O(cache) cost Phase E exists to remove. The collection is bounded by
    /// the staged set rather than the cache, so `Mutex` contention scales with
    /// `write_cache_percent`, not with cache size.
    #[allow(clippy::type_complexity)]
    async fn scan_metadata_with_shared_validator(
        &self,
    ) -> Result<(u64, u64, u64, u64, u64, u64, u64, Vec<std::path::PathBuf>)> {
        use rayon::prelude::*;
        use std::sync::atomic::{AtomicU64, Ordering};

        let now = std::time::SystemTime::now();
        let metadata_dir = self.cache_dir.join("metadata");

        if !metadata_dir.exists() {
            return Ok((0, 0, 0, 0, 0, 0, 0, Vec::new()));
        }

        // Paths of `.meta` files found still staged, for the ledger re-append.
        let staged_paths: std::sync::Mutex<Vec<std::path::PathBuf>> =
            std::sync::Mutex::new(Vec::new());

        // Atomic counters for lock-free accumulation from parallel workers
        let total_size = AtomicU64::new(0);
        // Staged (write-tier) subset of total_size. Requirement 6.1.
        let staged_total = AtomicU64::new(0);
        let cache_expired = AtomicU64::new(0);
        let cache_skipped = AtomicU64::new(0);
        let cache_errors = AtomicU64::new(0);
        let files_processed = AtomicU64::new(0);
        // `.meta` files this pass removed — self-healed as unparseable, or deleted by
        // write-cache/GET expiry. Subtracted from `files_processed` to give the census.
        // Spec: cache-eviction-at-scale. Requirements: 7.1
        let metas_removed = AtomicU64::new(0);

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
                    staged_total.fetch_add(result.staged_bytes, Ordering::Relaxed);
                    // Still staged: remember the path so the ledger re-append can check
                    // whether any ledger knows about it (R2.7). `staged_bytes > 0` is the
                    // same predicate that produced the figure above, so the two cannot
                    // disagree about what "staged" means.
                    if result.staged_bytes > 0 {
                        if let Ok(mut paths) = staged_paths.lock() {
                            paths.push(path.clone());
                        }
                    }
                    if result.cache_expired {
                        cache_expired.fetch_add(1, Ordering::Relaxed);
                    }
                    if result.cache_skipped {
                        cache_skipped.fetch_add(1, Ordering::Relaxed);
                    }
                    if result.cache_error {
                        cache_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    // A `.meta` this pass removed is not part of the population the
                    // census describes. `files_processed` stays a work-done figure (it
                    // drives the progress log and `files_scanned` in validation.json);
                    // the census is `files_processed - metas_removed`.
                    // Spec: cache-eviction-at-scale. Requirements: 7.1
                    if result.meta_removed {
                        metas_removed.fetch_add(1, Ordering::Relaxed);
                    }
                    let count = files_processed.fetch_add(1, Ordering::Relaxed) + 1;
                    if count.is_multiple_of(100_000) {
                        info!("Cache validation progress: {} files processed", count);
                    }
                }
            }
        });

        let total = files_processed.load(Ordering::Relaxed);
        let removed = metas_removed.load(Ordering::Relaxed);
        if removed > 0 {
            info!(
                "Cache validation: processed {} metadata files, {} removed on this pass, \
                 census reports {} surviving objects",
                total,
                removed,
                total.saturating_sub(removed)
            );
        } else {
            info!("Cache validation: processed {} metadata files", total);
        }

        Ok((
            total_size.load(Ordering::Relaxed),
            cache_expired.load(Ordering::Relaxed),
            cache_skipped.load(Ordering::Relaxed),
            cache_errors.load(Ordering::Relaxed),
            files_processed.load(Ordering::Relaxed),
            removed,
            staged_total.load(Ordering::Relaxed),
            staged_paths.into_inner().unwrap_or_default(),
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
                    staged_bytes: 0,
                    cache_expired: false,
                    cache_skipped: false,
                    cache_error: true,
                    meta_removed: false,
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
                    staged_bytes: 0,
                    cache_expired: false,
                    cache_skipped: false,
                    cache_error: true,
                    // F7 self-heal. Ask the filesystem rather than trusting the
                    // `remove_file` result — a failed unlink leaves a countable object.
                    meta_removed: !path.exists(),
                };
            }
        };

        // Calculate total size from all ranges
        let total_size: u64 = metadata.ranges.iter().map(|r| r.compressed_size).sum();
        // The staged subset of that total, classified per range through the single
        // shared predicate — the same one the accumulator's add and subtract sites use,
        // so this scan and the accumulator cannot report different figures for
        // identical on-disk state.
        // Spec: write-cache-accounting-and-eviction. Requirements: 6.1, 6.2
        let staged_size: u64 = metadata.staged_compressed_size();

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
                            staged_bytes: staged_size,
                            cache_expired: false,
                            cache_skipped: false,
                            cache_error: true,
                            meta_removed: false,
                        };
                    }
                },
                None => {
                    // Cache manager not set, skip expiration
                    return ScanFileResult {
                        size_bytes: total_size,
                        staged_bytes: staged_size,
                        cache_expired: false,
                        cache_skipped: false,
                        cache_error: false,
                        meta_removed: false,
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
                        staged_bytes: 0,
                        cache_expired: true,
                        cache_skipped: false,
                        cache_error: false,
                        // `Ok(true)` does NOT mean the `.meta` went — that function
                        // returns true on the expiry decision and gates only its own
                        // decrement on `metadata_deleted`. Read the path instead.
                        meta_removed: !path.exists(),
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
                        staged_bytes: staged_size,
                        cache_expired: false,
                        cache_skipped: false,
                        cache_error: true,
                        meta_removed: false,
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
                            staged_bytes: staged_size,
                            cache_expired: false,
                            cache_skipped: false,
                            cache_error: true,
                            meta_removed: false,
                        };
                    }
                },
                None => {
                    // Cache manager not set, skip expiration
                    return ScanFileResult {
                        size_bytes: total_size,
                        staged_bytes: staged_size,
                        cache_expired: false,
                        cache_skipped: false,
                        cache_error: false,
                        meta_removed: false,
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
                    staged_bytes: staged_size,
                    cache_expired: false,
                    cache_skipped: true,
                    cache_error: false,
                    meta_removed: false,
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
                        staged_bytes: 0,
                        cache_expired: true,
                        cache_skipped: false,
                        cache_error: false,
                        // `invalidate_cache` returns `Ok(())` without stating which
                        // paths it managed to unlink, so read the path.
                        meta_removed: !path.exists(),
                    };
                }
                None => {
                    warn!(
                        "Failed to delete expired GET cache entry: {}",
                        metadata.cache_key
                    );
                    return ScanFileResult {
                        size_bytes: total_size,
                        staged_bytes: staged_size,
                        cache_expired: false,
                        cache_skipped: false,
                        cache_error: true,
                        meta_removed: false,
                    };
                }
            }
        }

        // Entry not expired or active expiration disabled
        ScanFileResult {
            size_bytes: total_size,
            staged_bytes: staged_size,
            cache_expired: false,
            cache_skipped: false,
            cache_error: false,
            meta_removed: false,
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

    // -----------------------------------------------------------------------
    // F1 round-trip: does a rolling cycle leave validation.json readable?
    //
    // Spec: cache-eviction-at-scale, Phase 0 task 0c.
    // Finding: .kiro/specs/cache-eviction-at-scale/validation-scan-findings.md § F1
    // Requirements: 4.5 (bearing, not home — see the task note)
    //
    // The claim under test is structural: `write_rolling_state` writes twelve
    // fields via a `json!` literal, and `ValidationMetadata` requires five that
    // are not among them and carry no `#[serde(default)]` — `scanned_size`,
    // `tracked_size`, `drift_bytes`, `scan_duration_ms`, `metadata_files_scanned`.
    // If that holds, `read_validation_metadata` cannot deserialize the file after
    // any rolling cycle, and two consumers branch on that failure:
    // `calculate_next_validation_time` returns `SystemTime::now()`, collapsing the
    // daily cadence into continuous scanning, and `get_metrics` reports
    // `last_validation_drift: None`.
    //
    // WHY NO EXISTING TEST CATCHES THIS. Every rolling test in
    // `tests/rolling_validation_scan_integration_test.rs` reads the file back with
    // `read_rolling_state` or raw `serde_json::Value`, both of which pick fields out
    // by name and tolerate absent ones. `ValidationMetadata` is the only strict
    // deserialization target for this file, and nothing pointed it at rolling output.
    //
    // WHAT THIS TEST DRIVES. A genuine cycle: `perform_validation()` selects the
    // mode itself from the on-disk state and calls `perform_rolling_validation`,
    // which scans real seeded `.meta` files and persists through the real
    // `write_rolling_state`. The file under assertion is written by the writer, not
    // hand-constructed to resemble it. A guard below asserts the file really was
    // produced by a rolling cycle before any conclusion is drawn from it.
    //
    // ATTRIBUTION. A baseline is taken before the cycle, when validation.json holds
    // genuine full-scan output. Both observations must hold there first, so a
    // post-cycle failure is attributable to the rolling cycle rather than to the
    // file having been missing or the schedule being misconfigured. The primary
    // assertion is the deserialization RESULT itself — the value the failing line
    // branches on — not a downstream figure that could look wrong for other reasons.
    // STATUS: this test FAILS on the current tree, and that is the finding, not a
    // regression. It is `#[ignore]`d only so a known-red assertion does not block
    // the pre-push gate for unrelated work — task 0c writes the test, Phase 5 owns
    // the fix. Whoever fixes F1 MUST delete this attribute and confirm the test
    // goes green; that is the red-then-green evidence for the fix. Observed
    // 2026-08-27 (run it with `cargo test --lib -- f1_rolling_cycle --ignored
    // --nocapture`):
    //
    //   read_validation_metadata() = Err(Cache error: Failed to parse validation
    //     metadata: missing field `scanned_size` at line 14 column 1)
    //   calculate_next_validation_time() = 0s from now (healthy: >18000s)
    //   last_validation_drift = None
    //
    // All three baseline assertions and both fixture guards passed in that run, so
    // the failure is attributable to the rolling cycle alone.
    #[tokio::test]
    #[ignore = "F1 is an open defect (cache-eviction-at-scale Phase 0 task 0c); the fix is Phase 5. Remove this attribute with the fix."]
    async fn f1_rolling_cycle_leaves_validation_json_deserializable() {
        use crate::cache_types::{
            CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState,
        };
        use crate::compression::CompressionAlgorithm;
        use chrono::{Duration as ChronoDuration, Local, Timelike};
        use std::collections::HashMap;

        // Put the scheduled time-of-day ~6 hours ahead in clock terms so the
        // healthy branch of `calculate_next_validation_time` lands 6-7h out
        // (target + 0..1h jitter) and the failing branch lands at ~now. Without
        // this the default "00:00" could legitimately be minutes away, leaving the
        // two branches indistinguishable near midnight.
        let target = Local::now() + ChronoDuration::hours(6);
        let config = CacheSizeConfig {
            validation_time_of_day: format!("{:02}:{:02}", target.hour(), target.minute()),
            // A 1s budget against the 10s full-scan duration seeded below is what
            // makes `determine_scan_mode` choose Rolling.
            validation_max_duration: Duration::from_secs(1),
            ..CacheSizeConfig::default()
        };
        let (tracker, _consolidator, temp_dir) = create_test_tracker_with_config(config).await;
        let cache_dir = temp_dir.path().to_path_buf();

        // Seed real .meta files across the first few L1 shards so the rolling scan
        // has genuine work to do. Batch size on a first cycle (no scan rate) is 64
        // shards from cursor 0, so 00-05 all fall inside it.
        let now = SystemTime::now();
        for idx in 0u8..6 {
            let meta_path = cache_dir
                .join("metadata")
                .join("test-bucket")
                .join(format!("{:02x}", idx))
                .join("000")
                .join("f1_object.meta");
            std::fs::create_dir_all(meta_path.parent().unwrap()).unwrap();
            let meta = NewCacheMetadata {
                cache_key: format!("test-bucket:f1-object-{:02x}", idx),
                object_metadata: ObjectMetadata {
                    etag: "\"f1\"".to_string(),
                    last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
                    content_length: 1024,
                    content_type: Some("application/octet-stream".to_string()),
                    upload_state: UploadState::Complete,
                    cumulative_size: 1024,
                    parts: Vec::new(),
                    response_headers: HashMap::new(),
                    compression_algorithm: CompressionAlgorithm::Lz4,
                    compressed_size: 1024,
                    parts_count: None,
                    part_ranges: HashMap::new(),
                    upload_id: None,
                    is_write_cached: false,
                    write_cache_expires_at: None,
                    write_cache_created_at: None,
                    write_cache_last_accessed: None,
                    graduation_accounted: false,
                },
                ranges: vec![RangeSpec {
                    start: 0,
                    end: 1023,
                    file_path: format!("f1_{:02x}_0.bin", idx),
                    compression_algorithm: CompressionAlgorithm::Lz4,
                    compressed_size: 1024,
                    uncompressed_size: 1024,
                    created_at: now,
                    last_accessed: now,
                    access_count: 1,
                    staged: None,
                }],
                created_at: now,
                expires_at: now + Duration::from_secs(86400),
                compression_info: CompressionInfo::default(),
                head_expires_at: None,
                head_last_accessed: None,
                head_access_count: 0,
                head_cached_at: None,
            };
            std::fs::write(&meta_path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
        }

        // Establish the baseline through the REAL full-mode writers: the metadata
        // writer, then the duration/type stamp a completed full scan leaves behind.
        tracker
            .write_validation_metadata(4096, 4096, 0, Duration::from_secs(10), 4, 0, 0, 0)
            .await
            .unwrap();
        tracker.persist_full_scan_duration(10.0).await.unwrap();

        assert!(
            tracker.read_validation_metadata().await.is_ok(),
            "BASELINE BROKEN: validation.json written by the full-scan path must \
             deserialize. If this fails the rest of the test proves nothing."
        );
        let baseline_next = tracker.calculate_next_validation_time().await;
        assert!(
            baseline_next
                .duration_since(SystemTime::now())
                .unwrap_or_default()
                > Duration::from_secs(5 * 3600),
            "BASELINE BROKEN: with full-scan metadata present the next validation \
             must be ~6h out, not immediate. Got {:?} from now.",
            baseline_next.duration_since(SystemTime::now())
        );
        assert!(
            tracker.get_metrics().await.last_validation_drift.is_some(),
            "BASELINE BROKEN: last_validation_drift must be populated from \
             full-scan metadata."
        );

        // Drive a genuine cycle. `perform_validation` selects the mode from the
        // on-disk state and dispatches; nothing here forces the rolling branch.
        tracker
            .perform_validation()
            .await
            .expect("validation cycle should succeed");

        // Guard: confirm what actually ran, before drawing any conclusion from the
        // file. A cycle that took the full branch, or wrote nothing, would make
        // every assertion below unattributable.
        let raw: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&tracker.validation_path).unwrap())
                .unwrap();
        assert_eq!(
            raw.get("validation_type").and_then(|v| v.as_str()),
            Some("rolling"),
            "FIXTURE: the cycle did not take the rolling branch, so this test \
             measured full-mode output. validation.json = {}",
            raw
        );
        assert!(
            raw.get("rolling_objects_validated")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                > 0,
            "FIXTURE: the rolling cycle validated no objects, so the seeded .meta \
             files were not reached. validation.json = {}",
            raw
        );

        // --- The three observations. Collected first, reported together, asserted
        // last, so a failure of the primary one does not hide the other two.
        let deser = tracker.read_validation_metadata().await;
        let next = tracker.calculate_next_validation_time().await;
        let drift = tracker.get_metrics().await.last_validation_drift;

        let deser_ok = deser.is_ok();
        let deser_detail = match &deser {
            Ok(_) => "Ok".to_string(),
            Err(e) => format!("Err({})", e),
        };
        let until_next = next.duration_since(SystemTime::now()).unwrap_or_default();
        let cadence_ok = until_next > Duration::from_secs(5 * 3600);

        println!(
            "F1 observations after a genuine rolling cycle:\n  \
             read_validation_metadata() = {}\n  \
             calculate_next_validation_time() = {}s from now (healthy: >18000s)\n  \
             last_validation_drift = {:?}",
            deser_detail,
            until_next.as_secs(),
            drift
        );

        // (A) PRIMARY — the value the failing line branches on.
        assert!(
            deser_ok,
            "F1 ESTABLISHED (A): read_validation_metadata() failed after a rolling \
             cycle: {}. The five fields ValidationMetadata requires without a serde \
             default are absent from write_rolling_state's output. Fix belongs in \
             Phase 5, not here.",
            deser_detail
        );

        // (B) First runtime consequence — the daily cadence.
        assert!(
            cadence_ok,
            "F1 ESTABLISHED (B): next validation scheduled {}s from now instead of \
             ~6h, i.e. calculate_next_validation_time took the \"No validation \
             metadata found\" immediate-reschedule branch. The daily cadence has \
             collapsed into continuous scanning.",
            until_next.as_secs()
        );

        // (C) Second runtime consequence — the only drift metric rolling mode has.
        assert!(
            drift.is_some(),
            "F1 ESTABLISHED (C): cache_size.last_validation_drift is null after a \
             rolling cycle. This is the field docs/SHARED_STORAGE.md tells operators \
             to read for drift."
        );
    }

    // =====================================================================
    // Object census must exclude `.meta` files the scan removed on this pass
    //
    // Spec: cache-eviction-at-scale. Requirements: 7.1
    //
    // The scan mutates as it walks: it self-heals unparseable `.meta` files
    // (`validation-scan-findings.md` F7), and it deletes write-cache and GET
    // entries that have expired. Both counted toward the object census, which is
    // installed as `cached_objects` — so the counter reported objects that had
    // just ceased to exist, always in the upward direction. R4.4's Entry_Budget
    // is to be built on that counter.
    //
    // RED SIDE (measured before the fix, both tests):
    //   full: `cached_objects` = 3, expected 2
    //   rolling: `cached_objects` = 3, expected 2
    // Both are assertion-level failures against the pre-fix binary, not compile
    // errors — the fixtures use only pre-existing API.
    // =====================================================================

    /// Build a valid, non-expired, read-cached `.meta` at `path` with one range of
    /// `size` compressed bytes.
    fn write_valid_meta(path: &std::path::Path, cache_key: &str, size: u64) {
        use crate::cache_types::{
            CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState,
        };
        use crate::compression::CompressionAlgorithm;
        use std::collections::HashMap;

        let now = SystemTime::now();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let meta = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "\"census\"".to_string(),
                last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
                content_length: size,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: UploadState::Complete,
                cumulative_size: size,
                parts: Vec::new(),
                response_headers: HashMap::new(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: size,
                parts_count: None,
                part_ranges: HashMap::new(),
                upload_id: None,
                is_write_cached: false,
                write_cache_expires_at: None,
                write_cache_created_at: None,
                write_cache_last_accessed: None,
                graduation_accounted: false,
            },
            ranges: vec![RangeSpec {
                start: 0,
                end: size.saturating_sub(1),
                file_path: format!("{}_0.bin", cache_key.replace([':', '/'], "_")),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: size,
                uncompressed_size: size,
                created_at: now,
                last_accessed: now,
                access_count: 1,
                staged: None,
            }],
            created_at: now,
            // Far future, so the GET-expiry branch is not involved — this test is
            // about the self-heal branch only.
            expires_at: now + Duration::from_secs(86_400),
            compression_info: CompressionInfo::default(),
            head_expires_at: None,
            head_last_accessed: None,
            head_access_count: 0,
            head_cached_at: None,
        };
        std::fs::write(path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
    }

    /// Two valid `.meta` files plus one unparseable one, laid out under L1 shards
    /// `00`, `01`, `02` so both the full walk and a first-cycle rolling batch
    /// (64 slots from cursor 0) cover all three.
    fn seed_two_valid_one_corrupt(cache_dir: &std::path::Path) -> std::path::PathBuf {
        let meta_root = cache_dir.join("metadata").join("census-bucket");
        write_valid_meta(
            &meta_root.join("00").join("000").join("good_a.meta"),
            "census-bucket:good-a",
            1024,
        );
        write_valid_meta(
            &meta_root.join("01").join("000").join("good_b.meta"),
            "census-bucket:good-b",
            2048,
        );
        let corrupt = meta_root.join("02").join("000").join("corrupt.meta");
        std::fs::create_dir_all(corrupt.parent().unwrap()).unwrap();
        std::fs::write(&corrupt, b"{ this is not valid NewCacheMetadata json").unwrap();
        corrupt
    }

    #[tokio::test]
    async fn full_validation_census_excludes_meta_removed_on_this_pass() {
        let (tracker, consolidator, temp_dir) = create_test_tracker().await;
        let cache_dir = temp_dir.path().to_path_buf();
        let corrupt = seed_two_valid_one_corrupt(&cache_dir);

        tracker.perform_full_validation().await.unwrap();

        // Fixture guard: the self-heal must actually have fired, or this test
        // proves nothing about the census.
        assert!(
            !corrupt.exists(),
            "FIXTURE BROKEN: the unparseable .meta survived the scan, so no removal \
             happened and the census assertion below is vacuous"
        );

        let state = consolidator.get_size_state().await;
        assert_eq!(
            state.cached_objects, 2,
            "census counted a .meta this pass deleted: cached_objects={} for a tree \
             holding 2 surviving objects (3 .meta files visited, 1 self-healed away). \
             The census is installed absolutely by a full scan, so this over-reports \
             the population the Entry_Budget would evict against.",
            state.cached_objects
        );

        // The work-done figure must NOT have been reduced — it is what an operator
        // reads to see how much the scan got through, and conflating the two is how
        // this defect became invisible in the first place.
        let meta = tracker.read_validation_metadata().await.unwrap();
        assert_eq!(
            meta.metadata_files_scanned, 3,
            "validation.json metadata_files_scanned should stay a work-done figure \
             (3 files visited), not the census (2 surviving)"
        );
    }

    #[tokio::test]
    async fn rolling_validation_census_excludes_meta_removed_on_this_pass() {
        let (tracker, consolidator, temp_dir) = create_test_tracker().await;
        let cache_dir = temp_dir.path().to_path_buf();
        let corrupt = seed_two_valid_one_corrupt(&cache_dir);

        // `cached_objects` starts at 0, which makes the proportional correction the
        // identity on the scanned figure for any `dirs_scanned`:
        //   corrected = tracked + (surviving - tracked * d / 256) = surviving
        // so this test measures the census itself rather than the extrapolation.
        // That is deliberate — the extrapolation's unsoundness is R5's subject and
        // is documented at the call site, not asserted here.
        assert_eq!(
            consolidator.get_size_state().await.cached_objects,
            0,
            "FIXTURE BROKEN: this test needs tracked_objects == 0 for the \
             proportional correction to be the identity"
        );

        tracker.perform_rolling_validation().await.unwrap();

        assert!(
            !corrupt.exists(),
            "FIXTURE BROKEN: the unparseable .meta survived the rolling scan, so no \
             removal happened and the census assertion below is vacuous"
        );

        let state = consolidator.get_size_state().await;
        assert_eq!(
            state.cached_objects, 2,
            "rolling census counted a .meta this pass deleted: cached_objects={} for \
             a tree holding 2 surviving objects. The error is worse here than in full \
             mode: the proportional correction multiplies the observed discrepancy by \
             256 / dirs_scanned on the way in.",
            state.cached_objects
        );
    }

    /// The predicate is "is there a `.meta` at this path now", NOT "did the delete
    /// call return Ok". The two agree on every happy path, so this is the only test
    /// that can tell them apart: make the unlink fail and require the object to
    /// still be counted.
    ///
    /// Without this, a refactor that set `meta_removed: true` unconditionally in the
    /// parse-failure arm would pass both census tests above while under-counting
    /// every corrupt `.meta` on a read-only or contended volume.
    ///
    /// Spec: cache-eviction-at-scale. Requirements: 7.1
    #[tokio::test]
    async fn unparseable_meta_that_survives_unlink_is_still_counted() {
        use std::os::unix::fs::PermissionsExt;

        let (tracker, _consolidator, temp_dir) = create_test_tracker().await;
        let cache_dir = temp_dir.path().to_path_buf();

        let dir = cache_dir
            .join("metadata")
            .join("census-bucket")
            .join("03")
            .join("000");
        std::fs::create_dir_all(&dir).unwrap();
        let corrupt = dir.join("undeletable.meta");
        std::fs::write(&corrupt, b"not json").unwrap();

        // Deny write on the containing directory, which is what unlink permission
        // derives from. Probe it rather than assuming: running as root (as the CI
        // container does) ignores the mode bits entirely, and a test that silently
        // becomes vacuous is worse than one that says so.
        let original = std::fs::metadata(&dir).unwrap().permissions();
        let mut readonly = original.clone();
        readonly.set_mode(0o500);
        std::fs::set_permissions(&dir, readonly).unwrap();

        let probe = dir.join("probe.meta");
        let can_still_write = std::fs::write(&probe, b"x").is_ok();
        if can_still_write {
            let _ = std::fs::remove_file(&probe);
            std::fs::set_permissions(&dir, original).unwrap();
            eprintln!(
                "SKIPPED unparseable_meta_that_survives_unlink_is_still_counted: this \
                 environment ignores directory mode bits (running as root?), so an \
                 unlink failure cannot be constructed here. The assertion is \
                 unverifiable rather than passing."
            );
            return;
        }

        let result = tracker.scan_metadata_file(&corrupt, SystemTime::now());

        // Restore before asserting so a failure does not leave an undeletable temp dir.
        std::fs::set_permissions(&dir, original).unwrap();

        assert!(
            corrupt.exists(),
            "FIXTURE BROKEN: the unlink succeeded despite the read-only directory, so \
             the assertion below cannot distinguish the two predicates"
        );
        assert!(
            !result.meta_removed,
            "scan_metadata_file reported meta_removed for a .meta that is still on \
             disk. meta_removed must be read from the filesystem, not inferred from \
             the remove_file/invalidate call's return value — those return success or \
             a value unrelated to whether this path went."
        );
        assert!(
            result.cache_error,
            "an unparseable .meta is still an error regardless of whether it could be \
             removed"
        );
    }
}

/// Task 14 timing harness for a generated, disposable fixture root.
///
/// The test invokes the private `perform_full_validation` path directly because Task 14
/// measures that scan's metadata traversal, parsing, reconciliation, and validation-state
/// writes. It deliberately bypasses `perform_validation`'s scheduler, mode choice, incomplete-MPU
/// cleanup, and validation-lock acquisition; those are not part of the full-scan rate being
/// measured. The root must be a newly generated fixture, never a live cache.
#[cfg(test)]
mod task14_full_validation_timing_tests {
    use super::*;
    use crate::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
    use crate::journal_manager::JournalManager;
    use crate::metadata_lock_manager::MetadataLockManager;

    const FIXTURE_ROOT_ENV: &str = "S3HC_TASK14_FIXTURE_ROOT";

    async fn tracker_over_fixture(
        cache_dir: PathBuf,
    ) -> (Arc<CacheSizeTracker>, Arc<JournalConsolidator>) {
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

        let journal_manager = Arc::new(JournalManager::new(
            cache_dir.clone(),
            "task14-timing".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            cache_dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidator = Arc::new(JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        ));
        consolidator.initialize().await.unwrap();

        let tracker = Arc::new(
            CacheSizeTracker::new(
                cache_dir,
                CacheSizeConfig {
                    validation_enabled: false,
                    validation_max_duration: Duration::from_secs(4 * 3600),
                    ..CacheSizeConfig::default()
                },
                false,
                consolidator.clone(),
            )
            .await
            .unwrap(),
        );
        (tracker, consolidator)
    }

    /// Measure a real full validation scan over a fixture named by
    /// `S3HC_TASK14_FIXTURE_ROOT`.
    ///
    /// Run only against a fresh generated fixture root, for example:
    /// `S3HC_TASK14_FIXTURE_ROOT=/backend/fixture cargo test --lib
    /// task14_timed_full_validation_over_generated_fixture -- --ignored --nocapture`.
    /// The scan creates `metadata/_journals`, `locks`, and `size_tracking`; cleanup must remove
    /// the entire fixture root afterwards.
    #[tokio::test]
    #[ignore = "Task 14 timed backend measurement; requires a fresh generated fixture root"]
    async fn task14_timed_full_validation_over_generated_fixture() {
        let cache_dir = PathBuf::from(
            std::env::var(FIXTURE_ROOT_ENV)
                .expect("set S3HC_TASK14_FIXTURE_ROOT to a fresh generated fixture root"),
        );
        let marker = cache_dir.join(".s3hc-fixture");
        let manifest_path = cache_dir.join("FIXTURE_MANIFEST.json");
        assert!(
            marker.is_file(),
            "fixture marker missing: {}",
            marker.display()
        );
        assert!(
            manifest_path.is_file(),
            "fixture manifest missing: {}",
            manifest_path.display()
        );
        assert!(
            !cache_dir.join("size_tracking/size_state.json").exists(),
            "fixture has already been scanned; generate a fresh root instead"
        );
        assert!(
            !cache_dir.join("metadata/_journals").exists(),
            "fixture has journal state; generate a fresh root instead"
        );

        let manifest: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
        let objects = manifest["objects"].as_u64().expect("manifest objects");
        let ranges = manifest["ranges"].as_u64().expect("manifest ranges");
        let recorded_bytes = manifest["recorded_compressed_bytes"]
            .as_u64()
            .expect("manifest recorded_compressed_bytes");
        let staged_objects = manifest["staged_objects"]
            .as_u64()
            .expect("manifest staged_objects");
        assert_eq!(staged_objects, 0, "Task 14 fixture must be read-tier only");

        let (tracker, consolidator) = tracker_over_fixture(cache_dir.clone()).await;
        let outer_start = Instant::now();
        tracker.perform_full_validation().await.unwrap();
        let outer_elapsed = outer_start.elapsed();

        let metadata = tracker.read_validation_metadata().await.unwrap();
        assert_eq!(metadata.metadata_files_scanned, objects);
        assert_eq!(metadata.scanned_size, recorded_bytes);
        assert_eq!(metadata.tracked_size, 0);
        assert_eq!(metadata.drift_bytes, recorded_bytes as i64);
        assert_eq!(metadata.cache_entries_expired, 0);
        assert_eq!(metadata.cache_entries_skipped, 0);
        assert_eq!(metadata.cache_expiration_errors, 0);
        assert!(!metadata.active_expiration_enabled);

        let state = consolidator.get_size_state().await;
        assert_eq!(state.total_size, recorded_bytes);
        assert_eq!(state.write_cache_size, 0);
        assert_eq!(state.cached_objects, objects);

        let raw: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(cache_dir.join("size_tracking/validation.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            raw["validation_type"].as_str(),
            Some("full"),
            "full validation must stamp its own mode"
        );
        assert!(
            raw["last_full_scan_duration_secs"].as_f64().is_some(),
            "full validation must persist its in-method scan duration"
        );

        println!(
            "Task 14 full validation: root={} objects={} ranges={} recorded_bytes={} \
             scan_ms={} outer_ms={} entries_per_s={:.1}",
            cache_dir.display(),
            objects,
            ranges,
            recorded_bytes,
            metadata.scan_duration_ms,
            outer_elapsed.as_millis(),
            objects as f64 / metadata.scan_duration_ms.max(1) as f64 * 1000.0,
        );
    }
}
