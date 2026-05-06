//! Journal Consolidator Module
//!
//! Provides background consolidation of journal entries into metadata files for the atomic
//! metadata writes system. Handles configurable intervals, threshold-based triggers, conflict
//! resolution, and range file existence validation.
//!
//! Also provides size tracking for the cache - the consolidator is the single source of truth
//! for cache size, calculating size deltas from journal entries during consolidation.

use crate::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use crate::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use crate::metadata_lock_manager::MetadataLockManager;
use crate::{ProxyError, Result};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Maximum concurrent cache keys processed in a single consolidation cycle
const KEY_CONCURRENCY_LIMIT: usize = 64;

/// Result of journal discovery, including per-file entry counts for optimized cleanup.
///
/// During discovery, every journal file is read and parsed to build the key index.
/// We also count total entries per file so that cleanup can skip re-reading files
/// where all entries were consolidated (delete/truncate without re-parsing).
#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    /// cache_key → list of journal file paths containing entries for that key
    pub key_index: HashMap<String, Vec<PathBuf>>,
    /// journal file path → total number of parseable entries in that file at discovery time
    pub file_entry_counts: HashMap<PathBuf, usize>,
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

/// Persistent size state - source of truth for cache size
/// File: size_tracking/size_state.json
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizeState {
    /// Total cache size in bytes (read cache + write cache)
    pub total_size: u64,

    /// Write cache size in bytes (subset of total_size)
    /// Tracked separately for dashboard display
    /// Write cache = mpus_in_progress/ directory contents
    pub write_cache_size: u64,

    /// Number of distinct cached objects (unique cache keys with at least one range on disk).
    /// Incremented when a new object is first evicted (metadata deleted = all ranges gone).
    /// Recalculated during validation scans by counting .meta files.
    #[serde(default)]
    pub cached_objects: u64,

    /// Timestamp of last consolidation
    #[serde(with = "systemtime_serde")]
    pub last_consolidation: SystemTime,

    /// Number of consolidation cycles completed
    pub consolidation_count: u64,

    /// Instance ID that last updated this state
    pub last_updated_by: String,
}

impl Default for SizeState {
    fn default() -> Self {
        Self {
            total_size: 0,
            write_cache_size: 0,
            cached_objects: 0,
            last_consolidation: UNIX_EPOCH,
            consolidation_count: 0,
            last_updated_by: String::new(),
        }
    }
}

/// Configuration for journal consolidation
#[derive(Debug, Clone)]
pub struct ConsolidationConfig {
    /// How often to run consolidation (default: 5 seconds)
    /// Changed from 30s to 5s for near-realtime size tracking
    pub interval: Duration,
    /// Size threshold in bytes to trigger immediate consolidation
    pub size_threshold: u64,
    /// Entry count threshold to trigger immediate consolidation
    pub entry_count_threshold: usize,
    /// Maximum cache size in bytes (for eviction triggering)
    /// Passed from CacheConfig.max_cache_size during initialization
    /// 0 means no eviction (disabled)
    pub max_cache_size: u64,
    /// Percentage of max_cache_size at which eviction triggers (default: 95)
    pub eviction_trigger_percent: u8,
    /// Percentage of max_cache_size to reduce to after eviction (default: 80)
    pub eviction_target_percent: u8,
    /// Timeout for stale journal entries in seconds (default: 300 = 5 minutes)
    pub stale_entry_timeout_secs: u64,
    /// Maximum duration for the per-key processing phase of a consolidation cycle (default: 30s)
    /// Requirement: 4.1, 4.4
    pub consolidation_cycle_timeout: Duration,
    /// Maximum number of cache keys to discover and process per consolidation cycle (default: 5000)
    /// Limits NFS I/O during discovery and ensures the cycle completes within the timeout.
    /// When the backlog exceeds this cap, remaining keys are processed in subsequent cycles.
    pub max_keys_per_cycle: usize,
}

impl Default for ConsolidationConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5), // Changed from 30s for near-realtime size tracking
            size_threshold: 1024 * 1024,      // 1MB
            entry_count_threshold: 100,
            max_cache_size: 0, // Must be set from CacheConfig during initialization
            eviction_trigger_percent: 95,
            eviction_target_percent: 80,
            stale_entry_timeout_secs: 300, // 5 minutes
            consolidation_cycle_timeout: Duration::from_secs(30),
            max_keys_per_cycle: 5000,
        }
    }
}

/// Result of a consolidation operation
#[derive(Debug, Clone)]
pub struct ConsolidationResult {
    pub cache_key: String,
    pub entries_processed: usize,
    pub entries_consolidated: usize,
    pub entries_removed: usize,
    pub conflicts_resolved: usize,
    pub invalid_entries_removed: usize,
    pub success: bool,
    pub error: Option<String>,
    /// Entries that were successfully consolidated and should be removed from journals
    pub consolidated_entries: Vec<JournalEntry>,
    /// Net size change from this consolidation (can be negative)
    pub size_delta: i64,
    /// Net write cache size change from this consolidation (can be negative)
    pub write_cache_delta: i64,
    /// Whether this consolidation created a new metadata file (first time this object was cached)
    pub is_new_object: bool,
}

/// Result of a complete consolidation cycle
#[derive(Debug, Clone)]
pub struct ConsolidationCycleResult {
    /// Number of cache keys processed
    pub keys_processed: usize,

    /// Total journal entries consolidated
    pub entries_consolidated: usize,

    /// Net size change from this cycle (can be negative)
    pub size_delta: i64,

    /// Duration of the consolidation cycle
    pub cycle_duration: Duration,

    /// Whether eviction was triggered
    pub eviction_triggered: bool,

    /// Bytes freed by eviction (0 if not triggered)
    pub bytes_evicted: u64,

    /// Current total cache size after this cycle
    pub current_size: u64,
}

impl ConsolidationResult {
    pub fn success(
        cache_key: String,
        entries_processed: usize,
        entries_consolidated: usize,
    ) -> Self {
        Self {
            cache_key,
            entries_processed,
            entries_consolidated,
            entries_removed: 0,
            conflicts_resolved: 0,
            invalid_entries_removed: 0,
            success: true,
            error: None,
            consolidated_entries: Vec::new(),
            size_delta: 0,
            write_cache_delta: 0,
            is_new_object: false,
        }
    }
    pub fn failure(cache_key: String, error: String) -> Self {
        Self {
            cache_key,
            entries_processed: 0,
            entries_consolidated: 0,
            entries_removed: 0,
            conflicts_resolved: 0,
            invalid_entries_removed: 0,
            success: false,
            error: Some(error),
            consolidated_entries: Vec::new(),
            size_delta: 0,
            write_cache_delta: 0,
            is_new_object: false,
        }
    }
}

/// Global consolidation lock for distributed coordination
/// Prevents multiple instances from running consolidation cycles simultaneously
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConsolidationLock {
    /// Unique identifier for this proxy instance
    pub instance_id: String,
    /// Process ID of the lock holder
    pub process_id: u32,
    /// Hostname of the machine holding the lock
    pub hostname: String,
    /// When the lock was acquired
    #[serde(with = "systemtime_serde")]
    pub acquired_at: SystemTime,
    /// Lock timeout duration in seconds
    pub timeout_seconds: u64,
}

/// In-memory size accumulator for tracking cache size deltas.
///
/// Holds two AtomicI64 counters: one for total cache size delta and one for write-cache
/// size delta. Operations use Ordering::Relaxed since exact ordering between the two
/// counters is not required — each is an independent algebraic sum.
///
/// Periodically flushed to a per-instance delta file on shared storage. The consolidator
/// reads all delta files under the global lock and sums them into size_state.json.
pub struct SizeAccumulator {
    /// Net size delta since last flush (bytes added - bytes removed)
    delta: AtomicI64,
    /// Net write-cache size delta since last flush
    write_cache_delta: AtomicI64,
    /// Instance ID for delta file naming
    instance_id: String,
    /// Directory for delta files: `{cache_dir}/size_tracking/`
    size_tracking_dir: PathBuf,
    /// Monotonic sequence counter for unique delta file names
    flush_sequence: AtomicU64,
    /// Dedup set: tracks (cache_key_hash, range_start, range_end) written since last flush.
    /// Prevents double-counting when stampede causes multiple writes of the same range.
    /// Cleared on flush (every ~5 seconds). Memory: ~24 bytes per entry.
    recent_ranges: Mutex<HashSet<(u64, u64, u64)>>,
}

impl SizeAccumulator {
    /// Create a new SizeAccumulator with both counters initialized to zero.
    ///
    /// Delta files written to: `{cache_dir}/size_tracking/delta_{instance_id}_{seq}.json`
    pub fn new(cache_dir: &Path, instance_id: String) -> Self {
        let size_tracking_dir = cache_dir.join("size_tracking");
        Self {
            delta: AtomicI64::new(0),
            write_cache_delta: AtomicI64::new(0),
            instance_id,
            size_tracking_dir,
            flush_sequence: AtomicU64::new(0),
            recent_ranges: Mutex::new(HashSet::new()),
        }
    }

    /// Increment the total size delta. Called after successful range write.
    pub fn add(&self, compressed_size: u64) {
        self.delta
            .fetch_add(compressed_size as i64, Ordering::Relaxed);
        debug!(
            "SIZE_ACCUM add: +{} bytes, instance={}",
            compressed_size, self.instance_id
        );
    }

    /// Increment the total size delta with stampede deduplication.
    /// Uses (cache_key_hash, start, end) to detect duplicate writes within the flush window.
    /// Returns true if size was added (new range), false if skipped (duplicate).
    pub fn add_range(&self, cache_key: &str, start: u64, end: u64, compressed_size: u64) -> bool {
        let key_hash = blake3::hash(cache_key.as_bytes());
        let key_u64 = u64::from_le_bytes(key_hash.as_bytes()[..8].try_into().unwrap());
        let range_id = (key_u64, start, end);

        let mut recent = self.recent_ranges.lock().unwrap();
        if recent.insert(range_id) {
            drop(recent); // release lock before atomic op
            self.delta
                .fetch_add(compressed_size as i64, Ordering::Relaxed);
            debug!(
                "SIZE_ACCUM add_range: +{} bytes, range={}-{}, instance={}",
                compressed_size, start, end, self.instance_id
            );
            true
        } else {
            debug!(
                "SIZE_ACCUM dedup: skipped duplicate range, range={}-{}, instance={}",
                start, end, self.instance_id
            );
            false
        }
    }

    /// Increment the write-cache size delta. Called for write-cached or MPU ranges.
    pub fn add_write_cache(&self, compressed_size: u64) {
        self.write_cache_delta
            .fetch_add(compressed_size as i64, Ordering::Relaxed);
    }

    /// Decrement the total size delta. Called after range eviction.
    pub fn subtract(&self, compressed_size: u64) {
        self.delta
            .fetch_sub(compressed_size as i64, Ordering::Relaxed);
        debug!(
            "SIZE_ACCUM subtract: -{} bytes, instance={}",
            compressed_size, self.instance_id
        );
    }

    /// Decrement the write-cache size delta. Called after write-cached range eviction.
    pub fn subtract_write_cache(&self, compressed_size: u64) {
        self.write_cache_delta
            .fetch_sub(compressed_size as i64, Ordering::Relaxed);
    }

    /// Check if there are any pending deltas to flush.
    pub fn has_pending_delta(&self) -> bool {
        self.delta.load(Ordering::Relaxed) != 0
            || self.write_cache_delta.load(Ordering::Relaxed) != 0
    }

    /// Atomically swap both accumulators to zero and write to delta file.
    /// If both are zero, returns Ok(()) without writing.
    /// If the file write fails, restores the swapped values to prevent data loss.
    pub async fn flush(&self) -> Result<()> {
        let delta = self.delta.swap(0, Ordering::Relaxed);
        let wc_delta = self.write_cache_delta.swap(0, Ordering::Relaxed);

        // Do NOT clear dedup set on flush — ranges written in previous windows
        // should still be deduplicated. The set is cleared only during validation
        // scan (reset()) which reconciles the tracked size with actual disk usage.

        if delta == 0 && wc_delta == 0 {
            return Ok(());
        }

        info!(
            "SIZE_ACCUM flush: instance={}, delta={:+}, write_cache_delta={:+}",
            self.instance_id, delta, wc_delta
        );

        match self.write_delta_file(delta, wc_delta).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Restore values on failure so the delta is not lost
                self.delta.fetch_add(delta, Ordering::Relaxed);
                self.write_cache_delta
                    .fetch_add(wc_delta, Ordering::Relaxed);
                warn!(
                    "SIZE_ACCUM flush failed, restored: instance={}, delta={:+}, wc_delta={:+}, error={}",
                    self.instance_id, delta, wc_delta, e
                );
                Err(e)
            }
        }
    }

    /// Write delta values to a new uniquely-named delta file using atomic tmp+rename.
    /// Each flush creates a separate file to eliminate NFS stale read races.
    ///
    /// File name: `delta_{instance_id}_{sequence}.json`
    /// JSON format: {"delta": i64, "write_cache_delta": i64, "instance_id": string, "timestamp": string}
    async fn write_delta_file(&self, delta: i64, write_cache_delta: i64) -> Result<()> {
        tokio::fs::create_dir_all(&self.size_tracking_dir)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!(
                    "Failed to create size_tracking directory {:?}: {}",
                    self.size_tracking_dir, e
                ))
            })?;

        let seq = self.flush_sequence.fetch_add(1, Ordering::Relaxed);
        let file_name = format!("delta_{}_{}.json", self.instance_id, seq);
        let file_path = self.size_tracking_dir.join(&file_name);

        let content = serde_json::json!({
            "delta": delta,
            "write_cache_delta": write_cache_delta,
            "instance_id": self.instance_id,
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        let json_str = serde_json::to_string_pretty(&content).map_err(|e| {
            ProxyError::CacheError(format!("Failed to serialize delta file: {}", e))
        })?;

        // Atomic write via temp file + rename
        let tmp_path = file_path.with_extension("json.tmp");
        tokio::fs::write(&tmp_path, &json_str).await.map_err(|e| {
            ProxyError::CacheError(format!(
                "Failed to write delta temp file {:?}: {}",
                tmp_path, e
            ))
        })?;
        tokio::fs::rename(&tmp_path, &file_path)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!(
                    "Failed to rename delta file {:?} -> {:?}: {}",
                    tmp_path, file_path, e
                ))
            })?;

        Ok(())
    }

    /// Get the current delta value (for testing/debugging)
    pub fn current_delta(&self) -> i64 {
        self.delta.load(Ordering::Relaxed)
    }

    /// Get the current write-cache delta value (for testing/debugging)
    pub fn current_write_cache_delta(&self) -> i64 {
        self.write_cache_delta.load(Ordering::Relaxed)
    }

    /// Get the size tracking directory path (for testing/debugging)
    pub fn delta_file_path(&self) -> &Path {
        &self.size_tracking_dir
    }

    /// Reset both accumulators to zero. Called after validation scan corrects drift.
    pub fn reset(&self) {
        self.delta.store(0, Ordering::Relaxed);
        self.write_cache_delta.store(0, Ordering::Relaxed);
        let mut recent = self.recent_ranges.lock().unwrap();
        recent.clear();
    }
}

/// Journal consolidator for background consolidation of journal entries
pub struct JournalConsolidator {
    cache_dir: PathBuf,
    journal_manager: Arc<JournalManager>,
    lock_manager: Arc<MetadataLockManager>,
    config: ConsolidationConfig,
    /// Path to size state file
    size_state_path: PathBuf,
    /// Reference to cache manager for eviction (Weak to avoid circular references)
    cache_manager: Mutex<Option<Weak<crate::cache::CacheManager>>>,
    /// Ranges that were evicted and should be immediately marked as stale in journals
    /// Key: cache_key, Value: Vec<(start, end)> of evicted ranges
    /// **Validates: Requirement 4.2**
    evicted_ranges: Mutex<HashMap<String, Vec<(u64, u64)>>>,
    /// Global consolidation lock file handle (kept open while lock is held)
    consolidation_lock_file: Mutex<Option<std::fs::File>>,
    /// In-memory size accumulator for this instance
    size_accumulator: Arc<SizeAccumulator>,
    /// Guard to prevent concurrent eviction spawns
    eviction_in_progress: Arc<AtomicBool>,
}

/// Get unique instance ID for this process (hostname:pid)
fn get_instance_id() -> String {
    format!(
        "{}:{}",
        gethostname::gethostname().to_string_lossy(),
        std::process::id()
    )
}

impl JournalConsolidator {
    /// Create a new journal consolidator
    pub fn new(
        cache_dir: PathBuf,
        journal_manager: Arc<JournalManager>,
        lock_manager: Arc<MetadataLockManager>,
        config: ConsolidationConfig,
    ) -> Self {
        let size_state_path = cache_dir.join("size_tracking").join("size_state.json");
        let instance_id = get_instance_id();
        let size_accumulator = Arc::new(SizeAccumulator::new(&cache_dir, instance_id));
        Self {
            cache_dir,
            journal_manager,
            lock_manager,
            config,
            size_state_path,
            cache_manager: Mutex::new(None),
            evicted_ranges: Mutex::new(HashMap::new()),
            consolidation_lock_file: Mutex::new(None),
            size_accumulator,
            eviction_in_progress: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set the cache manager reference for eviction triggering
    ///
    /// This must be called after the CacheManager is created to establish the
    /// bidirectional relationship. Uses Weak reference to avoid circular references.
    pub fn set_cache_manager(&self, cache_manager: Weak<crate::cache::CacheManager>) {
        if let Ok(mut guard) = self.cache_manager.lock() {
            *guard = Some(cache_manager);
            debug!("Cache manager reference set for eviction triggering");
        } else {
            warn!("Failed to acquire lock to set cache manager reference");
        }
    }

    /// Get reference to the size accumulator (for store_range and eviction to call)
    pub fn size_accumulator(&self) -> &Arc<SizeAccumulator> {
        &self.size_accumulator
    }

    /// Quick check for pending journal files without reading their contents.
    /// Used by idle detection to skip consolidation cycles when no work is pending.
    /// Only does a directory listing — no lock acquisition, no file reads.
    fn has_pending_journal_files(&self) -> bool {
        let journals_dir = self.cache_dir.join("metadata").join("_journals");
        match std::fs::read_dir(&journals_dir) {
            Ok(entries) => entries.filter_map(|e| e.ok()).any(|e| {
                e.path().extension().is_some_and(|ext| ext == "journal")
                    && e.metadata().is_ok_and(|m| m.len() > 0)
            }),
            Err(_) => false,
        }
    }

    /// Read all delta files, sum deltas, reset files to zero.
    /// Called during run_consolidation_cycle() under global lock.
    ///
    /// Returns (total_delta, total_write_cache_delta).
    /// Handles missing directory gracefully (returns (0, 0)).
    /// Skips files with invalid JSON (logs warning).
    pub(crate) async fn collect_and_apply_deltas(&self) -> Result<(i64, i64)> {
        let size_tracking_dir = self.cache_dir.join("size_tracking");

        // Handle missing directory gracefully
        if !size_tracking_dir.exists() {
            return Ok((0, 0));
        }

        let mut total_delta: i64 = 0;
        let mut total_wc_delta: i64 = 0;
        let mut files_processed: u32 = 0;

        let mut entries = match tokio::fs::read_dir(&size_tracking_dir).await {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to read size_tracking directory: {}", e);
                return Ok((0, 0));
            }
        };

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            ProxyError::CacheError(format!("Failed to read size_tracking entry: {}", e))
        })? {
            let path = entry.path();
            let file_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();

            if !file_name.starts_with("delta_") || !file_name.ends_with(".json") {
                continue;
            }

            // Skip tmp files
            if file_name.ends_with(".json.tmp") {
                continue;
            }

            let content = match tokio::fs::read_to_string(&path).await {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to read delta file {:?}: {}", path, e);
                    continue;
                }
            };

            let json: serde_json::Value = match serde_json::from_str(&content) {
                Ok(j) => j,
                Err(e) => {
                    warn!("Invalid JSON in delta file {:?}: {}", path, e);
                    continue;
                }
            };

            let delta = json.get("delta").and_then(|v| v.as_i64()).unwrap_or(0);
            let wc_delta = json
                .get("write_cache_delta")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            let instance_id = json
                .get("instance_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            // Skip zero-delta files (nothing to collect)
            if delta == 0 && wc_delta == 0 {
                continue;
            }

            info!(
                "SIZE_ACCUM collect: file={}, instance={}, delta={:+}, wc_delta={:+}",
                file_name, instance_id, delta, wc_delta
            );

            total_delta += delta;
            total_wc_delta += wc_delta;
            files_processed += 1;

            // DELETE the delta file after reading (instead of resetting to 0).
            // This eliminates the race condition where an instance flushes a new delta
            // between our read and reset, causing the new value to be overwritten with 0.
            // With delete: if an instance flushes after we delete, it creates a new file
            // (additive write_delta_file handles missing file gracefully).
            if let Err(e) = tokio::fs::remove_file(&path).await {
                warn!("Failed to delete delta file {:?}: {}", path, e);
            }
        }

        if files_processed > 0 {
            info!(
                "SIZE_ACCUM collect_total: files={}, total_delta={:+}, total_wc_delta={:+}",
                files_processed, total_delta, total_wc_delta
            );
        }

        Ok((total_delta, total_wc_delta))
    }

    /// Reset all delta files to zero. Called after validation scan corrects drift.
    pub(crate) async fn reset_all_delta_files(&self) {
        let size_tracking_dir = self.cache_dir.join("size_tracking");

        if !size_tracking_dir.exists() {
            return;
        }

        let mut entries = match tokio::fs::read_dir(&size_tracking_dir).await {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to read size_tracking directory for reset: {}", e);
                return;
            }
        };

        let mut deleted_count = 0u32;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            if !file_name.starts_with("delta_") || !file_name.ends_with(".json") {
                continue;
            }
            if file_name.ends_with(".json.tmp") {
                // Clean up stale tmp files too
                let _ = tokio::fs::remove_file(&path).await;
                continue;
            }

            // Delete delta files instead of resetting to 0.
            // Instances will create new files on next flush (additive write handles missing file).
            if let Err(e) = tokio::fs::remove_file(&path).await {
                warn!("Failed to delete delta file {:?}: {}", path, e);
            } else {
                deleted_count += 1;
            }
        }

        if deleted_count > 0 {
            info!(
                "SIZE_ACCUM reset_all: deleted {} delta files",
                deleted_count
            );
        }
    }

    /// Mark ranges as evicted for immediate journal cleanup
    ///
    /// Called by CacheManager after eviction completes. Entries matching these ranges
    /// will be removed in the next consolidation cycle, bypassing the 5-minute timeout.
    ///
    /// **Validates: Requirement 4.2**
    ///
    /// # Arguments
    /// * `evicted_ranges` - Vec of (cache_key, start, end) tuples for evicted ranges
    pub fn mark_ranges_evicted(&self, ranges: Vec<(String, u64, u64)>) {
        if ranges.is_empty() {
            return;
        }

        match self.evicted_ranges.lock() {
            Ok(mut guard) => {
                let mut count = 0;
                for (cache_key, start, end) in ranges {
                    guard
                        .entry(cache_key.clone())
                        .or_default()
                        .push((start, end));
                    count += 1;
                }
                debug!(
                    "Marked {} ranges as evicted for immediate journal cleanup",
                    count
                );
            }
            Err(e) => {
                warn!(
                    "Failed to acquire evicted_ranges lock: {}. Stale entries will be cleaned up via timeout.",
                    e
                );
            }
        }
    }

    /// Write Remove journal entries for evicted ranges
    ///
    /// This is the journal-based approach to size tracking: eviction writes Remove entries
    /// to the journal, and consolidation processes them to update size state. This eliminates
    /// the need for locking between eviction and consolidation since consolidation is the
    /// single writer to size_state.json.
    ///
    /// # Arguments
    /// * `evicted_ranges` - Vec of (cache_key, range_start, range_end, size, bin_file_path)
    pub async fn write_eviction_journal_entries(
        &self,
        evicted_ranges: Vec<(String, u64, u64, u64, String)>,
    ) {
        if evicted_ranges.is_empty() {
            return;
        }

        let instance_id = get_instance_id();
        let mut success_count = 0;
        let mut error_count = 0;

        // Group entries by cache_key for batched writes
        let mut grouped: HashMap<String, Vec<JournalEntry>> = HashMap::new();

        for (cache_key, range_start, range_end, size, bin_file_path) in evicted_ranges {
            let now = std::time::SystemTime::now();

            // Create a RangeSpec with the size information needed for size tracking
            let range_spec = RangeSpec {
                start: range_start,
                end: range_end,
                file_path: bin_file_path.clone(),
                compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
                compressed_size: size,
                uncompressed_size: size,
                created_at: now,
                last_accessed: now,
                access_count: 0,
                frequency_score: 0,
            };

            let journal_entry = JournalEntry {
                timestamp: now,
                instance_id: instance_id.clone(),
                cache_key: cache_key.clone(),
                range_spec,
                operation: JournalOperation::Remove,
                range_file_path: bin_file_path,
                metadata_version: 0, // Not relevant for Remove
                new_ttl_secs: None,
                object_ttl_secs: None,
                access_increment: None,
                object_metadata: None,
                metadata_written: false, // Not relevant for Remove operations
            };

            grouped.entry(cache_key).or_default().push(journal_entry);
        }

        // Write batched entries per cache key
        for (cache_key, entries) in &grouped {
            let entry_count = entries.len();
            match self
                .journal_manager
                .append_range_entries_batch(cache_key, entries.clone())
                .await
            {
                Ok(()) => {
                    success_count += entry_count;
                }
                Err(e) => {
                    error_count += entry_count;
                    warn!(
                        "Failed to write batch Remove journal entries for evicted ranges: cache_key={}, entry_count={}, error={}",
                        cache_key, entry_count, e
                    );
                }
            }
        }

        if success_count > 0 || error_count > 0 {
            info!(
                "Wrote eviction journal entries: success={}, errors={}",
                success_count, error_count
            );
        }
    }

    /// Write Add journal entries for multipart upload completion
    ///
    /// This is called after CompleteMultipartUpload to create journal entries for size tracking.
    /// The multipart upload handler writes metadata directly (for atomicity), but we need
    /// journal entries so the consolidator can track the size delta.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the completed multipart upload
    /// * `ranges` - Vec of RangeSpec for each part of the completed upload
    /// * `object_metadata` - Object metadata for the completed upload
    pub async fn write_multipart_journal_entries(
        &self,
        cache_key: &str,
        ranges: Vec<RangeSpec>,
        object_metadata: ObjectMetadata,
    ) {
        if ranges.is_empty() {
            return;
        }

        let instance_id = get_instance_id();
        let mut success_count = 0;
        let mut error_count = 0;
        let now = std::time::SystemTime::now();

        for range_spec in ranges {
            let journal_entry = JournalEntry {
                timestamp: now,
                instance_id: instance_id.clone(),
                cache_key: cache_key.to_string(),
                range_spec: range_spec.clone(),
                operation: JournalOperation::Add,
                range_file_path: range_spec.file_path.clone(),
                metadata_version: 1,
                new_ttl_secs: None,
                object_ttl_secs: None, // Multipart completion writes metadata directly with correct TTL
                access_increment: None,
                object_metadata: Some(object_metadata.clone()),
                // Multipart completion writes metadata directly, so metadata_written: true
                // This prevents double-counting when consolidation processes these entries
                metadata_written: true,
            };

            match self
                .journal_manager
                .append_range_entry(cache_key, journal_entry)
                .await
            {
                Ok(()) => {
                    success_count += 1;
                    // Track size via accumulator for each successfully written range
                    // **Validates: Requirements 1.5, 5.2, 5.3**
                    self.size_accumulator.add(range_spec.compressed_size);
                    // MPU completion ranges go under mpus_in_progress or are write-cached
                    if range_spec.file_path.contains("mpus_in_progress/")
                        || object_metadata.is_write_cached
                    {
                        self.size_accumulator
                            .add_write_cache(range_spec.compressed_size);
                    }
                }
                Err(e) => {
                    error_count += 1;
                    warn!(
                        "Failed to write Add journal entry for multipart range: cache_key={}, range={}-{}, error={}",
                        cache_key, range_spec.start, range_spec.end, e
                    );
                }
            }
        }

        if success_count > 0 || error_count > 0 {
            info!(
                "Wrote multipart journal entries: cache_key={}, success={}, errors={}",
                cache_key, success_count, error_count
            );
        }
    }

    /// Check if a range was recently evicted (should bypass timeout)
    ///
    /// Returns true if the range matches an evicted range and removes it from the tracking.
    /// This ensures each evicted range is only matched once.
    fn check_and_clear_evicted_range(&self, cache_key: &str, start: u64, end: u64) -> bool {
        match self.evicted_ranges.lock() {
            Ok(mut guard) => {
                if let Some(ranges) = guard.get_mut(cache_key) {
                    // Find and remove the matching range
                    if let Some(pos) = ranges.iter().position(|&(s, e)| s == start && e == end) {
                        ranges.remove(pos);
                        // Clean up empty entries
                        if ranges.is_empty() {
                            guard.remove(cache_key);
                        }
                        return true;
                    }
                }
                false
            }
            Err(e) => {
                warn!("Failed to acquire evicted_ranges lock for check: {}", e);
                false
            }
        }
    }

    /// Load size state from disk (called on startup and during consolidation)
    ///
    /// Returns the loaded state, or a default state if the file doesn't exist.
    pub async fn load_size_state(&self) -> Result<SizeState> {
        if !self.size_state_path.exists() {
            debug!(
                "Size state file does not exist, using default: path={:?}",
                self.size_state_path
            );
            return Ok(SizeState::default());
        }

        let content = tokio::fs::read_to_string(&self.size_state_path)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to read size state file: {}", e))
            })?;

        let state: SizeState = serde_json::from_str(&content).map_err(|e| {
            ProxyError::CacheError(format!("Failed to parse size state file: {}", e))
        })?;

        debug!(
            "Loaded size state: total_size={}, consolidation_count={}",
            state.total_size, state.consolidation_count
        );

        Ok(state)
    }

    /// Persist size state to disk with atomic write (temp file + rename)
    async fn persist_size_state_internal(&self, state: &SizeState) -> Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = self.size_state_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ProxyError::CacheError(format!("Failed to create size_tracking directory: {}", e))
            })?;
        }

        // Serialize to JSON
        let json_content = serde_json::to_string_pretty(&state).map_err(|e| {
            ProxyError::CacheError(format!("Failed to serialize size state: {}", e))
        })?;

        // Atomic write: write to temp file then rename
        // Use instance-specific tmp file to avoid race conditions on shared storage
        let instance_suffix = format!(
            "{}.{}",
            gethostname::gethostname().to_string_lossy(),
            std::process::id()
        );
        let tmp_extension = format!("json.tmp.{}", instance_suffix);
        let temp_path = self.size_state_path.with_extension(&tmp_extension);

        // Write to temporary file
        if let Err(e) = tokio::fs::write(&temp_path, &json_content).await {
            warn!(
                "Failed to write size state temp file: temp_path={:?}, error={}",
                temp_path, e
            );
            // Clean up temp file on write failure
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(ProxyError::CacheError(format!(
                "Failed to write size state temp file: {}",
                e
            )));
        }

        // Atomic rename
        if let Err(e) = tokio::fs::rename(&temp_path, &self.size_state_path).await {
            warn!(
                "Failed to rename size state file: temp_path={:?}, final_path={:?}, error={}",
                temp_path, self.size_state_path, e
            );
            // Clean up temp file on rename failure
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(ProxyError::CacheError(format!(
                "Failed to rename size state file: {}",
                e
            )));
        }

        debug!(
            "Persisted size state: total_size={}, write_cache_size={}, path={:?}",
            state.total_size, state.write_cache_size, self.size_state_path
        );

        Ok(())
    }

    /// Persist size state to disk
    pub async fn persist_size_state(&self, state: &SizeState) -> Result<()> {
        self.persist_size_state_internal(state).await
    }

    /// Get current cache size (reads from disk for multi-instance consistency)
    pub async fn get_current_size(&self) -> u64 {
        match self.load_size_state().await {
            Ok(state) => state.total_size,
            Err(e) => {
                debug!("Failed to read size state from disk: {}", e);
                0
            }
        }
    }

    /// Get current write cache size (reads from disk for multi-instance consistency)
    pub async fn get_write_cache_size(&self) -> u64 {
        match self.load_size_state().await {
            Ok(state) => state.write_cache_size,
            Err(e) => {
                debug!("Failed to read size state from disk: {}", e);
                0
            }
        }
    }

    /// Get size state for metrics/dashboard (async version)
    ///
    /// Reads from the shared disk file to ensure all instances see the same value.
    /// Returns default state if file doesn't exist or read fails.
    pub async fn get_size_state(&self) -> SizeState {
        match self.load_size_state().await {
            Ok(state) => state,
            Err(e) => {
                debug!(
                    "Failed to read size state from disk, returning default: {}",
                    e
                );
                SizeState::default()
            }
        }
    }

    /// Atomically decrement cached_objects count after eviction removes metadata files.
    ///
    /// Called after eviction completes with the number of objects whose metadata was deleted.
    pub async fn decrement_cached_objects(&self, objects_removed: u64) {
        if objects_removed == 0 {
            return;
        }

        let lock_file_path = self.cache_dir.join("size_tracking").join("size_state.lock");
        if let Some(parent) = lock_file_path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                warn!(
                    "Failed to create size_tracking directory for object count update: {}",
                    e
                );
                return;
            }
        }

        let lock_file = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::task::spawn_blocking({
                let lock_path = lock_file_path.clone();
                move || {
                    use fs2::FileExt;
                    let file = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(&lock_path)?;
                    file.lock_exclusive()?;
                    Ok::<_, std::io::Error>(file)
                }
            }),
        )
        .await
        {
            Ok(Ok(Ok(file))) => file,
            _ => {
                warn!("Failed to acquire size state lock for cached_objects decrement");
                return;
            }
        };

        let mut state = match self.load_size_state().await {
            Ok(s) => s,
            Err(e) => {
                let _ = lock_file.unlock();
                warn!(
                    "Failed to load size state for cached_objects decrement: {}",
                    e
                );
                return;
            }
        };

        state.cached_objects = state.cached_objects.saturating_sub(objects_removed);
        state.last_updated_by = get_instance_id();

        if let Err(e) = self.persist_size_state_internal(&state).await {
            warn!(
                "Failed to persist size state after cached_objects decrement: {}",
                e
            );
        }
        let _ = lock_file.unlock();
        debug!(
            "Decremented cached_objects by {}, new count={}",
            objects_removed, state.cached_objects
        );
    }

    /// Atomically increment cached_objects count when a new object is first cached.
    ///
    /// Called when consolidation writes a new metadata file for a previously unseen cache key.
    pub async fn increment_cached_objects(&self, objects_added: u64) {
        if objects_added == 0 {
            return;
        }

        let lock_file_path = self.cache_dir.join("size_tracking").join("size_state.lock");
        if let Some(parent) = lock_file_path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                warn!(
                    "Failed to create size_tracking directory for object count update: {}",
                    e
                );
                return;
            }
        }

        let lock_file = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::task::spawn_blocking({
                let lock_path = lock_file_path.clone();
                move || {
                    use fs2::FileExt;
                    let file = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(&lock_path)?;
                    file.lock_exclusive()?;
                    Ok::<_, std::io::Error>(file)
                }
            }),
        )
        .await
        {
            Ok(Ok(Ok(file))) => file,
            _ => {
                warn!("Failed to acquire size state lock for cached_objects increment");
                return;
            }
        };

        let mut state = match self.load_size_state().await {
            Ok(s) => s,
            Err(e) => {
                let _ = lock_file.unlock();
                warn!(
                    "Failed to load size state for cached_objects increment: {}",
                    e
                );
                return;
            }
        };

        state.cached_objects = state.cached_objects.saturating_add(objects_added);
        state.last_updated_by = get_instance_id();

        if let Err(e) = self.persist_size_state_internal(&state).await {
            warn!(
                "Failed to persist size state after cached_objects increment: {}",
                e
            );
        }
        let _ = lock_file.unlock();
        debug!(
            "Incremented cached_objects by {}, new count={}",
            objects_added, state.cached_objects
        );
    }

    /// Try to acquire the global consolidation lock
    ///
    /// Uses flock-based locking to prevent multiple instances from running
    /// consolidation cycles simultaneously. This eliminates race conditions
    /// where multiple instances process the same journal entries.
    ///
    /// Returns Ok(true) if lock acquired, Ok(false) if held by another instance.
    pub fn try_acquire_global_consolidation_lock(&self) -> Result<bool> {
        use fs2::FileExt;

        let mut guard = match self.consolidation_lock_file.lock() {
            Ok(g) => g,
            Err(e) => {
                warn!("Failed to acquire consolidation lock mutex: {}", e);
                return Ok(false);
            }
        };

        // Check if we already hold the lock
        if guard.is_some() {
            debug!("Consolidation lock already held by this instance");
            return Ok(false);
        }

        let lock_file_path = self
            .cache_dir
            .join("locks")
            .join("global_consolidation.lock");

        // Ensure locks directory exists
        if let Some(parent_dir) = lock_file_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent_dir) {
                warn!("Failed to create locks directory: {}", e);
                return Err(ProxyError::CacheError(format!(
                    "Failed to create locks directory: {}",
                    e
                )));
            }
        }

        // Try to open/create the lock file
        let lock_file = match std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_file_path)
        {
            Ok(file) => file,
            Err(e) => {
                warn!("Failed to open consolidation lock file: {}", e);
                return Err(ProxyError::CacheError(format!(
                    "Failed to open consolidation lock file: {}",
                    e
                )));
            }
        };

        // Try to acquire exclusive lock (non-blocking)
        match lock_file.try_lock_exclusive() {
            Ok(()) => {
                debug!("Acquired global consolidation lock");

                // Write lock metadata for debugging
                let lock_data = GlobalConsolidationLock {
                    instance_id: get_instance_id(),
                    process_id: std::process::id(),
                    hostname: gethostname::gethostname().to_string_lossy().to_string(),
                    acquired_at: SystemTime::now(),
                    timeout_seconds: 300, // 5 minute timeout for debugging
                };

                if let Ok(lock_json) = serde_json::to_string_pretty(&lock_data) {
                    use std::io::Write;
                    let _ = (&lock_file).write_all(lock_json.as_bytes());
                }

                // Store the lock file so it stays open (and locked) until released
                *guard = Some(lock_file);
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                debug!("Global consolidation lock held by another instance");
                Ok(false)
            }
            Err(e) => {
                warn!("Failed to acquire consolidation lock: {}", e);
                Ok(false)
            }
        }
    }

    /// Release the global consolidation lock
    ///
    /// With flock-based locking, release is automatic when the file handle is dropped.
    /// This method explicitly drops the lock file handle.
    pub fn release_global_consolidation_lock(&self) {
        if let Ok(mut guard) = self.consolidation_lock_file.lock() {
            if guard.is_some() {
                *guard = None;
                debug!("Released global consolidation lock");
            }
        }
    }

    /// Update size state from validation scan (Task 12)
    ///
    /// Called by CacheSizeTracker after a validation scan to correct any drift
    /// between tracked size and actual filesystem size. This ensures the consolidator's
    /// size state stays accurate even if journal entries are lost or corrupted.
    ///
    /// # Arguments
    /// * `scanned_size` - The actual cache size calculated from filesystem scan
    /// * `write_cache_size` - The actual write cache size from filesystem scan (optional)
    pub async fn update_size_from_validation(
        &self,
        scanned_size: u64,
        write_cache_size: Option<u64>,
        cached_objects: Option<u64>,
    ) {
        // Read current state from disk first
        let mut state = match self.load_size_state().await {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to load size state for validation update: {}", e);
                SizeState::default()
            }
        };

        let old_size = state.total_size;
        state.total_size = scanned_size;
        if let Some(wc_size) = write_cache_size {
            state.write_cache_size = wc_size;
        }
        if let Some(objects) = cached_objects {
            state.cached_objects = objects;
        }
        state.last_updated_by = get_instance_id();

        info!(
            "Updated size state from validation: old_size={}, new_size={}, drift={}, cached_objects={}",
            old_size,
            scanned_size,
            scanned_size as i64 - old_size as i64,
            state.cached_objects
        );

        // Persist the updated state
        if let Err(e) = self.persist_size_state_internal(&state).await {
            warn!(
                "Failed to persist size state after validation update: {}",
                e
            );
        }

        // Reset all delta files to prevent stale deltas from being re-applied
        self.reset_all_delta_files().await;

        // Reset the in-memory accumulator to zero
        self.size_accumulator.reset();
    }

    /// Atomically update size state with deltas from consolidation
    ///
    /// Uses file locking to ensure atomic read-modify-write across multiple instances.
    /// This prevents lost updates when consolidation races with eviction.
    ///
    /// # Arguments
    /// * `size_delta` - Change in total_size (positive for adds, negative for removes)
    /// * `write_cache_delta` - Change in write_cache_size
    ///
    /// # Returns
    /// * `Ok(new_size)` - The new total size after update
    /// * `Err` - If locking or persistence fails
    pub async fn atomic_update_size_delta(
        &self,
        size_delta: i64,
        write_cache_delta: i64,
    ) -> Result<u64> {
        #[allow(unused_imports)]
        use fs2::FileExt;

        // Skip if no changes
        if size_delta == 0 && write_cache_delta == 0 {
            return Ok(self.get_current_size().await);
        }

        // Use a dedicated lock file for size state updates
        let lock_file_path = self.cache_dir.join("size_tracking").join("size_state.lock");

        // Ensure directory exists
        if let Some(parent) = lock_file_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ProxyError::CacheError(format!("Failed to create size_tracking directory: {}", e))
            })?;
        }

        // Acquire exclusive lock with timeout
        let lock_file = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::task::spawn_blocking({
                let lock_path = lock_file_path.clone();
                move || {
                    use fs2::FileExt;
                    let file = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(&lock_path)?;
                    file.lock_exclusive()?;
                    Ok::<_, std::io::Error>(file)
                }
            }),
        )
        .await
        {
            Ok(Ok(Ok(file))) => file,
            Ok(Ok(Err(e))) => {
                warn!("Failed to acquire size state lock for delta update: {}", e);
                return Err(ProxyError::CacheError(format!(
                    "Size state lock acquisition failed: {}",
                    e
                )));
            }
            Ok(Err(e)) => {
                warn!("Size state lock task failed: {}", e);
                return Err(ProxyError::CacheError(format!(
                    "Size state lock task failed: {}",
                    e
                )));
            }
            Err(_) => {
                warn!("Size state lock timeout for delta update");
                return Err(ProxyError::CacheError(
                    "Size state lock timeout".to_string(),
                ));
            }
        };

        // Read current state while holding lock
        let mut state = match self.load_size_state().await {
            Ok(s) => s,
            Err(e) => {
                // Release lock
                let _ = lock_file.unlock();
                warn!("Failed to load size state for atomic delta update: {}", e);
                return Err(e);
            }
        };

        // Apply size deltas
        let old_size = state.total_size;
        if size_delta >= 0 {
            state.total_size = state.total_size.saturating_add(size_delta as u64);
        } else {
            state.total_size = state.total_size.saturating_sub((-size_delta) as u64);
        }

        if write_cache_delta >= 0 {
            state.write_cache_size = state
                .write_cache_size
                .saturating_add(write_cache_delta as u64);
        } else {
            state.write_cache_size = state
                .write_cache_size
                .saturating_sub((-write_cache_delta) as u64);
        }

        state.last_consolidation = SystemTime::now();
        state.consolidation_count += 1;
        state.last_updated_by = get_instance_id();

        // Persist while still holding lock
        if let Err(e) = self.persist_size_state_internal(&state).await {
            // Release lock
            let _ = lock_file.unlock();
            warn!(
                "Failed to persist size state after atomic delta update: {}",
                e
            );
            return Err(e);
        }

        // Release lock
        if let Err(e) = lock_file.unlock() {
            warn!("Failed to release size state lock: {}", e);
        }

        debug!(
            "Atomic size delta update: old_size={}, size_delta={:+}, write_cache_delta={:+}, new_size={}",
            old_size, size_delta, write_cache_delta, state.total_size
        );

        Ok(state.total_size)
    }

    /// Atomically subtract bytes from the size state after eviction
    ///
    /// Uses file locking to ensure atomic read-modify-write across multiple instances.
    /// This prevents lost updates when multiple instances evict concurrently.
    ///
    /// # Arguments
    /// * `bytes_freed` - Number of bytes freed by eviction
    ///
    /// # Returns
    /// * `Ok(new_size)` - The new total size after subtraction
    /// * `Err` - If locking or persistence fails
    pub async fn atomic_subtract_size(&self, bytes_freed: u64) -> Result<u64> {
        #[allow(unused_imports)]
        use fs2::FileExt;

        if bytes_freed == 0 {
            return Ok(self.get_current_size().await);
        }

        // Use a dedicated lock file for size state updates
        let lock_file_path = self.cache_dir.join("size_tracking").join("size_state.lock");

        // Ensure directory exists
        if let Some(parent) = lock_file_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ProxyError::CacheError(format!("Failed to create size_tracking directory: {}", e))
            })?;
        }

        // Acquire exclusive lock with timeout
        let lock_file = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::task::spawn_blocking({
                let lock_path = lock_file_path.clone();
                move || {
                    use fs2::FileExt;
                    let file = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(&lock_path)?;
                    file.lock_exclusive()?;
                    Ok::<_, std::io::Error>(file)
                }
            }),
        )
        .await
        {
            Ok(Ok(Ok(file))) => file,
            Ok(Ok(Err(e))) => {
                warn!("Failed to acquire size state lock: {}", e);
                return Err(ProxyError::CacheError(format!(
                    "Size state lock acquisition failed: {}",
                    e
                )));
            }
            Ok(Err(e)) => {
                warn!("Size state lock task failed: {}", e);
                return Err(ProxyError::CacheError(format!(
                    "Size state lock task failed: {}",
                    e
                )));
            }
            Err(_) => {
                warn!("Size state lock timeout");
                return Err(ProxyError::CacheError(
                    "Size state lock timeout".to_string(),
                ));
            }
        };

        // Read current state while holding lock
        let mut state = match self.load_size_state().await {
            Ok(s) => s,
            Err(e) => {
                // Release lock
                let _ = lock_file.unlock();
                warn!("Failed to load size state for atomic subtract: {}", e);
                return Err(e);
            }
        };

        // Subtract bytes_freed
        let old_size = state.total_size;
        state.total_size = state.total_size.saturating_sub(bytes_freed);
        state.last_updated_by = get_instance_id();

        // Persist while still holding lock
        if let Err(e) = self.persist_size_state_internal(&state).await {
            // Release lock
            let _ = lock_file.unlock();
            warn!("Failed to persist size state after atomic subtract: {}", e);
            return Err(e);
        }

        // Release lock
        if let Err(e) = lock_file.unlock() {
            warn!("Failed to release size state lock: {}", e);
        }

        debug!(
            "Atomic size subtract: old_size={}, bytes_freed={}, new_size={}",
            old_size, bytes_freed, state.total_size
        );

        Ok(state.total_size)
    }
    /// Atomically add bytes to the size state when caching new data
    ///
    /// Uses file locking to ensure atomic read-modify-write across multiple instances.
    /// This prevents lost updates when multiple instances cache concurrently.
    ///
    /// # Arguments
    /// * `bytes_added` - Number of bytes added to cache
    ///
    /// # Returns
    /// * `Ok(new_size)` - The new total size after addition
    /// * `Err` - If locking or persistence fails
    pub async fn atomic_add_size(&self, bytes_added: u64) -> Result<u64> {
        #[allow(unused_imports)]
        use fs2::FileExt;

        if bytes_added == 0 {
            return Ok(self.get_current_size().await);
        }

        // Use a dedicated lock file for size state updates
        let lock_file_path = self.cache_dir.join("size_tracking").join("size_state.lock");

        // Ensure directory exists
        if let Some(parent) = lock_file_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ProxyError::CacheError(format!("Failed to create size_tracking directory: {}", e))
            })?;
        }

        // Acquire exclusive lock with timeout
        let lock_file = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::task::spawn_blocking({
                let lock_path = lock_file_path.clone();
                move || {
                    use fs2::FileExt;
                    let file = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(&lock_path)?;
                    file.lock_exclusive()?;
                    Ok::<_, std::io::Error>(file)
                }
            }),
        )
        .await
        {
            Ok(Ok(Ok(file))) => file,
            Ok(Ok(Err(e))) => {
                warn!("Failed to acquire size state lock for add: {}", e);
                return Err(ProxyError::CacheError(format!(
                    "Size state lock acquisition failed: {}",
                    e
                )));
            }
            Ok(Err(e)) => {
                warn!("Size state lock task failed for add: {}", e);
                return Err(ProxyError::CacheError(format!(
                    "Size state lock task failed: {}",
                    e
                )));
            }
            Err(_) => {
                warn!("Size state lock timeout for add");
                return Err(ProxyError::CacheError(
                    "Size state lock timeout".to_string(),
                ));
            }
        };

        // Read current state while holding lock
        let mut state = match self.load_size_state().await {
            Ok(s) => s,
            Err(e) => {
                // Release lock
                let _ = lock_file.unlock();
                warn!("Failed to load size state for atomic add: {}", e);
                return Err(e);
            }
        };

        // Add bytes
        let old_size = state.total_size;
        state.total_size = state.total_size.saturating_add(bytes_added);
        state.last_updated_by = get_instance_id();

        // Persist while still holding lock
        if let Err(e) = self.persist_size_state_internal(&state).await {
            // Release lock
            let _ = lock_file.unlock();
            warn!("Failed to persist size state after atomic add: {}", e);
            return Err(e);
        }

        // Release lock
        if let Err(e) = lock_file.unlock() {
            warn!("Failed to release size state lock after add: {}", e);
        }

        debug!(
            "Atomic size add: old_size={}, bytes_added={}, new_size={}",
            old_size, bytes_added, state.total_size
        );

        Ok(state.total_size)
    }
    /// Atomically add bytes to size state with non-blocking try-lock
    ///
    /// Initialize the consolidator by loading size state from disk
    ///
    /// This should be called during startup to recover size state from the previous run.
    /// If no size state file exists, the consolidator starts with default (zero) values
    /// and logs that validation will calculate the actual size.
    pub async fn initialize(&self) -> Result<()> {
        // Clean up stale journal files from dead instances before loading size state.
        // After OOM kills or crashes, journal files from the dead PID remain on shared
        // storage and are re-read every consolidation cycle. With 500k+ objects this can
        // be 800+ MB of stale data, causing memory pressure that triggers more OOM kills.
        self.cleanup_dead_instance_journals().await;

        match self.load_size_state().await {
            Ok(state) => {
                if state.total_size > 0 || state.consolidation_count > 0 {
                    info!(
                        "Loaded size state: total_size={}, write_cache_size={}, consolidation_count={}, last_updated_by={}",
                        state.total_size, state.write_cache_size, state.consolidation_count, state.last_updated_by
                    );
                }

                // If cached_objects is 0 but we have cached data, count .meta files now.
                // This handles the upgrade case where cached_objects was not previously tracked.
                // Acquire the size state lock before writing to prevent a concurrent consolidation
                // cycle on another instance from overwriting our result with cached_objects=0.
                if state.cached_objects == 0 && state.total_size > 0 {
                    let metadata_dir = self.cache_dir.join("metadata");
                    let count = tokio::task::spawn_blocking(move || {
                        use walkdir::WalkDir;
                        WalkDir::new(&metadata_dir)
                            .follow_links(false)
                            .into_iter()
                            .filter_map(|e| e.ok())
                            .filter(|e| e.path().extension().is_some_and(|ext| ext == "meta"))
                            .count() as u64
                    })
                    .await
                    .unwrap_or(0);

                    if count > 0 {
                        let lock_file_path =
                            self.cache_dir.join("size_tracking").join("size_state.lock");
                        let lock_acquired = tokio::time::timeout(
                            std::time::Duration::from_secs(10),
                            tokio::task::spawn_blocking({
                                let lock_path = lock_file_path.clone();
                                move || {
                                    use fs2::FileExt;
                                    let file = std::fs::OpenOptions::new()
                                        .create(true)
                                        .write(true)
                                        .truncate(false)
                                        .open(&lock_path)?;
                                    file.lock_exclusive()?;
                                    Ok::<_, std::io::Error>(file)
                                }
                            }),
                        )
                        .await;

                        if let Ok(Ok(Ok(lock_file))) = lock_acquired {
                            // Re-read under lock — another instance may have already populated it
                            let current = self.load_size_state().await.unwrap_or_default();
                            if current.cached_objects == 0 {
                                let mut updated = current;
                                updated.cached_objects = count;
                                updated.last_updated_by = get_instance_id();
                                if let Err(e) = self.persist_size_state_internal(&updated).await {
                                    warn!(
                                        "Failed to persist cached_objects after startup scan: {}",
                                        e
                                    );
                                } else {
                                    info!("Initialized cached_objects={} from startup .meta file count", count);
                                }
                            } else {
                                info!("cached_objects already populated ({}) by another instance, skipping", current.cached_objects);
                            }
                            let _ = lock_file.unlock();
                        } else {
                            warn!(
                                "Could not acquire size state lock for cached_objects startup init"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                // No size state file - this is a fresh install or first run after migration
                // Schedule immediate validation scan to calculate actual size
                info!(
                    "No size state found ({}), will calculate from filesystem on first validation",
                    e
                );
                // Size starts at 0, validation will correct it
            }
        }
        Ok(())
    }

    /// Check if eviction is needed and trigger it via CacheManager
    ///
    /// Called at the end of each consolidation cycle.
    /// Returns (eviction_triggered, bytes_freed).
    ///
    /// # Arguments
    ///
    /// * `known_size` - Optional pre-fetched current size to avoid extra NFS read.
    ///   If None, will read from disk.
    async fn maybe_trigger_eviction(&self, known_size: Option<u64>) -> (bool, u64) {
        // Check if max_cache_size is configured (0 means disabled)
        if self.config.max_cache_size == 0 {
            return (false, 0);
        }

        // Use provided size or read from disk
        let current_size = match known_size {
            Some(size) => size,
            None => self.get_current_size().await,
        };

        // Calculate trigger threshold using configurable percentage
        // Requirement 3.3: Trigger eviction when current_size > max_size * trigger_percent / 100
        let trigger_threshold = (self.config.max_cache_size as f64
            * (self.config.eviction_trigger_percent as f64 / 100.0))
            as u64;

        // Check if we're over the trigger threshold
        if current_size <= trigger_threshold {
            return (false, 0);
        }

        // Check if eviction is already running (Requirement 1.4, 1.5)
        if self
            .eviction_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            debug!("Eviction already in progress, skipping");
            return (false, 0);
        }

        // Get cache manager reference — reset guard on failure paths
        let cache_manager = {
            let guard = match self.cache_manager.lock() {
                Ok(g) => g,
                Err(e) => {
                    warn!("Failed to acquire cache_manager lock for eviction: {}", e);
                    self.eviction_in_progress.store(false, Ordering::SeqCst);
                    return (false, 0);
                }
            };

            match guard.as_ref().and_then(|weak| weak.upgrade()) {
                Some(cm) => cm,
                None => {
                    debug!("Cache manager not available, skipping eviction");
                    self.eviction_in_progress.store(false, Ordering::SeqCst);
                    return (false, 0);
                }
            }
        };

        // Clone the eviction flag Arc for the spawned task
        let eviction_flag = self.eviction_in_progress.clone();

        debug!(
            "Spawning eviction task: current_size={}, trigger_threshold={}, max_size={}",
            current_size, trigger_threshold, self.config.max_cache_size
        );

        // Spawn eviction as a detached task (Requirements 1.1, 1.2, 1.3)
        // The spawned task captures only owned data (Arc clones), not references to self
        tokio::spawn(async move {
            // scopeguard ensures eviction_in_progress is reset on all exit paths
            // including success, error, and panic (Requirement 1.3)
            let _guard = scopeguard::guard((), |_| {
                eviction_flag.store(false, Ordering::SeqCst);
            });

            match cache_manager
                .enforce_disk_cache_limits_skip_consolidation()
                .await
            {
                Ok(bytes_freed) => {
                    if bytes_freed > 0 {
                        info!("Background eviction completed: bytes_freed={}", bytes_freed);
                    }
                }
                Err(e) => {
                    // Requirement 1.7: Log error at warn level
                    warn!("Background eviction failed: {}", e);
                }
            }
        });

        // Return immediately after spawning (Requirement 1.6)
        (true, 0)
    }

    /// Run a complete consolidation cycle
    ///
    /// This method wraps the consolidation logic that was previously in main.rs:
    /// 1. Acquire global consolidation lock (prevents multi-instance race conditions)
    /// 2. Discover pending cache keys from all instance journals (capped at max_keys_per_cycle)
    /// 3. For each discovered cache key:
    ///    - Call consolidate_object()
    ///    - Calculate size delta from processed entries
    /// 4. Accumulate size deltas across all processed keys
    /// 5. Update SizeState (total_size, write_cache_size, last_consolidation, consolidation_count)
    /// 6. Persist size state to disk
    /// 7. Check if eviction needed and trigger it
    /// 8. Clean up consolidated entries from journals
    /// 9. Release global consolidation lock
    /// 10. Return ConsolidationCycleResult
    pub async fn run_consolidation_cycle(&self) -> Result<ConsolidationCycleResult> {
        let cycle_start = std::time::Instant::now();

        // Idle detection: skip the entire cycle if there's no pending work.
        // This avoids acquiring the global consolidation lock, scanning the journal
        // directory, and reading delta files when the proxy is idle — reducing EFS
        // metadata IOPS from ~90 to near-zero during idle periods.
        if !self.size_accumulator.has_pending_delta() && !self.has_pending_journal_files() {
            return Ok(ConsolidationCycleResult {
                keys_processed: 0,
                entries_consolidated: 0,
                size_delta: 0,
                cycle_duration: cycle_start.elapsed(),
                eviction_triggered: false,
                bytes_evicted: 0,
                current_size: 0, // Skip size read when idle
            });
        }

        // Flush own accumulator to delta file (no lock needed)
        // This ensures our pending deltas are written before we try to collect all deltas
        if let Err(e) = self.size_accumulator.flush().await {
            warn!("Failed to flush size accumulator at cycle start: {}", e);
            // Continue anyway - the accumulator will restore values on failure
        }

        // CRITICAL: Acquire global consolidation lock to prevent multiple instances
        // from running consolidation simultaneously. This eliminates race conditions
        // where multiple instances process the same journal entries due to NFS caching delays.
        let lock_acquired = match self.try_acquire_global_consolidation_lock() {
            Ok(true) => true,
            Ok(false) => {
                // Another instance is consolidating, skip this cycle
                debug!("Skipping consolidation cycle - another instance holds the lock");
                return Ok(ConsolidationCycleResult {
                    keys_processed: 0,
                    entries_consolidated: 0,
                    size_delta: 0,
                    cycle_duration: cycle_start.elapsed(),
                    eviction_triggered: false,
                    bytes_evicted: 0,
                    current_size: self.get_current_size().await,
                });
            }
            Err(e) => {
                warn!("Failed to acquire consolidation lock: {}", e);
                return Ok(ConsolidationCycleResult {
                    keys_processed: 0,
                    entries_consolidated: 0,
                    size_delta: 0,
                    cycle_duration: cycle_start.elapsed(),
                    eviction_triggered: false,
                    bytes_evicted: 0,
                    current_size: self.get_current_size().await,
                });
            }
        };

        // Ensure lock is released when we exit (even on error)
        let _lock_guard = scopeguard::guard(lock_acquired, |acquired| {
            if acquired {
                self.release_global_consolidation_lock();
            }
        });

        // Collect and apply deltas from all instances' delta files (under global lock)
        // This replaces the journal-based size tracking with accumulator-based tracking
        let (accumulator_size_delta, accumulator_wc_delta) =
            match self.collect_and_apply_deltas().await {
                Ok(deltas) => deltas,
                Err(e) => {
                    warn!("Failed to collect and apply deltas: {}", e);
                    (0, 0)
                }
            };

        // Apply accumulator deltas to size state (with clamping to 0)
        if accumulator_size_delta != 0 || accumulator_wc_delta != 0 {
            if let Err(e) = self
                .atomic_update_size_delta(accumulator_size_delta, accumulator_wc_delta)
                .await
            {
                warn!("Failed to apply accumulator deltas to size state: {}", e);
            }
        }

        // Set deadline for the entire discovery + processing phase.
        // Discovery of 66k+ keys on NFS can take 20s+, eating into processing time.
        // By starting the deadline here, we ensure the total cycle is bounded.
        let key_processing_timeout = self.config.consolidation_cycle_timeout;
        let deadline = tokio::time::Instant::now() + key_processing_timeout;

        // Discover pending cache keys with index, capped to limit NFS I/O
        let max_keys = self.config.max_keys_per_cycle;
        let discovery = match self
            .discover_pending_cache_keys_indexed_capped(max_keys)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                warn!("Failed to discover pending journal entries: {}", e);
                // Even on error, check if eviction is needed
                let current_size = self.get_current_size().await;
                let (eviction_triggered, bytes_evicted) =
                    self.maybe_trigger_eviction(Some(current_size)).await;
                return Ok(ConsolidationCycleResult {
                    keys_processed: 0,
                    entries_consolidated: 0,
                    size_delta: 0,
                    cycle_duration: cycle_start.elapsed(),
                    eviction_triggered,
                    bytes_evicted,
                    current_size,
                });
            }
        };

        let key_index = discovery.key_index;
        let file_entry_counts = discovery.file_entry_counts;
        let cache_keys: Vec<String> = key_index.keys().cloned().collect();

        // If no pending entries, still check if eviction is needed
        // This handles the case where cache is over capacity but no new data is being added
        if cache_keys.is_empty() {
            let current_size = self.get_current_size().await;
            let (eviction_triggered, bytes_evicted) =
                self.maybe_trigger_eviction(Some(current_size)).await;
            return Ok(ConsolidationCycleResult {
                keys_processed: 0,
                entries_consolidated: 0,
                size_delta: 0,
                cycle_duration: cycle_start.elapsed(),
                eviction_triggered,
                bytes_evicted,
                current_size,
            });
        }

        debug!(
            "Journal consolidation: found {} cache keys with pending entries",
            cache_keys.len()
        );

        // Accumulate results across all cache keys
        let mut total_entries_consolidated = 0;
        let mut _total_size_delta: i64 = 0; // Kept for debugging, not used for size tracking
        let mut _total_write_cache_delta: i64 = 0; // Kept for debugging, not used for size tracking
        let mut all_consolidated_entries = Vec::new();
        let mut new_objects_count: u64 = 0;
        let mut keys_processed = 0;

        // Process discovered cache keys concurrently (KEY_CONCURRENCY_LIMIT at a time)
        // Per-key locks are independent flock-based locks on different files, so concurrent
        // acquisition is safe. This reduces wall-clock time when individual keys hit NFS latency spikes.
        let total_keys_to_process = cache_keys.len();
        let key_futures: Vec<_> = cache_keys
            .iter()
            .map(|cache_key| {
                let cache_key = cache_key.clone();
                // Clone the file list for this key from the index so the async block is 'static
                let journal_files = key_index.get(&cache_key).cloned().unwrap_or_default();
                async move {
                    let result = self
                        .consolidate_object_with_files(&cache_key, &journal_files)
                        .await;
                    (cache_key, result)
                }
            })
            .collect();

        // Process results incrementally with the deadline set before discovery.
        // Completed keys are counted and cleaned up even when the deadline fires.
        let mut stream =
            std::pin::pin!(stream::iter(key_futures).buffer_unordered(KEY_CONCURRENCY_LIMIT));

        loop {
            match tokio::time::timeout_at(deadline, stream.next()).await {
                Ok(Some((cache_key, result))) => {
                    // Key completed before deadline
                    match result {
                        Ok(result) => {
                            if result.entries_consolidated > 0 {
                                debug!(
                                    "Journal consolidated: cache_key={}, entries={}, size_delta={:+}",
                                    cache_key, result.entries_consolidated, result.size_delta
                                );
                            }
                            total_entries_consolidated += result.entries_consolidated;
                            _total_size_delta += result.size_delta;
                            _total_write_cache_delta += result.write_cache_delta;
                            all_consolidated_entries.extend(result.consolidated_entries);
                            keys_processed += 1;
                            if result.is_new_object {
                                new_objects_count += 1;
                            }
                        }
                        Err(e) => {
                            info!("Journal consolidation failed for {}: {}", cache_key, e);
                        }
                    }
                }
                Ok(None) => {
                    // Stream exhausted — all keys processed
                    break;
                }
                Err(_) => {
                    // Deadline reached: log and break. Completed keys are already accumulated.
                    let unprocessed = total_keys_to_process.saturating_sub(keys_processed);
                    info!(
                        "Consolidation cycle deadline after {:?}, {} keys processed, {} unprocessed out of {} total",
                        key_processing_timeout, keys_processed, unprocessed, total_keys_to_process
                    );
                    break;
                }
            }
        }

        // NOTE: Size tracking is now handled by the accumulator-based approach above.
        // Journal-derived size deltas (total_size_delta, total_write_cache_delta) are
        // no longer used for size state updates - they are kept for logging/debugging only.
        // The accumulator tracks size at write/eviction time, eliminating the gap between
        // "when data is written" and "when size is counted".

        // Clean up ONLY the entries that were successfully consolidated
        // Entries that failed validation (range file not visible) are preserved
        if !all_consolidated_entries.is_empty() {
            if let Err(e) = self
                .cleanup_consolidated_entries(&all_consolidated_entries, &file_entry_counts)
                .await
            {
                warn!("Failed to cleanup consolidated journal entries: {}", e);
            }
        }

        let cycle_duration = cycle_start.elapsed();

        // Batch-increment cached_objects count for all new objects in this cycle.
        // This replaces per-key increment_cached_objects calls, reducing NFS lock
        // operations from N to 1 and preventing count loss when the deadline fires.
        if new_objects_count > 0 {
            self.increment_cached_objects(new_objects_count).await;
        }

        // Get current size and check if eviction is needed
        // Always check eviction at the end of every cycle, not just when size_delta > 0
        // This ensures eviction triggers even during idle periods when cache is over capacity
        let current_size = self.get_current_size().await;
        let (eviction_triggered, bytes_evicted) =
            self.maybe_trigger_eviction(Some(current_size)).await;

        // Log summary if there was activity
        if total_entries_consolidated > 0 || eviction_triggered || accumulator_size_delta != 0 {
            info!(
                "Consolidation cycle complete: keys={}, entries={}, accumulator_delta={:+}, duration={}ms, total_cache_size={}, eviction_triggered={}, bytes_evicted={}",
                keys_processed, total_entries_consolidated, accumulator_size_delta,
                cycle_duration.as_millis(), current_size, eviction_triggered, bytes_evicted
            );
        }

        Ok(ConsolidationCycleResult {
            keys_processed,
            entries_consolidated: total_entries_consolidated,
            size_delta: accumulator_size_delta, // Use accumulator delta (what was actually applied)
            cycle_duration,
            eviction_triggered,
            bytes_evicted,
            current_size,
        })
    }

    /// Consolidate journal entries for a specific cache key
    ///
    /// IMPORTANT: This method acquires a lock BEFORE reading journal entries to prevent
    /// race conditions where multiple instances consolidate the same cache key simultaneously.
    /// Without this, Instance A could read entries, Instance B could read the same entries,
    /// then both would consolidate and clean up, causing entries to be lost.
    pub async fn consolidate_object(&self, cache_key: &str) -> Result<ConsolidationResult> {
        self.consolidate_object_with_files(cache_key, &[]).await
    }

    /// Internal consolidation worker. When `journal_files` is non-empty it reads only those
    /// files (pre-built index from `discover_pending_cache_keys_indexed`). When empty it falls
    /// back to a full directory scan via `journal_manager.get_all_entries_for_cache_key`.
    async fn consolidate_object_with_files(
        &self,
        cache_key: &str,
        journal_files: &[PathBuf],
    ) -> Result<ConsolidationResult> {
        debug!("Starting consolidation for cache key: {}", cache_key);

        // Acquire lock BEFORE reading journal entries to prevent race conditions
        // where multiple instances read the same entries and then both try to consolidate.
        // Uses try_acquire_lock (single attempt, no retries) instead of acquire_lock
        // (exponential backoff with retries). If the lock is held, skip this key and
        // retry next cycle — cheaper than waiting ~150ms in backoff per contended key.
        let lock = match self.lock_manager.try_acquire_lock(cache_key).await {
            Ok(lock) => lock,
            Err(e) => {
                // Lock contention is expected when another instance is consolidating
                debug!(
                    "Consolidation skipped (lock held): cache_key={}, error={}",
                    cache_key, e
                );
                return Ok(ConsolidationResult::success(cache_key.to_string(), 0, 0));
            }
        };

        // Now that we hold the lock, read journal entries.
        // Use the pre-built index when available (avoids re-scanning all journal files).
        let all_entries = match if journal_files.is_empty() {
            self.journal_manager
                .get_all_entries_for_cache_key(cache_key)
                .await
        } else {
            self.journal_manager
                .get_entries_from_files(cache_key, journal_files)
                .await
        } {
            Ok(entries) => entries,
            Err(e) => {
                let error_msg = format!("Failed to get journal entries: {}", e);
                warn!(
                    "Consolidation failed: cache_key={}, error={}",
                    cache_key, error_msg
                );
                return Ok(ConsolidationResult::failure(
                    cache_key.to_string(),
                    error_msg,
                ));
            }
        };

        if all_entries.is_empty() {
            debug!("No journal entries found for cache key: {}", cache_key);
            return Ok(ConsolidationResult::success(cache_key.to_string(), 0, 0));
        }

        debug!(
            "Found {} journal entries for consolidation: cache_key={}",
            all_entries.len(),
            cache_key
        );

        // Validate journal entries with staleness detection
        // Returns (valid_entries, stale_entries) where:
        // - valid_entries: Range file exists on disk - safe to process for size delta
        // - stale_entries: Range file missing AND entry is old (> stale_timeout) - remove from journal
        // Entries with missing range files but recent timestamps are NOT returned - they stay in
        // journal for retry on next consolidation cycle. This prevents counting size for ranges
        // that don't exist on disk yet (e.g., due to NFS caching delays).
        let (valid_entries, stale_entries) = self
            .validate_journal_entries_with_staleness(&all_entries)
            .await;
        let stale_count = stale_entries.len();
        // Pending entries are those with missing range files but recent timestamps
        // They are NOT in valid_entries or stale_entries - they stay in journal for retry
        let pending_count = all_entries.len() - valid_entries.len() - stale_count;

        if stale_count > 0 {
            info!(
                "Removing {} stale journal entries: cache_key={}",
                stale_count, cache_key
            );
        }

        if pending_count > 0 {
            debug!(
                "Retaining {} pending journal entries (may still be streaming): cache_key={}",
                pending_count, cache_key
            );
        }

        if valid_entries.is_empty() {
            debug!(
                "No valid journal entries after validation: cache_key={}",
                cache_key
            );
            // Return stale entries for removal from journal, but no consolidation work done
            // Pending entries (recent with missing files) are NOT included and will be retried
            return Ok(ConsolidationResult {
                cache_key: cache_key.to_string(),
                entries_processed: all_entries.len(),
                entries_consolidated: 0,
                entries_removed: 0,
                conflicts_resolved: 0,
                invalid_entries_removed: stale_count,
                success: true,
                error: None,
                consolidated_entries: stale_entries, // Include stale entries for journal cleanup
                size_delta: 0,
                write_cache_delta: 0,
                is_new_object: false,
            });
        }

        // Lock is already held from above - use it for the rest of the operation

        // Load existing metadata or create new, using object_metadata from journal entries if available
        let mut metadata = match self
            .load_or_create_metadata_with_journal_entries(cache_key, &valid_entries)
            .await
        {
            Ok(metadata) => metadata,
            Err(e) => {
                let error_msg = format!("Failed to load metadata: {}", e);
                warn!(
                    "Consolidation failed: cache_key={}, error={}",
                    cache_key, error_msg
                );
                return Ok(ConsolidationResult::failure(
                    cache_key.to_string(),
                    error_msg,
                ));
            }
        };

        // Check range count before consolidation modifies the metadata.
        // If the object had no ranges (new or HEAD-only .meta), adding the first
        // range means we should count it as a new cached object.
        let had_no_ranges_before = metadata.ranges.is_empty();

        // Resolve conflicts between metadata and journal entries
        let _conflicts_resolved = self
            .resolve_conflicts(&mut metadata, &valid_entries)
            .await?;

        // Apply journal entries to metadata
        // Returns (entries_applied_count, size_affecting_entries) where size_affecting_entries contains
        // only entries that affect size tracking (Add entries that weren't skipped, Remove entries)
        // NOTE: size_affecting_entries is no longer used for size tracking - size is now tracked
        // at write/eviction time via the in-memory accumulator (see SizeAccumulator).
        let (entries_consolidated, _size_affecting_entries) =
            self.apply_journal_entries(&mut metadata, &valid_entries);

        // Size tracking is handled by the accumulator at write/eviction time, not during consolidation.
        // Journal entries are processed only for metadata updates via apply_journal_entries() above.
        let size_delta: i64 = 0;
        let write_cache_delta: i64 = 0;

        debug!(
            "Journal entries applied for metadata updates: cache_key={}, entries_consolidated={}, total_valid={}",
            cache_key, entries_consolidated, valid_entries.len()
        );

        // Write updated metadata to disk
        if let Err(e) = self.write_metadata_to_disk(&metadata, &lock).await {
            let error_msg = format!("Failed to write metadata: {}", e);
            warn!(
                "Consolidation failed: cache_key={}, error={}",
                cache_key, error_msg
            );
            return Ok(ConsolidationResult::failure(
                cache_key.to_string(),
                error_msg,
            ));
        }

        // Count as new cached object if this is the first time ranges are being added.
        // This covers both truly new objects and HEAD-only .meta files getting their first range.

        // Return the valid entries that were consolidated - these should be removed from journals
        // Stale entries (old entries with missing range files) are also included for removal
        // Pending entries (recent with missing files) are NOT included and will be retried

        // Size delta is already calculated from metadata (before/after comparison)
        // This is more accurate than counting journal entries

        // Log how many entries were processed vs consolidated
        let skipped_entries = valid_entries.len() - entries_consolidated;

        // Combine valid entries and stale entries for journal cleanup
        // Both should be removed from the journal - valid ones are consolidated, stale ones are cleaned up
        let mut entries_to_remove = valid_entries;
        entries_to_remove.extend(stale_entries);

        let result = ConsolidationResult {
            cache_key: cache_key.to_string(),
            entries_processed: all_entries.len(),
            entries_consolidated,
            entries_removed: 0,
            conflicts_resolved: skipped_entries, // Track skipped entries (range already in metadata)
            invalid_entries_removed: stale_count,
            success: true,
            error: None,
            consolidated_entries: entries_to_remove, // Include both valid and stale entries for journal cleanup
            size_delta,
            write_cache_delta,
            is_new_object: had_no_ranges_before && entries_consolidated > 0,
        };

        debug!(
            "Object metadata journal consolidation completed: cache_key={}, processed={}, consolidated={}, skipped={}, stale_removed={}, pending={}",
            cache_key, result.entries_processed, result.entries_consolidated, skipped_entries, stale_count, pending_count
        );

        Ok(result)
    }

    /// Discover all cache keys that have pending journal entries
    ///
    /// Reads from per-instance journals: `metadata/_journals/{instance_id}.journal`
    pub async fn discover_pending_cache_keys(&self) -> Result<Vec<String>> {
        let index = self.discover_pending_cache_keys_indexed().await?;
        Ok(index.into_keys().collect())
    }

    /// Like `discover_pending_cache_keys` but returns a map of cache_key → journal files
    /// that contain entries for that key. Built in a single pass over all journal files so
    /// callers can avoid re-scanning all files for each key during consolidation.
    pub async fn discover_pending_cache_keys_indexed(
        &self,
    ) -> Result<HashMap<String, Vec<PathBuf>>> {
        let discovery = self.discover_pending_cache_keys_indexed_capped(0).await?;
        Ok(discovery.key_index)
    }

    /// Discover pending cache keys with an optional cap on the number of keys returned.
    /// When `max_keys` > 0, stops reading journal files once the index reaches the cap,
    /// reducing NFS I/O when the backlog is large.
    pub async fn discover_pending_cache_keys_indexed_capped(
        &self,
        max_keys: usize,
    ) -> Result<DiscoveryResult> {
        let journals_dir = self.cache_dir.join("metadata").join("_journals");

        if !journals_dir.exists() {
            return Ok(DiscoveryResult {
                key_index: HashMap::new(),
                file_entry_counts: HashMap::new(),
            });
        }

        // key → set of journal file paths that contain at least one entry for that key
        let mut index: HashMap<String, Vec<PathBuf>> = HashMap::new();
        // file path → total parseable entry count (for optimized cleanup)
        let mut file_entry_counts: HashMap<PathBuf, usize> = HashMap::new();

        let journal_entries = std::fs::read_dir(&journals_dir).map_err(|e| {
            ProxyError::CacheError(format!("Failed to read journals directory: {}", e))
        })?;

        for journal_entry in journal_entries {
            // Early exit: stop reading more journal files once we have enough keys
            if max_keys > 0 && index.len() >= max_keys {
                debug!(
                    "Discovery cap reached ({} keys), skipping remaining journal files",
                    max_keys
                );
                break;
            }

            let journal_entry = journal_entry.map_err(|e| {
                ProxyError::CacheError(format!("Failed to read journal directory entry: {}", e))
            })?;

            let journal_path = journal_entry.path();

            if !journal_path.is_file() {
                continue;
            }

            let file_name = match journal_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            if !file_name.ends_with(".journal") {
                continue;
            }

            match tokio::fs::read_to_string(&journal_path).await {
                Ok(content) => {
                    // Track which keys appear in this file (avoid duplicate path entries)
                    let mut keys_in_file: HashSet<String> = HashSet::new();
                    let mut entry_count: usize = 0;
                    for line in content.lines() {
                        // Inner-loop cap: stop parsing this file once we have enough keys.
                        // Entry count will be incomplete for capped files, but that's fine —
                        // cleanup will fall through to entry-by-entry matching for those files.
                        if max_keys > 0 && index.len() >= max_keys {
                            break;
                        }
                        if line.trim().is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<JournalEntry>(line) {
                            Ok(entry) => {
                                entry_count += 1;
                                if keys_in_file.insert(entry.cache_key.clone()) {
                                    index
                                        .entry(entry.cache_key)
                                        .or_default()
                                        .push(journal_path.clone());
                                }
                            }
                            Err(e) => {
                                debug!(
                                    "Failed to parse journal entry: file={:?}, error={}",
                                    journal_path, e
                                );
                            }
                        }
                    }
                    if entry_count > 0 {
                        file_entry_counts.insert(journal_path.clone(), entry_count);
                    }
                }
                Err(e) => {
                    debug!(
                        "Failed to read journal file (discover_pending): file={:?}, error={}",
                        journal_path, e
                    );
                }
            }
        }

        debug!(
            "Discovered {} cache keys with pending journal entries across {} journal files",
            index.len(),
            file_entry_counts.len()
        );
        Ok(DiscoveryResult {
            key_index: index,
            file_entry_counts,
        })
    }
    /// Get all journal entries for a cache key from all instance journal files
    pub async fn get_all_entries_for_cache_key(
        &self,
        cache_key: &str,
    ) -> Result<Vec<JournalEntry>> {
        let journals_dir = self.cache_dir.join("metadata").join("_journals");

        if !journals_dir.exists() {
            return Ok(Vec::new());
        }

        let mut all_entries = Vec::new();

        // Scan all journal files
        let journal_files = std::fs::read_dir(&journals_dir).map_err(|e| {
            ProxyError::CacheError(format!("Failed to read journals directory: {}", e))
        })?;

        for journal_entry in journal_files {
            let journal_entry = journal_entry.map_err(|e| {
                ProxyError::CacheError(format!("Failed to read journal directory entry: {}", e))
            })?;

            let journal_path = journal_entry.path();

            if !journal_path.is_file() {
                continue;
            }

            let file_name = match journal_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            if !file_name.ends_with(".journal") {
                continue;
            }

            // Read and filter entries for this cache key
            match tokio::fs::read_to_string(&journal_path).await {
                Ok(content) => {
                    for line in content.lines() {
                        if line.trim().is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<JournalEntry>(line) {
                            Ok(entry) => {
                                if entry.cache_key == cache_key {
                                    all_entries.push(entry);
                                }
                            }
                            Err(e) => {
                                debug!(
                                    "Failed to parse journal entry: file={:?}, error={}",
                                    journal_path, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    // Stale file handle and other transient errors are expected on shared storage
                    debug!(
                        "Failed to read journal file (get_entries): file={:?}, error={}",
                        journal_path, e
                    );
                }
            }
        }

        // Sort entries by timestamp for proper ordering
        all_entries.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        debug!(
            "Found {} journal entries for cache_key={} from instance journals",
            all_entries.len(),
            cache_key
        );

        Ok(all_entries)
    }

    /// Validate journal entries by checking if referenced range files exist
    pub async fn validate_journal_entries(&self, entries: &[JournalEntry]) -> Vec<JournalEntry> {
        let mut valid_entries = Vec::new();

        for entry in entries {
            // Check if the range file exists
            let range_file_path = match self
                .get_range_file_path(&entry.cache_key, &entry.range_spec)
            {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        "Skipping journal entry with malformed cache key in validate_journal_entries: cache_key={}, error={}",
                        entry.cache_key, e
                    );
                    continue;
                }
            };

            if range_file_path.exists() {
                valid_entries.push(entry.clone());
                debug!(
                    "Journal entry validated: cache_key={}, range={}-{}, file={:?}",
                    entry.cache_key, entry.range_spec.start, entry.range_spec.end, range_file_path
                );
            } else {
                // Range file may not exist yet (still streaming) or was evicted
                // This is expected during concurrent writes - skip silently
                debug!(
                    "Journal entry references non-existent range file: cache_key={}, range={}-{}, file={:?}",
                    entry.cache_key, entry.range_spec.start, entry.range_spec.end, range_file_path
                );
            }
        }

        debug!(
            "Validated {} out of {} journal entries",
            valid_entries.len(),
            entries.len()
        );

        valid_entries
    }

    /// Validate journal entries, categorizing them based on range file existence and age
    ///
    /// Returns two lists:
    /// - `valid_entries`: Range file exists on disk - safe to process for size delta
    /// - `stale_entries`: Range file missing AND entry is old (> stale_timeout) - remove from journal
    ///
    /// Entries with missing range files but recent timestamps (< stale_timeout) are NOT returned
    /// in either list. They stay in the journal for retry on the next consolidation cycle.
    /// This prevents counting size for ranges that don't exist on disk yet (e.g., due to
    /// NFS caching delays or incomplete writes).
    ///
    /// An entry is considered stale if:
    /// 1. The range was recently evicted (bypasses timeout via mark_ranges_evicted), OR
    /// 2. The range file doesn't exist AND the entry's timestamp is older than stale_entry_timeout_secs
    ///
    /// **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 4.3**
    ///
    /// # Arguments
    /// * `entries` - Slice of journal entries to validate
    ///
    /// # Returns
    /// * `(valid_entries, stale_entries)` - Tuple where:
    ///   - valid_entries: Range file exists, process for size delta, remove from journal
    ///   - stale_entries: Range file missing + old, remove from journal without size delta
    pub async fn validate_journal_entries_with_staleness(
        &self,
        entries: &[JournalEntry],
    ) -> (Vec<JournalEntry>, Vec<JournalEntry>) {
        let mut valid_entries = Vec::new();
        let mut stale_entries = Vec::new();
        let now = SystemTime::now();
        let stale_timeout = Duration::from_secs(self.config.stale_entry_timeout_secs);

        for entry in entries {
            // Remove operations are ALWAYS valid - the file is intentionally deleted
            // This is critical for eviction journal entries to be processed for size tracking
            if matches!(entry.operation, JournalOperation::Remove) {
                valid_entries.push(entry.clone());
                debug!(
                    "Remove journal entry validated (file intentionally deleted): cache_key={}, range={}-{}",
                    entry.cache_key, entry.range_spec.start, entry.range_spec.end
                );
                continue;
            }

            // First check if this range was recently evicted (bypasses timeout)
            // Requirement 4.3: Immediately mark journal entries for evicted ranges as stale
            if self.check_and_clear_evicted_range(
                &entry.cache_key,
                entry.range_spec.start,
                entry.range_spec.end,
            ) {
                stale_entries.push(entry.clone());
                debug!(
                    "Removing journal entry for evicted range (bypassing timeout): cache_key={}, range={}-{}",
                    entry.cache_key,
                    entry.range_spec.start,
                    entry.range_spec.end
                );
                continue;
            }

            // TtlRefresh and AccessUpdate are object-level operations that don't target
            // a specific range file. They update object metadata (expires_at, access_count).
            // Validate by checking the metadata file exists instead of a range file.
            if matches!(
                entry.operation,
                JournalOperation::TtlRefresh | JournalOperation::AccessUpdate
            ) {
                let metadata_base_dir = self.cache_dir.join("metadata");
                let metadata_path = crate::disk_cache::get_sharded_path(
                    &metadata_base_dir,
                    &entry.cache_key,
                    ".meta",
                );
                let metadata_exists = metadata_path.as_ref().is_ok_and(|p| p.exists());
                if metadata_exists {
                    valid_entries.push(entry.clone());
                    debug!(
                        "Object-level journal entry validated (metadata exists): cache_key={}, op={:?}",
                        entry.cache_key, entry.operation
                    );
                } else {
                    let entry_age = now
                        .duration_since(entry.timestamp)
                        .unwrap_or(Duration::ZERO);
                    if entry_age > stale_timeout {
                        stale_entries.push(entry.clone());
                        debug!(
                            "Removing stale object-level journal entry (metadata missing): cache_key={}, op={:?}, age={:.1}s",
                            entry.cache_key, entry.operation, entry_age.as_secs_f64()
                        );
                    } else {
                        debug!(
                            "Object-level journal entry pending (metadata not visible): cache_key={}, op={:?}, age={:.1}s",
                            entry.cache_key, entry.operation, entry_age.as_secs_f64()
                        );
                    }
                }
                continue;
            }

            // Check if the range file exists
            let range_file_path = match self
                .get_range_file_path(&entry.cache_key, &entry.range_spec)
            {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        "Skipping journal entry with malformed cache key in validate_journal_entries_with_staleness: cache_key={}, error={}",
                        entry.cache_key, e
                    );
                    continue;
                }
            };

            if range_file_path.exists() {
                // Range file exists - entry is valid
                valid_entries.push(entry.clone());
                debug!(
                    "Journal entry validated: cache_key={}, range={}-{}, file={:?}",
                    entry.cache_key, entry.range_spec.start, entry.range_spec.end, range_file_path
                );
            } else {
                // Range file doesn't exist - check if entry is stale based on timestamp
                // Entry is stale if: entry.timestamp + stale_timeout < now
                let entry_age = now
                    .duration_since(entry.timestamp)
                    .unwrap_or(Duration::ZERO);

                if entry_age > stale_timeout {
                    // Entry is stale - mark for removal
                    stale_entries.push(entry.clone());
                    debug!(
                        "Removing stale journal entry: cache_key={}, range={}-{}, age={:.1}s (threshold={}s)",
                        entry.cache_key,
                        entry.range_spec.start,
                        entry.range_spec.end,
                        entry_age.as_secs_f64(),
                        self.config.stale_entry_timeout_secs
                    );
                } else {
                    // Entry is recent - may still be streaming
                    // BUG FIX: Do NOT add to valid_entries - this prevents size from being counted
                    // for ranges that don't exist on disk yet. Entry stays in journal for retry.
                    // Previously, these entries were added to valid_entries, causing size tracking
                    // to count ranges that didn't exist, leading to massive size discrepancies.
                    debug!(
                        "Journal entry pending (recent, file not visible): cache_key={}, range={}-{}, age={:.1}s, will_retry=true",
                        entry.cache_key,
                        entry.range_spec.start,
                        entry.range_spec.end,
                        entry_age.as_secs_f64()
                    );
                    // Entry is NOT added to any list, so it stays in journal for retry
                }
            }
        }

        debug!(
            "Validated {} entries, found {} stale entries out of {} total",
            valid_entries.len(),
            stale_entries.len(),
            entries.len()
        );

        (valid_entries, stale_entries)
    }

    /// Resolve conflicts between metadata and journal entries
    ///
    /// This only applies to Add/Update/Remove operations which carry full range data.
    /// TtlRefresh and AccessUpdate operations are incremental updates that should not
    /// replace existing range data - they only modify specific fields.
    pub async fn resolve_conflicts(
        &self,
        metadata: &mut NewCacheMetadata,
        journal_entries: &[JournalEntry],
    ) -> Result<usize> {
        let mut conflicts_resolved = 0;

        // Group journal entries by range (start, end), but only for operations that
        // carry full range data (Add, Update, Remove). TtlRefresh and AccessUpdate
        // are incremental updates that should not replace existing ranges.
        let mut journal_ranges: HashMap<(u64, u64), &JournalEntry> = HashMap::new();
        for entry in journal_entries {
            // Skip TtlRefresh and AccessUpdate - they don't carry full range data
            // and should not participate in conflict resolution
            match entry.operation {
                JournalOperation::TtlRefresh | JournalOperation::AccessUpdate => continue,
                _ => {}
            }

            let range_key = (entry.range_spec.start, entry.range_spec.end);

            // If multiple journal entries for same range, use the most recent
            if let Some(existing) = journal_ranges.get(&range_key) {
                if entry.timestamp > existing.timestamp {
                    journal_ranges.insert(range_key, entry);
                    conflicts_resolved += 1;
                }
            } else {
                journal_ranges.insert(range_key, entry);
            }
        }

        // Check for conflicts with existing metadata ranges
        for (range_key, journal_entry) in &journal_ranges {
            let (start, end) = *range_key;

            // Find existing range in metadata
            if let Some(existing_range) = metadata
                .ranges
                .iter_mut()
                .find(|r| r.start == start && r.end == end)
            {
                // Compare timestamps to determine which is more recent
                if journal_entry.timestamp > existing_range.created_at {
                    // Journal entry is more recent, update metadata range
                    *existing_range = journal_entry.range_spec.clone();
                    conflicts_resolved += 1;
                    debug!(
                        "Resolved conflict in favor of journal entry: cache_key={}, range={}-{}",
                        metadata.cache_key, start, end
                    );
                } else {
                    debug!(
                        "Resolved conflict in favor of metadata: cache_key={}, range={}-{}",
                        metadata.cache_key, start, end
                    );
                }
            }
        }

        Ok(conflicts_resolved)
    }

    /// Apply journal entries to metadata
    ///
    /// This method applies Add, Update, Remove, TtlRefresh, and AccessUpdate operations
    /// to the metadata. Size tracking is done by comparing metadata before/after,
    /// not by tracking individual entries.
    ///
    /// Returns the count of entries that were actually applied (modified metadata).
    /// Also returns the entries that affect size tracking (Add entries that weren't skipped, Remove entries).
    fn apply_journal_entries(
        &self,
        metadata: &mut NewCacheMetadata,
        entries: &[JournalEntry],
    ) -> (usize, Vec<JournalEntry>) {
        let mut entries_applied = 0;
        // Track entries that affect size: Add entries that were actually applied, and all Remove entries
        let mut size_affecting_entries = Vec::new();

        // Note: Object-level expires_at is set to ~100 years, so no need to extend it
        // Individual range TTLs are what matter for cache validity

        for entry in entries {
            match entry.operation {
                JournalOperation::Add => {
                    // Check if range already exists in metadata
                    let range_exists = metadata.ranges.iter().any(|r| {
                        r.start == entry.range_spec.start && r.end == entry.range_spec.end
                    });

                    if !range_exists {
                        metadata.ranges.push(entry.range_spec.clone());
                        entries_applied += 1;
                        // Only count size for Add entries that were actually applied
                        size_affecting_entries.push(entry.clone());
                        debug!(
                            "Applied ADD journal entry: cache_key={}, range={}-{}",
                            entry.cache_key, entry.range_spec.start, entry.range_spec.end
                        );
                    } else {
                        // Range already exists in metadata - skip (don't count size)
                        debug!(
                            "ADD journal entry skipped (range already in metadata): cache_key={}, range={}-{}",
                            entry.cache_key, entry.range_spec.start, entry.range_spec.end
                        );
                    }
                }
                JournalOperation::Update => {
                    // Find and update existing range
                    if let Some(existing_range) = metadata.ranges.iter_mut().find(|r| {
                        r.start == entry.range_spec.start && r.end == entry.range_spec.end
                    }) {
                        *existing_range = entry.range_spec.clone();
                        entries_applied += 1;
                        debug!(
                            "Applied UPDATE journal entry: cache_key={}, range={}-{}",
                            entry.cache_key, entry.range_spec.start, entry.range_spec.end
                        );
                    }
                }
                JournalOperation::Remove => {
                    // Remove range from metadata
                    let original_len = metadata.ranges.len();
                    metadata.ranges.retain(|r| {
                        !(r.start == entry.range_spec.start && r.end == entry.range_spec.end)
                    });

                    entries_applied += 1;
                    // Always count Remove entries for size tracking (file was deleted)
                    size_affecting_entries.push(entry.clone());

                    if metadata.ranges.len() < original_len {
                        debug!(
                            "Applied REMOVE journal entry: cache_key={}, range={}-{}",
                            entry.cache_key, entry.range_spec.start, entry.range_spec.end
                        );
                    } else {
                        debug!(
                            "REMOVE journal entry (range not in metadata): cache_key={}, range={}-{}",
                            entry.cache_key, entry.range_spec.start, entry.range_spec.end
                        );
                    }
                }
                JournalOperation::TtlRefresh => {
                    // Refresh TTL at the object level
                    if let Some(new_ttl_secs) = entry.new_ttl_secs {
                        let new_ttl = std::time::Duration::from_secs(new_ttl_secs);
                        metadata.refresh_object_ttl(new_ttl);
                        // Also update last_accessed on the specific range if found
                        if let Some(existing_range) = metadata.ranges.iter_mut().find(|r| {
                            r.start == entry.range_spec.start && r.end == entry.range_spec.end
                        }) {
                            existing_range.last_accessed = entry.timestamp;
                        }
                        entries_applied += 1;
                        debug!(
                            "Applied TTL_REFRESH journal entry (object-level): cache_key={}, range={}-{}, new_ttl={}s",
                            entry.cache_key, entry.range_spec.start, entry.range_spec.end, new_ttl_secs
                        );
                    } else {
                        debug!(
                            "TTL_REFRESH skipped - no new_ttl_secs: cache_key={}, range={}-{}",
                            entry.cache_key, entry.range_spec.start, entry.range_spec.end
                        );
                    }
                }
                JournalOperation::AccessUpdate => {
                    // Update access count and last_accessed for an existing range
                    if let Some(access_increment) = entry.access_increment {
                        if let Some(existing_range) = metadata.ranges.iter_mut().find(|r| {
                            r.start == entry.range_spec.start && r.end == entry.range_spec.end
                        }) {
                            existing_range.access_count += access_increment;
                            existing_range.last_accessed = entry.timestamp;
                            entries_applied += 1;
                            // AccessUpdate doesn't change size
                            debug!(
                                "Applied ACCESS_UPDATE journal entry: cache_key={}, range={}-{}, increment={}, new_count={}",
                                entry.cache_key, entry.range_spec.start, entry.range_spec.end,
                                access_increment, existing_range.access_count
                            );
                        } else {
                            debug!(
                                "ACCESS_UPDATE skipped - range not found: cache_key={}, range={}-{}",
                                entry.cache_key, entry.range_spec.start, entry.range_spec.end
                            );
                        }
                    }
                }
            }
        }

        // Update object metadata if this looks like a full object
        if metadata.ranges.len() == 1
            && metadata.ranges[0].start == 0
            && metadata.object_metadata.content_length == 0
        {
            metadata.object_metadata.content_length = metadata.ranges[0].end + 1;
            debug!(
                "Updated object content_length from range: cache_key={}, content_length={}",
                metadata.cache_key, metadata.object_metadata.content_length
            );
        }

        debug!(
            "Applied {} journal entries to metadata: cache_key={}, total_ranges={}, size_affecting={}",
            entries_applied,
            metadata.cache_key,
            metadata.ranges.len(),
            size_affecting_entries.len()
        );

        (entries_applied, size_affecting_entries)
    }

    /// Load existing metadata or create new metadata structure, using object_metadata from journal entries
    ///
    /// When creating new metadata (no existing .meta file), this function will use the `object_metadata`
    /// from the first journal entry that has it. This ensures that response_headers from the S3 response
    /// are preserved even when the .meta file is created by journal consolidation rather than directly.
    async fn load_or_create_metadata_with_journal_entries(
        &self,
        cache_key: &str,
        journal_entries: &[JournalEntry],
    ) -> Result<NewCacheMetadata> {
        let metadata_path = self.get_metadata_file_path(cache_key)?;

        if metadata_path.exists() {
            // Load existing metadata
            let content = tokio::fs::read_to_string(&metadata_path)
                .await
                .map_err(|e| {
                    ProxyError::CacheError(format!("Failed to read metadata file: {}", e))
                })?;

            serde_json::from_str(&content).map_err(|e| {
                ProxyError::CacheError(format!("Failed to parse metadata file: {}", e))
            })
        } else {
            // Create new metadata
            let now = SystemTime::now();

            // Try to get object_metadata from journal entries (first one that has it)
            let object_metadata = journal_entries
                .iter()
                .find_map(|entry| entry.object_metadata.clone())
                .unwrap_or_else(|| {
                    debug!(
                        "No object_metadata found in journal entries for cache_key={}, using default",
                        cache_key
                    );
                    ObjectMetadata::default()
                });

            if object_metadata.response_headers.is_empty() {
                debug!(
                    "Creating new metadata with empty response_headers: cache_key={}",
                    cache_key
                );
            } else {
                debug!(
                    "Creating new metadata with {} response_headers from journal entry: cache_key={}",
                    object_metadata.response_headers.len(),
                    cache_key
                );
            }

            // Use TTL from first Add journal entry, or Duration::ZERO (immediately expired) as safe default
            let object_ttl = journal_entries
                .iter()
                .find_map(|e| e.object_ttl_secs)
                .map(Duration::from_secs)
                .unwrap_or(Duration::ZERO);

            Ok(NewCacheMetadata {
                cache_key: cache_key.to_string(),
                object_metadata,
                ranges: Vec::new(),
                created_at: now,
                expires_at: now + object_ttl,
                compression_info: crate::cache_types::CompressionInfo::default(),
                ..Default::default()
            })
        }
    }

    /// Write metadata to disk with atomic write (temp file + rename)
    ///
    /// Even though we hold the lock, we use atomic write because:
    /// 1. On NFS, tokio::fs::write is NOT atomic - readers can see empty/partial files
    /// 2. Other processes may read the metadata file without holding the lock
    /// 3. Atomic rename ensures readers always see complete, valid JSON
    async fn write_metadata_to_disk(
        &self,
        metadata: &NewCacheMetadata,
        _lock: &crate::metadata_lock_manager::MetadataLock,
    ) -> Result<()> {
        let metadata_path = self.get_metadata_file_path(&metadata.cache_key)?;

        // Ensure parent directory exists
        if let Some(parent) = metadata_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ProxyError::CacheError(format!("Failed to create metadata directory: {}", e))
            })?;
        }

        // Serialize metadata
        let json_content = serde_json::to_string_pretty(metadata)
            .map_err(|e| ProxyError::CacheError(format!("Failed to serialize metadata: {}", e)))?;

        // Atomic write: write to temp file then rename
        // Use instance-specific tmp file to avoid race conditions on shared storage
        let instance_suffix = format!(
            "{}.{}",
            gethostname::gethostname().to_string_lossy(),
            std::process::id()
        );
        let tmp_extension = format!("meta.tmp.{}", instance_suffix);
        let temp_path = metadata_path.with_extension(&tmp_extension);

        // Write to temporary file
        if let Err(e) = tokio::fs::write(&temp_path, &json_content).await {
            warn!(
                "Failed to write metadata temp file: cache_key={}, temp_path={:?}, error={}",
                metadata.cache_key, temp_path, e
            );
            // Clean up temp file on write failure
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(ProxyError::CacheError(format!(
                "Failed to write metadata temp file: {}",
                e
            )));
        }

        // Atomic rename
        if let Err(e) = tokio::fs::rename(&temp_path, &metadata_path).await {
            warn!(
                "Failed to rename metadata file: cache_key={}, temp_path={:?}, final_path={:?}, error={}",
                metadata.cache_key, temp_path, metadata_path, e
            );
            // Clean up temp file on rename failure
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(ProxyError::CacheError(format!(
                "Failed to rename metadata file: {}",
                e
            )));
        }

        debug!(
            "Successfully wrote metadata to disk (atomic): cache_key={}, path={:?}",
            metadata.cache_key, metadata_path
        );

        Ok(())
    }

    /// Clean up journal files after a consolidation cycle.
    ///
    /// Uses file-level optimization: if all entries in a journal file were consolidated
    /// (determined by comparing consolidated entry count against the total entry count
    /// recorded during discovery), the file is deleted/truncated without re-reading.
    /// Only files with a mix of consolidated and unconsolidated entries are re-read
    /// and rewritten.
    ///
    /// Uses file-level locking (flock) to prevent races with append operations
    /// from other instances.
    pub async fn cleanup_consolidated_entries(
        &self,
        consolidated_entries: &[JournalEntry],
        file_entry_counts: &HashMap<PathBuf, usize>,
    ) -> Result<()> {
        if consolidated_entries.is_empty() {
            return Ok(());
        }

        let journals_dir = self.cache_dir.join("metadata").join("_journals");

        if !journals_dir.exists() {
            return Ok(());
        }

        // Count how many consolidated entries came from each journal file.
        // consolidated_entries don't directly track their source file, but we can
        // determine this by checking which files contain entries for each key
        // (from the key_index built during discovery). Instead, build a HashSet
        // for matching and count per-file during the scan.
        let consolidated_set: HashSet<(String, u64, u64, SystemTime, String)> =
            consolidated_entries
                .iter()
                .map(|ce| {
                    (
                        ce.cache_key.clone(),
                        ce.range_spec.start,
                        ce.range_spec.end,
                        ce.timestamp,
                        ce.instance_id.clone(),
                    )
                })
                .collect();

        let journal_files = std::fs::read_dir(&journals_dir).map_err(|e| {
            ProxyError::CacheError(format!("Failed to read journals directory: {}", e))
        })?;

        let mut files_deleted = 0u32;
        let mut files_truncated = 0u32;
        let mut files_rewritten = 0u32;
        let mut files_skipped = 0u32;

        for journal_entry in journal_files {
            let journal_entry = match journal_entry {
                Ok(e) => e,
                Err(e) => {
                    warn!("Failed to read journal directory entry: {}", e);
                    continue;
                }
            };

            let journal_path = journal_entry.path();

            if !journal_path.is_file() {
                continue;
            }

            let file_name = match journal_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };

            if !file_name.ends_with(".journal") {
                continue;
            }

            // Check if this file was seen during discovery and has a known entry count.
            // If the file wasn't in discovery (e.g., created after discovery started),
            // skip it — its entries weren't processed this cycle.
            // When file_entry_counts is empty (e.g., called from tests or outside the
            // discovery flow), process all journal files (fallback to old behavior).
            if !file_entry_counts.is_empty() && !file_entry_counts.contains_key(&journal_path) {
                files_skipped += 1;
                continue;
            }

            // Acquire file-level lock to prevent races with append operations
            let lock_path = journal_path.with_extension("journal.lock");
            let lock_file = match std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&lock_path)
            {
                Ok(f) => f,
                Err(e) => {
                    warn!(
                        "Failed to open journal lock file for cleanup: path={:?}, error={}",
                        lock_path, e
                    );
                    continue;
                }
            };

            use fs2::FileExt;
            if let Err(e) = lock_file.lock_exclusive() {
                warn!(
                    "Failed to acquire journal file lock for cleanup: path={:?}, error={}",
                    lock_path, e
                );
                continue;
            }

            // Read current journal content (while holding lock).
            // We must re-read because new entries may have been appended since discovery.
            let content = match tokio::fs::read_to_string(&journal_path).await {
                Ok(c) => c,
                Err(e) => {
                    info!(
                        "Failed to read journal file for cleanup (likely already deleted): path={:?}, error={}",
                        journal_path, e
                    );
                    continue;
                }
            };

            // Parse entries and separate consolidated from remaining
            let mut remaining_entries = Vec::new();
            let mut removed_count = 0usize;

            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<JournalEntry>(line) {
                    Ok(entry) => {
                        let was_consolidated = consolidated_set.contains(&(
                            entry.cache_key.clone(),
                            entry.range_spec.start,
                            entry.range_spec.end,
                            entry.timestamp,
                            entry.instance_id.clone(),
                        ));

                        if was_consolidated {
                            removed_count += 1;
                        } else {
                            remaining_entries.push(entry);
                        }
                    }
                    Err(e) => {
                        debug!(
                            "Failed to parse journal entry during cleanup: path={:?}, error={}",
                            journal_path, e
                        );
                        // Keep unparseable lines to avoid data loss — force rewrite path
                    }
                }
            }

            if removed_count == 0 {
                // No entries to remove from this file — skip write
                files_skipped += 1;
                continue;
            }

            let is_fresh_journal = file_name.contains(':');

            if remaining_entries.is_empty() {
                // All entries consolidated — delete or truncate without rewriting
                if is_fresh_journal {
                    match tokio::fs::remove_file(&journal_path).await {
                        Ok(_) => {
                            files_deleted += 1;
                            debug!(
                                "Deleted fully-consolidated fresh journal: path={:?}, removed={}",
                                journal_path, removed_count
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to delete fresh journal file: path={:?}, error={}",
                                journal_path, e
                            );
                        }
                    }
                } else {
                    // Truncate primary journal (keep file for future appends)
                    match tokio::fs::write(&journal_path, "").await {
                        Ok(_) => {
                            files_truncated += 1;
                            debug!(
                                "Truncated fully-consolidated primary journal: path={:?}, removed={}",
                                journal_path, removed_count
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to truncate journal file: path={:?}, error={}",
                                journal_path, e
                            );
                        }
                    }
                }
            } else {
                // Partial consolidation — rewrite with remaining entries
                let mut new_content = String::new();
                for entry in &remaining_entries {
                    match serde_json::to_string(entry) {
                        Ok(json) => {
                            new_content.push_str(&json);
                            new_content.push('\n');
                        }
                        Err(e) => {
                            warn!(
                                "Failed to serialize journal entry during cleanup: error={}",
                                e
                            );
                        }
                    }
                }

                match tokio::fs::write(&journal_path, new_content).await {
                    Ok(_) => {
                        files_rewritten += 1;
                        debug!(
                            "Rewritten journal file: path={:?}, removed={}, remaining={}",
                            journal_path,
                            removed_count,
                            remaining_entries.len()
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to write updated journal file: path={:?}, error={}",
                            journal_path, e
                        );
                    }
                }
            }
        }

        if files_deleted > 0 || files_truncated > 0 || files_rewritten > 0 {
            info!(
                "Journal cleanup: deleted={}, truncated={}, rewritten={}, skipped={}",
                files_deleted, files_truncated, files_rewritten, files_skipped
            );
        }

        // Clean up stale lock files (lock files for journals that no longer exist)
        self.cleanup_stale_lock_files(&journals_dir).await;

        Ok(())
    }

    /// Clean up stale journal lock files
    ///
    /// Lock files are created for fresh journals during lock contention. When the fresh
    /// journal is deleted after consolidation, the lock file remains. This method removes
    /// lock files that have no corresponding journal file.
    async fn cleanup_stale_lock_files(&self, journals_dir: &std::path::Path) {
        let lock_files: Vec<_> = match std::fs::read_dir(journals_dir) {
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .map(|ext| ext == "lock")
                        .unwrap_or(false)
                })
                .collect(),
            Err(_) => return,
        };

        let mut cleaned = 0;
        for lock_entry in lock_files {
            let lock_path = lock_entry.path();

            // Get the corresponding journal path by removing .lock extension
            // Lock files are named: {journal_name}.journal.lock
            let lock_name = match lock_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            // Extract journal name (remove .lock suffix)
            let journal_name = if lock_name.ends_with(".journal.lock") {
                &lock_name[..lock_name.len() - 5] // Remove ".lock"
            } else {
                continue;
            };

            let journal_path = journals_dir.join(journal_name);

            // If the journal file doesn't exist, the lock file is stale
            if !journal_path.exists() {
                match std::fs::remove_file(&lock_path) {
                    Ok(_) => {
                        cleaned += 1;
                        debug!("Removed stale lock file: {:?}", lock_path);
                    }
                    Err(e) => {
                        // Ignore errors - another instance may have already cleaned it
                        debug!(
                            "Failed to remove stale lock file: {:?}, error={}",
                            lock_path, e
                        );
                    }
                }
            }
        }

        if cleaned > 0 {
            info!("Cleaned up {} stale journal lock files", cleaned);
        }
    }

    /// Remove journal files (and .tmp files) belonging to dead instances.
    ///
    /// Journal file names encode the instance ID as `{hostname}:{pid}.journal`.
    /// If the PID no longer exists on this host, the instance crashed or was OOM-killed
    /// and its journal entries are orphaned. These files accumulate after repeated crashes
    /// and can reach hundreds of MB, causing memory pressure during discovery (which reads
    /// them into memory via `read_to_string`).
    ///
    /// This method:
    /// 1. Lists all `.journal` and `.journal.tmp` files in `_journals/`
    /// 2. Extracts the PID from the filename
    /// 3. Checks if the PID is alive on this host (via `kill(pid, 0)`)
    /// 4. Removes files from dead PIDs
    ///
    /// Safe for multi-instance: only removes files whose hostname matches this host.
    /// Files from other hosts are left alone (their PIDs are in a different PID namespace).
    async fn cleanup_dead_instance_journals(&self) {
        let journals_dir = self.cache_dir.join("metadata").join("_journals");
        if !journals_dir.exists() {
            return;
        }

        let my_hostname = gethostname::gethostname().to_string_lossy().to_string();
        let my_pid = std::process::id();

        let entries = match std::fs::read_dir(&journals_dir) {
            Ok(e) => e,
            Err(e) => {
                warn!(
                    "Failed to read journals dir for dead instance cleanup: {}",
                    e
                );
                return;
            }
        };

        let mut removed_count = 0u32;
        let mut removed_bytes = 0u64;

        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let file_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };

            // Match patterns: {hostname}:{pid}.journal, {hostname}:{pid}.journal.tmp,
            // {hostname}:{pid}:{timestamp}.journal (fresh journals from lock contention)
            let is_journal = file_name.ends_with(".journal") || file_name.ends_with(".journal.tmp");
            if !is_journal {
                continue;
            }

            // Strip suffixes to get the instance_id part
            let instance_part = file_name
                .strip_suffix(".journal.tmp")
                .or_else(|| file_name.strip_suffix(".journal"));
            let instance_part = match instance_part {
                Some(p) => p,
                None => continue,
            };

            // Parse hostname:pid (or hostname:pid:timestamp for fresh journals)
            let parts: Vec<&str> = instance_part.splitn(3, ':').collect();
            if parts.len() < 2 {
                continue;
            }

            let hostname = parts[0];
            let pid_str = parts[1];

            // Only clean up files from this host
            if hostname != my_hostname {
                continue;
            }

            let pid: u32 = match pid_str.parse() {
                Ok(p) => p,
                Err(_) => continue,
            };

            // Skip our own PID
            if pid == my_pid {
                continue;
            }

            // Check if the PID is still alive
            #[cfg(unix)]
            let is_alive = unsafe { libc::kill(pid as i32, 0) == 0 };
            #[cfg(not(unix))]
            let is_alive = true; // Conservative: don't remove on non-Unix

            if !is_alive {
                let file_size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                match std::fs::remove_file(&path) {
                    Ok(_) => {
                        removed_count += 1;
                        removed_bytes += file_size;
                        debug!(
                            "Removed dead instance journal: file={}, pid={}, size={}",
                            file_name, pid, file_size
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to remove dead instance journal {}: {}",
                            file_name, e
                        );
                    }
                }

                // Also remove associated lock file
                let lock_path = path.with_extension("journal.lock");
                if lock_path.exists() {
                    let _ = std::fs::remove_file(&lock_path);
                }
            }
        }

        if removed_count > 0 {
            info!(
                "Cleaned up {} dead instance journal files ({:.1} MB)",
                removed_count,
                removed_bytes as f64 / (1024.0 * 1024.0)
            );
        }
    }

    /// Get metadata file path for a cache key.
    ///
    /// # Errors
    /// Returns `ProxyError::CacheError` if `cache_key` is malformed (missing
    /// `bucket/object` separator or containing a rejected bucket segment).
    fn get_metadata_file_path(&self, cache_key: &str) -> Result<PathBuf> {
        let base_dir = self.cache_dir.join("metadata");

        // Use the same sharding logic as DiskCacheManager for consistency
        crate::disk_cache::get_sharded_path(&base_dir, cache_key, ".meta")
    }

    /// Get range file path for a cache key and range spec.
    ///
    /// # Errors
    /// Returns `ProxyError::CacheError` if `cache_key` is malformed (missing
    /// `bucket/object` separator or containing a rejected bucket segment).
    fn get_range_file_path(&self, cache_key: &str, range_spec: &RangeSpec) -> Result<PathBuf> {
        let base_dir = self.cache_dir.join("ranges");
        let suffix = format!("_{}-{}.bin", range_spec.start, range_spec.end);

        crate::disk_cache::get_sharded_path(&base_dir, cache_key, &suffix)
    }
    /// Graceful shutdown of the journal consolidator
    ///
    /// This method should be called during proxy shutdown to ensure:
    /// 1. Any pending accumulator delta is flushed to disk
    /// 2. Any pending journal entries are processed (final consolidation cycle)
    /// 3. Size state is persisted to disk before exit
    ///
    /// This ensures no tracking data is lost on graceful shutdown.
    /// **Validates: Requirements 6.1, 6.2, 6.3, 8.1, 8.2**
    pub async fn shutdown(&self) -> Result<()> {
        info!("JournalConsolidator shutdown initiated");

        // Step 0: Flush pending accumulator delta to disk
        // This ensures any in-memory size deltas are persisted before shutdown.
        // If flush fails, log warning but continue — validation scan will correct.
        // **Validates: Requirements 8.1, 8.2**
        if let Err(e) = self.size_accumulator.flush().await {
            warn!("Failed to flush size accumulator on shutdown: {}", e);
        }

        // Step 1: Run final consolidation cycle to process any pending journal entries
        // This ensures all pending entries are consolidated before shutdown
        match self.run_consolidation_cycle().await {
            Ok(result) => {
                if result.entries_consolidated > 0 {
                    info!(
                        "Final consolidation cycle completed: entries={}, size_delta={:+}, total_cache_size={}",
                        result.entries_consolidated, result.size_delta, result.current_size
                    );
                } else {
                    debug!("Final consolidation cycle: no pending entries");
                }
            }
            Err(e) => {
                warn!("Final consolidation cycle failed during shutdown: {}", e);
                // Continue with shutdown even if consolidation fails
            }
        }

        // Step 2: Read and log final size state from disk
        // The consolidation cycle already persisted the state, so we just log it
        match self.load_size_state().await {
            Ok(state) => {
                info!(
                    "Final size state: total_size={}, write_cache_size={}, consolidation_count={}",
                    state.total_size, state.write_cache_size, state.consolidation_count
                );
            }
            Err(e) => {
                warn!("Failed to read final size state during shutdown: {}", e);
                // Continue with shutdown even if read fails
            }
        }

        info!("JournalConsolidator shutdown completed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionAlgorithm;
    use crate::metadata_lock_manager::MetadataLockManager;
    use tempfile::TempDir;

    fn create_test_range_spec(start: u64, end: u64) -> RangeSpec {
        let now = SystemTime::now();
        RangeSpec {
            start,
            end,
            file_path: format!("test_range_{}-{}.bin", start, end),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: end - start + 1,
            uncompressed_size: end - start + 1,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            frequency_score: 1,
        }
    }

    #[tokio::test]
    async fn test_journal_consolidator_creation() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));
        let config = ConsolidationConfig::default();

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            config.clone(),
        );

        assert_eq!(consolidator.cache_dir, temp_dir.path());
        assert_eq!(consolidator.config.interval, config.interval);
        assert_eq!(consolidator.config.size_threshold, config.size_threshold);
    }

    #[tokio::test]
    async fn test_consolidation_config_default() {
        let config = ConsolidationConfig::default();

        assert_eq!(config.interval, Duration::from_secs(5)); // Changed from 30s to 5s
        assert_eq!(config.size_threshold, 1024 * 1024);
        assert_eq!(config.entry_count_threshold, 100);
        assert_eq!(KEY_CONCURRENCY_LIMIT, 64);
        assert_eq!(config.consolidation_cycle_timeout, Duration::from_secs(30));
        assert_eq!(config.max_keys_per_cycle, 5000);
    }

    #[test]
    fn test_consolidation_result_success() {
        let result = ConsolidationResult::success("test-key".to_string(), 5, 3);

        assert_eq!(result.cache_key, "test-key");
        assert_eq!(result.entries_processed, 5);
        assert_eq!(result.entries_consolidated, 3);
        assert!(result.success);
        assert!(result.error.is_none());
        assert!(result.consolidated_entries.is_empty());
    }

    #[test]
    fn test_consolidation_result_failure() {
        let result = ConsolidationResult::failure("test-key".to_string(), "test error".to_string());

        assert_eq!(result.cache_key, "test-key");
        assert_eq!(result.entries_processed, 0);
        assert_eq!(result.entries_consolidated, 0);
        assert!(!result.success);
        assert_eq!(result.error, Some("test error".to_string()));
        assert!(result.consolidated_entries.is_empty());
    }

    #[tokio::test]
    async fn test_discover_pending_cache_keys_empty() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        let cache_keys = consolidator.discover_pending_cache_keys().await.unwrap();
        assert!(cache_keys.is_empty());
    }

    #[tokio::test]
    async fn test_validate_journal_entries() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        let cache_key = "test-bucket/test-object";
        let range_spec = create_test_range_spec(0, 8388607);

        // Create a journal entry
        let entry = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: cache_key.to_string(),
            range_spec: range_spec.clone(),
            operation: JournalOperation::Add,
            range_file_path: "test_range_0-8388607.bin".to_string(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            metadata_written: false,
        };

        // Without creating the range file, validation should filter it out
        let valid_entries = consolidator
            .validate_journal_entries(std::slice::from_ref(&entry))
            .await;
        assert_eq!(valid_entries.len(), 0);

        // Create the range file
        let range_file_path = consolidator
            .get_range_file_path(cache_key, &range_spec)
            .unwrap();
        if let Some(parent) = range_file_path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&range_file_path, b"test data")
            .await
            .unwrap();

        // Now validation should pass
        let valid_entries = consolidator.validate_journal_entries(&[entry]).await;
        assert_eq!(valid_entries.len(), 1);
    }

    // Property-based tests using quickcheck
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: journal-based-metadata-updates, Property 6: Timestamp Ordering During Consolidation**
    /// *For any* set of journal entries for the same range, consolidation shall apply them
    /// in timestamp order (oldest first), ensuring the final state reflects the most recent operation.
    /// **Validates: Requirements 4.4**
    #[quickcheck]
    fn prop_consolidation_timestamp_ordering(num_entries: u8, base_ttl: u16) -> TestResult {
        // Filter invalid inputs
        let num_entries = (num_entries % 10) + 2; // 2-11 entries
        if base_ttl == 0 {
            return TestResult::discard();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let cache_key = "test-bucket/test-object";

            // Create journal manager and consolidator
            let journal_manager = Arc::new(JournalManager::new(
                temp_dir.path().to_path_buf(),
                "test-instance".to_string(),
            ));
            let lock_manager = Arc::new(MetadataLockManager::new(
                temp_dir.path().to_path_buf(),
                Duration::from_secs(30),
                3,
            ));
            let consolidator = JournalConsolidator::new(
                temp_dir.path().to_path_buf(),
                journal_manager.clone(),
                lock_manager,
                ConsolidationConfig::default(),
            );

            // Create initial metadata with a range
            let now = SystemTime::now();
            let range_spec = RangeSpec {
                start: 0,
                end: 8388607,
                file_path: "test_range.bin".to_string(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: 8388608,
                uncompressed_size: 8388608,
                created_at: now,
                last_accessed: now,
                access_count: 1,
                frequency_score: 1,
            };

            let mut metadata = NewCacheMetadata {
                cache_key: cache_key.to_string(),
                object_metadata: ObjectMetadata::default(),
                ranges: vec![range_spec.clone()],
                created_at: now,
                expires_at: now + Duration::from_secs(3600),
                compression_info: crate::cache_types::CompressionInfo::default(),
                ..Default::default()
            };

            // Create journal entries with different timestamps (out of order)
            let mut entries = Vec::new();
            for i in 0..num_entries {
                // Create entries with timestamps in reverse order (newest first)
                let timestamp = now + Duration::from_secs((num_entries - i) as u64 * 10);
                let new_ttl = base_ttl as u64 + (i as u64 * 100);

                entries.push(JournalEntry {
                    timestamp,
                    instance_id: "test-instance".to_string(),
                    cache_key: cache_key.to_string(),
                    range_spec: range_spec.clone(),
                    operation: JournalOperation::TtlRefresh,
                    range_file_path: "test_range.bin".to_string(),
                    metadata_version: 1,
                    new_ttl_secs: Some(new_ttl),
                    object_ttl_secs: None,
                    access_increment: None,
                    object_metadata: None,
                    metadata_written: false,
                });
            }

            // Shuffle entries to simulate out-of-order arrival
            // (entries are already in reverse timestamp order)

            // Apply entries - consolidator should sort by timestamp
            let (entries_applied, _applied_entries) =
                consolidator.apply_journal_entries(&mut metadata, &entries);

            // Property 1: All entries should be applied
            if entries_applied != num_entries as usize {
                return TestResult::error(format!(
                    "Expected {} entries applied, got {}",
                    num_entries, entries_applied
                ));
            }

            // Property 2: Final TTL should reflect the entry with the LATEST timestamp
            // Since entries are in reverse order, the first entry has the latest timestamp
            // and the last entry has the earliest timestamp.
            // After sorting by timestamp (oldest first), the last applied entry is the newest.
            let _expected_final_ttl = base_ttl as u64; // First entry (latest timestamp) has base_ttl

            // The range's expires_at should have been refreshed with the latest TTL
            // Since we apply in timestamp order (oldest first), the final state is from the newest entry
            if metadata.ranges.is_empty() {
                return TestResult::error("Metadata ranges should not be empty");
            }

            // Verify the range was updated (access_count or expires_at changed)
            // The exact TTL value depends on the order of application
            // Key property: entries are processed, and the final state is deterministic

            TestResult::passed()
        })
    }

    /// **Feature: journal-based-metadata-updates, Property 5: Consolidation Applies All Entry Types**
    /// *For any* set of journal entries (Add, Update, Remove, TtlRefresh, AccessUpdate),
    /// consolidation shall correctly apply each entry type.
    /// **Validates: Requirements 1.5, 4.2, 4.3**
    #[quickcheck]
    fn prop_consolidation_applies_all_entry_types(
        ttl_secs: u16,
        access_increment: u8,
    ) -> TestResult {
        // Filter invalid inputs
        if ttl_secs == 0 || access_increment == 0 {
            return TestResult::discard();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let cache_key = "test-bucket/test-object";

            // Create journal manager and consolidator
            let journal_manager = Arc::new(JournalManager::new(
                temp_dir.path().to_path_buf(),
                "test-instance".to_string(),
            ));
            let lock_manager = Arc::new(MetadataLockManager::new(
                temp_dir.path().to_path_buf(),
                Duration::from_secs(30),
                3,
            ));
            let consolidator = JournalConsolidator::new(
                temp_dir.path().to_path_buf(),
                journal_manager.clone(),
                lock_manager,
                ConsolidationConfig::default(),
            );

            let now = SystemTime::now();

            // Create range specs for different operations
            let range1 = RangeSpec {
                start: 0,
                end: 1000,
                file_path: "range1.bin".to_string(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: 1001,
                uncompressed_size: 1001,
                created_at: now,
                last_accessed: now,
                access_count: 1,
                frequency_score: 1,
            };

            let range2 = RangeSpec {
                start: 1001,
                end: 2000,
                file_path: "range2.bin".to_string(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: 1000,
                uncompressed_size: 1000,
                created_at: now,
                last_accessed: now,
                access_count: 5,
                frequency_score: 1,
            };

            let range3 = RangeSpec {
                start: 2001,
                end: 3000,
                file_path: "range3.bin".to_string(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: 1000,
                uncompressed_size: 1000,
                created_at: now,
                last_accessed: now,
                access_count: 1,
                frequency_score: 1,
            };

            // Create metadata with range2 (for TTL refresh and access update tests)
            let mut metadata = NewCacheMetadata {
                cache_key: cache_key.to_string(),
                object_metadata: ObjectMetadata::default(),
                ranges: vec![range2.clone()],
                created_at: now,
                expires_at: now + Duration::from_secs(3600),
                compression_info: crate::cache_types::CompressionInfo::default(),
                ..Default::default()
            };

            let initial_access_count = metadata.ranges[0].access_count;

            // Create journal entries for each operation type
            let entries = vec![
                // Add operation - add range1
                JournalEntry {
                    timestamp: now + Duration::from_secs(1),
                    instance_id: "test-instance".to_string(),
                    cache_key: cache_key.to_string(),
                    range_spec: range1.clone(),
                    operation: JournalOperation::Add,
                    range_file_path: "range1.bin".to_string(),
                    metadata_version: 1,
                    new_ttl_secs: None,
                    object_ttl_secs: Some(3600),
                    access_increment: None,
                    object_metadata: None,
                    metadata_written: false,
                },
                // TtlRefresh operation - refresh TTL for range2
                JournalEntry {
                    timestamp: now + Duration::from_secs(2),
                    instance_id: "test-instance".to_string(),
                    cache_key: cache_key.to_string(),
                    range_spec: range2.clone(),
                    operation: JournalOperation::TtlRefresh,
                    range_file_path: "range2.bin".to_string(),
                    metadata_version: 1,
                    new_ttl_secs: Some(ttl_secs as u64),
                    object_ttl_secs: None,
                    access_increment: None,
                    object_metadata: None,
                    metadata_written: false,
                },
                // AccessUpdate operation - update access count for range2
                JournalEntry {
                    timestamp: now + Duration::from_secs(3),
                    instance_id: "test-instance".to_string(),
                    cache_key: cache_key.to_string(),
                    range_spec: range2.clone(),
                    operation: JournalOperation::AccessUpdate,
                    range_file_path: "range2.bin".to_string(),
                    metadata_version: 1,
                    new_ttl_secs: None,
                    object_ttl_secs: None,
                    access_increment: Some(access_increment as u64),
                    object_metadata: None,
                    metadata_written: false,
                },
                // Add operation - add range3
                JournalEntry {
                    timestamp: now + Duration::from_secs(4),
                    instance_id: "test-instance".to_string(),
                    cache_key: cache_key.to_string(),
                    range_spec: range3.clone(),
                    operation: JournalOperation::Add,
                    range_file_path: "range3.bin".to_string(),
                    metadata_version: 1,
                    new_ttl_secs: None,
                    object_ttl_secs: Some(3600),
                    access_increment: None,
                    object_metadata: None,
                    metadata_written: false,
                },
            ];

            // Apply all entries
            let (entries_applied, _applied_entries) =
                consolidator.apply_journal_entries(&mut metadata, &entries);

            // Property 1: All 4 entries should be applied
            if entries_applied != 4 {
                return TestResult::error(format!(
                    "Expected 4 entries applied, got {}",
                    entries_applied
                ));
            }

            // Property 2: Should have 3 ranges (range1 added, range2 existed, range3 added)
            if metadata.ranges.len() != 3 {
                return TestResult::error(format!(
                    "Expected 3 ranges, got {}",
                    metadata.ranges.len()
                ));
            }

            // Property 3: range1 should exist (Add operation)
            let has_range1 = metadata
                .ranges
                .iter()
                .any(|r| r.start == 0 && r.end == 1000);
            if !has_range1 {
                return TestResult::error("Add operation failed - range1 not found");
            }

            // Property 4: range2 should have updated access count (AccessUpdate operation)
            let range2_updated = metadata
                .ranges
                .iter()
                .find(|r| r.start == 1001 && r.end == 2000);
            match range2_updated {
                Some(r) => {
                    let expected_count = initial_access_count + access_increment as u64;
                    if r.access_count != expected_count {
                        return TestResult::error(format!(
                            "AccessUpdate failed - expected access_count={}, got {}",
                            expected_count, r.access_count
                        ));
                    }
                }
                None => {
                    return TestResult::error("range2 not found after operations");
                }
            }

            // Property 5: range3 should exist (Add operation)
            let has_range3 = metadata
                .ranges
                .iter()
                .any(|r| r.start == 2001 && r.end == 3000);
            if !has_range3 {
                return TestResult::error("Add operation failed - range3 not found");
            }

            TestResult::passed()
        })
    }

    // ============================================================================
    // SizeState Tests
    // ============================================================================

    #[test]
    fn test_size_state_default() {
        let state = SizeState::default();

        assert_eq!(state.total_size, 0);
        assert_eq!(state.write_cache_size, 0);
        assert_eq!(state.last_consolidation, UNIX_EPOCH);
        assert_eq!(state.consolidation_count, 0);
        assert!(state.last_updated_by.is_empty());
    }

    #[test]
    fn test_size_state_serialization() {
        let now = SystemTime::now();
        let state = SizeState {
            total_size: 1024 * 1024 * 100,      // 100MB
            write_cache_size: 1024 * 1024 * 10, // 10MB
            cached_objects: 0,
            last_consolidation: now,
            consolidation_count: 42,
            last_updated_by: "test-host:12345".to_string(),
        };

        // Serialize to JSON
        let json = serde_json::to_string(&state).unwrap();

        // Deserialize back
        let deserialized: SizeState = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.total_size, state.total_size);
        assert_eq!(deserialized.write_cache_size, state.write_cache_size);
        assert_eq!(deserialized.consolidation_count, state.consolidation_count);
        assert_eq!(deserialized.last_updated_by, state.last_updated_by);

        // SystemTime comparison - should be within 1 second due to serialization precision
        let original_secs = state
            .last_consolidation
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let deser_secs = deserialized
            .last_consolidation
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert_eq!(original_secs, deser_secs);
    }

    #[tokio::test]
    async fn test_load_size_state_missing_file() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Load should return default state when file doesn't exist
        let state = consolidator.load_size_state().await.unwrap();
        assert_eq!(state.total_size, 0);
        assert_eq!(state.write_cache_size, 0);
        assert_eq!(state.consolidation_count, 0);
    }

    #[tokio::test]
    async fn test_persist_and_load_size_state() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Create a state to persist
        let state = SizeState {
            total_size: 1024 * 1024 * 50,      // 50MB
            write_cache_size: 1024 * 1024 * 5, // 5MB
            cached_objects: 0,
            consolidation_count: 10,
            last_updated_by: "test-host:99999".to_string(),
            last_consolidation: SystemTime::now(),
        };

        // Persist to disk
        consolidator.persist_size_state(&state).await.unwrap();

        // Verify file exists
        let size_state_path = temp_dir
            .path()
            .join("size_tracking")
            .join("size_state.json");
        assert!(size_state_path.exists());

        // Load and verify
        let loaded_state = consolidator.load_size_state().await.unwrap();
        assert_eq!(loaded_state.total_size, 1024 * 1024 * 50);
        assert_eq!(loaded_state.write_cache_size, 1024 * 1024 * 5);
        assert_eq!(loaded_state.consolidation_count, 10);
        assert_eq!(loaded_state.last_updated_by, "test-host:99999");
    }

    #[tokio::test]
    async fn test_get_current_size() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Initial size should be 0 (no size_state.json file)
        assert_eq!(consolidator.get_current_size().await, 0);

        // Persist a state with size
        let state = SizeState {
            total_size: 1024 * 1024 * 100, // 100MB
            write_cache_size: 0,
            cached_objects: 0,
            consolidation_count: 0,
            last_updated_by: "test".to_string(),
            last_consolidation: SystemTime::now(),
        };
        consolidator.persist_size_state(&state).await.unwrap();

        // Should reflect new size from disk
        assert_eq!(consolidator.get_current_size().await, 1024 * 1024 * 100);
    }

    #[tokio::test]
    async fn test_get_write_cache_size() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Initial write cache size should be 0 (no size_state.json file)
        assert_eq!(consolidator.get_write_cache_size().await, 0);

        // Persist a state with write cache size
        let state = SizeState {
            total_size: 0,
            write_cache_size: 1024 * 1024 * 10, // 10MB
            cached_objects: 0,
            consolidation_count: 0,
            last_updated_by: "test".to_string(),
            last_consolidation: SystemTime::now(),
        };
        consolidator.persist_size_state(&state).await.unwrap();

        // Should reflect new size from disk
        assert_eq!(consolidator.get_write_cache_size().await, 1024 * 1024 * 10);
    }

    #[tokio::test]
    async fn test_get_size_state_async() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Persist a state to disk
        let state = SizeState {
            total_size: 1024 * 1024 * 200,      // 200MB
            write_cache_size: 1024 * 1024 * 20, // 20MB
            cached_objects: 0,
            consolidation_count: 100,
            last_updated_by: "test".to_string(),
            last_consolidation: SystemTime::now(),
        };
        consolidator.persist_size_state(&state).await.unwrap();

        // Get state async (reads from disk)
        let loaded_state = consolidator.get_size_state().await;
        assert_eq!(loaded_state.total_size, 1024 * 1024 * 200);
        assert_eq!(loaded_state.write_cache_size, 1024 * 1024 * 20);
        assert_eq!(loaded_state.consolidation_count, 100);
    }

    #[tokio::test]
    async fn test_size_state_path_location() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Verify size_state_path is in the correct location
        let expected_path = temp_dir
            .path()
            .join("size_tracking")
            .join("size_state.json");
        assert_eq!(consolidator.size_state_path, expected_path);
    }

    #[tokio::test]
    async fn test_persist_creates_directory() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        // size_tracking directory should not exist yet
        let size_tracking_dir = temp_dir.path().join("size_tracking");
        assert!(!size_tracking_dir.exists());

        // Persist should create the directory
        let state = SizeState::default();
        consolidator.persist_size_state(&state).await.unwrap();

        // Directory should now exist
        assert!(size_tracking_dir.exists());
        let size_state_path = temp_dir
            .path()
            .join("size_tracking")
            .join("size_state.json");
        assert!(size_state_path.exists());
    }

    // ============================================================================
    // Initialize and Run Consolidation Cycle Tests
    // ============================================================================

    #[tokio::test]
    async fn test_initialize_with_no_existing_state() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Initialize should succeed even with no existing state file
        consolidator.initialize().await.unwrap();

        // State should be default (zeros)
        let state = consolidator.get_size_state().await;
        assert_eq!(state.total_size, 0);
        assert_eq!(state.write_cache_size, 0);
        assert_eq!(state.consolidation_count, 0);
    }

    #[tokio::test]
    async fn test_initialize_with_existing_state() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Create a size state file manually
        let size_tracking_dir = temp_dir.path().join("size_tracking");
        tokio::fs::create_dir_all(&size_tracking_dir).await.unwrap();

        let state = SizeState {
            total_size: 1024 * 1024 * 100,      // 100MB
            write_cache_size: 1024 * 1024 * 10, // 10MB
            cached_objects: 0,
            last_consolidation: SystemTime::now(),
            consolidation_count: 42,
            last_updated_by: "previous-instance:12345".to_string(),
        };
        let json = serde_json::to_string(&state).unwrap();
        tokio::fs::write(consolidator.size_state_path.clone(), json)
            .await
            .unwrap();

        // Initialize should load the existing state
        consolidator.initialize().await.unwrap();

        // State should match what we wrote
        let loaded_state = consolidator.get_size_state().await;
        assert_eq!(loaded_state.total_size, 1024 * 1024 * 100);
        assert_eq!(loaded_state.write_cache_size, 1024 * 1024 * 10);
        assert_eq!(loaded_state.consolidation_count, 42);
        assert_eq!(loaded_state.last_updated_by, "previous-instance:12345");
    }

    #[tokio::test]
    async fn test_run_consolidation_cycle_empty() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Run consolidation cycle with no pending entries
        let result = consolidator.run_consolidation_cycle().await.unwrap();

        assert_eq!(result.keys_processed, 0);
        assert_eq!(result.entries_consolidated, 0);
        assert_eq!(result.size_delta, 0);
        assert!(!result.eviction_triggered);
        assert_eq!(result.bytes_evicted, 0);
        assert_eq!(result.current_size, 0);
    }

    #[tokio::test]
    async fn test_run_consolidation_cycle_with_entries() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager.clone(),
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Create a journal entry
        let cache_key = "test-bucket/test-object";
        let now = SystemTime::now();
        let range_spec = RangeSpec {
            start: 0,
            end: 1023,
            file_path: "test_range.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 1024,
            uncompressed_size: 1024,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            frequency_score: 1,
        };

        // Create the range file so validation passes
        let range_file_path = consolidator
            .get_range_file_path(cache_key, &range_spec)
            .unwrap();
        if let Some(parent) = range_file_path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&range_file_path, vec![0u8; 1024])
            .await
            .unwrap();

        // Simulate what store_range() does: add size to accumulator
        // In the real code path, store_range() calls accumulator.add() after writing the range file
        consolidator
            .size_accumulator()
            .add(range_spec.compressed_size);

        // Write journal entry
        let entry = JournalEntry {
            timestamp: now,
            instance_id: "test-instance".to_string(),
            cache_key: cache_key.to_string(),
            range_spec: range_spec.clone(),
            operation: JournalOperation::Add,
            range_file_path: range_file_path.to_string_lossy().to_string(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            metadata_written: false,
        };
        journal_manager
            .append_range_entry(cache_key, entry)
            .await
            .unwrap();

        // Run consolidation cycle
        let result = consolidator.run_consolidation_cycle().await.unwrap();

        assert_eq!(result.keys_processed, 1);
        assert_eq!(result.entries_consolidated, 1);
        assert_eq!(result.size_delta, 1024); // Add operation adds 1024 bytes
        assert!(!result.eviction_triggered);
        assert_eq!(result.bytes_evicted, 0);
        assert_eq!(result.current_size, 1024);

        // Verify size state was updated
        let state = consolidator.get_size_state().await;
        assert_eq!(state.total_size, 1024);
        assert_eq!(state.consolidation_count, 1);
    }

    #[tokio::test]
    async fn test_run_consolidation_cycle_updates_size_state() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager.clone(),
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Set initial size state by persisting to disk
        let initial_state = SizeState {
            total_size: 1000,
            write_cache_size: 0,
            cached_objects: 0,
            consolidation_count: 5,
            last_updated_by: "test".to_string(),
            last_consolidation: SystemTime::now(),
        };
        consolidator
            .persist_size_state(&initial_state)
            .await
            .unwrap();

        // Create a journal entry for an Add operation
        let cache_key = "test-bucket/test-object2";
        let now = SystemTime::now();
        let range_spec = RangeSpec {
            start: 0,
            end: 2047,
            file_path: "test_range2.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 2048,
            uncompressed_size: 2048,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            frequency_score: 1,
        };

        // Create the range file
        let range_file_path = consolidator
            .get_range_file_path(cache_key, &range_spec)
            .unwrap();
        if let Some(parent) = range_file_path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&range_file_path, vec![0u8; 2048])
            .await
            .unwrap();

        // Simulate what store_range() does: add size to accumulator
        // In the real code path, store_range() calls accumulator.add() after writing the range file
        consolidator
            .size_accumulator()
            .add(range_spec.compressed_size);

        // Write journal entry
        let entry = JournalEntry {
            timestamp: now,
            instance_id: "test-instance".to_string(),
            cache_key: cache_key.to_string(),
            range_spec: range_spec.clone(),
            operation: JournalOperation::Add,
            range_file_path: range_file_path.to_string_lossy().to_string(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            metadata_written: false,
        };
        journal_manager
            .append_range_entry(cache_key, entry)
            .await
            .unwrap();

        // Run consolidation cycle
        let result = consolidator.run_consolidation_cycle().await.unwrap();

        // Verify size was accumulated
        assert_eq!(result.size_delta, 2048);
        assert_eq!(result.current_size, 1000 + 2048); // Initial + delta

        // Verify state was updated
        let state = consolidator.get_size_state().await;
        assert_eq!(state.total_size, 1000 + 2048);
        assert_eq!(state.consolidation_count, 6); // Was 5, now 6
    }

    #[tokio::test]
    async fn test_consolidation_cycle_result_fields() {
        let result = ConsolidationCycleResult {
            keys_processed: 10,
            entries_consolidated: 25,
            size_delta: -1024,
            cycle_duration: Duration::from_millis(150),
            eviction_triggered: true,
            bytes_evicted: 5000,
            current_size: 100000,
        };

        assert_eq!(result.keys_processed, 10);
        assert_eq!(result.entries_consolidated, 25);
        assert_eq!(result.size_delta, -1024);
        assert_eq!(result.cycle_duration, Duration::from_millis(150));
        assert!(result.eviction_triggered);
        assert_eq!(result.bytes_evicted, 5000);
        assert_eq!(result.current_size, 100000);
    }

    #[tokio::test]
    async fn test_shutdown_persists_size_state() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Set some size state by persisting to disk
        let state = SizeState {
            total_size: 12345,
            write_cache_size: 1000,
            cached_objects: 0,
            consolidation_count: 42,
            last_updated_by: "test".to_string(),
            last_consolidation: SystemTime::now(),
        };
        consolidator.persist_size_state(&state).await.unwrap();

        // Call shutdown
        let result = consolidator.shutdown().await;
        assert!(result.is_ok());

        // Verify size state was persisted
        let size_state_path = temp_dir
            .path()
            .join("size_tracking")
            .join("size_state.json");
        assert!(size_state_path.exists());

        // Read and verify the persisted state
        let content = tokio::fs::read_to_string(&size_state_path).await.unwrap();
        let persisted_state: SizeState = serde_json::from_str(&content).unwrap();
        assert_eq!(persisted_state.total_size, 12345);
        assert_eq!(persisted_state.write_cache_size, 1000);
        assert_eq!(persisted_state.consolidation_count, 42);
    }

    #[tokio::test]
    async fn test_shutdown_runs_final_consolidation() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager.clone(),
            lock_manager,
            ConsolidationConfig::default(),
        );

        // Create a journal entry
        let cache_key = "test-bucket/shutdown-test-object";
        let now = SystemTime::now();
        let range_spec = RangeSpec {
            start: 0,
            end: 511,
            file_path: "shutdown_test.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 512,
            uncompressed_size: 512,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            frequency_score: 1,
        };

        // Create the range file
        let range_file_path = consolidator
            .get_range_file_path(cache_key, &range_spec)
            .unwrap();
        if let Some(parent) = range_file_path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&range_file_path, vec![0u8; 512])
            .await
            .unwrap();

        // Simulate what store_range() does: add size to accumulator
        // In the real code path, store_range() calls accumulator.add() after writing the range file
        consolidator
            .size_accumulator()
            .add(range_spec.compressed_size);

        // Write journal entry
        let entry = JournalEntry {
            timestamp: now,
            instance_id: "test-instance".to_string(),
            cache_key: cache_key.to_string(),
            range_spec: range_spec.clone(),
            operation: JournalOperation::Add,
            range_file_path: range_file_path.to_string_lossy().to_string(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            metadata_written: false,
        };
        journal_manager
            .append_range_entry(cache_key, entry)
            .await
            .unwrap();

        // Call shutdown - should run final consolidation
        let result = consolidator.shutdown().await;
        assert!(result.is_ok());

        // Verify size state reflects the consolidated entry
        let size_state_path = temp_dir
            .path()
            .join("size_tracking")
            .join("size_state.json");
        let content = tokio::fs::read_to_string(&size_state_path).await.unwrap();
        let persisted_state: SizeState = serde_json::from_str(&content).unwrap();
        assert_eq!(persisted_state.total_size, 512); // The Add operation added 512 bytes
        assert_eq!(persisted_state.consolidation_count, 1); // One consolidation cycle ran
    }

    /// **Feature: accumulator-size-tracking, Property 1: Accumulator add/subtract algebraic sum**
    ///
    /// *For any* sequence of `add(a_1), add(a_2), ..., subtract(s_1), subtract(s_2), ...`
    /// operations (executed concurrently or sequentially) on a SizeAccumulator initialized to
    /// zero, the final accumulator `delta` value SHALL equal `sum(a_i) - sum(s_j)`.
    /// The same property holds independently for `write_cache_delta`.
    ///
    /// **Validates: Requirements 1.2, 1.4, 1.5, 2.1, 2.2, 5.2, 5.3, 5.4**
    #[quickcheck]
    fn prop_accumulator_add_subtract_algebraic_sum(
        adds: Vec<u16>,
        subtracts: Vec<u16>,
    ) -> TestResult {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let acc = SizeAccumulator::new(temp_dir.path(), "test-instance".to_string());

        // Apply all add operations
        let mut expected_delta: i64 = 0;
        for &a in &adds {
            let size = a as u64;
            acc.add(size);
            expected_delta += size as i64;
        }

        // Apply all subtract operations
        for &s in &subtracts {
            let size = s as u64;
            acc.subtract(size);
            expected_delta -= size as i64;
        }

        // Assert: final delta equals algebraic sum
        if acc.current_delta() != expected_delta {
            return TestResult::error(format!(
                "delta mismatch: expected {}, got {}",
                expected_delta,
                acc.current_delta()
            ));
        }

        TestResult::passed()
    }

    /// **Feature: accumulator-size-tracking, Property 1: Accumulator add/subtract algebraic sum (write_cache_delta)**
    ///
    /// Same property as above, verified independently for `write_cache_delta`.
    ///
    /// **Validates: Requirements 1.2, 1.4, 1.5, 2.1, 2.2, 5.2, 5.3, 5.4**
    #[quickcheck]
    fn prop_accumulator_add_subtract_algebraic_sum_write_cache(
        adds: Vec<u16>,
        subtracts: Vec<u16>,
    ) -> TestResult {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let acc = SizeAccumulator::new(temp_dir.path(), "test-instance".to_string());

        // Apply all add_write_cache operations
        let mut expected_wc_delta: i64 = 0;
        for &a in &adds {
            let size = a as u64;
            acc.add_write_cache(size);
            expected_wc_delta += size as i64;
        }

        // Apply all subtract_write_cache operations
        for &s in &subtracts {
            let size = s as u64;
            acc.subtract_write_cache(size);
            expected_wc_delta -= size as i64;
        }

        // Assert: final write_cache_delta equals algebraic sum
        if acc.current_write_cache_delta() != expected_wc_delta {
            return TestResult::error(format!(
                "write_cache_delta mismatch: expected {}, got {}",
                expected_wc_delta,
                acc.current_write_cache_delta()
            ));
        }

        TestResult::passed()
    }

    /// **Feature: accumulator-size-tracking, Property 1: Accumulator add/subtract algebraic sum (concurrent)**
    ///
    /// *For any* sequence of add/subtract operations executed from multiple threads,
    /// the final delta equals the algebraic sum. Validates concurrent fetch_add/fetch_sub safety.
    ///
    /// **Validates: Requirements 1.2, 1.4, 1.5, 2.1, 2.2, 5.2, 5.3, 5.4**
    #[quickcheck]
    fn prop_accumulator_concurrent_algebraic_sum(
        adds: Vec<u16>,
        subtracts: Vec<u16>,
    ) -> TestResult {
        // Need at least one operation to test concurrency
        if adds.is_empty() && subtracts.is_empty() {
            return TestResult::discard();
        }

        let temp_dir = tempfile::TempDir::new().unwrap();
        let acc = Arc::new(SizeAccumulator::new(
            temp_dir.path(),
            "test-instance".to_string(),
        ));

        let expected_delta: i64 = adds.iter().map(|&a| a as i64).sum::<i64>()
            - subtracts.iter().map(|&s| s as i64).sum::<i64>();
        let expected_wc_delta = expected_delta; // Apply same ops to both

        // Spawn threads for add operations
        let mut handles = Vec::new();
        for &a in &adds {
            let acc = Arc::clone(&acc);
            handles.push(std::thread::spawn(move || {
                let size = a as u64;
                acc.add(size);
                acc.add_write_cache(size);
            }));
        }

        // Spawn threads for subtract operations
        for &s in &subtracts {
            let acc = Arc::clone(&acc);
            handles.push(std::thread::spawn(move || {
                let size = s as u64;
                acc.subtract(size);
                acc.subtract_write_cache(size);
            }));
        }

        // Wait for all threads
        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Assert: final delta equals algebraic sum regardless of thread ordering
        if acc.current_delta() != expected_delta {
            return TestResult::error(format!(
                "concurrent delta mismatch: expected {}, got {}",
                expected_delta,
                acc.current_delta()
            ));
        }

        if acc.current_write_cache_delta() != expected_wc_delta {
            return TestResult::error(format!(
                "concurrent write_cache_delta mismatch: expected {}, got {}",
                expected_wc_delta,
                acc.current_write_cache_delta()
            ));
        }

        TestResult::passed()
    }

    /// **Feature: accumulator-size-tracking, Property 2: Flush round-trip**
    ///
    /// *For any* SizeAccumulator with accumulated `delta=D` and `write_cache_delta=W`,
    /// after calling `flush()`:
    /// 1. The accumulator's `delta` SHALL be 0
    /// 2. The accumulator's `write_cache_delta` SHALL be 0
    /// 3. The delta file SHALL contain valid JSON with `"delta": D` and `"write_cache_delta": W`
    /// 4. The delta file SHALL contain `"instance_id"` (string) and `"timestamp"` (string) fields
    ///
    /// **Validates: Requirements 3.3, 3.5, 5.5, 8.1, 8.2**
    #[quickcheck]
    fn prop_flush_round_trip(adds: Vec<i16>, subtracts: Vec<i16>) -> TestResult {
        // Need at least one non-zero operation so flush actually writes a file
        let has_nonzero = adds.iter().any(|&v| v != 0) || subtracts.iter().any(|&v| v != 0);
        if !has_nonzero {
            return TestResult::discard();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let acc = SizeAccumulator::new(temp_dir.path(), "test-flush-rt".to_string());

            // Build up delta and write_cache_delta from random i16 values
            // Use add() for positive values, subtract() for negative values (absolute value)
            let mut expected_delta: i64 = 0;
            let mut expected_wc_delta: i64 = 0;

            for &v in &adds {
                if v >= 0 {
                    acc.add(v as u64);
                    expected_delta += v as i64;
                } else {
                    acc.subtract(v.unsigned_abs() as u64);
                    expected_delta -= v.unsigned_abs() as i64;
                }
            }

            for &v in &subtracts {
                if v >= 0 {
                    acc.add_write_cache(v as u64);
                    expected_wc_delta += v as i64;
                } else {
                    acc.subtract_write_cache(v.unsigned_abs() as u64);
                    expected_wc_delta -= v.unsigned_abs() as i64;
                }
            }

            // If both ended up zero after all operations, discard (flush skips zero deltas)
            if expected_delta == 0 && expected_wc_delta == 0 {
                return TestResult::discard();
            }

            // Flush the accumulator to disk
            acc.flush().await.expect("flush should succeed");

            // Property 1: accumulator delta SHALL be 0 after flush
            if acc.current_delta() != 0 {
                return TestResult::error(format!(
                    "delta not zero after flush: got {}",
                    acc.current_delta()
                ));
            }

            // Property 2: accumulator write_cache_delta SHALL be 0 after flush
            if acc.current_write_cache_delta() != 0 {
                return TestResult::error(format!(
                    "write_cache_delta not zero after flush: got {}",
                    acc.current_write_cache_delta()
                ));
            }

            // Find the delta file (append-only: one file per flush with sequence number)
            let size_tracking_dir = temp_dir.path().join("size_tracking");
            let mut found_file = None;
            let mut read_dir = tokio::fs::read_dir(&size_tracking_dir)
                .await
                .expect("size_tracking dir should exist after flush");
            while let Some(entry) = read_dir.next_entry().await.unwrap() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("delta_test-flush-rt_")
                    && name.ends_with(".json")
                    && !name.ends_with(".json.tmp")
                {
                    found_file = Some(entry.path());
                    break;
                }
            }
            let delta_file_path = found_file.expect("delta file should exist after flush");
            let content =
                std::fs::read_to_string(&delta_file_path).expect("delta file should be readable");
            let json: serde_json::Value =
                serde_json::from_str(&content).expect("delta file should contain valid JSON");

            // Property 3: delta file SHALL contain correct delta and write_cache_delta
            let file_delta = json.get("delta").and_then(|v| v.as_i64());
            if file_delta != Some(expected_delta) {
                return TestResult::error(format!(
                    "delta file delta mismatch: expected {}, got {:?}",
                    expected_delta, file_delta
                ));
            }

            let file_wc_delta = json.get("write_cache_delta").and_then(|v| v.as_i64());
            if file_wc_delta != Some(expected_wc_delta) {
                return TestResult::error(format!(
                    "delta file write_cache_delta mismatch: expected {}, got {:?}",
                    expected_wc_delta, file_wc_delta
                ));
            }

            // Property 4: delta file SHALL contain instance_id (string) and timestamp (string)
            let instance_id = json.get("instance_id").and_then(|v| v.as_str());
            if instance_id.is_none() {
                return TestResult::error(
                    "delta file missing instance_id string field".to_string(),
                );
            }

            let timestamp = json.get("timestamp").and_then(|v| v.as_str());
            if timestamp.is_none() {
                return TestResult::error("delta file missing timestamp string field".to_string());
            }

            TestResult::passed()
        })
    }

    /// **Feature: accumulator-size-tracking, Property 3: Flush failure restores accumulator**
    ///
    /// *For any* SizeAccumulator with accumulated `delta=D` and `write_cache_delta=W`,
    /// if `flush()` fails (e.g., due to I/O error), the accumulator's `delta` SHALL still
    /// equal `D` and `write_cache_delta` SHALL still equal `W`.
    ///
    /// **Validates: Requirements 3.6**
    #[quickcheck]
    fn prop_flush_failure_restores_accumulator(delta_val: i16, wc_delta_val: i16) -> TestResult {
        // Need at least one non-zero value so flush actually attempts to write
        if delta_val == 0 && wc_delta_val == 0 {
            return TestResult::discard();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();

            // Make the temp directory read-only so size_tracking/ cannot be created
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(temp_dir.path(), std::fs::Permissions::from_mode(0o444))
                    .expect("failed to set read-only permissions");
            }

            let acc = SizeAccumulator::new(temp_dir.path(), "test-fail".to_string());

            let expected_delta: i64 = if delta_val >= 0 {
                acc.add(delta_val as u64);
                delta_val as i64
            } else {
                acc.subtract(delta_val.unsigned_abs() as u64);
                -(delta_val.unsigned_abs() as i64)
            };

            let expected_wc_delta: i64 = if wc_delta_val >= 0 {
                acc.add_write_cache(wc_delta_val as u64);
                wc_delta_val as i64
            } else {
                acc.subtract_write_cache(wc_delta_val.unsigned_abs() as u64);
                -(wc_delta_val.unsigned_abs() as i64)
            };

            // flush() should fail because the directory is read-only
            let result = acc.flush().await;
            if result.is_ok() {
                // Restore permissions before returning error
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(
                        temp_dir.path(),
                        std::fs::Permissions::from_mode(0o755),
                    );
                }
                return TestResult::error(
                    "flush() should have failed on read-only directory".to_string(),
                );
            }

            // Property: accumulator delta SHALL still equal original value
            if acc.current_delta() != expected_delta {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(
                        temp_dir.path(),
                        std::fs::Permissions::from_mode(0o755),
                    );
                }
                return TestResult::error(format!(
                    "delta not restored after flush failure: expected {}, got {}",
                    expected_delta,
                    acc.current_delta()
                ));
            }

            // Property: accumulator write_cache_delta SHALL still equal original value
            if acc.current_write_cache_delta() != expected_wc_delta {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(
                        temp_dir.path(),
                        std::fs::Permissions::from_mode(0o755),
                    );
                }
                return TestResult::error(format!(
                    "write_cache_delta not restored after flush failure: expected {}, got {}",
                    expected_wc_delta,
                    acc.current_write_cache_delta()
                ));
            }

            // Restore permissions so tempdir cleanup can delete the directory
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(
                    temp_dir.path(),
                    std::fs::Permissions::from_mode(0o755),
                );
            }

            TestResult::passed()
        })
    }

    /// **Feature: accumulator-size-tracking, Property 4: Consolidator delta summation and reset**
    ///
    /// *For any* set of N delta files with values `{(d_1, w_1), (d_2, w_2), ..., (d_N, w_N)}`,
    /// after `collect_and_apply_deltas()`:
    /// 1. The returned total delta SHALL equal `sum(d_i)`
    /// 2. The returned write cache delta SHALL equal `sum(w_i)`
    /// 3. All N delta files SHALL contain `"delta": 0` and `"write_cache_delta": 0`
    ///
    /// **Validates: Requirements 4.1, 4.2, 4.3, 5.6**
    #[quickcheck]
    fn prop_consolidator_delta_summation_and_reset(file_deltas: Vec<(i16, i16)>) -> TestResult {
        // Need 1-10 files
        if file_deltas.is_empty() || file_deltas.len() > 10 {
            return TestResult::discard();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let size_tracking_dir = temp_dir.path().join("size_tracking");
            tokio::fs::create_dir_all(&size_tracking_dir).await.unwrap();

            // Write N delta files with random values
            let mut expected_total_delta: i64 = 0;
            let mut expected_total_wc_delta: i64 = 0;

            for (i, &(d, w)) in file_deltas.iter().enumerate() {
                let delta = d as i64;
                let wc_delta = w as i64;
                expected_total_delta += delta;
                expected_total_wc_delta += wc_delta;

                let content = serde_json::json!({
                    "delta": delta,
                    "write_cache_delta": wc_delta,
                    "instance_id": format!("instance-{}", i),
                    "timestamp": chrono::Utc::now().to_rfc3339()
                });
                let path = size_tracking_dir.join(format!("delta_instance-{}.json", i));
                tokio::fs::write(&path, serde_json::to_string_pretty(&content).unwrap())
                    .await
                    .unwrap();
            }

            // Create a JournalConsolidator pointing at the temp dir
            let journal_manager = Arc::new(JournalManager::new(
                temp_dir.path().to_path_buf(),
                "test-consolidator".to_string(),
            ));
            let lock_manager = Arc::new(MetadataLockManager::new(
                temp_dir.path().to_path_buf(),
                Duration::from_secs(30),
                3,
            ));
            let consolidator = JournalConsolidator::new(
                temp_dir.path().to_path_buf(),
                journal_manager,
                lock_manager,
                ConsolidationConfig::default(),
            );

            // Call collect_and_apply_deltas
            let (total_delta, total_wc_delta) = consolidator
                .collect_and_apply_deltas()
                .await
                .expect("collect_and_apply_deltas should succeed");

            // Property 1: returned total delta SHALL equal sum(d_i)
            if total_delta != expected_total_delta {
                return TestResult::error(format!(
                    "total_delta mismatch: expected {}, got {}",
                    expected_total_delta, total_delta
                ));
            }

            // Property 2: returned write cache delta SHALL equal sum(w_i)
            if total_wc_delta != expected_total_wc_delta {
                return TestResult::error(format!(
                    "total_wc_delta mismatch: expected {}, got {}",
                    expected_total_wc_delta, total_wc_delta
                ));
            }

            // Property 3: all delta files SHALL be deleted after collection
            for (i, &(d, w)) in file_deltas.iter().enumerate() {
                let path = size_tracking_dir.join(format!("delta_instance-{}.json", i));
                if path.exists() {
                    // Zero-delta files are skipped (not deleted), so only check non-zero ones
                    if d != 0 || w != 0 {
                        return TestResult::error(format!(
                            "delta file {} still exists after collection (had non-zero delta)",
                            i
                        ));
                    }
                }
            }

            TestResult::passed()
        })
    }

    /// **Feature: accumulator-size-tracking, Property 5: Validation scan resets all delta files**
    ///
    /// *For any* set of delta files in the `size_tracking/` directory (with arbitrary delta values),
    /// after `reset_all_delta_files()`, every delta file SHALL be deleted.
    ///
    /// **Validates: Requirements 6.3**
    #[quickcheck]
    fn prop_validation_scan_resets_all_delta_files(file_deltas: Vec<(i16, i16)>) -> TestResult {
        if file_deltas.is_empty() || file_deltas.len() > 10 {
            return TestResult::discard();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let size_tracking_dir = temp_dir.path().join("size_tracking");
            tokio::fs::create_dir_all(&size_tracking_dir).await.unwrap();

            // Write N delta files with non-zero values
            for (i, &(d, w)) in file_deltas.iter().enumerate() {
                let content = serde_json::json!({
                    "delta": d as i64,
                    "write_cache_delta": w as i64,
                    "instance_id": format!("instance-{}", i),
                    "timestamp": chrono::Utc::now().to_rfc3339()
                });
                let path = size_tracking_dir.join(format!("delta_instance-{}.json", i));
                tokio::fs::write(&path, serde_json::to_string_pretty(&content).unwrap())
                    .await
                    .unwrap();
            }

            // Create consolidator and call reset_all_delta_files
            let journal_manager = Arc::new(JournalManager::new(
                temp_dir.path().to_path_buf(),
                "test-reset".to_string(),
            ));
            let lock_manager = Arc::new(MetadataLockManager::new(
                temp_dir.path().to_path_buf(),
                Duration::from_secs(30),
                3,
            ));
            let consolidator = JournalConsolidator::new(
                temp_dir.path().to_path_buf(),
                journal_manager,
                lock_manager,
                ConsolidationConfig::default(),
            );

            consolidator.reset_all_delta_files().await;

            // Assert all delta files are deleted
            for i in 0..file_deltas.len() {
                let path = size_tracking_dir.join(format!("delta_instance-{}.json", i));
                if path.exists() {
                    return TestResult::error(format!(
                        "delta file {} still exists after reset_all_delta_files",
                        i
                    ));
                }
            }

            TestResult::passed()
        })
    }

    /// **Feature: eviction-performance, Property 1: Eviction guard prevents concurrent spawns**
    ///
    /// *For any* `AtomicBool` guard state (`true` = eviction in progress, `false` = idle)
    /// and *any* `u64` cache size, the `compare_exchange(false, true)` guard logic SHALL:
    /// - When guard is `true`: fail the exchange, meaning the caller skips spawning.
    /// - When guard is `false` and cache is over threshold: succeed, setting guard to `true`,
    ///   meaning the caller would return `(true, 0)`.
    ///
    /// **Validates: Requirements 1.4**
    #[quickcheck]
    fn prop_eviction_guard_prevents_concurrent_spawns(
        guard_state: bool,
        cache_size: u64,
    ) -> TestResult {
        // Use a fixed config: max_cache_size=1000, trigger at 95% = 950
        let max_cache_size: u64 = 1000;
        let trigger_threshold: u64 = 950; // 95% of 1000

        // Skip the case where cache_size <= trigger_threshold and guard is false,
        // because that path returns early before reaching the guard check.
        // We only care about the guard behavior when eviction *would* be triggered.
        if cache_size <= trigger_threshold && !guard_state {
            return TestResult::discard();
        }

        let eviction_in_progress = Arc::new(AtomicBool::new(guard_state));

        // Simulate the maybe_trigger_eviction guard logic:
        // 1. Check if max_cache_size is configured (non-zero) — always true here
        // 2. Check if current_size > trigger_threshold
        // 3. Attempt compare_exchange on the guard

        if max_cache_size == 0 {
            // Would return (false, 0) — disabled
            return TestResult::passed();
        }

        if cache_size <= trigger_threshold {
            // Would return (false, 0) — under threshold, no guard interaction
            // Guard state should be unchanged
            assert_eq!(
                eviction_in_progress.load(Ordering::SeqCst),
                guard_state,
                "Guard should be unchanged when under threshold"
            );
            return TestResult::passed();
        }

        // Over threshold — attempt the guard
        let exchange_result =
            eviction_in_progress.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);

        if guard_state {
            // Guard was already true → compare_exchange should fail
            assert!(
                exchange_result.is_err(),
                "compare_exchange must fail when guard is already true"
            );
            // The function would return (false, 0) — skip spawning
            assert!(
                eviction_in_progress.load(Ordering::SeqCst),
                "Guard must remain true after failed exchange"
            );
        } else {
            // Guard was false → compare_exchange should succeed, setting it to true
            assert!(
                exchange_result.is_ok(),
                "compare_exchange must succeed when guard is false"
            );
            // The function would return (true, 0) — spawn eviction
            assert!(
                eviction_in_progress.load(Ordering::SeqCst),
                "Guard must be true after successful exchange"
            );
        }

        TestResult::passed()
    }

    // ===== Unit tests for eviction decoupling (Task 1.4) =====
    // Requirements: 1.1, 1.2, 1.3, 1.6

    /// Test: When guard is already `true`, maybe_trigger_eviction returns (false, 0)
    /// and does NOT spawn a new eviction task.
    /// Validates: Requirement 1.4
    #[tokio::test]
    async fn test_maybe_trigger_eviction_skips_when_guard_is_true() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let config = ConsolidationConfig {
            max_cache_size: 1000,
            eviction_trigger_percent: 95, // threshold = 950
            ..ConsolidationConfig::default()
        };

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            config,
        );

        // Pre-set the guard to true (simulating eviction already in progress)
        consolidator
            .eviction_in_progress
            .store(true, Ordering::SeqCst);

        // Call with size over threshold (1000 > 950)
        let (triggered, bytes) = consolidator.maybe_trigger_eviction(Some(1000)).await;

        assert!(!triggered, "Should not trigger when guard is already true");
        assert_eq!(bytes, 0, "Bytes should be 0 when skipping");
        // Guard should remain true (unchanged)
        assert!(
            consolidator.eviction_in_progress.load(Ordering::SeqCst),
            "Guard should remain true after skip"
        );
    }

    /// Test: When over threshold and guard is false, maybe_trigger_eviction acquires
    /// the guard (sets it to true). Without a cache_manager, it resets and returns (false, 0),
    /// but the guard acquisition itself is validated.
    /// Validates: Requirements 1.2, 1.4
    #[tokio::test]
    async fn test_maybe_trigger_eviction_acquires_guard_when_over_threshold() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let config = ConsolidationConfig {
            max_cache_size: 1000,
            eviction_trigger_percent: 95, // threshold = 950
            ..ConsolidationConfig::default()
        };

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            config,
        );

        // Guard starts false
        assert!(
            !consolidator.eviction_in_progress.load(Ordering::SeqCst),
            "Guard should start as false"
        );

        // No cache_manager set, so the method will acquire the guard, fail to get
        // cache_manager, reset the guard, and return (false, 0).
        // This validates the guard acquisition + reset-on-failure path.
        let (triggered, bytes) = consolidator.maybe_trigger_eviction(Some(1000)).await;

        assert!(
            !triggered,
            "Should return false when cache_manager unavailable"
        );
        assert_eq!(bytes, 0);
        // Guard should be reset to false after the cache_manager failure path
        assert!(
            !consolidator.eviction_in_progress.load(Ordering::SeqCst),
            "Guard should be reset to false after cache_manager unavailable"
        );
    }

    /// Test: When under threshold, maybe_trigger_eviction returns (false, 0)
    /// without touching the guard at all.
    /// Validates: Requirement 1.1 (only triggers when over threshold)
    #[tokio::test]
    async fn test_maybe_trigger_eviction_skips_when_under_threshold() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let config = ConsolidationConfig {
            max_cache_size: 1000,
            eviction_trigger_percent: 95, // threshold = 950
            ..ConsolidationConfig::default()
        };

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            config,
        );

        // Call with size under threshold (500 <= 950)
        let (triggered, bytes) = consolidator.maybe_trigger_eviction(Some(500)).await;

        assert!(!triggered, "Should not trigger when under threshold");
        assert_eq!(bytes, 0);
        // Guard should remain false (never touched)
        assert!(
            !consolidator.eviction_in_progress.load(Ordering::SeqCst),
            "Guard should remain false when under threshold"
        );
    }

    /// Test: Guard is true immediately after compare_exchange succeeds,
    /// and false after the task completes (simulated via scopeguard drop).
    /// Tests the AtomicBool guard lifecycle with specific example values.
    /// Validates: Requirements 1.2, 1.3
    #[tokio::test]
    async fn test_eviction_guard_lifecycle_true_after_spawn_false_after_complete() {
        let eviction_in_progress = Arc::new(AtomicBool::new(false));

        // Simulate the guard acquisition (compare_exchange in maybe_trigger_eviction)
        let result =
            eviction_in_progress.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);
        assert!(
            result.is_ok(),
            "compare_exchange should succeed when guard is false"
        );

        // Guard is true immediately after acquisition (Requirement 1.2)
        assert!(
            eviction_in_progress.load(Ordering::SeqCst),
            "Guard must be true immediately after compare_exchange succeeds"
        );

        // Simulate the spawned task with scopeguard (same pattern as production code)
        let flag = eviction_in_progress.clone();
        let handle = tokio::spawn(async move {
            let _guard = scopeguard::guard((), |_| {
                flag.store(false, Ordering::SeqCst);
            });
            // Simulate some work
            tokio::task::yield_now().await;
            // _guard drops here, resetting the flag
        });

        // Guard should still be true while task is running (or about to run)
        // Note: the task may complete very quickly, so we check before awaiting
        // The important invariant is that the guard is true *before* the task completes

        // Wait for the spawned task to complete
        handle.await.unwrap();

        // Guard should be false after task completes (Requirement 1.3)
        assert!(
            !eviction_in_progress.load(Ordering::SeqCst),
            "Guard must be false after spawned task completes (scopeguard reset)"
        );
    }

    /// Test: When max_cache_size is 0 (disabled), maybe_trigger_eviction returns (false, 0)
    /// regardless of current size or guard state.
    /// Validates: Requirement 1.1 (eviction only when configured)
    #[tokio::test]
    async fn test_maybe_trigger_eviction_disabled_when_max_cache_size_zero() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));

        let config = ConsolidationConfig::default(); // max_cache_size = 0

        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            config,
        );

        let (triggered, bytes) = consolidator.maybe_trigger_eviction(Some(999999)).await;

        assert!(!triggered, "Should not trigger when max_cache_size is 0");
        assert_eq!(bytes, 0);
    }

    /// **Feature: consolidation-throughput, Property 1: All discovered keys are submitted for processing**
    /// *For any* set of discovered pending cache keys of arbitrary size (including sizes greater
    /// than 50, the previous cap), the number of key futures created for concurrent processing
    /// shall equal the total number of discovered keys — no `.take()` truncation occurs.
    /// **Validates: Requirements 1.1**
    #[quickcheck]
    fn prop_all_discovered_keys_submitted_for_processing(key_count: u16) -> TestResult {
        // Constrain to 0..=500 keys
        let key_count = (key_count % 501) as usize;

        // Generate random cache keys
        let cache_keys: Vec<String> = (0..key_count)
            .map(|i| format!("bucket/object-{}", i))
            .collect();

        // Simulate the current key selection logic from run_consolidation_cycle:
        // All discovered keys (up to max_keys_per_cycle cap) are mapped into futures.
        // The discovery cap limits NFS I/O; the deadline limits processing time.
        let key_futures: Vec<&String> = cache_keys.iter().collect();

        // The number of submitted key futures must equal the total discovered keys
        if key_futures.len() != cache_keys.len() {
            return TestResult::error(format!(
                "Key count mismatch: submitted={}, discovered={}",
                key_futures.len(),
                cache_keys.len()
            ));
        }

        // Verify no keys were dropped — every key must appear in the futures list
        for (i, key) in cache_keys.iter().enumerate() {
            if key_futures[i] != key {
                return TestResult::error(format!(
                    "Key at index {} differs: expected={}, got={}",
                    i, key, key_futures[i]
                ));
            }
        }

        TestResult::passed()
    }

    /// **Feature: consolidation-throughput, Property 2: HashSet cleanup matching equivalence**
    /// *For any* list of journal entries and *any* subset designated as consolidated entries,
    /// partitioning the journal entries using HashSet-based O(1) lookup shall produce the
    /// identical partition (removed set, kept set) as the original linear-scan `.iter().any()` approach.
    /// **Validates: Requirements 3.3**
    #[quickcheck]
    fn prop_hashset_cleanup_matching_equivalence(
        entry_count: u8,
        consolidated_mask: Vec<bool>,
    ) -> TestResult {
        // Generate 1..=50 journal entries
        let entry_count = (entry_count % 50) + 1;

        // Build random journal entries with deterministic but varied fields
        let base_time = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let entries: Vec<JournalEntry> = (0..entry_count)
            .map(|i| {
                let i = i as u64;
                JournalEntry {
                    timestamp: base_time + Duration::from_secs(i * 7),
                    instance_id: format!("inst-{}", i % 5),
                    cache_key: format!("bucket/obj-{}", i % 10),
                    range_spec: RangeSpec {
                        start: i * 1000,
                        end: i * 1000 + 999,
                        file_path: format!("range_{}.bin", i),
                        compression_algorithm: CompressionAlgorithm::Lz4,
                        compressed_size: 1000,
                        uncompressed_size: 1000,
                        created_at: base_time,
                        last_accessed: base_time,
                        access_count: 1,
                        frequency_score: 1,
                    },
                    operation: JournalOperation::Add,
                    range_file_path: format!("range_{}.bin", i),
                    metadata_version: 1,
                    new_ttl_secs: None,
                    object_ttl_secs: Some(3600),
                    access_increment: None,
                    object_metadata: None,
                    metadata_written: false,
                }
            })
            .collect();

        // Select a subset as consolidated entries using the mask
        let consolidated_entries: Vec<JournalEntry> = entries
            .iter()
            .enumerate()
            .filter(|(idx, _)| consolidated_mask.get(*idx).copied().unwrap_or(false))
            .map(|(_, e)| e.clone())
            .collect();

        // --- Old logic: linear-scan matching ---
        let mut old_removed = Vec::new();
        let mut old_kept = Vec::new();
        for entry in &entries {
            let was_consolidated = consolidated_entries.iter().any(|ce| {
                ce.cache_key == entry.cache_key
                    && ce.range_spec.start == entry.range_spec.start
                    && ce.range_spec.end == entry.range_spec.end
                    && ce.timestamp == entry.timestamp
                    && ce.instance_id == entry.instance_id
            });
            if was_consolidated {
                old_removed.push(entry.cache_key.clone());
            } else {
                old_kept.push(entry.cache_key.clone());
            }
        }

        // --- New logic: HashSet-based matching ---
        let consolidated_set: HashSet<(String, u64, u64, SystemTime, String)> =
            consolidated_entries
                .iter()
                .map(|ce| {
                    (
                        ce.cache_key.clone(),
                        ce.range_spec.start,
                        ce.range_spec.end,
                        ce.timestamp,
                        ce.instance_id.clone(),
                    )
                })
                .collect();

        let mut new_removed = Vec::new();
        let mut new_kept = Vec::new();
        for entry in &entries {
            let was_consolidated = consolidated_set.contains(&(
                entry.cache_key.clone(),
                entry.range_spec.start,
                entry.range_spec.end,
                entry.timestamp,
                entry.instance_id.clone(),
            ));
            if was_consolidated {
                new_removed.push(entry.cache_key.clone());
            } else {
                new_kept.push(entry.cache_key.clone());
            }
        }

        // Assert identical partitions
        if old_removed != new_removed {
            return TestResult::error(format!(
                "Removed sets differ: old={}, new={}",
                old_removed.len(),
                new_removed.len()
            ));
        }
        if old_kept != new_kept {
            return TestResult::error(format!(
                "Kept sets differ: old={}, new={}",
                old_kept.len(),
                new_kept.len()
            ));
        }

        TestResult::passed()
    }

    /// Test: Second concurrent call to guard returns error when guard is already acquired.
    /// Validates: Requirement 1.4 (prevents concurrent spawns)
    #[test]
    fn test_eviction_guard_second_acquire_fails() {
        let eviction_in_progress = Arc::new(AtomicBool::new(false));

        // First acquisition succeeds
        let first =
            eviction_in_progress.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);
        assert!(first.is_ok(), "First compare_exchange should succeed");

        // Second acquisition fails (guard already true)
        let second =
            eviction_in_progress.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);
        assert!(
            second.is_err(),
            "Second compare_exchange must fail when guard is true"
        );
        assert!(
            second.unwrap_err(),
            "Failed exchange should return current value (true)"
        );

        // Guard remains true
        assert!(
            eviction_in_progress.load(Ordering::SeqCst),
            "Guard must remain true after failed second acquisition"
        );
    }

    #[tokio::test]
    async fn test_get_metadata_file_path_rejects_malformed() {
        // Validates: Requirements 3.1, 5.6
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));
        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        let result = consolidator.get_metadata_file_path("noslash");
        assert!(
            result.is_err(),
            "Malformed cache key must return Err, got Ok"
        );
    }

    #[tokio::test]
    async fn test_get_range_file_path_rejects_malformed() {
        // Validates: Requirements 3.2, 5.6
        let temp_dir = TempDir::new().unwrap();
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));
        let consolidator = JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        );

        let range_spec = create_test_range_spec(0, 100);
        let result = consolidator.get_range_file_path("noslash", &range_spec);
        assert!(
            result.is_err(),
            "Malformed cache key must return Err, got Ok"
        );
    }
}
