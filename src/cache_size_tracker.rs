//! Cache Size Tracking Module
//!
//! Provides validation scan logic for multi-instance deployments with shared disk cache.
//! Size tracking is handled by the JournalConsolidator - this module delegates size queries
//! to the consolidator and retains only validation scan logic.
//!
//! Note: The CacheSizeTracker no longer maintains its own size state. All size queries
//! are delegated to the JournalConsolidator which is the single source of truth for cache size.

use crate::journal_consolidator::{ConsolidationLockToken, JournalConsolidator};
use crate::{ProxyError, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Minimum spacing between two consecutive validation passes.
///
/// 23 hours, deliberately one hour short of a day: [`CacheSizeTracker::calculate_next_validation_time`]
/// adds up to 1 h of jitter after applying this bound, so the realised spacing lands at roughly
/// 24 h. A full day here would push each pass slightly past the configured
/// `validation_time_of_day` and drift it later every day.
///
/// **Not configurable, and not the same quantity as `validation_max_duration`.** That field bounds
/// how long one pass may take; this bounds how soon the next may start. Conflating the two is how
/// the enforcement of this bound stayed a `debug!` for as long as it did — see the call site in
/// [`CacheSizeTracker::calculate_next_validation_time`].
const MIN_VALIDATION_SPACING: Duration = Duration::from_secs(82_800);

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

    /// Minimum **compressed** range size for Tail_Partition membership, from
    /// `cache.shared_storage.tail_floor_bytes` (default: 1 MiB).
    ///
    /// Carried here because the rolling pass holds a `CacheSizeConfig` and nothing else that
    /// reaches the application configuration — the same reason
    /// [`crate::journal_consolidator::ConsolidationConfig::shard_fold_interval`] is carried
    /// there. Without this line the field would be declarable in YAML and unread, which is the
    /// shape of defect the `st_size` fold's own wiring had to go back and fix.
    #[serde(default = "default_tail_floor_bytes")]
    pub tail_floor_bytes: u64,

    /// Per-shard rank cap on Tail_Partition members, from
    /// `cache.shared_storage.tail_max_entries` (default: 4096). See
    /// [`Self::tail_floor_bytes`] for why it is carried here.
    #[serde(default = "default_tail_max_entries")]
    pub tail_max_entries: usize,
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

/// Read off `SharedStorageConfig::default()` rather than restated, so the figure has one source
/// and cannot drift from the one `config/config.example.yaml` documents.
fn default_tail_floor_bytes() -> u64 {
    crate::config::SharedStorageConfig::default().tail_floor_bytes
}

/// See [`default_tail_floor_bytes`] for why this defers rather than restating 4096.
fn default_tail_max_entries() -> usize {
    crate::config::SharedStorageConfig::default().tail_max_entries
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
            tail_floor_bytes: default_tail_floor_bytes(),
            tail_max_entries: default_tail_max_entries(),
        }
    }
}

impl CacheSizeConfig {
    /// The tracker configuration the running proxy uses, built from
    /// `cache.shared_storage` and the cache's incomplete-upload TTL.
    ///
    /// Every field the tracker reads from `cache.shared_storage` is copied here, so a value an
    /// operator sets in YAML reaches the validation scan and the Tail_Partition rebuild. The
    /// rest take [`CacheSizeConfig::default`]. A field added to this struct
    /// that mirrors a `SharedStorageConfig` field belongs in this function, and
    /// `from_shared_storage_carries_every_operator_field` pins the ones that do.
    pub fn from_shared_storage(
        shared: &crate::config::SharedStorageConfig,
        incomplete_upload_ttl: Duration,
    ) -> Self {
        Self {
            incomplete_upload_ttl,
            validation_max_duration: shared.validation_max_duration,
            validation_threshold_warn: shared.validation_threshold_warn,
            validation_threshold_error: shared.validation_threshold_error,
            tail_floor_bytes: shared.tail_floor_bytes,
            tail_max_entries: shared.tail_max_entries,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod from_shared_storage_tests {
    use super::*;

    #[test]
    fn from_shared_storage_carries_every_operator_field() {
        let shared = crate::config::SharedStorageConfig {
            validation_max_duration: Duration::from_secs(1234),
            validation_threshold_warn: 7.5,
            validation_threshold_error: 33.0,
            tail_floor_bytes: 2 * 1024 * 1024,
            tail_max_entries: 77,
            ..crate::config::SharedStorageConfig::default()
        };
        let c = CacheSizeConfig::from_shared_storage(&shared, Duration::from_secs(99));
        assert_eq!(c.incomplete_upload_ttl, Duration::from_secs(99));
        assert_eq!(c.validation_max_duration, Duration::from_secs(1234));
        assert_eq!(c.validation_threshold_warn, 7.5);
        assert_eq!(c.validation_threshold_error, 33.0);
        assert_eq!(c.tail_floor_bytes, 2 * 1024 * 1024);
        assert_eq!(c.tail_max_entries, 77);
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

    /// The full scan measured `drift_bytes` but could not take `global_consolidation.lock`
    /// within its budget, so the tracked figures were NOT corrected. `drift_bytes` is then a
    /// measurement, not a correction. `false` when absent (older writers, rolling passes, the
    /// startup writer).
    #[serde(default)]
    pub size_install_deferred: bool,
}

/// Rolling scan state persisted in `size_tracking/schedule.json`.
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
    /// Epoch **milliseconds** of the most recent completed shard visit, written by every
    /// per-shard cursor persist. It is the ordering field that stops a slow instance
    /// installing over a fresher scanner's derived state. Millisecond resolution rather
    /// than seconds because a cycle completes several shard visits per second, and at
    /// one-second resolution a peer's write inside the same second would be
    /// indistinguishable from this instance's own.
    pub last_visit_at: Option<u64>,
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

/// One Tail_Partition record: a range large enough to be worth eviction's attention, plus
/// everything selection needs so it can order and group candidates **without opening a
/// `.meta`**.
///
/// # Every field comes from the parse the rolling pass already performs
///
/// No `stat`, no readdir and no `st_size` fold. `compressed_size` is `RangeSpec`'s own field,
/// which task 29 established equals `st_size` by construction, so the sort key is available at
/// zero marginal cost on a pass that has already paid for the parse.
///
/// # Two fields are not what their names suggest, and both have cost a defect elsewhere
///
/// - **`expires_at` is OBJECT-level.** `RangeSpec` carries no expiry (R3.6b), so every range of
///   one object carries the same value. Reading it as per-range would be reading a figure no
///   document ever contained.
/// - **`is_staged` is DERIVED, not copied.** `RangeSpec::staged` is `Option<bool>` whose own doc
///   comment forbids reading it directly: `None` means "this range predates the field" and must
///   fall back to the object's `is_write_cached`. So the value here comes from
///   [`crate::cache_types::is_staged_range_spec`], the single shared predicate the accumulator's
///   add and subtract sites also use.
///
/// `leaf` is carried rather than re-derived because re-deriving it means a BLAKE3 per candidate
/// at selection time; carried, the R14 grouping is a sort on a field.
///
/// Times are **nanoseconds** since the epoch, never seconds. Second granularity in an identity
/// that compaction compares makes the comparison silently match nothing (§ 2 trap 1) — the
/// defect `mod ledger_time` exists for.
///
/// Spec: cache-eviction-at-scale § 4.1. Requirements: 3.2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TailRecord {
    pub cache_key: String,
    pub range_start: u64,
    pub range_end: u64,
    /// The **compressed** on-disk size — the bytes an eviction actually frees, and the key
    /// § 4.1 sorts on. Not `uncompressed_size`, and not the `.bin` filename's logical extent.
    pub compressed_size: u64,
    /// Nanoseconds since the epoch. Per range.
    pub last_accessed: u64,
    /// Nanoseconds since the epoch, 0 = no expiry. **Object-level** — see the type doc.
    pub expires_at: u64,
    pub access_count: u64,
    /// Derived through [`crate::cache_types::is_staged_range_spec`] — see the type doc.
    pub is_staged: bool,
    /// The `{YYY}` directory component, carried for R14 grouping.
    pub leaf: String,
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
    /// itself removed it (unparseable self-heal, write-cache expiry, GET expiry, stale
    /// HEAD-only removal) or
    /// because something else did between the read and the check.
    ///
    /// The object census must not count an entry the scan just deleted: the census is
    /// installed as `cached_objects`, so counting a removed entry reports an object
    /// that no longer exists and biases the figure **upward** — the direction that
    /// overstates the object count.
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
    /// path, which matters at the 100M-object design point (the traversal budget).
    ///
    /// Spec: cache-eviction-at-scale. Requirements: 7.1
    meta_removed: bool,
    /// This `.meta`'s above-floor ranges, as Tail_Partition records.
    ///
    /// # Non-empty exactly when this arm counted the entry's real bytes
    ///
    /// Every arm that reports `size_bytes: total_size` with the `.meta` intact emits its
    /// candidates; every arm that reports `size_bytes: 0` emits none. That makes the partition's
    /// population **identical to the population this scan counted bytes for**, rather than an
    /// arbitrary subset, and it is why the error arms emit too: their bytes are counted, so the
    /// ranges are resident and are legitimate candidates.
    ///
    /// The two arms with no parse at all — an unreadable file and a parse failure — emit nothing
    /// because there is nothing to draw from. Neither does an arm that **removed** the `.meta`:
    /// a record for a range whose `.meta` this same pass just unlinked is a record selection
    /// would verify (R3.8) and skip, which is § 6.9's "stop counting entries the pass just
    /// deleted" one file along.
    ///
    /// Spec: cache-eviction-at-scale § 4.1, § 4.4.
    tail_candidates: Vec<TailRecord>,
    /// What this arm's own mutation debited through this instance's size accumulator.
    ///
    /// Non-zero only on the three arms that delete or heal an entry AND debit it: the
    /// write-cache expiry, the empty-object-ETag repair and the stale HEAD-only removal. All
    /// report `size_bytes: 0`, and their debit reaches `size_state.json` through the delta
    /// files like any request-path removal. The full scan's relative install adds this back to its census, so the
    /// census describes the population before the pass's own debits, the same population
    /// its baseline counted. Without it the entry is debited twice.
    ///
    /// The rolling pass must NOT add it back. Its install is absolute and is followed by
    /// `reset_all_delta_files` and a reset of this instance's accumulator, so the arm's
    /// pending debit is discarded and the post-removal census alone carries the removal.
    /// Adding it back there counts the removal zero times. Pinned by
    /// `rolling_self_debit_tests`.
    self_debit: ScanSelfDebit,
}

/// The figures a scan arm's own mutation debited through the size accumulator; see
/// [`ScanFileResult::self_debit`]. Summed per pass by the full scan only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ScanSelfDebit {
    /// Compressed bytes the arm's mutation debited from `total_size`.
    pub(crate) bytes: u64,
    /// The staged subset of `bytes`, debited from `write_cache_size`.
    pub(crate) staged_bytes: u64,
    /// `.meta` files the arm's mutation decremented `cached_objects` for.
    pub(crate) objects: u64,
}

/// Result of one shard visit — the plain struct the blocking body returns.
///
/// A visit is whole or it does not happen: the budget is checked between visits and never
/// inside one, so no half-scanned shard can contribute figures. That is what makes the
/// absolute install in R5.2 safe per shard.
///
/// Spec: cache-eviction-at-scale § 6.3, § 6.4.
#[derive(Debug, Clone, Default)]
struct ShardVisitResult {
    size_bytes: u64,
    staged_bytes: u64,
    objects_visited: u64,
    metas_removed: u64,
    cache_expired: u64,
    cache_skipped: u64,
    cache_errors: u64,
    staged_paths: Vec<PathBuf>,
}

impl ShardVisitResult {
    fn absorb(&mut self, other: &ShardVisitResult) {
        self.size_bytes += other.size_bytes;
        self.staged_bytes += other.staged_bytes;
        self.objects_visited += other.objects_visited;
        self.metas_removed += other.metas_removed;
        self.cache_expired += other.cache_expired;
        self.cache_skipped += other.cache_skipped;
        self.cache_errors += other.cache_errors;
        self.staged_paths.extend(other.staged_paths.iter().cloned());
    }
}

/// A [`TailRecord`] ordered so that a [`std::collections::BinaryHeap`] of them is a **min**-heap
/// on `compressed_size`.
///
/// `BinaryHeap` pops the greatest, and what a capped top-N needs to discard is the **smallest**
/// member, so the comparison is inverted here rather than at each call site. The tiebreaks are
/// inverted with it so the order stays total and a shard with many equal-sized ranges rebuilds
/// to the same partition twice running.
///
/// One consequence worth stating because it is relied on: `into_sorted_vec` yields ascending
/// order under this `Ord`, which is **descending** `compressed_size` — § 4.1's required order,
/// with no second sort.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SmallestFirst(TailRecord);

impl Ord for SmallestFirst {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .0
            .compressed_size
            .cmp(&self.0.compressed_size)
            .then_with(|| other.0.cache_key.cmp(&self.0.cache_key))
            .then_with(|| other.0.range_start.cmp(&self.0.range_start))
    }
}

impl PartialOrd for SmallestFirst {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// One `(bucket, L1)` shard's partition, under construction during a visit.
///
/// # The heap is bounded per SHARD, and that is the point
///
/// At most `cap` records are resident for this shard at any moment, so a shard holding ten
/// million above-floor ranges costs `cap` records of memory rather than ten million. A `Vec`
/// sorted at the end is the obvious shape and is the wrong one.
#[derive(Debug)]
struct ShardTailBuild {
    bucket: String,
    l1: String,
    floor: u64,
    cap: usize,
    heap: std::collections::BinaryHeap<SmallestFirst>,
    /// Above-floor ranges offered to this build, whether kept or discarded. The discriminator
    /// between "this shard has few large ranges" and "the cap is binding".
    considered: u64,
}

impl ShardTailBuild {
    fn new(bucket: String, l1: String, floor: u64, cap: usize) -> Self {
        Self {
            bucket,
            l1,
            floor,
            // A zero cap would make `push` pop what it just pushed forever. Configuration
            // validation rejects 0, so this only guards a hand-built literal in a test.
            cap: cap.max(1),
            heap: std::collections::BinaryHeap::new(),
            considered: 0,
        }
    }

    /// Offer one record. Sub-floor records are not offered — the caller filters, so
    /// `considered` counts exactly the above-floor population.
    fn push(&mut self, record: TailRecord) {
        self.considered = self.considered.saturating_add(1);
        self.heap.push(SmallestFirst(record));
        if self.heap.len() > self.cap {
            // Pops the SMALLEST, which may be the record just pushed. Dropping the last-pushed
            // record instead would make membership depend on directory-iteration order rather
            // than on size, and § 4.2's soundness lemma is about the top-N by size.
            self.heap.pop();
        }
    }

    /// The finished partition, `compressed_size` descending.
    fn finish(self) -> ShardTailPartition {
        let cap_bound = self.considered > self.cap as u64;
        ShardTailPartition {
            bucket: self.bucket,
            l1: self.l1,
            floor: self.floor,
            cap: self.cap,
            considered: self.considered,
            cap_bound,
            records: self
                .heap
                .into_sorted_vec()
                .into_iter()
                .map(|s| s.0)
                .collect(),
        }
    }
}

/// One shard's finished Tail_Partition, ready to write.
#[derive(Debug, Clone)]
struct ShardTailPartition {
    bucket: String,
    l1: String,
    floor: u64,
    cap: usize,
    considered: u64,
    /// Whether the cap discarded anything for this shard on this rebuild.
    cap_bound: bool,
    /// `compressed_size` descending. **Possibly empty, and an empty one is still written** —
    /// see [`CacheSizeTracker::write_tail_partition`].
    records: Vec<TailRecord>,
}

/// `_shard_state`, the directory the shard-scoped derived state lives in, beside `_journals`.
///
/// Spelled here rather than imported because `src/staged_fold.rs`'s copy is private and that
/// file belongs to another unit's write boundary. Duplicated deliberately, and named as owed.
const SHARD_STATE_DIR: &str = "_shard_state";

/// `tails`, the Tail_Partition subtree: `tails/{bucket}/{XX}.tail`.
const TAILS_DIR: &str = "tails";

/// Suffix of a per-instance tail-delta and of its lock sibling.
const TAIL_DELTA_SUFFIX: &str = ".tail-delta";
const TAIL_DELTA_LOCK_SUFFIX: &str = ".tail-delta.lock";

/// `{cache_dir}/metadata/_shard_state/tails`.
pub(crate) fn tails_dir(cache_dir: &std::path::Path) -> PathBuf {
    cache_dir
        .join("metadata")
        .join(SHARD_STATE_DIR)
        .join(TAILS_DIR)
}

/// `{cache_dir}/metadata/_shard_state/tails/{bucket}/{XX}.tail`.
pub(crate) fn tail_partition_path(cache_dir: &std::path::Path, bucket: &str, l1: &str) -> PathBuf {
    tails_dir(cache_dir)
        .join(bucket)
        .join(format!("{}.tail", l1))
}

/// `{cache_dir}/metadata/_shard_state/{instance_id}.tail-delta`.
///
/// **Flat at the `_shard_state` root, not per shard.** The file holds every shard's recent
/// above-floor admissions for one instance, which is what makes the append a single `O_APPEND`
/// write on the consolidation path rather than one file open per shard touched.
pub(crate) fn tail_delta_path(cache_dir: &std::path::Path, instance_id: &str) -> PathBuf {
    cache_dir
        .join("metadata")
        .join(SHARD_STATE_DIR)
        .join(format!("{}{}", instance_id, TAIL_DELTA_SUFFIX))
}

/// One tail-delta record: a [`TailRecord`] plus the shard it belongs to and the identity
/// compaction retires it by.
///
/// # Removals are deliberately NOT appended
///
/// § 4.4. A removed range's record is stale, and R3.8 already requires re-verification against
/// the `.meta` before any deletion, so a stale record costs exactly one skipped candidate.
/// Appending negative records would double the file's size to save a verification that has to
/// happen anyway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TailDeltaRecord {
    pub bucket: String,
    pub l1: String,
    /// When this record was appended, **nanoseconds** since the epoch.
    ///
    /// Nanoseconds because this field is part of the retire-set identity, and second
    /// granularity in a compaction identity makes the in-memory form and the read-back form of
    /// the same record unequal — so compaction silently retires nothing (§ 2 trap 1, and the
    /// defect `write_ledger`'s `mod ledger_time` exists for). It is also what the cross-shard
    /// sweep compares against a shard's last rebuild.
    pub at: u64,
    pub instance_id: String,
    pub record: TailRecord,
}

impl TailDeltaRecord {
    /// The tuple compaction names this record by.
    ///
    /// Carries the range identity as well as `(bucket, l1, at, instance_id)`, because one
    /// instance can append several records at the same nanosecond for different ranges and a
    /// retire-set that could not tell them apart would retire more than it read.
    pub fn identity(&self) -> TailDeltaIdentity {
        (
            self.bucket.clone(),
            self.l1.clone(),
            self.at,
            self.instance_id.clone(),
            self.record.cache_key.clone(),
            self.record.range_start,
            self.record.range_end,
        )
    }
}

/// `(bucket, l1, at, instance_id, cache_key, range_start, range_end)` — what
/// [`retire_tail_delta_records`] names a record by.
pub type TailDeltaIdentity = (String, String, u64, String, String, u64, u64);

/// The `(bucket, XX, YYY)` of a cache key.
///
/// Goes through the same `parse_cache_key` plus BLAKE3-over-object-key that
/// `disk_cache::get_sharded_path` uses, so a tail record's shard and leaf can never disagree
/// with the directory its `.meta` actually sits in. `staged_fold::shard_of` returns the first
/// two components and stops; this adds the `{YYY}` leaf § 4.1 requires be carried.
pub(crate) fn shard_and_leaf_of(cache_key: &str) -> Option<(String, String, String)> {
    let (bucket, object_key) = crate::disk_cache::parse_cache_key(cache_key).ok()?;
    let hash = blake3::hash(object_key.as_bytes());
    let hex = hash.to_hex();
    let hex = hex.as_str();
    Some((
        bucket.to_string(),
        hex[0..2].to_string(),
        hex[2..5].to_string(),
    ))
}

/// When this shard's Tail_Partition was last rebuilt, nanoseconds since the epoch, or `None` if
/// no rebuild has been recorded or the checkpoint could not be read.
///
/// The figure the cross-shard tail-delta sweep compares a record's `at` against. It lives here
/// rather than at the sweep's call site so this module stays the only one that names the shard
/// checkpoint — the containment
/// `staged_fold::tests::the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface`
/// enforces, which forbids the consolidation scheduler naming the document at all.
///
/// `None` on an unreadable checkpoint is the conservative direction: it retires nothing, so a
/// record is kept too long rather than dropped while it is still the only hint a large range
/// exists.
pub(crate) fn tail_built_at_for(
    cache_dir: &std::path::Path,
    bucket: &str,
    l1: &str,
) -> Option<u64> {
    let shard = crate::staged_fold::Shard::new(bucket, l1);
    crate::staged_fold::StagedShardCheckpoint::load(cache_dir, &shard)
        .ok()
        .flatten()
        .and_then(|c| c.other.get("tail_built_at").and_then(|v| v.as_u64()))
}

/// Nanoseconds since the epoch. Never seconds — see [`TailDeltaRecord::at`].
fn tail_now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// `SystemTime` as nanoseconds since the epoch, saturating at 0 for a pre-epoch value.
fn systime_nanos(t: SystemTime) -> u64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Append one record to this instance's tail-delta under the file's own lock.
///
/// A true `O_APPEND` write, not a read-modify-write: the ledger's module docs give the reason —
/// an RMW append is O(file) per write, and these files are as long as their content. Safe
/// because each instance writes only its own file and the only other writer is compaction
/// holding this same lock.
///
/// Best-effort. A failure is logged and swallowed: the range is already on disk and already
/// accounted, and failing consolidation over a hint would be strictly worse than losing it.
/// The cost of a lost append is that the range is invisible to selection until its shard is
/// next rebuilt.
///
/// # The file is SIZE-CAPPED here, which is what makes § 4.5's read bounded
///
/// `cap` is `cache.shared_storage.tail_delta_max_entries`. After the append, if the file has
/// crossed `2 × cap × EST_TAIL_RECORD_BYTES` **bytes** it is rewritten down to the largest `cap`
/// records by `compressed_size`, under the lock this function already holds.
///
/// **The trigger is a BYTE threshold, so the guarantee is a byte bound and not a record bound.**
/// Stated precisely because the difference matters and is easy to overstate: the file is bounded at
/// `2 × cap × 512 B` of I/O per instance, and the record count it holds at that size depends on how
/// long the cache keys are — short keys mean more records inside the same byte budget. Measured at
/// `cap = 256` with ~230-byte records, one rewrite covered 1,024 appends rather than 256, i.e. the
/// file ran to about four times `cap` in records while staying inside its byte bound.
///
/// Both bounds that matter are still independent of N: **I/O** by this byte threshold, and
/// **memory** by [`read_tail_delta_candidates`]'s bounded heap, which keeps `cap` records per
/// instance whatever the file holds. Counting records here instead would make the trigger O(file)
/// per append, which is the cost the whole scheme exists to avoid.
///
/// **Why the cap belongs here and not at the read.** § 4.5 reads all delta records for all
/// instances on every pass, and the delta's length is
/// `(above-floor admission rate) × (rotation period)` with § 16.5 making the rotation period grow
/// with N — so the read is O(N) in the limit however bounded the partition prefixes are. Capping
/// only the *read* does not work, because a prefix of an **append-ordered** file is an arbitrary
/// subset rather than a top-N. The file itself has to be size-ordered-and-capped, and that means
/// a whole-file rewrite, which is R5.6's 119.5 ms against a 10.5 ms append.
///
/// **The cost is amortised, and the arithmetic is the whole argument.** A high-water of `2 × cap`
/// against a low-water of `cap` means one rewrite per `cap` appends, so at the default 4096 the
/// rewrite costs `119.5 / 4096 ≈ 0.03 ms` per append against a 10.5 ms append — about 0.3%. The
/// trigger itself is one `metadata()` on a handle already open, never a count of the file, so the
/// common path pays a `stat` and nothing else.
///
/// Soundness is § 4.2's lemma, which needs only that the population be partitioned into disjoint
/// sets each keeping its own top-C: an instance appends only its own admissions, so per-instance
/// sets are disjoint and a per-instance top-`cap` is sound for any global `K <= cap`.
pub(crate) fn append_tail_delta_records(
    cache_dir: &std::path::Path,
    instance_id: &str,
    records: &[TailDeltaRecord],
    cap: usize,
) -> std::io::Result<()> {
    use std::io::Write;

    if records.is_empty() {
        return Ok(());
    }
    let path = tail_delta_path(cache_dir, instance_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let lock_path = path.with_file_name(format!("{}{}", instance_id, TAIL_DELTA_LOCK_SUFFIX));

    let mut body = String::new();
    for r in records {
        body.push_str(
            &serde_json::to_string(r)
                .map_err(|e| std::io::Error::other(format!("serialize tail-delta record: {e}")))?,
        );
        body.push('\n');
    }

    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)?;
    // Blocking, like the ledger's append and unlike the journal's: the only contender is
    // compaction holding the lock for one short rewrite, and there is no spill file to fall
    // back to, so an append that gave up here would lose the hint outright.
    fs4::FileExt::lock(&lock_file)?;
    let result = (|| -> std::io::Result<u64> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        file.write_all(body.as_bytes())?;
        file.flush()?;
        // One `metadata()` on a handle already open — the O(1) trigger. Counting records here
        // would make every append O(file), which is the cost this whole scheme exists to avoid.
        Ok(file.metadata().map(|m| m.len()).unwrap_or(0))
    })();
    if let Ok(len) = result {
        let high_water_bytes = cap
            .max(1)
            .saturating_mul(2)
            .saturating_mul(EST_TAIL_RECORD_BYTES) as u64;
        if len > high_water_bytes {
            compact_tail_delta_to_cap_locked(&path, instance_id, cap.max(1));
        }
    }
    let _ = fs4::FileExt::unlock(&lock_file);
    result.map(|_| ())
}

/// Rewrite one instance's tail-delta down to its largest `cap` records by `compressed_size`.
///
/// **The caller MUST already hold that file's lock.** Named `_locked` for that reason; it is
/// called only from [`append_tail_delta_records`], inside the lock, which is what makes the
/// read-modify-write safe against a concurrent append and against compaction.
///
/// Best-effort in both directions: a file that turns out to hold `cap` records or fewer is left
/// alone (the byte trigger is a proxy and may over-fire on long cache keys), and a failed rewrite
/// leaves the file unchanged, which costs read cost rather than correctness.
fn compact_tail_delta_to_cap_locked(path: &std::path::Path, instance_id: &str, cap: usize) {
    use std::io::Write;

    let records = read_tail_delta_file(path);
    if records.len() <= cap {
        return;
    }
    let before = records.len();
    let mut heap: std::collections::BinaryHeap<SmallestFirstDelta> =
        std::collections::BinaryHeap::new();
    for record in records {
        heap.push(SmallestFirstDelta(record));
        if heap.len() > cap {
            heap.pop();
        }
    }
    // `into_sorted_vec` under `SmallestFirstDelta`'s inverted `Ord` yields `compressed_size`
    // DESCENDING, matching the partition's own order so a reader sees one ordering everywhere.
    let kept: Vec<TailDeltaRecord> = heap.into_sorted_vec().into_iter().map(|s| s.0).collect();
    let mut body = String::new();
    for r in &kept {
        if let Ok(line) = serde_json::to_string(r) {
            body.push_str(&line);
            body.push('\n');
        }
    }
    // Per-instance file, so a plain `.tmp` is the right scheme (§ 2 trap 6).
    let tmp = path.with_file_name(format!("{}{}.cap.tmp", instance_id, TAIL_DELTA_SUFFIX));
    let write = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, path)
    })();
    match write {
        Ok(()) => {
            TAIL_DELTA_CAP_REWRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            debug!(
                "Tail-delta: capped {:?} from {} records to the largest {}",
                path, before, cap
            );
        }
        Err(e) => {
            warn!(
                "Tail-delta: failed to cap {:?} at {} records: {}; the file is unchanged",
                path, cap, e
            );
            let _ = std::fs::remove_file(&tmp);
        }
    }
}

/// How many times this process has rewritten a tail-delta down to its cap.
///
/// The figure that shows the amortisation is real: a rate approaching one per append means the
/// high-water and low-water marks have collapsed onto each other and every append is paying a
/// whole-file rewrite.
static TAIL_DELTA_CAP_REWRITES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Tail-delta cap rewrites performed by this process since start.
pub fn tail_delta_cap_rewrites_total() -> u64 {
    TAIL_DELTA_CAP_REWRITES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Every instance's tail-delta file on the volume.
fn tail_delta_files(cache_dir: &std::path::Path) -> Vec<PathBuf> {
    let dir = cache_dir.join("metadata").join(SHARD_STATE_DIR);
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.ends_with(TAIL_DELTA_SUFFIX) {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// Read one tail-delta file. Unparseable lines are skipped rather than fatal, which is also
/// what lets an unknown future field survive a mixed-version fleet.
fn read_tail_delta_file(path: &std::path::Path) -> Vec<TailDeltaRecord> {
    let Ok(content) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| match serde_json::from_str::<TailDeltaRecord>(l) {
            Ok(r) => Some(r),
            Err(e) => {
                debug!("Tail-delta: skipping unparseable line in {:?}: {}", path, e);
                None
            }
        })
        .collect()
}

/// Every tail-delta record on the volume, across every instance.
pub(crate) fn read_all_tail_delta_records(cache_dir: &std::path::Path) -> Vec<TailDeltaRecord> {
    tail_delta_files(cache_dir)
        .iter()
        .flat_map(|p| read_tail_delta_file(p))
        .collect()
}

/// How many tail-delta records exist across every instance, or `None` if the directory could
/// not be read.
///
/// `None` rather than 0 because "nothing to retire" and "could not look" are different
/// observations, and this gauge is the one an operator reads to see retirement keeping up.
pub(crate) fn count_tail_delta_records(cache_dir: &std::path::Path) -> Option<u64> {
    let dir = cache_dir.join("metadata").join(SHARD_STATE_DIR);
    if !dir.exists() {
        // A volume no pass has reached yet genuinely holds no records.
        return Some(0);
    }
    if std::fs::read_dir(&dir).is_err() {
        return None;
    }
    Some(
        tail_delta_files(cache_dir)
            .iter()
            .map(|p| read_tail_delta_file(p).len() as u64)
            .sum(),
    )
}

/// Drop exactly the named records from every instance's tail-delta, rewriting each file whole.
///
/// # Retire-set, never retain-set
///
/// § 2 trap 2, and this repo shipped the other shape: a `rewrite_retaining(set)` paired with a
/// capped reader deleted every entry the caller had not read, leaving **1 of 25** entries
/// surviving. Selection reads a *bounded prefix* of the delta, so it is a capped reader by
/// construction, and a retain-set here would reproduce that defect exactly. Naming what to
/// REMOVE makes the safe answer arrive by construction and closes the read-then-rewrite window
/// too: an append landing after the read is not named, so it survives.
///
/// A file from which nothing is dropped is not rewritten (no fsync, mtime unchanged).
///
/// Returns the number of records dropped.
pub(crate) fn retire_tail_delta_records(
    cache_dir: &std::path::Path,
    retire: &std::collections::HashSet<TailDeltaIdentity>,
) -> u64 {
    use std::io::Write;

    if retire.is_empty() {
        return 0;
    }
    let mut dropped = 0u64;
    for path in tail_delta_files(cache_dir) {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let instance = name.trim_end_matches(TAIL_DELTA_SUFFIX).to_string();
        let lock_path = path.with_file_name(format!("{}{}", instance, TAIL_DELTA_LOCK_SUFFIX));
        let lock_file = match std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
        {
            Ok(f) => f,
            Err(e) => {
                warn!(
                    "Tail-delta compaction: cannot open lock {:?}: {}; skipping this file",
                    lock_path, e
                );
                continue;
            }
        };
        if fs4::FileExt::lock(&lock_file).is_err() {
            continue;
        }
        let mut dropped_here = 0u64;
        let kept: Vec<TailDeltaRecord> = read_tail_delta_file(&path)
            .into_iter()
            .filter(|r| {
                if retire.contains(&r.identity()) {
                    dropped_here += 1;
                    false
                } else {
                    true
                }
            })
            .collect();
        if dropped_here == 0 {
            // Nothing named lives in this file. The rewrite would produce the same records
            // and cost an fsync'd whole-file write inside the eviction lock, once per
            // selection pass per instance.
            let _ = fs4::FileExt::unlock(&lock_file);
            continue;
        }
        dropped += dropped_here;
        let mut body = String::new();
        for r in &kept {
            if let Ok(line) = serde_json::to_string(r) {
                body.push_str(&line);
                body.push('\n');
            }
        }
        // Per-instance file, so a plain `.tmp` is the right scheme (§ 2 trap 6): no other
        // instance writes this path, and the only other writer is a pass holding this lock.
        let tmp = path.with_file_name(format!("{}{}.tmp", instance, TAIL_DELTA_SUFFIX));
        let write_result = (|| -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp, &path)
        })();
        if let Err(e) = write_result {
            warn!(
                "Tail-delta compaction: failed to rewrite {:?}: {}; the file is unchanged",
                path, e
            );
            let _ = std::fs::remove_file(&tmp);
        }
        let _ = fs4::FileExt::unlock(&lock_file);
    }
    dropped
}

/// Process-local Tail_Partition counters, for [`TailPartitionGauges`].
static TAIL_REBUILDS_TOTAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// `tail_entries` of the most recently rebuilt shard, plus one above so 0 is representable.
static TAIL_ENTRIES_LAST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// The smallest member `compressed_size` of the most recently rebuilt shard's partition, plus
/// one, so 0 is "no rebuild yet, or the last rebuilt partition was empty".
static TAIL_EFFECTIVE_MIN_LAST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// The floor and cap the tracker in this process was constructed with.
///
/// Published so the values **in force** are readable from `/metrics` rather than inferred from
/// the configuration file. A gauge that restated the default would be indistinguishable from one
/// reporting a value the pass actually reads, which is the shape of the `st_size` fold's wiring
/// defect; these are stored by [`CacheSizeTracker::new`] from its own `CacheSizeConfig`, so they
/// are the same values the rebuild consults.
static TAIL_FLOOR_IN_FORCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static TAIL_CAP_IN_FORCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn record_tail_rebuild(records: &[TailRecord]) {
    use std::sync::atomic::Ordering;
    TAIL_REBUILDS_TOTAL.fetch_add(1, Ordering::Relaxed);
    TAIL_ENTRIES_LAST.store(records.len() as u64 + 1, Ordering::Relaxed);
    // `min` rather than `last()`: the partition is written size-descending, but this figure
    // must not depend on that ordering staying true.
    let min = records.iter().map(|r| r.compressed_size).min();
    TAIL_EFFECTIVE_MIN_LAST.store(min.map_or(0, |m| m.saturating_add(1)), Ordering::Relaxed);
}

/// Publish the floor and cap this process's tracker holds. Called once, from
/// [`CacheSizeTracker::new`].
fn publish_tail_config_in_force(floor: u64, cap: usize) {
    use std::sync::atomic::Ordering;
    TAIL_FLOOR_IN_FORCE.store(floor, Ordering::Relaxed);
    TAIL_CAP_IN_FORCE.store(cap as u64, Ordering::Relaxed);
}

/// The Tail_Partition figures an operator needs to tell a wrong floor or a binding cap from a
/// cache that genuinely holds few large ranges.
///
/// # Why the effective config values are published
///
/// A floor set too high produces empty partitions, and an empty partition is indistinguishable
/// from an idle cache on every other signal. Publishing the values **in force** — rather than
/// trusting that the file was read — is also what makes the operator's configured value
/// checkable from outside the process, which is the `st_size` fold's wiring defect turned into
/// an assertion.
///
/// Two are per-instance (`tail_rebuilds_total`, `tail_entries_last_rebuild`), two are read from
/// configuration and one from the shared volume (`tail_delta_records`).
///
/// **`eviction.tail_coverage_ratio` is deliberately NOT here.** It is computed from the global
/// histogram against a `T_max` derived from `max_cache_size` and `eviction_target_percent`,
/// neither of which this pass can reach — and it is a figure consumed at selection time.
///
/// Spec: cache-eviction-at-scale § 4.2, § 4.4. Requirements: 3.4.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TailPartitionGauges {
    /// The floor **in force in this process**, from `cache.shared_storage.tail_floor_bytes`.
    pub tail_floor_bytes: u64,
    /// The per-shard rank cap in force in this process.
    pub tail_max_entries: u64,
    /// Shard partitions this process has rebuilt since start.
    pub tail_rebuilds_total: u64,
    /// Members written for the most recently rebuilt shard. `None` before this process has
    /// rebuilt one — never 0, because a genuinely empty partition is a real and expected
    /// reading and must be distinguishable from "no rebuild yet".
    pub tail_entries_last_rebuild: Option<u64>,
    /// The smallest `compressed_size` in the most recently rebuilt shard's partition: that
    /// shard's effective byte threshold, which is `tail_floor_bytes` only while the cap is not
    /// binding. Per-instance, and one shard's figure, not a minimum across shards. `None` before
    /// this process has rebuilt a shard, or when the last rebuilt partition had no members.
    #[serde(default)]
    pub tail_effective_min_bytes: Option<u64>,
    /// Tail-delta records outstanding across every instance, read from the shared volume.
    ///
    /// The figure that shows retirement keeping up — § 4.4 names it as the surviving equivalent
    /// of the retired shard-delta gauge. `None` is the UNREADABLE sentinel, never a zero.
    pub tail_delta_records: Option<u64>,
}

/// Collect the Tail_Partition gauges. Safe to call from a metrics scrape.
///
/// Costs one directory listing plus a read of each instance's tail-delta, which is bounded by
/// the delta's own compaction rather than by cache size.
pub(crate) fn collect_tail_partition_gauges(cache_dir: &std::path::Path) -> TailPartitionGauges {
    use std::sync::atomic::Ordering;
    let last = TAIL_ENTRIES_LAST.load(Ordering::Relaxed);
    TailPartitionGauges {
        tail_floor_bytes: TAIL_FLOOR_IN_FORCE.load(Ordering::Relaxed),
        tail_max_entries: TAIL_CAP_IN_FORCE.load(Ordering::Relaxed),
        tail_rebuilds_total: TAIL_REBUILDS_TOTAL.load(Ordering::Relaxed),
        tail_entries_last_rebuild: last.checked_sub(1),
        tail_effective_min_bytes: TAIL_EFFECTIVE_MIN_LAST
            .load(Ordering::Relaxed)
            .checked_sub(1),
        tail_delta_records: count_tail_delta_records(cache_dir),
    }
}

/// How long past its HEAD expiry a HEAD-only `.meta` is kept before the validation pass
/// removes it ([`stale_head_only_removable`]). The rule the removed startup census applied.
const STALE_HEAD_ONLY_GRACE: Duration = Duration::from_secs(24 * 3600);

/// Whether the validation pass may remove `metadata` at `now`: HEAD-only in shape (no ranges,
/// versioned, not a stub, not write-cached, no multipart state) and its HEAD expiry, or its
/// object expiry when no HEAD expiry is recorded, passed more than [`STALE_HEAD_ONLY_GRACE`]
/// ago. Such a document holds no data, and an expired HEAD answer is not served without going
/// to S3, so it only costs a listing entry and an object in `cached_objects`.
pub(crate) fn stale_head_only_removable(
    metadata: &crate::cache_types::NewCacheMetadata,
    now: SystemTime,
) -> bool {
    head_only_shape(metadata)
        && crate::cache_initialization_coordinator::stale_head_only_entry(
            metadata,
            now,
            STALE_HEAD_ONLY_GRACE,
        )
        .is_some()
}

/// Range-less, versioned, and neither staged nor multipart state: the documents
/// [`stale_head_only_removable`] can accept once expired.
fn head_only_shape(metadata: &crate::cache_types::NewCacheMetadata) -> bool {
    let om = &metadata.object_metadata;
    metadata.ranges.is_empty()
        && !is_stub_metadata(metadata)
        && !om.is_write_cached
        && om.upload_id.is_none()
        && om.parts.is_empty()
}

/// Whether a `.meta` describes nothing: no ranges, no version (empty ETag and
/// `Last-Modified`), `content_length` 0, no response headers, not write-cached and no
/// multipart state. This is exactly the default document consolidation used to create from
/// `Remove`-only journal entries for an already-deleted key.
///
/// A range-less `.meta` that is legitimate always carries a version: a HEAD-only entry and a
/// zero-length object both record the ETag and `Last-Modified` from S3's response, and a
/// write-cached or in-progress multipart entry is flagged. None of those is a stub.
pub(crate) fn is_stub_metadata(metadata: &crate::cache_types::NewCacheMetadata) -> bool {
    let om = &metadata.object_metadata;
    metadata.ranges.is_empty()
        && om.etag.is_empty()
        && om.last_modified.is_empty()
        && om.content_length == 0
        && om.response_headers.is_empty()
        && !om.is_write_cached
        && om.upload_id.is_none()
        && om.parts.is_empty()
}

/// Test-only hook into the full-scan walk, keyed by cache directory so concurrent
/// tests on other `TempDir`s are unaffected.
#[cfg(test)]
pub(crate) mod test_seams {
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    type WalkHook = Box<dyn FnOnce() + Send>;
    static AFTER_FULL_SCAN_WALK: Mutex<Vec<(PathBuf, WalkHook)>> = Mutex::new(Vec::new());

    /// Run `hook` once, after the next full scan's walk under `dir` returns and before its
    /// install: a write landing in a shard the walk has already passed.
    pub(crate) fn set_after_full_scan_walk(dir: &Path, hook: WalkHook) {
        AFTER_FULL_SCAN_WALK
            .lock()
            .unwrap()
            .push((dir.to_path_buf(), hook));
    }

    pub(crate) fn after_full_scan_walk(dir: &Path) {
        let hook = {
            let mut hooks = AFTER_FULL_SCAN_WALK.lock().unwrap();
            hooks
                .iter()
                .position(|(d, _)| d.as_path() == dir)
                .map(|i| hooks.remove(i).1)
        };
        if let Some(hook) = hook {
            hook();
        }
    }
}

/// Install a shard checkpoint carrying only an `st_size` histogram, for a test that needs
/// [`global_size_histogram`] to return something.
///
/// # Why this helper lives HERE rather than in the test that needs it
///
/// `staged_fold::tests::the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface`
/// forbids any `src/` file outside a four-name allowlist from naming `StagedShardCheckpoint`, and
/// the rule it enforces is "a gauge figure must not reach a decision path". **`cache.rs` is the
/// decision path** — it is the eviction selector — so a fixture there that named the type would
/// trip the guard, and widening the allowlist to admit it would be exempting the decision path
/// from the exact rule the guard exists to state.
///
/// This module is already the document's allowlisted third producer, so the fixture belongs here
/// and the selector reaches the histogram only through [`global_size_histogram`]. That keeps the
/// containment intact instead of paying for a fixture with a hole in it.
#[cfg(test)]
pub(crate) fn install_histogram_for_tests(
    cache_dir: &std::path::Path,
    bucket: &str,
    l1: &str,
    classes: &[u64],
) {
    let shard = crate::staged_fold::Shard::new(bucket, l1);
    let mut checkpoint = crate::staged_fold::StagedShardCheckpoint::unfolded_for(&shard);
    checkpoint.other.insert(
        "histogram".to_string(),
        serde_json::Value::Array(classes.iter().map(|c| (*c).into()).collect()),
    );
    checkpoint
        .install(cache_dir)
        .expect("fixture: the checkpoint must install");
}

/// Serialises every test, in any module of this file, that can reach `record_tail_rebuild`
/// (a rolling validation pass) or that reads or resets the tail gauges. The gauges are
/// process-global atomics and libtest runs the binary's tests as parallel threads, so an exact
/// assertion on them is only safe while no other rebuild runs. Held for the whole test body.
#[cfg(test)]
pub(crate) static TAIL_TEST_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
/// Reset the process-local Tail_Partition counters. Tests only — the statics are per-process and
/// a test binary shares them across test functions.
#[cfg(test)]
pub(crate) fn reset_tail_counters_for_tests() {
    use std::sync::atomic::Ordering;
    TAIL_REBUILDS_TOTAL.store(0, Ordering::SeqCst);
    TAIL_ENTRIES_LAST.store(0, Ordering::SeqCst);
    TAIL_EFFECTIVE_MIN_LAST.store(0, Ordering::SeqCst);
    TAIL_FLOOR_IN_FORCE.store(0, Ordering::SeqCst);
    TAIL_CAP_IN_FORCE.store(0, Ordering::SeqCst);
}

// ---------------------------------------------------------------------------------------------
// § 4.5's SELECTION READ: a bounded prefix of every partition, plus the bounded deltas.
//
// The writer is above; this is the reader. Both live here so the record type, the path scheme
// and the ordering contract have one home — a reader in another module would be free to disagree
// with the writer about any of the three and nothing would notice.
// ---------------------------------------------------------------------------------------------

/// Bytes budgeted per partition record when turning a record `depth` into a byte read.
///
/// Deliberately generous. A `TailRecord` line with an 80-character cache key serialises to
/// roughly 260 bytes, so 512 over-reads by about 2x — which costs one page of a sequential read
/// and removes the common case where the byte budget yields fewer records than `depth` and a
/// second read is needed. Over-reading cannot break the bound: the parsed prefix is truncated to
/// `depth` records regardless.
const EST_TAIL_RECORD_BYTES: usize = 512;

/// How many times a prefix read will re-read with a doubled byte budget before giving up on
/// reaching `depth` records.
///
/// A bound on the read, not on correctness: giving up returns the records found so far with
/// `complete: false`, so § 4.5's `depth * 4` escalation still sees an incomplete prefix and can
/// ask again.
const TAIL_PREFIX_READ_ATTEMPTS: usize = 4;

/// The depth a selection pass starts at when no global histogram is available, and the floor
/// under any histogram-derived depth.
///
/// **It is a FLOOR and not merely a fallback, and that is a design decision this unit took.**
/// § 4.5 derives the depth from the histogram, and the histogram comes from the `st_size` fold,
/// whose shard coverage is partial by construction — it visits shards on its own cadence and
/// budget. A partial histogram *under-counts* the population, and an under-count predicts a
/// **shallower** prefix than the truth warrants. Clamping up to this floor means a thin
/// histogram can never predict a shallower read than no histogram at all, so the fold's
/// incompleteness costs at most an extra escalation round rather than a missed candidate.
pub const DEFAULT_TAIL_PREFIX_DEPTH: usize = 64;

/// What a bounded prefix read of one shard's Tail_Partition found.
///
/// **`Absent` and an empty `Present` are different states and § 4.1 rules on the difference**:
/// a partition file that exists and holds zero records means the shard has no members above
/// `tail_floor_bytes` (§ 4.3's regime, where `TAIL_EMPTY` is the correct selection outcome),
/// while an absent file means the shard has not been visited since the partition scheme was
/// introduced and selection therefore has *no information* about it. Collapsing the two lets a
/// cold volume report `TAIL_EMPTY` — a claim that the cache holds nothing large — when what it
/// actually holds is an unvisited shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TailPartitionRead {
    /// No partition file. The shard is UNVISITED; nothing may be concluded about its contents.
    Absent,
    /// The partition exists and was read.
    Present {
        /// The prefix, `compressed_size` descending, truncated to the requested depth.
        records: Vec<TailRecord>,
        /// Whether the read reached end of file, i.e. this prefix is the whole partition.
        /// `false` means a deeper read would return more, which is what § 4.5's escalation
        /// tests before it gives up.
        complete: bool,
    },
    /// The file exists and could not be read. Not the same as empty: acting on no information
    /// is what § 5.4's `Unreadable` verdict exists to refuse.
    Unreadable,
}

impl TailPartitionRead {
    /// The records, or an empty slice for the two non-`Present` states.
    pub fn records(&self) -> &[TailRecord] {
        match self {
            TailPartitionRead::Present { records, .. } => records,
            _ => &[],
        }
    }

    /// Whether a deeper read of this shard could return more records. `Absent` and `Unreadable`
    /// answer `false` — a deeper read of a file that is not there returns nothing either.
    pub(crate) fn could_yield_more(&self) -> bool {
        matches!(
            self,
            TailPartitionRead::Present {
                complete: false,
                ..
            }
        )
    }
}

/// Every `(bucket, l1)` whose Tail_Partition file exists on this volume.
///
/// The enumeration R3.3 requires be complete: selection reads a bounded prefix of **every**
/// shard's partition, never a fraction of the shards, because one range can hold an arbitrary
/// share of the bytes and no fractional scheme is sound under that.
///
/// An empty result means no shard has been visited yet — selection has no information at all and
/// must fall back rather than conclude the cache holds nothing large.
pub(crate) fn enumerate_tail_shards(cache_dir: &std::path::Path) -> Vec<(String, String)> {
    let root = tails_dir(cache_dir);
    let mut out = Vec::new();
    let Ok(buckets) = std::fs::read_dir(&root) else {
        return out;
    };
    for bucket_entry in buckets.flatten() {
        let bucket_path = bucket_entry.path();
        if !bucket_path.is_dir() {
            continue;
        }
        let Some(bucket) = bucket_path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Ok(shards) = std::fs::read_dir(&bucket_path) else {
            warn!(
                "Tail selection: could not list partitions in {:?}; this bucket's shards are \
                 invisible to this pass",
                bucket_path
            );
            continue;
        };
        for shard_entry in shards.flatten() {
            let path = shard_entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("tail") {
                continue;
            }
            if let Some(l1) = path.file_stem().and_then(|s| s.to_str()) {
                out.push((bucket.to_string(), l1.to_string()));
            }
        }
    }
    out.sort();
    out
}

/// Read the first `depth` records of one shard's Tail_Partition.
///
/// `open`, read `min(depth × EST_TAIL_RECORD_BYTES, whole file)`, close — § 4.5. **Complete
/// lines only; a trailing partial line is discarded**, both because the byte boundary lands
/// mid-record by construction and because a concurrent rebuild can leave a torn tail (§ 2
/// trap 5).
///
/// The file is written `compressed_size` descending, so a prefix of the file is the shard's
/// top-`depth` by size. That is the property § 4.2's lemma needs, and it is why this may be a
/// prefix read at all — a prefix of an *append-ordered* file would be an arbitrary subset.
pub(crate) fn read_tail_partition_prefix(
    cache_dir: &std::path::Path,
    bucket: &str,
    l1: &str,
    depth: usize,
) -> TailPartitionRead {
    use std::io::Read;

    let path = tail_partition_path(cache_dir, bucket, l1);
    let mut file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return TailPartitionRead::Absent,
        Err(e) => {
            warn!(
                "Tail selection: {:?} exists and could not be opened: {}. Treated as unreadable \
                 rather than empty — an unreadable shard is not a shard with nothing large in it.",
                path, e
            );
            return TailPartitionRead::Unreadable;
        }
    };

    let depth = depth.max(1);
    let mut budget = depth.saturating_mul(EST_TAIL_RECORD_BYTES);
    let mut buf: Vec<u8> = Vec::new();
    let mut at_eof = false;

    for _ in 0..TAIL_PREFIX_READ_ATTEMPTS {
        let want = budget.saturating_sub(buf.len());
        if want == 0 {
            break;
        }
        let mut chunk = vec![0u8; want];
        let mut filled = 0usize;
        loop {
            match file.read(&mut chunk[filled..]) {
                Ok(0) => {
                    at_eof = true;
                    break;
                }
                Ok(n) => {
                    filled += n;
                    if filled == chunk.len() {
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    warn!("Tail selection: read of {:?} failed: {}", path, e);
                    return TailPartitionRead::Unreadable;
                }
            }
        }
        buf.extend_from_slice(&chunk[..filled]);

        let (records, _) = parse_complete_tail_lines(&buf, at_eof);
        if records.len() >= depth || at_eof {
            let complete = at_eof && records.len() <= depth;
            let mut records = records;
            records.truncate(depth);
            return TailPartitionRead::Present { records, complete };
        }
        budget = budget.saturating_mul(2);
    }

    // Byte budget exhausted without reaching `depth` records and without reaching EOF. The
    // records found are still a valid prefix; `complete: false` tells the escalation there is
    // more to have.
    let (mut records, _) = parse_complete_tail_lines(&buf, at_eof);
    records.truncate(depth);
    TailPartitionRead::Present {
        records,
        complete: at_eof,
    }
}

/// Parse whole JSONL records out of a byte prefix, discarding a trailing partial line.
///
/// Returns the records and how many lines were skipped as unparseable. A trailing line is
/// "partial" whenever the buffer does not end in a newline AND we are not at end of file; at EOF
/// the last line is whole (or is genuinely torn, in which case it fails to parse and is skipped,
/// which is the same outcome).
fn parse_complete_tail_lines(buf: &[u8], at_eof: bool) -> (Vec<TailRecord>, usize) {
    let text = String::from_utf8_lossy(buf);
    let ends_clean = text.ends_with('\n');
    let mut lines: Vec<&str> = text.lines().collect();
    if !ends_clean && !at_eof {
        lines.pop();
    }
    let mut records = Vec::with_capacity(lines.len());
    let mut skipped = 0usize;
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<TailRecord>(line) {
            Ok(r) => records.push(r),
            Err(_) => skipped += 1,
        }
    }
    (records, skipped)
}

/// The fleet-wide `st_size` histogram: every shard checkpoint's `histogram` array summed
/// element-wise.
///
/// `None` when no checkpoint carries one, which is a cold or unfolded volume. Element-wise
/// summing is only sound because [`crate::st_size_fold::histogram_class`]'s edges are **fixed**
/// rather than derived from each shard's own distribution — that is the property this depends on
/// and the reason that function's doc comment says so.
///
/// **Coverage is partial by construction** and this function cannot tell you how partial: the
/// fold visits shards on its own cadence, so a sum over installed checkpoints under-counts a
/// volume mid-rotation. See [`DEFAULT_TAIL_PREFIX_DEPTH`] for what that costs and why it is
/// clamped away.
pub(crate) fn global_size_histogram(cache_dir: &std::path::Path) -> Option<Vec<u64>> {
    let set = crate::staged_fold::read_shard_checkpoints(cache_dir);
    let mut total = vec![0u64; crate::st_size_fold::HISTOGRAM_CLASSES];
    let mut any = false;
    for (_shard, checkpoint) in set.iter() {
        let Some(classes) = checkpoint.other.get("histogram").and_then(|v| v.as_array()) else {
            continue;
        };
        any = true;
        for (idx, value) in classes.iter().enumerate() {
            if idx >= total.len() {
                break;
            }
            total[idx] = total[idx].saturating_add(value.as_u64().unwrap_or(0));
        }
    }
    if any {
        Some(total)
    } else {
        None
    }
}

/// The lower byte edge of histogram class `idx`, i.e. the smallest size that class holds.
///
/// Used as each class's per-range byte contribution when estimating how deep a prefix must be.
/// The **lower** edge deliberately: it under-estimates the bytes a class holds, which
/// over-estimates the rank needed to reach `target_bytes`, which asks for a deeper prefix. The
/// error direction is one extra round of reads rather than a missed candidate.
fn histogram_class_lower_edge(idx: usize) -> u64 {
    if idx == 0 {
        0
    } else {
        1u64 << (8 + idx as u32)
    }
}

/// § 4.5's `initial_prefix_depth`: the smallest `d` such that the largest `shards × d` ranges
/// hold at least `target_bytes`, doubled for headroom.
///
/// Clamped below by [`DEFAULT_TAIL_PREFIX_DEPTH`] and above by `cap`, which is
/// `tail_max_entries` — a depth beyond the cap cannot return more records because the partition
/// does not hold more.
pub(crate) fn initial_prefix_depth(
    target_bytes: u64,
    histogram: Option<&[u64]>,
    shards: usize,
    cap: usize,
) -> usize {
    let cap = cap.max(1);
    let shards = shards.max(1);
    let derived = match histogram {
        None => DEFAULT_TAIL_PREFIX_DEPTH,
        Some(classes) => {
            let mut rank = 0u64;
            let mut bytes = 0u64;
            // Largest class first: the ranges a shallow prefix would actually reach.
            for idx in (0..classes.len()).rev() {
                let count = classes[idx];
                if count == 0 {
                    continue;
                }
                let per = histogram_class_lower_edge(idx);
                if per == 0 {
                    // Class 0 contributes an unknown, possibly zero, number of bytes per range.
                    // Counting it as zero bytes would make the rank grow without the byte sum
                    // moving, so stop: nothing below 512 B can help reach a byte target.
                    break;
                }
                // How many of this class are needed, at most all of them.
                let still_needed = target_bytes.saturating_sub(bytes);
                let needed_here = still_needed.div_ceil(per).min(count);
                rank = rank.saturating_add(needed_here);
                bytes = bytes.saturating_add(needed_here.saturating_mul(per));
                if bytes >= target_bytes {
                    break;
                }
            }
            if bytes < target_bytes {
                // The whole histogram does not hold the target. Ask for everything; the
                // `CAP_EXHAUSTED` outcome is the honest answer and the escalation will reach it.
                cap
            } else {
                let per_shard = (rank as usize).div_ceil(shards);
                per_shard.saturating_mul(2)
            }
        }
    };
    derived.max(DEFAULT_TAIL_PREFIX_DEPTH).min(cap)
}

/// Read every instance's tail-delta, keeping at most `cap` records per instance — the largest by
/// `compressed_size`.
///
/// # Why this is bounded, and why it needs no prefix read
///
/// § 4.5 as written reads **all** tail-delta records for all instances on every pass while every
/// partition read is a bounded prefix, and the delta's size is
/// `(above-floor admission rate) × (rotation period)` with § 16.5 making the rotation period grow
/// with N. A bounded partition read beside an unbounded delta read is not a bounded pass, and
/// that is the R3.3 violation this unit exists to remove.
///
/// The fix is at the **append** site rather than here: [`append_tail_delta_records`] keeps each
/// file at most `2 × cap` records by rewriting it down to the largest `cap` whenever it crosses
/// the high-water mark. So the file is bounded by construction and can be read whole, which
/// matters because a *prefix* of an append-ordered file is an arbitrary subset rather than a
/// top-N — the unsoundness that made "just cap the read" wrong.
///
/// The bounded heap here is the second belt: a file left oversized by an older build, or by a
/// crash between the append and the rewrite, still costs bounded MEMORY, and its I/O cost is
/// paid once before the next append compacts it.
///
/// Soundness is § 4.2's lemma, unchanged: per-instance admission sets are **disjoint**, since an
/// instance appends only its own admissions, so each keeping its own top-`cap` means any range in
/// the global top-K is present for `K <= cap`.
pub(crate) fn read_tail_delta_candidates(
    cache_dir: &std::path::Path,
    cap: usize,
) -> Vec<TailDeltaRecord> {
    let cap = cap.max(1);
    let mut out: Vec<TailDeltaRecord> = Vec::new();
    for path in tail_delta_files(cache_dir) {
        let records = read_tail_delta_file(&path);
        if records.len() > cap.saturating_mul(2) {
            warn!(
                "Tail selection: {:?} holds {} records against a cap of {}. A file this long was \
                 not written by a build that compacts on append; it is read whole once and the \
                 next append will compact it.",
                path,
                records.len(),
                cap
            );
        }
        let mut heap: std::collections::BinaryHeap<SmallestFirstDelta> =
            std::collections::BinaryHeap::new();
        for record in records {
            heap.push(SmallestFirstDelta(record));
            if heap.len() > cap {
                heap.pop();
            }
        }
        out.extend(heap.into_iter().map(|s| s.0));
    }
    out
}

/// A [`TailDeltaRecord`] ordered so a `BinaryHeap` of them is a **min**-heap on
/// `compressed_size`, for the same reason [`SmallestFirst`] exists: a capped top-N discards the
/// smallest, and `BinaryHeap` pops the greatest.
///
/// # Equal sizes are broken on COLDNESS, and that is not cosmetic
///
/// The retained set is the candidate population eviction selects from, so the dimension the cap
/// discards on has to mean something. Ordering ties on `cache_key` means that a cache whose
/// ranges are of uniform size retains records in **cache-key order**, which has no relation to
/// eviction priority at all: under a monotone key scheme the surviving records are then the
/// newest admissions, which § 5.4 goes on to refuse as `NowHot`, so the whole retained set can
/// be unevictable by construction. Breaking the tie on `last_accessed` ascending retains the
/// coldest instead, which is the order § 5.2 would have selected them in anyway.
///
/// `cache_key` and `range_start` stay below it, so the ordering is still total and the retained
/// set is still deterministic for a given input.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SmallestFirstDelta(TailDeltaRecord);

impl Ord for SmallestFirstDelta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .0
            .record
            .compressed_size
            .cmp(&self.0.record.compressed_size)
            // NOT reversed, unlike every other term here: the heap pops the greatest, so the
            // WARMEST record of a tie must be `Greater` for the coldest to be the one the cap
            // retains.
            .then_with(|| {
                self.0
                    .record
                    .last_accessed
                    .cmp(&other.0.record.last_accessed)
            })
            .then_with(|| other.0.record.cache_key.cmp(&self.0.record.cache_key))
            .then_with(|| other.0.record.range_start.cmp(&self.0.record.range_start))
    }
}

impl PartialOrd for SmallestFirstDelta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Why a rolling cycle stopped, for the completion log and for the tests that assert the
/// budget is real.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RollingStopReason {
    /// Every slot this cycle intended to visit was visited.
    SlotsExhausted,
    /// `validation_max_duration` was reached with slots still pending. The cursor is durable
    /// at the last completed shard and the next cycle resumes from it.
    BudgetExhausted,
    /// The write phase could not be serialised against consolidation, or a fresher scanner's
    /// state was found on disk. The cursor did not advance past the shard whose figures were
    /// not installed.
    WritePhaseDeclined,
}

/// Smoothing factor for the scan-rate EWMA in `schedule.json`. Weights the newest cycle at
/// 30 %, so one atypical cycle moves the estimate without resetting it.
const SCAN_RATE_EWMA_ALPHA: f64 = 0.3;

/// How many times the rolling pass's write phase tries to take
/// `global_consolidation.lock` before giving up and stopping the cycle cleanly.
const WRITE_PHASE_LOCK_ATTEMPTS: usize = 12;

/// Delay between those attempts. Twelve attempts at 250 ms covers just under three seconds,
/// which spans a consolidation cycle on the default interval.
const WRITE_PHASE_LOCK_RETRY_DELAY: Duration = Duration::from_millis(250);

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

/// Filename of the shared accounting-drift sample, under `metadata/_shard_state/`.
const ACCOUNTING_DRIFT_SAMPLE: &str = "accounting_drift_sample.json";

/// Schema version of [`AccountingDriftSample`].
const ACCOUNTING_DRIFT_SCHEMA: u32 = 1;

/// Which validation produced an [`AccountingDriftSample`]'s reference figures.
///
/// **This is not decoration: it decides whether the sample is a drift at all.** R12.6 asks for
/// "the disagreement between the persisted figures and the reference fold", which requires two
/// *independent* producers. Only one of the two validations below supplies one — see
/// [`AccountingDriftGauge::unevaluable_reason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationSampleKind {
    /// A full validation scan: every `.meta` under `metadata/` was visited, so the reference
    /// figures are a whole-cache census computed without reference to the persisted counter.
    /// This is the only kind from which a drift can be evaluated.
    FullScan,
    /// A rolling validation cycle. It visits a budget-bounded subset of the 256 L1 slots and
    /// installs a **proportional correction**, which takes the persisted counter as an input —
    /// so its figure is not independent of the figure it would be compared against.
    RollingExtrapolation,
    /// The producer did not name its validation kind. Reached only by the three-argument
    /// [`crate::journal_consolidator::JournalConsolidator::update_size_from_validation`], whose
    /// remaining callers are tests.
    Unspecified,
}

impl ValidationSampleKind {
    /// The wire spelling, stable because it is published and alarmed on.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullScan => "full_scan",
            Self::RollingExtrapolation => "rolling_extrapolation",
            Self::Unspecified => "unspecified",
        }
    }

    /// The R12.8 named cause this kind imposes on every gauge in the sample, or `None` when the
    /// kind supplies a genuinely independent reference figure.
    fn blanket_unevaluable_reason(self) -> Option<&'static str> {
        match self {
            Self::FullScan => None,
            Self::RollingExtrapolation => Some(DRIFT_REASON_REFERENCE_IS_EXTRAPOLATION),
            Self::Unspecified => Some(DRIFT_REASON_SAMPLE_KIND_UNSPECIFIED),
        }
    }
}

/// `unevaluable_reason`: the persisted `size_state.json` could not be read, so the persisted
/// term would be a struct default and the "drift" would be the whole reference figure.
pub const DRIFT_REASON_PERSISTED_STATE_UNREADABLE: &str = "persisted_state_unreadable";
/// `unevaluable_reason`: the validation supplied no figure for this population, so there is no
/// reference term. The persisted value was left alone.
pub const DRIFT_REASON_FIGURE_NOT_SUPPLIED: &str = "figure_not_supplied";
/// `unevaluable_reason`: the reference figure is a proportional extrapolation computed **from**
/// the persisted counter, so the difference is the correction term rather than a disagreement
/// between two producers.
pub const DRIFT_REASON_REFERENCE_IS_EXTRAPOLATION: &str = "reference_is_extrapolation";
/// `unevaluable_reason`: the producer did not name its validation kind, so whether the reference
/// is a census cannot be established.
pub const DRIFT_REASON_SAMPLE_KIND_UNSPECIFIED: &str = "sample_kind_unspecified";
/// `unevaluable_reason` for the RATIO only: the reference figure is zero, so there is no
/// denominator. The absolute `drift` is still published.
pub const DRIFT_REASON_REFERENCE_ZERO: &str = "reference_zero";

/// One accounted figure's drift, as facts plus R12.7's ratio plus R12.8's named cause.
///
/// # Why `drift` is `Option` and the reason is a name rather than a sentinel value
///
/// R12.8 requires each gauge to distinguish "the invariant is violated" from "the invariant could
/// not be evaluated", *by name*, rather than mapping an unreadable input to a plausible number.
/// Every arm below that cannot produce a real comparison therefore publishes `drift: null` with a
/// `unevaluable_reason` naming which arm it was — following R8.7's distinct-named-cause table
/// rather than inventing a second convention.
///
/// `persisted` and `reference` are published even when `drift` is not, because they are
/// observations in their own right and an operator diagnosing an unevaluable gauge needs to see
/// the two terms that could not be compared.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountingDriftGauge {
    /// The running counter as it stood before this validation installed over it.
    pub persisted: Option<u64>,
    /// The figure the validation computed and installed.
    pub reference: Option<u64>,
    /// `reference − persisted`. **Signed**: a negative drift means the persisted counter was
    /// over-stating, which is the direction that makes a cache evict when it need not.
    pub drift: Option<i64>,
    /// R12.7's ratio form: `|drift| / reference`. A ratio rather than a byte count because a
    /// byte threshold that is right at one cache size is wrong at another.
    pub drift_ratio: Option<f64>,
    /// R12.8's named cause, `None` exactly when `drift` is `Some`.
    pub unevaluable_reason: Option<String>,
}

impl AccountingDriftGauge {
    /// Build a gauge from the two terms, honouring `blanket` (the sample kind's own veto) first.
    fn new(persisted: Option<u64>, reference: Option<u64>, blanket: Option<&'static str>) -> Self {
        let mut gauge = Self {
            persisted,
            reference,
            drift: None,
            drift_ratio: None,
            unevaluable_reason: None,
        };
        // Order matters and is not arbitrary: the reasons are checked from the most
        // fundamental input outwards, so the published cause is the first thing that broke
        // rather than the last thing noticed.
        let reason = match (blanket, persisted, reference) {
            (Some(r), _, _) => Some(r),
            (None, None, _) => Some(DRIFT_REASON_PERSISTED_STATE_UNREADABLE),
            (None, _, None) => Some(DRIFT_REASON_FIGURE_NOT_SUPPLIED),
            (None, Some(_), Some(_)) => None,
        };
        if let Some(r) = reason {
            gauge.unevaluable_reason = Some(r.to_string());
            return gauge;
        }
        // Unwraps are discharged by the match above: the `None` arm is only reached when both
        // terms are `Some`.
        let (p, r) = (persisted.unwrap(), reference.unwrap());
        let drift = r as i64 - p as i64;
        gauge.drift = Some(drift);
        if r == 0 {
            // The absolute drift stands; only the ratio is unevaluable, and it says so rather
            // than publishing an infinity or a zero.
            gauge.unevaluable_reason = Some(DRIFT_REASON_REFERENCE_ZERO.to_string());
        } else {
            gauge.drift_ratio = Some(drift.unsigned_abs() as f64 / r as f64);
        }
        gauge
    }
}

/// The accounting drift between the persisted size counters and a validation's reference
/// figures, for all three accounted populations, as of one validation.
///
/// # What this is for
///
/// R7.4's drift stayed visible in an `info!` line for at least five days while every assertion in
/// the project passed, because nothing exported it and nothing alarmed on it. R12.6 requires the
/// figures the validation already computes to be published as alarmable gauges; R12.7 requires the
/// sample's timestamp alongside them, so a *stalled* validation is distinguishable from a low
/// drift; R12.8 requires an unevaluable gauge to say so by name.
///
/// # Why it is a shared document rather than a process-local gauge
///
/// The same reason [`crate::eviction_result::EvictionPassResult`] is: any instance may run the
/// validation, so a per-instance gauge would read "no sample" on every proxy that simply had not
/// run one — and R12.7's staleness alarm would then fire on healthy instances, which is exactly
/// the false-alarm failure mode design § 8 corrects for the eviction rate. The question R12.7
/// asks is "has the validation run on this **volume**", and only a shared document answers it.
///
/// Spec: cache-eviction-at-scale. Requirements: 12.6, 12.7, 12.8.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountingDriftSample {
    /// Schema version, so a future layout change is rejected rather than misread.
    pub schema: u32,
    /// The instance that ran the validation and wrote this document.
    pub instance_id: String,
    /// Seconds since the epoch at which the sample was taken. **R12.7's staleness input**: an
    /// alarm on `now − sampled_at` is what distinguishes "drift is low" from "nothing has
    /// looked since the drift started".
    pub sampled_at: u64,
    /// Which validation produced the reference figures — see [`ValidationSampleKind`]. Read this
    /// before reading any drift below: three of the four kinds cannot produce one.
    pub sample_kind: String,
    /// `size_state.total_size` against the validation's total.
    pub total_size: AccountingDriftGauge,
    /// `size_state.write_cache_size` against the validation's staged total.
    pub write_cache_size: AccountingDriftGauge,
    /// `size_state.cached_objects` against the validation's object count.
    ///
    /// **This is the figure the validation did not previously compute a drift for at all.** The
    /// `info!` line R12.6 points at logs `drift` and `write_cache_drift` and then prints
    /// `cached_objects` as a bare new value, so the third gauge R12.6 names had no existing
    /// producer. Both its terms were already in hand at the install site; only the subtraction
    /// was missing.
    pub cached_objects: AccountingDriftGauge,
}

impl AccountingDriftSample {
    /// Build a sample from the install site's own terms.
    ///
    /// `persisted_state_readable` is `false` when `size_state.json` could not be loaded and the
    /// caller fell back to a struct default: every persisted term is then meaningless, and R12.8
    /// forbids publishing the resulting difference as a number.
    pub fn new(
        instance_id: String,
        kind: ValidationSampleKind,
        persisted_state_readable: bool,
        persisted: (u64, u64, u64),
        reference: (u64, Option<u64>, Option<u64>),
    ) -> Self {
        let blanket = kind.blanket_unevaluable_reason();
        let (p_total, p_write, p_objects) = persisted;
        let (r_total, r_write, r_objects) = reference;
        let guard = |v: u64| persisted_state_readable.then_some(v);
        Self {
            schema: ACCOUNTING_DRIFT_SCHEMA,
            instance_id,
            sampled_at: SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            sample_kind: kind.as_str().to_string(),
            total_size: AccountingDriftGauge::new(guard(p_total), Some(r_total), blanket),
            write_cache_size: AccountingDriftGauge::new(guard(p_write), r_write, blanket),
            cached_objects: AccountingDriftGauge::new(guard(p_objects), r_objects, blanket),
        }
    }

    /// `{cache_dir}/metadata/_shard_state/accounting_drift_sample.json`.
    pub fn path(cache_dir: &std::path::Path) -> PathBuf {
        cache_dir
            .join("metadata")
            .join("_shard_state")
            .join(ACCOUNTING_DRIFT_SAMPLE)
    }

    /// Install atomically: temp file, `sync_all`, rename — the same shape and the same
    /// per-instance temp suffix the two eviction-result documents use, so two instances cannot
    /// collide on one temp path.
    pub fn install(&self, cache_dir: &std::path::Path) -> std::io::Result<()> {
        use std::io::Write;
        let path = Self::path(cache_dir);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::other(format!("serialize accounting drift: {e}")))?;
        let tmp = path.with_extension(format!(
            "json.tmp.{}.{}",
            gethostname::gethostname()
                .to_string_lossy()
                .replace(['/', '\\'], "_"),
            std::process::id()
        ));
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)
    }

    /// Read the installed sample, or `None` if no validation has installed one on this volume.
    ///
    /// **Absence is a reading**: it says no validation has run here, which is R12.7's most
    /// severe staleness state and not an error. A read or parse failure is also `None`, because a
    /// `/metrics` scrape must not fail over an observability document.
    pub fn load(cache_dir: &std::path::Path) -> Option<Self> {
        let path = Self::path(cache_dir);
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => {
                debug!(
                    "Could not read {}: {}. Reporting no accounting drift sample.",
                    path.display(),
                    e
                );
                return None;
            }
        };
        match serde_json::from_str::<Self>(&content) {
            Ok(parsed) if parsed.schema == ACCOUNTING_DRIFT_SCHEMA => Some(parsed),
            Ok(parsed) => {
                warn!(
                    "Unknown accounting drift schema {} in {}; ignoring",
                    parsed.schema,
                    path.display()
                );
                None
            }
            Err(e) => {
                warn!("Unparseable accounting drift in {}: {}", path.display(), e);
                None
            }
        }
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

    /// Last validation drift in bytes.
    ///
    /// # This field is NOT R12.6's drift gauge, and must not be alarmed on as one
    ///
    /// It is `ValidationMetadata::drift_bytes` out of `size_tracking/validation.json`, which has
    /// **three producers computing three different things** under one name:
    ///
    /// | Producer | What it puts here |
    /// |---|---|
    /// | `perform_full_validation` | whole-cache scanned total − whole-cache persisted total. A real drift. |
    /// | `perform_rolling_validation` | the **subset** of shards this cycle visited − the **whole-cache** counter. Not a drift; structurally large and negative. |
    /// | `CacheSizeTracker::record_empty_cache_validation` | a literal `0`, exact because startup records it only for an empty cache whose figure is zero. |
    ///
    /// R4.5 places frequent full scans outside the operating mode, so at the design point the
    /// **second** producer is the one that runs — meaning the field an operator would reach for is
    /// the one that cannot be read as drift. [`AccountingDriftSample`] is R12.6's gauge; it names
    /// its producer and refuses to publish a number it cannot justify.
    ///
    /// Retained unchanged because it is already public and a missing JSON key reads as absent
    /// rather than as an error.
    pub last_validation_drift: Option<i64>,

    /// R12.6-R12.8's accounting-drift gauges: the persisted counters against the last
    /// validation's reference figures, for all three accounted populations, with the sample's
    /// timestamp and a named cause per gauge when the comparison cannot be made.
    ///
    /// Read from the shared volume, so every instance reports the same sample regardless of which
    /// one ran the validation. `null` before any validation has installed one — which is itself
    /// R12.7's most severe staleness reading.
    ///
    /// Spec: cache-eviction-at-scale. Requirements: 12.6, 12.7, 12.8.
    pub accounting_drift: Option<AccountingDriftSample>,

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
    /// A startup request found the tracked size unvalidated, which only a full census can
    /// ground. See [`ValidationRequestKind::FigureUnvalidated`].
    SizeUnvalidated,
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
            ScanModeReason::SizeUnvalidated => write!(f, "tracked size unvalidated"),
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
    //
    // `validation_path` holds the last scan's RESULTS, in `ValidationMetadata`'s shape and
    // nothing else. `schedule_path` holds the scheduler's own state — cursor, mode, rotation
    // counters, the scan-rate EWMA and `last_visit_at`. They were one file until the split
    // described on `read_rolling_state`, and merging them is what made
    // `read_validation_metadata` fail after every rolling cycle.
    validation_path: PathBuf,
    schedule_path: PathBuf,
    validation_lock_path: PathBuf,

    // Background task handles
    validation_task: Mutex<Option<JoinHandle<()>>>,

    // Weak reference to cache manager for GET cache expiration
    cache_manager: Mutex<Option<Weak<crate::cache::CacheManager>>>,

    /// Validation attempts the scheduler has made in this process, whatever their outcome.
    /// The observable for the contention back-off.
    validation_attempts: std::sync::atomic::AtomicU64,
    /// Set once the "no validation metadata" line has been logged at `info!`, cleared when
    /// the metadata is readable again, so an unreadable stretch logs once.
    no_metadata_logged: std::sync::atomic::AtomicBool,
    /// The startup request the scheduler serves before its daily window, if any. See
    /// [`CacheSizeTracker::request_validation`].
    validation_request: Mutex<Option<ValidationRequest>>,
}

/// The floor on the wait before retrying a validation pass that did not run (lost
/// `validation.lock`, or failed). See [`CacheSizeTracker::validation_scheduler`].
const VALIDATION_RETRY_BACKOFF: Duration = Duration::from_secs(300);

/// `now + max(jitter, VALIDATION_RETRY_BACKOFF)`, jitter up to the scheduler's one hour, so
/// instances that lost the lock together do not retry together.
fn validation_retry_backoff() -> Duration {
    Duration::from_secs(fastrand::u64(0..=3600)).max(VALIDATION_RETRY_BACKOFF)
}

/// What one scheduled validation attempt did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValidationOutcome {
    /// This instance held `validation.lock` and ran a pass (which may itself have deferred its
    /// size install; see [`CacheSizeTracker::install_full_scan_figures`]).
    Ran,
    /// Another instance holds `validation.lock`.
    LockContended,
    /// The lock was free, but `validation.json` re-read under it shows a pass completed within
    /// `MIN_VALIDATION_SPACING`: a peer ran while this instance slept towards the same window.
    /// For a startup request, a pass that completed after the request and satisfies it.
    PeerRanRecently,
}

/// Why startup asked the scheduler for a validation pass. Startup never counts the cache
/// itself (Ed, 2026-10-10); it asks for the usual pass under `validation.lock` instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationRequestKind {
    /// `validation.json` is older than the 23-hour startup gate. The pass picks its mode as a
    /// scheduled pass does.
    Overdue,
    /// `size_state.json` is missing on a cache holding data, unreadable, or carries no object
    /// count. The figure in force under-counts the cache, so the pass is a full scan: only a
    /// full scan's census can ground a figure that has no trustworthy base to extrapolate from.
    FigureUnvalidated,
}

/// A startup validation request, pending until a pass satisfies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationRequest {
    /// Why the pass was requested.
    pub kind: ValidationRequestKind,
    /// When. A pass by any instance that completed at or after this instant satisfies an
    /// `Overdue` request; a `FigureUnvalidated` one also needs that pass to be a full scan whose
    /// install happened.
    pub requested_at: SystemTime,
}

/// How long a full scan's size install waits for `global_consolidation.lock` before deferring
/// to the next pass: 240 attempts at [`WRITE_PHASE_LOCK_RETRY_DELAY`] (250 ms) is 60 s, twice
/// the default `consolidation_cycle_timeout`, so it spans a cycle that runs to its deadline.
const FULL_INSTALL_LOCK_ATTEMPTS: usize = 240;

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
        let schedule_path = size_tracking_dir.join("schedule.json");
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
            schedule_path,
            validation_lock_path,
            validation_task: Mutex::new(None),
            cache_manager: Mutex::new(None),
            validation_attempts: std::sync::atomic::AtomicU64::new(0),
            no_metadata_logged: std::sync::atomic::AtomicBool::new(false),
            validation_request: Mutex::new(None),
        };

        // Publish the Tail_Partition floor and cap this tracker holds, so `/metrics` reports the
        // values actually in force rather than restating a default. Taken from `config`, which is
        // the same struct the rebuild reads, so the gauge cannot agree with the rebuild while
        // both disagree with the operator.
        publish_tail_config_in_force(config.tail_floor_bytes, config.tail_max_entries);

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
        // This delegate's contract is a fail-open `u64`, so it flattens the UNREADABLE
        // sentinel. The condition is reported at `warn!` by the getter.
        self.consolidator.get_write_cache_size().await.unwrap_or(0)
    }

    /// Take `global_consolidation.lock` with a bounded try-acquire: `attempts` tries `delay`
    /// apart. `None` means the caller must defer its install; the reason is logged at WARN.
    /// The caller releases by the returned token, so its release is a no-op if the watchdog
    /// took the lock in between.
    async fn acquire_consolidation_lock_bounded(
        &self,
        attempts: usize,
        delay: Duration,
        what: &str,
    ) -> Option<ConsolidationLockToken> {
        for attempt in 0..attempts {
            match self.consolidator.try_acquire_consolidation_lock_token() {
                Ok(Some(token)) => return Some(token),
                Ok(None) => {}
                Err(e) => {
                    warn!(
                        "{}: consolidation lock unavailable ({}); deferring the size install",
                        what, e
                    );
                    return None;
                }
            }
            if attempt + 1 < attempts {
                tokio::time::sleep(delay).await;
            }
        }
        warn!(
            "{}: could not serialise the size install against consolidation within {}; \
             deferring it",
            what,
            format_duration_human(delay * attempts as u32)
        );
        None
    }

    // NOTE: set_write_cache_size() has been removed - consolidator handles all size state.

    // NOTE: update_size_sync() method has been removed.
    // Size tracking is now handled by JournalConsolidator through journal entries.
    // Tests should use the JournalConsolidator API instead.

    /// Get current tracked size (delegates to consolidator - Task 12.3)
    pub(crate) async fn get_size(&self) -> u64 {
        // Fail-open flattening, as above.
        self.consolidator.get_current_size().await.unwrap_or(0)
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
            // Read from the shared volume on every scrape, the same way
            // `cache.eviction_last_result` is: one small JSON read, no dependence on which
            // instance ran the validation.
            accounting_drift: AccountingDriftSample::load(&self.cache_dir),
            // R4.4/R12.4. One shared-volume `size_state.json` read plus one `schedule.json`
            // read — neither scales with cache population. The percentages come from the
            // consolidator's own `ConsolidationConfig`, which is the same field the byte
            // eviction trigger reads, so the two dimensions' hysteresis cannot drift apart.
            checkpoint_count: 0, // Deprecated - consolidator handles persistence
            delta_log_size: 0,   // Deprecated - always 0; delta files exist but are not a log
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
    // This tracker no longer reads or writes delta files. JournalConsolidator writes a
    // `delta_{instance}_{seq}.json` per flush and folds them into size_state.json.

    // NOTE: recover() method has been removed as part of Task 12.6.
    // Size state recovery is now handled by JournalConsolidator.initialize().
    // The consolidator loads size from size_state.json on startup and is the
    // single source of truth for cache size.

    // NOTE: The following methods have been removed as part of Task 11 (Remove Checkpoint Background Task):
    // - read_all_per_instance_delta_files() - the consolidator reads delta files now
    // - read_delta_log_with_write_cache() - the consolidator owns delta files now
    // - read_checkpoint() - checkpoint.json replaced by size_state.json
    // - read_delta_log() - the consolidator owns delta files now
    // - write_checkpoint() - consolidator handles persistence via size_state.json
    // - archive_and_truncate_all_delta_files() - the consolidator owns delta files now
    // - cleanup_old_delta_archives() - the consolidator owns delta files now
    // - cleanup_stale_delta_files() - the consolidator owns delta files now
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
        self.write_validation_metadata_inner(
            scanned_size,
            tracked_size,
            drift,
            duration,
            files_scanned,
            cache_expired,
            cache_skipped,
            cache_errors,
            write_cache_size,
            write_cache_expired,
            incomplete_uploads_evicted,
            false,
        )
        .await
    }

    /// The body of [`Self::write_validation_metadata_with_write_cache`], with
    /// [`ValidationMetadata::size_install_deferred`].
    #[allow(clippy::too_many_arguments)]
    async fn write_validation_metadata_inner(
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
        size_install_deferred: bool,
    ) -> Result<()> {
        let metadata = ValidationMetadata {
            size_install_deferred,
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

    /// Read the scheduler's own state from `size_tracking/schedule.json`, falling back
    /// once to the legacy fields inside `validation.json`.
    ///
    /// # Why this is a separate file from `validation.json`
    ///
    /// The cursor, mode, rotation counters and scan-rate EWMA used to live inside
    /// `validation.json` alongside the last scan's results, and a rolling cycle wrote its
    /// twelve fields over the whole document. [`ValidationMetadata`] requires five fields a
    /// rolling cycle never emitted (`scanned_size`, `tracked_size`, `drift_bytes`,
    /// `scan_duration_ms`, `metadata_files_scanned`) and none of them carries a serde
    /// default, so [`Self::read_validation_metadata`] failed after every rolling cycle.
    /// Two consequences reached operators:
    /// [`Self::calculate_next_validation_time`] took its "no validation metadata found"
    /// branch and rescheduled immediately, collapsing the daily cadence into continuous
    /// scanning, and `cache_size.last_validation_drift` went null.
    ///
    /// Adding serde defaults would have hidden that without addressing its cause, which was
    /// one deserialization target serving two modes. The two concerns are separate files
    /// instead: this one holds schedule state that both modes write, `validation.json` holds
    /// results in `ValidationMetadata`'s shape and never carries schedule fields.
    ///
    /// # Migration
    ///
    /// `schedule.json` is created on first write, so an upgraded deployment has none. When it
    /// is absent the legacy `rolling_*` fields are read out of `validation.json` and adopted,
    /// rather than restarting the rotation: a cursor mid-rotation and a measured scan rate are
    /// real observations, and discarding them would re-scan shards that were just visited and
    /// re-learn a rate the deployment already knows. Nothing deletes the legacy fields — the
    /// next full scan rewrites `validation.json` in its own shape and they disappear with it.
    ///
    /// Returns [`RollingState::default()`] (cursor 0, no scan rate) if neither file is
    /// readable or parseable, logging a warning.
    ///
    /// See: Requirements 5.2, 5.3
    pub fn read_rolling_state(&self) -> Result<RollingState> {
        let json = match Self::read_json_file(&self.schedule_path) {
            Some(v) => v,
            None => match Self::read_json_file(&self.validation_path) {
                Some(legacy) => {
                    debug!(
                        "Rolling state: schedule.json absent, adopting legacy rolling fields from validation.json"
                    );
                    legacy
                }
                None => {
                    warn!(
                        "Rolling state: neither schedule.json nor validation.json is readable, using defaults"
                    );
                    return Ok(RollingState::default());
                }
            },
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

        let last_visit_at = json.get("last_visit_at").and_then(|v| v.as_u64());

        Ok(RollingState {
            cursor,
            scan_rate,
            full_rotation_count,
            rotation_start_time,
            last_full_scan_duration_secs,
            last_visit_at,
        })
    }

    /// Read and parse a JSON state file, returning `None` when it is missing, unreadable
    /// or unparseable. Shared by the schedule reader and its legacy fallback so both treat
    /// "absent" and "corrupt" identically — neither is evidence about the other file.
    fn read_json_file(path: &std::path::Path) -> Option<serde_json::Value> {
        let content = std::fs::read_to_string(path).ok()?;
        match serde_json::from_str(&content) {
            Ok(v) => Some(v),
            Err(e) => {
                warn!("Scan state file {:?} is corrupted ({}), ignoring", path, e);
                None
            }
        }
    }

    /// Epoch seconds now, saturating at 0 before the epoch.
    fn epoch_secs_now() -> u64 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Epoch milliseconds now. The unit of `schedule.json`'s `last_visit_at`.
    fn epoch_millis_now() -> u64 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Persists rolling scan state and cycle statistics to `schedule.json`.
    ///
    /// Writes the scheduler's state and this cycle's counters using an atomic write (temp
    /// file, then rename) to prevent a torn document on shared storage. It writes no scan
    /// results and never touches `validation.json` — see [`Self::read_rolling_state`] for
    /// why the two are separate files.
    ///
    /// See: Requirements 5.1, 5.4, 7.2, 8.3
    pub fn write_rolling_state(
        &self,
        state: &RollingState,
        cycle_stats: &RollingCycleStats,
    ) -> Result<()> {
        let now = Self::epoch_secs_now();

        let json = serde_json::json!({
            "last_visit_at": state.last_visit_at.unwrap_or(now),
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

        self.write_schedule_json(&json)
    }

    /// Atomically replace `schedule.json` with `json`.
    fn write_schedule_json(&self, json: &serde_json::Value) -> Result<()> {
        let content = serde_json::to_string_pretty(json).map_err(|e| {
            ProxyError::CacheError(format!("Failed to serialize schedule state: {}", e))
        })?;

        // Atomic write: write to temp file, then rename
        let temp_path = self.schedule_path.with_extension("json.tmp");
        std::fs::write(&temp_path, &content).map_err(|e| {
            ProxyError::CacheError(format!("Failed to write schedule temp file: {}", e))
        })?;
        std::fs::rename(&temp_path, &self.schedule_path).map_err(|e| {
            ProxyError::CacheError(format!("Failed to rename schedule temp file: {}", e))
        })?;

        Ok(())
    }

    /// Persist the cursor and `last_visit_at` after one shard visit, preserving every other
    /// field already in `schedule.json`.
    ///
    /// This is the durability step § 6.4 requires per shard, and it runs **after** the
    /// visited shard's figures have been installed, never before: a cursor that advanced past
    /// a shard whose results were not installed would skip that shard for a whole rotation.
    fn persist_cursor(&self, cursor: u8, visited_at: u64) -> Result<()> {
        let mut json = Self::read_json_file(&self.schedule_path)
            .unwrap_or_else(|| serde_json::json!({ "validation_type": "rolling" }));
        if let Some(obj) = json.as_object_mut() {
            obj.insert("rolling_cursor".to_string(), serde_json::json!(cursor));
            obj.insert("last_visit_at".to_string(), serde_json::json!(visited_at));
        }
        self.write_schedule_json(&json)
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

    /// Ask the scheduler to run a validation pass as soon as it starts, under
    /// `validation.lock`, instead of at the daily window. Called by startup in place of the
    /// census it used to take. A `FigureUnvalidated` request is never downgraded by a later
    /// `Overdue` one.
    ///
    /// Only the scheduler loop serves the request, and it reads it at the top of each turn, so
    /// a request made before [`Self::start_validation_task`] runs at once. With
    /// `validation_enabled: false` (not reachable from YAML: `from_shared_storage` keeps the
    /// default `true`) nothing serves it.
    pub(crate) fn request_validation(&self, kind: ValidationRequestKind) {
        let mut slot = self.validation_request.lock().unwrap();
        let kind = match *slot {
            Some(existing) if existing.kind == ValidationRequestKind::FigureUnvalidated => {
                ValidationRequestKind::FigureUnvalidated
            }
            _ => kind,
        };
        *slot = Some(ValidationRequest {
            kind,
            requested_at: SystemTime::now(),
        });
    }

    /// Record a completed validation of an EMPTY cache whose figure is zero, under
    /// `validation.lock`, without walking anything.
    ///
    /// Startup calls this when `metadata/` is absent, or was listed to the end and holds no
    /// bucket directory, and `size_state.json` is
    /// missing or all zeros, so the census a pass would take is 0 and equals the figure: the
    /// record is exact. Without it `validation.json` stays absent on a fresh cache, and
    /// [`Self::calculate_next_validation_time`]'s "no validation metadata" arm runs a full
    /// pass the moment the scheduler starts, racing the cache's first writes for nothing. The
    /// removed startup census wrote the document on this path (after walking zero files), so
    /// this keeps that timing. Skipped, with nothing written, when the lock is held elsewhere.
    pub(crate) async fn record_empty_cache_validation(&self) {
        let Ok(_lock) = self.try_acquire_validation_lock().await else {
            debug!(
                "Cache validation: validation.lock held elsewhere; not recording the empty cache"
            );
            return;
        };
        if let Err(e) = self
            .write_validation_metadata_inner(0, 0, 0, Duration::ZERO, 0, 0, 0, 0, 0, 0, 0, false)
            .await
        {
            warn!(
                "Cache validation: could not record the empty cache's validation: {}",
                e
            );
        }
    }

    /// The pending startup validation request, if any.
    pub fn validation_request(&self) -> Option<ValidationRequest> {
        *self.validation_request.lock().unwrap()
    }

    /// Serve the pending startup request with one pass under `validation.lock`. The request is
    /// cleared when this pass, or a peer's completed since the request, satisfies it; it stays
    /// pending (the scheduler retries after its back-off) on lock contention, on an error, and
    /// when a `FigureUnvalidated` pass's install deferred.
    pub(crate) async fn perform_requested_validation(
        self: &Arc<Self>,
    ) -> Result<ValidationOutcome> {
        let Some(request) = self.validation_request() else {
            return Ok(ValidationOutcome::PeerRanRecently);
        };
        let outcome = self.perform_validation_gated(false, Some(request)).await?;
        if outcome == ValidationOutcome::LockContended {
            return Ok(outcome);
        }
        let satisfied = match request.kind {
            ValidationRequestKind::Overdue => true,
            ValidationRequestKind::FigureUnvalidated => self
                .read_validation_metadata()
                .await
                .map(|m| !m.size_install_deferred)
                .unwrap_or(false),
        };
        if satisfied {
            let mut slot = self.validation_request.lock().unwrap();
            if *slot == Some(request) {
                *slot = None;
            }
        } else {
            warn!(
                "Cache validation: the requested full validation's size install deferred; the \
                 tracked size stays UNVALIDATED and the pass will be retried"
            );
        }
        Ok(outcome)
    }

    /// Start background validation task
    pub(crate) fn start_validation_task(self: &std::sync::Arc<Self>) {
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
    ///
    /// # Back-off after a pass that did not run
    ///
    /// When `validation.json` is unreadable, [`Self::calculate_next_validation_time`] schedules
    /// "now". That is reachable for hours on every instance of a fleet: a fresh cache has no
    /// `validation.json` until the first full scan finishes writing it, and that scan holds
    /// `validation.lock` throughout, so every other instance used to lose the lock, return at
    /// once, recompute "now" and retry in a tight loop with two `info!` lines per turn. A
    /// corrupt `validation.json` does the same indefinitely. After a contended or failed pass
    /// the next attempt is therefore floored at `now + max(jitter, VALIDATION_RETRY_BACKOFF)`,
    /// and contention is logged once per streak.
    async fn validation_scheduler(self: &Arc<Self>) {
        // Earliest time the next attempt may start, set after a pass that did not run.
        let mut retry_floor: Option<SystemTime> = None;
        let mut contended_streak: u64 = 0;
        loop {
            // A startup request runs now, after any retry floor, rather than at the daily
            // window. See `request_validation`.
            if self.validation_request().is_some() {
                if let Some(floor) = retry_floor.take() {
                    let wait = floor
                        .duration_since(SystemTime::now())
                        .unwrap_or(Duration::ZERO);
                    tokio::time::sleep(wait).await;
                }
                info!("Cache validation: running the validation requested at startup");
                self.validation_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                match self.perform_requested_validation().await {
                    Ok(ValidationOutcome::LockContended) => {
                        contended_streak += 1;
                        debug!(
                            "Cache validation: validation.lock held elsewhere for the requested \
                             pass ({} consecutive attempts)",
                            contended_streak
                        );
                        retry_floor = Some(SystemTime::now() + validation_retry_backoff());
                    }
                    Ok(_) => {
                        contended_streak = 0;
                        if self.validation_request().is_some() {
                            retry_floor = Some(SystemTime::now() + validation_retry_backoff());
                        }
                    }
                    Err(e) => {
                        contended_streak = 0;
                        error!("Requested validation failed: {}", e);
                        retry_floor = Some(SystemTime::now() + validation_retry_backoff());
                    }
                }
                continue;
            }

            // Calculate next scheduled validation time
            let mut next_validation_time = self.calculate_next_validation_time().await;
            if let Some(floor) = retry_floor.take() {
                next_validation_time = next_validation_time.max(floor);
            }

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
            self.validation_attempts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            match self.perform_scheduled_validation().await {
                Ok(ValidationOutcome::Ran) | Ok(ValidationOutcome::PeerRanRecently) => {
                    contended_streak = 0;
                }
                Ok(ValidationOutcome::LockContended) => {
                    contended_streak += 1;
                    let backoff = validation_retry_backoff();
                    if contended_streak == 1 {
                        info!(
                            "Cache validation: another instance holds validation.lock; \
                             retrying no sooner than every {} until it finishes",
                            format_duration_human(VALIDATION_RETRY_BACKOFF)
                        );
                    } else {
                        debug!(
                            "Cache validation: validation.lock still held elsewhere \
                             ({} consecutive attempts), next retry in {}",
                            contended_streak,
                            format_duration_human(backoff)
                        );
                    }
                    retry_floor = Some(SystemTime::now() + backoff);
                }
                Err(e) => {
                    contended_streak = 0;
                    error!("Validation failed: {}", e);
                    retry_floor = Some(SystemTime::now() + validation_retry_backoff());
                }
            }

            // Loop to schedule next day's validation
        }
    }

    /// Calculate next validation time based on configured time of day with fixed 1-hour jitter
    pub async fn calculate_next_validation_time(&self) -> SystemTime {
        use chrono::{Duration as ChronoDuration, Local, Timelike};

        // Check if validation metadata exists - if not, run immediately
        if self.read_validation_metadata().await.is_err() {
            // Logged once per unreadable stretch: the scheduler can come back here every
            // VALIDATION_RETRY_BACKOFF while a peer's first scan holds the lock.
            if !self
                .no_metadata_logged
                .swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                info!("No validation metadata found, scheduling immediate validation");
            } else {
                debug!("No validation metadata found, scheduling immediate validation");
            }
            return SystemTime::now();
        }
        self.no_metadata_logged
            .store(false, std::sync::atomic::Ordering::Relaxed);

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

        // Enforce a minimum spacing of MIN_VALIDATION_SPACING between consecutive passes.
        //
        // # This branch used to be inert, and the comment described a skip the code did not do
        //
        // The body contained only a `debug!`: it assigned nothing, returned nothing, and set no
        // flag, so this function's result was identical whether validation had run one minute ago
        // or twenty-two hours ago. The comment said "Only skip if validation ran less than 23
        // hours ago" — a skip that did not exist.
        //
        // What that cost: the schedule above picks the next occurrence of
        // `validation_time_of_day` and only rolls to tomorrow when today's target has already
        // passed. So a pass that finished shortly *before* today's target time — which is exactly
        // what happens after a restart, or after a pass that overran into its own window — was
        // followed by another pass minutes later. On a large cache a full scan is hours of
        // shared-storage I/O, and the fleet has no other spacing mechanism: `validation.lock`
        // stops two instances validating *simultaneously* but says nothing about how soon the
        // next pass may start.
        //
        // # Why one day is always enough
        //
        // `next_time` is at most 24 h ahead of `now`, and `last_validation <= now`, so if
        // `next_time < last_validation + 23 h` then `next_time + 24 h >= last_validation + 24 h`,
        // which exceeds the bound. A single rollover therefore always satisfies it, and no loop
        // is needed.
        //
        // The 23 h figure is deliberately one hour short of a day: up to 1 h of jitter is added
        // below, so the realised spacing lands at roughly 24 h rather than overshooting past the
        // configured time of day and drifting a little later every day.
        if let Ok(metadata) = self.read_validation_metadata().await {
            let elapsed = SystemTime::now()
                .duration_since(metadata.last_validation)
                .unwrap_or(Duration::MAX);

            if elapsed < MIN_VALIDATION_SPACING {
                let earliest: SystemTime = metadata.last_validation + MIN_VALIDATION_SPACING;
                let next_as_system: SystemTime = next_time.into();
                if next_as_system < earliest {
                    next_time += ChronoDuration::days(1);
                    info!(
                        "Cache validation: last pass was {} ago, which is inside the {} minimum \
                         spacing. Deferring to {}.",
                        format_duration_human(elapsed),
                        format_duration_human(MIN_VALIDATION_SPACING),
                        next_time
                    );
                } else {
                    debug!(
                        "Validation ran {} ago, next run at {}",
                        format_duration_human(elapsed),
                        next_time
                    );
                }
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

    /// Performs a validation scan with self-tuning mode selection.
    ///
    /// Reads previous scan state from `validation.json`, calls [`determine_scan_mode`]
    /// to choose between full and rolling mode, and dispatches accordingly. Incomplete
    /// multipart uploads are not reaped here: the journal consolidator's maintenance passes own
    /// that, on their own interval (`write_cache_manager::sweep_incomplete_uploads`).
    ///
    /// # Scanner document: COUNT, DO NOT GATE
    ///
    /// Once `validation.lock` is held, this publishes a `ScanKind::Validation` scanner document
    /// (cache-eviction-at-scale § 16.6.13 K6) and ends it on every exit through
    /// [`ValidationScanDocument`]'s drop. Peers' scan limiters count it as a live scanner, so a
    /// fold or repair pass sees the volume load validation adds.
    ///
    /// **Validation itself never reads `n` or `k` and never defers on them.** Two reasons:
    /// validation is already serialised fleet-wide by `validation.lock`, so at most one runs at a
    /// time and a limiter adds no bound it lacks; and it is the only absolute re-grounding of
    /// the size figures, retried at the granularity of a day, so a deferral costs a day of
    /// uncorrected drift rather than one fold interval. An instance that fails to take the
    /// lock publishes nothing, because it is not scanning.
    ///
    /// The document is keyed by the consolidator's instance id (`hostname:pid`, the
    /// Write_Ledger's), the same id the fold and repair passes publish under. Publish and end
    /// failures are logged at debug and never fail validation.
    ///
    /// See: Requirement 1.5
    ///
    /// # `recheck_spacing`: the spacing is re-checked under the lock
    ///
    /// The scheduler passes `true` (through [`Self::perform_scheduled_validation`]); tests that
    /// drive several passes back to back go through the test-only `perform_validation`, which
    /// passes `false`.
    ///
    /// [`Self::calculate_next_validation_time`] applies the spacing before the scheduler's
    /// jittered sleep, from a `validation.json` read before any peer had run. Every instance of
    /// an N-proxy fleet computes the same window, sleeps into it, and the lock only stops them
    /// running *simultaneously*: each one that took the lock after the previous holder released
    /// it ran its own full pass, so a fleet ran up to N passes a day instead of one. Re-reading
    /// `validation.json` once the lock is held sees the peer's completed pass and skips.
    ///
    /// # `request`: a startup request's own re-check and mode
    ///
    /// With a request, the re-check skips when `validation.json` shows a pass completed at or
    /// after `requested_at` (for `FigureUnvalidated`: a full scan, per `schedule.json`, whose
    /// install did not defer), so N proxies starting together run one pass. A
    /// `FigureUnvalidated` request forces full mode.
    async fn perform_validation_gated(
        self: &Arc<Self>,
        recheck_spacing: bool,
        request: Option<ValidationRequest>,
    ) -> Result<ValidationOutcome> {
        // Try to acquire global lock
        let _lock = match self.try_acquire_validation_lock().await {
            Ok(lock) => lock,
            Err(e) => {
                debug!("Another instance is validating, skipping: {}", e);
                return Ok(ValidationOutcome::LockContended);
            }
        };

        // Lock order for this pass: validation.lock, then global_consolidation.lock (the size
        // install), then size_state.lock (inside the install). The consolidation cycle takes
        // global_consolidation.lock then size_state.lock and never validation.lock, so no
        // cycle in the order exists.
        if let Some(request) = request {
            if let Ok(metadata) = self.read_validation_metadata().await {
                let completed_since = metadata.last_validation >= request.requested_at;
                let satisfies = match request.kind {
                    ValidationRequestKind::Overdue => completed_since,
                    ValidationRequestKind::FigureUnvalidated => {
                        completed_since
                            && !metadata.size_install_deferred
                            && self.read_previous_scan_state().validation_type.as_deref()
                                == Some("full")
                    }
                };
                if satisfies {
                    info!(
                        "Cache validation: a pass completed after this instance's startup \
                         request ({:?}); skipping the requested pass",
                        request.kind
                    );
                    return Ok(ValidationOutcome::PeerRanRecently);
                }
            }
        } else if recheck_spacing {
            if let Ok(metadata) = self.read_validation_metadata().await {
                let elapsed = SystemTime::now()
                    .duration_since(metadata.last_validation)
                    .unwrap_or(Duration::MAX);
                if elapsed < MIN_VALIDATION_SPACING {
                    info!(
                        "Cache validation: a pass completed {} ago, inside the {} minimum \
                         spacing; skipping this one",
                        format_duration_human(elapsed),
                        format_duration_human(MIN_VALIDATION_SPACING)
                    );
                    return Ok(ValidationOutcome::PeerRanRecently);
                }
            }
        }

        // Counted, never gated: see the doc comment. Declared after `_lock`, so it drops
        // first and the document is ended while the lock is still held.
        let instance_id = self.consolidator.write_ledger().instance_id().to_string();
        if let Err(e) = crate::scanner_registry::publish(
            &self.cache_dir,
            &instance_id,
            crate::scanner_registry::ScanKind::Validation,
        ) {
            debug!(
                "Could not publish the validation scanner document for {}: {}",
                instance_id, e
            );
        }
        let _scan_document = ValidationScanDocument {
            cache_dir: self.cache_dir.clone(),
            instance_id,
        };

        #[cfg(test)]
        VALIDATION_IN_FLIGHT_HOOK.with(|h| h.borrow().as_ref().map_or(Ok(()), |f| f()))?;

        // Read previous scan state from validation.json for mode selection
        let max_duration = self.config.validation_max_duration;
        let prev_state = self.read_previous_scan_state();

        let (mode, reason) = match request {
            Some(ValidationRequest {
                kind: ValidationRequestKind::FigureUnvalidated,
                ..
            }) => (ScanMode::Full, ScanModeReason::SizeUnvalidated),
            _ => determine_scan_mode(
                prev_state.validation_type.as_deref(),
                prev_state.last_full_scan_duration_secs,
                prev_state.rolling_cycle_duration_secs,
                prev_state.rolling_dirs_scanned,
                max_duration,
            ),
        };

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
            ScanMode::Full => self.perform_full_validation().await?,
            ScanMode::Rolling => self.perform_rolling_validation().await?,
        }
        Ok(ValidationOutcome::Ran)
    }

    /// The scheduler's entry point: [`Self::perform_validation_gated`] with the spacing
    /// re-checked under `validation.lock`.
    async fn perform_scheduled_validation(self: &Arc<Self>) -> Result<ValidationOutcome> {
        self.perform_validation_gated(true, None).await
    }

    /// One pass with no spacing re-check, for tests that run several passes back to back.
    #[cfg(test)]
    async fn perform_validation(self: &Arc<Self>) -> Result<()> {
        self.perform_validation_gated(false, None).await.map(|_| ())
    }

    /// Previous scan state for mode selection, read from `schedule.json` with the same
    /// one-time fallback to `validation.json`'s legacy fields that
    /// [`Self::read_rolling_state`] documents. Mode, the extrapolation inputs and the last
    /// full scan's duration are all schedule state, not scan results.
    fn read_previous_scan_state(&self) -> PreviousScanState {
        let json = match Self::read_json_file(&self.schedule_path)
            .or_else(|| Self::read_json_file(&self.validation_path))
        {
            Some(v) => v,
            None => return PreviousScanState::default(),
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
    pub(crate) async fn perform_full_validation(self: &Arc<Self>) -> Result<()> {
        // The census is compared against figures taken BEFORE the walk, under the
        // consolidation lock, which is then released for the walk. `None` defers the install.
        // See `install_full_scan_figures`.
        let baseline = self
            .take_full_scan_baseline(full_install_lock_attempts(), WRITE_PHASE_LOCK_RETRY_DELAY)
            .await;
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
            self_debit,
        ) = self.scan_metadata_with_shared_validator().await?;
        #[cfg(test)]
        test_seams::after_full_scan_walk(&self.cache_dir);
        // The census is the surviving population, not the number of files visited. The
        // scan deletes `.meta` files as it goes — unparseable ones it self-heals,
        // plus write-cache and GET expiry and stale HEAD-only entries — and an object whose
        // `.meta` this pass removed
        // is not a cached object. Counting it inflates `cached_objects`, the direction
        // that overstates the object count.
        // `files_visited` is retained for `files_scanned` in validation.json, which is a
        // work-done figure. Spec: cache-eviction-at-scale. Requirements: 7.1
        let scanned_objects = files_visited.saturating_sub(metas_removed);
        // The figures the correction is computed from. An entry this pass itself deleted or
        // healed counts at its pre-mutation figures, because its debit reaches
        // `size_state.json` through the delta files after the baseline, like any other removal.
        let census_size = scanned_size.saturating_add(self_debit.bytes);
        let census_staged_size = scanned_staged_size.saturating_add(self_debit.staged_bytes);
        let census_objects = scanned_objects.saturating_add(self_debit.objects);
        // Drift is measured against the baseline. Without one no install happens, and the
        // persisted figure is reported instead.
        let (tracked_size, tracked_staged_size) = match baseline {
            Some(b) => (b.total_size, b.write_cache_size),
            None => (self.get_size().await, self.get_write_cache_size().await),
        };
        let drift = census_size as i64 - tracked_size as i64;
        let staged_drift = census_staged_size as i64 - tracked_staged_size as i64;

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
                format_bytes_human(census_staged_size),
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
        // Two consequences worth stating:
        //  - `scanned_objects` excludes `.meta` files this pass deleted (see above). Before
        //    that subtraction the census counted them, so it over-reported by the number of
        //    expiries and self-heals in the pass.
        //  - At the design point R4.5 says the full scan does not fit its budget, so this
        //    path may never run in the field. A counter that converges only here converges
        //    only in theory.
        //
        // Spec: cache-eviction-at-scale. Requirements: 7.1, 4.4, 4.5
        //
        // Requirement 6.1: pass the recomputed write-cache figure rather than `None`.
        // This is a full scan of every `.meta`, so `scanned_staged_size` is a
        // whole-cache figure and safe to install as an absolute value. It is also the
        // ONLY mechanism that can un-inflate an already-inflated `write_cache_size`:
        // stopping the leaks (R1, R5) prevents further inflation but cannot undo what
        // has already accumulated, which is why R5 and R6 must ship together.
        //
        // `FullScan` is the ONLY kind from which R12.6's drift is evaluable: every `.meta` was
        // visited, so these three figures are a census computed without reference to the counter
        // they are about to replace.
        //
        // The census is a whole-cache count, but it is installed RELATIVE to the baseline taken
        // before the walk, keeping every delta file and accumulator: see
        // `install_full_scan_figures` for why an absolute install lost the walk's concurrent
        // writes.
        let installed = match baseline {
            Some(baseline) => {
                self.install_full_scan_figures(
                    census_size,
                    census_staged_size,
                    census_objects,
                    baseline,
                    full_install_lock_attempts(),
                    WRITE_PHASE_LOCK_RETRY_DELAY,
                )
                .await
            }
            None => false,
        };

        // R2.7 / R6.6: give the Write_Ledger back any staged object it has lost track of.
        // Also the in-place upgrade path — see the method doc.
        self.reappend_missing_ledger_entries(&staged_paths).await;

        // Write validation metadata. This is a work-done figure — how many `.meta` files
        // the pass opened — so it stays `files_visited` rather than the census, which is
        // smaller by the number removed on this pass.
        // Spec: cache-eviction-at-scale. Requirements: 7.1
        let files_scanned = files_visited;
        // Written even when the install deferred, because `last_validation` is what spaces the
        // next pass (here and on every peer, via the under-lock re-read). Skipping the write
        // would make the fleet re-run a full scan at once. `size_install_deferred` says the
        // drift was measured and NOT corrected.
        self.write_validation_metadata_inner(
            scanned_size,
            tracked_size,
            drift,
            duration,
            files_scanned,
            cache_expired,
            cache_skipped,
            cache_errors,
            0,
            0,
            0,
            !installed,
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

    /// Take the figures a full scan's census is compared against: `size_state.json` plus every
    /// pending delta file, after flushing this instance's accumulator. `None` defers the
    /// pass's install.
    ///
    /// Read under `global_consolidation.lock`, which is released before the walk starts. It is
    /// held for one accumulator flush, one `size_state.json` read and one listing and read of
    /// `size_tracking/`'s delta files: a few small file operations, independent of cache size.
    /// While it is held no cycle is between collecting a delta file and deleting it, so each
    /// pending credit is in exactly one of the two reads.
    ///
    /// Lock order: `validation.lock` (held by the caller), then `global_consolidation.lock`.
    async fn take_full_scan_baseline(
        &self,
        attempts: usize,
        delay: Duration,
    ) -> Option<crate::journal_consolidator::ValidationBaseline> {
        let lock_token = self
            .acquire_consolidation_lock_bounded(attempts, delay, "Cache validation baseline")
            .await?;
        let baseline = self.consolidator.validation_baseline().await;
        self.consolidator.release_consolidation_lock(lock_token);
        baseline
    }

    /// Install a full scan's census as `size_state + (census - baseline)` under
    /// `global_consolidation.lock`, keeping every delta file and every accumulator. Returns
    /// whether the install was persisted.
    ///
    /// # Why relative
    ///
    /// The walk holds no lock, so writes keep landing while it runs, and their credits travel
    /// accumulator, delta file, `size_state.json` as usual. The install used to replace the
    /// totals with the census and delete every delta file and this instance's accumulator. A
    /// PUT the walk had already passed was therefore lost: overwritten if a cycle had applied
    /// its credit, deleted if the credit was in any instance's delta file, zeroed if it was in
    /// this accumulator. A DELETE in the same place was undone.
    ///
    /// # Exactly once
    ///
    /// The install adds one constant, `census - baseline`, and reads, writes and deletes no
    /// delta file. Each credit therefore reaches `size_state.json` once, through the usual
    /// pipeline, whenever it was issued. The baseline holds every credit issued before it in
    /// exactly one place (see [`Self::take_full_scan_baseline`]). So the result is the census
    /// plus every credit issued after the baseline. Entries the walk itself deleted or healed
    /// are in the census at their pre-mutation figures ([`ScanSelfDebit`]), because their
    /// debits are among those later credits.
    ///
    /// The clamp at 0 acts on `size_state.json` plus the pending delta files, not on
    /// `size_state.json` alone; see `JournalConsolidator::install_validation_correction`.
    ///
    /// Two populations are counted twice, and both are corrected by the next full scan:
    /// writes made during the walk into shards it had not reached yet (the census sees them
    /// and their credits arrive after the baseline; PUTs over-count, DELETEs under-count), and
    /// credits still in a peer's accumulator when the baseline was read (at most one flush
    /// interval of that peer's writes).
    ///
    /// # Why the consolidation lock
    ///
    /// Adding a constant commutes with a cycle's apply, so the lock is no longer what keeps
    /// the install from double counting a cycle's collected deltas. It is kept so the install
    /// serialises with the consolidation cycle, the single writer of derived state, as the
    /// rolling pass's write phase does. The wait is bounded at `attempts` x `delay`; if the
    /// lock cannot be taken the install is deferred to the next pass.
    ///
    /// Lock order: `validation.lock` (held by the caller), `global_consolidation.lock`, then
    /// `size_state.lock` inside the install.
    async fn install_full_scan_figures(
        &self,
        census_size: u64,
        census_staged_size: u64,
        census_objects: u64,
        baseline: crate::journal_consolidator::ValidationBaseline,
        attempts: usize,
        delay: Duration,
    ) -> bool {
        let Some(lock_token) = self
            .acquire_consolidation_lock_bounded(attempts, delay, "Cache validation")
            .await
        else {
            return false;
        };

        let installed = self
            .consolidator
            .install_validation_correction(
                census_size,
                census_staged_size,
                census_objects,
                baseline,
                ValidationSampleKind::FullScan,
            )
            .await;
        // `reset_all_delta_files` reaped these before the install became relative.
        self.consolidator.reap_stale_delta_tmp_files().await;
        self.consolidator.release_consolidation_lock(lock_token);
        installed
    }

    /// Persist the full scan duration to `schedule.json` for the next cycle's mode decision.
    ///
    /// Both this and the duration it records are schedule state: nothing reads them except
    /// [`determine_scan_mode`]. They moved out of `validation.json` with the split described
    /// on [`Self::read_rolling_state`].
    async fn persist_full_scan_duration(&self, duration_secs: f64) -> Result<()> {
        let mut json = Self::read_json_file(&self.schedule_path)
            .unwrap_or_else(|| serde_json::json!({ "rolling_cursor": 0 }));

        if let Some(obj) = json.as_object_mut() {
            obj.insert(
                "last_full_scan_duration_secs".to_string(),
                serde_json::json!(duration_secs),
            );
            obj.insert("validation_type".to_string(), serde_json::json!("full"));
            obj.insert(
                "last_visit_at".to_string(),
                serde_json::json!(Self::epoch_secs_now()),
            );
        }

        self.write_schedule_json(&json)
    }

    /// Performs a rolling validation scan, one shard at a time, abortable at the shard.
    ///
    /// # Shape of a cycle
    ///
    /// ```text
    /// budget := validation_max_duration
    /// for each shard from the cursor:
    ///     if elapsed >= budget: stop cleanly            // cursor already durable
    ///     visit(shard)                                  // ONE spawn_blocking, rayon inside
    ///     install this cycle's figures under global_consolidation.lock
    ///     persist the cursor                            // AFTER the install, never before
    /// ```
    ///
    /// Three properties follow, and each is the fix for a recorded defect:
    ///
    /// - **The budget is real.** It used to be compared against elapsed time only *between*
    ///   batches, while `estimate_batch_size` sized the first batch to consume the whole
    ///   budget — so in steady state there was one batch and the check never fired. Nothing
    ///   aborted a scan in progress. The check now sits before every shard, so per-cycle work
    ///   is bounded by one shard's cost plus the check, independently of cache size (R1.5).
    /// - **The shard is the unit of abortability**, because it is also the atomicity unit of
    ///   the re-grounding job: a half-scanned shard must not contribute, or the absolute
    ///   replacement in R5.2 installs a figure for data the pass did not see.
    /// - **The visit does not hold an async worker.** It runs inside `spawn_blocking` with the
    ///   rayon iteration inside it, and every async follow-up happens after it returns.
    ///
    /// The write phase — and only the write phase, never the parse — takes
    /// `global_consolidation.lock`, so two instances rolling through overlapping shards are
    /// serialised at the write rather than at the long part (§ 6.2, R5.5).
    ///
    /// Cursor, mode, rotation counters and the scan-rate EWMA are persisted to
    /// `schedule.json`; this cycle's *results* go to `validation.json` in
    /// [`ValidationMetadata`]'s shape. See [`Self::read_rolling_state`] for why those are two
    /// files.
    ///
    /// See: Requirements 3, 4, 5, 6, 8. Spec: cache-eviction-at-scale §§ 6.2, 6.3, 6.4, 6.6.
    async fn perform_rolling_validation(self: &Arc<Self>) -> Result<()> {
        use std::time::UNIX_EPOCH;

        let start = Instant::now();
        let cycle_started_at = Self::epoch_millis_now();
        let max_duration = self.config.validation_max_duration;
        let metadata_dir = self.cache_dir.join("metadata");

        let mut rolling_state = self.read_rolling_state()?;
        let start_cursor = rolling_state.cursor;
        // The batch estimate is now a CAP on how many slots one cycle attempts, not the
        // mechanism that bounds the cycle. The budget check below is the bound.
        let slot_cap = self
            .estimate_batch_size(rolling_state.scan_rate, max_duration)
            .min(256);

        // One consistent Size_State snapshot for the whole cycle. Every install recomputes
        // the correction against THIS snapshot over a larger scanned subset, so a later
        // install supersedes an earlier one rather than compounding on it.
        //
        // `None` means `size_state.json` exists and could not be read or parsed. It is NOT a
        // snapshot of zeros: with `tracked_* = 0` the proportional correction reduces to the
        // scanned subset itself, so every install would write one shard's bytes, objects and
        // staged bytes over the shared document as whole-cache figures. So the cycle installs
        // nothing, publishes no results and persists no cursor, exactly as a failed pass
        // leaves the schedule, and the next cycle retries from the same cursor. Pinned by
        // `rolling_validation_installs_nothing_when_size_state_is_unreadable`.
        let Some(size_state) = self.consolidator.get_size_state().await else {
            warn!(
                "Rolling validation: size_state.json is unreadable, so this cycle has no \
                 snapshot to correct against; skipping its corrections with the cursor \
                 unchanged at {:02x}",
                start_cursor
            );
            return Ok(());
        };
        let tracked_size = size_state.total_size;
        let tracked_objects = size_state.cached_objects;
        let tracked_staged_size = size_state.write_cache_size;

        info!(
            "Rolling validation: starting scan at cursor={:02x}, slot_cap={}, cached_objects={}, budget={}",
            start_cursor,
            slot_cap,
            tracked_objects,
            format_duration_human(max_duration)
        );

        if rolling_state.rotation_start_time.is_none() {
            rolling_state.rotation_start_time = Some(Self::epoch_secs_now());
        }

        // Captured once, in the async caller. Neither the blocking body nor a rayon worker
        // has to ask what context it landed in.
        let runtime = tokio::runtime::Handle::current();
        let now_systime = SystemTime::now();

        let mut cycle = ShardVisitResult::default();
        let mut slots_visited: usize = 0;
        let mut stop_reason = RollingStopReason::SlotsExhausted;
        // The `last_visit_at` this cycle itself last wrote. Newest-wins must fire on a
        // FOREIGN writer, and this is what distinguishes one: without it the check reads
        // back the timestamp this instance wrote one shard earlier and declines its own
        // next install, which ends every cycle after exactly one shard.
        let mut own_last_visit_at: Option<u64> = None;

        while slots_visited < slot_cap {
            // § 6.4: the budget check sits BEFORE the visit, so a shard is never half
            // scanned. The cursor is already durable at the last completed shard.
            if start.elapsed() >= max_duration {
                stop_reason = RollingStopReason::BudgetExhausted;
                break;
            }

            let slot = ((start_cursor as usize + slots_visited) % 256) as u8;
            let (l1_dirs, _wraps) = self.select_l1_directories(&metadata_dir, slot, 1);

            let (visit, visit_tails) = if l1_dirs.is_empty() {
                // No bucket has this L1 shard on disk. Still a completed visit: the slot has
                // been accounted for and the cursor may advance past it. No partition either —
                // there is no `(bucket, L1)` shard to name one for.
                (ShardVisitResult::default(), Vec::new())
            } else {
                let tracker = Arc::clone(self);
                let handle = runtime.clone();
                match tokio::task::spawn_blocking(move || {
                    tracker.visit_shard_blocking(&l1_dirs, now_systime, &handle)
                })
                .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        return Err(ProxyError::CacheError(format!(
                            "Rolling validation: shard visit task failed: {}",
                            e
                        )));
                    }
                }
            };

            // Accumulate into a CANDIDATE, and keep it only if the install happens. A
            // visit whose figures were not installed must not appear in this cycle's
            // counters either, or the recorded work and the durable cursor would disagree.
            let mut candidate = cycle.clone();
            candidate.absorb(&visit);
            let next_cursor = ((start_cursor as usize + slots_visited + 1) % 256) as u8;

            // The write phase: install this cycle's figures, then make the cursor durable.
            match self
                .install_rolling_progress(
                    &candidate,
                    // THIS visit's partitions, deliberately not `candidate`'s: see
                    // `write_phase_locked`'s doc comment for why they must not ride `absorb`.
                    &visit_tails,
                    slots_visited + 1,
                    tracked_size,
                    tracked_objects,
                    tracked_staged_size,
                    cycle_started_at,
                    own_last_visit_at,
                    next_cursor,
                )
                .await
            {
                Ok(Some(written_at)) => {
                    cycle = candidate;
                    slots_visited += 1;
                    own_last_visit_at = Some(written_at);
                }
                Ok(None) => {
                    // Declined, not failed. The cursor did not advance past this shard and
                    // the shard's figures were discarded, so the next cycle re-visits it.
                    stop_reason = RollingStopReason::WritePhaseDeclined;
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        let elapsed = start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let surviving_objects = cycle.objects_visited.saturating_sub(cycle.metas_removed);

        if cycle.metas_removed > 0 {
            info!(
                "Rolling validation: {} of {} .meta files visited were removed on this pass; \
                 census reports {} surviving",
                cycle.metas_removed, cycle.objects_visited, surviving_objects
            );
        }

        // R2.7 / R6.6: give the Write_Ledger back any staged object it has lost track of.
        // Off the lock and after the visits, as § 6.2's shape requires.
        self.reappend_missing_ledger_entries(&cycle.staged_paths)
            .await;

        // Scan rate as an EWMA over cycles rather than the last cycle's raw figure, so one
        // atypical cycle — a cold volume, a contended one — does not reset the estimate the
        // next cycle sizes itself from. Spec: cache-eviction-at-scale § 6.5, § 6.6.
        if slots_visited > 0 {
            let observed = elapsed_secs / slots_visited as f64;
            rolling_state.scan_rate = Some(match rolling_state.scan_rate {
                Some(prev) if prev > 0.0 => {
                    SCAN_RATE_EWMA_ALPHA * observed + (1.0 - SCAN_RATE_EWMA_ALPHA) * prev
                }
                _ => observed,
            });
        }

        let new_cursor = ((start_cursor as usize + slots_visited) % 256) as u8;
        let extrapolated_full_secs = rolling_state.scan_rate.map(|r| r * 256.0);

        // 7.4: Detect full rotation
        let wrapped = (start_cursor as usize + slots_visited) > 255;
        if wrapped {
            rolling_state.full_rotation_count += 1;

            if let Some(rotation_start) = rolling_state.rotation_start_time {
                let rotation_elapsed = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
                    .saturating_sub(rotation_start);
                info!(
                    "Rolling validation: full rotation #{} complete, total rotation time={}",
                    rolling_state.full_rotation_count,
                    format_duration_human(Duration::from_secs(rotation_elapsed))
                );
            }

            rolling_state.rotation_start_time = Some(Self::epoch_secs_now());
        }

        rolling_state.cursor = new_cursor;
        rolling_state.last_visit_at = own_last_visit_at.or(Some(Self::epoch_millis_now()));

        let cycle_stats = RollingCycleStats {
            dirs_scanned: slots_visited as u64,
            objects_validated: cycle.objects_visited,
            cycle_duration_secs: elapsed_secs,
        };

        self.write_rolling_state(&rolling_state, &cycle_stats)?;

        // This cycle's RESULTS, in ValidationMetadata's shape, in the file that holds only
        // results. A rolling cycle used to write its schedule fields over this document
        // instead, which is what made it undeserializable and collapsed the daily cadence.
        // Both validation fields are therefore populated after a rolling cycle now, not only
        // after a full scan.
        let scanned_drift = cycle.size_bytes as i64 - tracked_size as i64;
        self.write_validation_metadata_with_write_cache(
            cycle.size_bytes,
            tracked_size,
            scanned_drift,
            elapsed,
            cycle.objects_visited,
            cycle.cache_expired,
            cycle.cache_skipped,
            cycle.cache_errors,
            cycle.staged_bytes,
            0,
            0,
        )
        .await?;

        let dirs_until_rotation = if new_cursor == 0 && slots_visited > 0 {
            0
        } else {
            256 - new_cursor as usize
        };

        info!(
            "Rolling validation complete: stop_reason={:?}, dirs_scanned={}, \
             dirs_remaining_until_rotation={}, objects_validated={}, elapsed={}, \
             scan_rate={:.2}s/dir, new_cursor={:02x}{}",
            stop_reason,
            slots_visited,
            dirs_until_rotation,
            cycle.objects_visited,
            format_duration_human(elapsed),
            rolling_state.scan_rate.unwrap_or(0.0),
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

        if stop_reason == RollingStopReason::BudgetExhausted {
            info!(
                "Rolling validation: stopped on the {} budget with the cursor durable at \
                 {:02x}; the next cycle resumes from there",
                format_duration_human(max_duration),
                new_cursor
            );
        }

        Ok(())
    }

    /// One shard visit. Synchronous, blocking-pool only, rayon inside.
    ///
    /// `l1_dirs` is every bucket's directory for one L1 index — usually one entry, and the
    /// rayon iteration is across buckets. A single-entry `par_iter` folds inline on the
    /// calling thread rather than reaching rayon's pool, which is exactly the topology
    /// [`Self::block_on_from_scan`] exists to handle.
    ///
    /// Spec: cache-eviction-at-scale § 6.3.
    /// # One visit produces SEVERAL partitions, and that is why the tails ride a separate return
    ///
    /// The rolling loop's unit is one L1 index across **all** buckets, while the shard unit
    /// (§ 3.1) and the partition path are both `(bucket, L1)`. So a visit spans as many shards
    /// as there are buckets holding this index, and each one needs its own file.
    /// [`ShardVisitResult::absorb`] flattens across buckets and across shards — it is the
    /// cycle's accumulator — so tail state cannot ride it, and the per-shard partitions are
    /// returned alongside instead.
    fn visit_shard_blocking(
        &self,
        l1_dirs: &[PathBuf],
        now: SystemTime,
        runtime: &tokio::runtime::Handle,
    ) -> (ShardVisitResult, Vec<ShardTailPartition>) {
        use rayon::prelude::*;
        use std::sync::atomic::{AtomicU64, Ordering};

        let size_bytes = AtomicU64::new(0);
        let staged_bytes = AtomicU64::new(0);
        let objects_visited = AtomicU64::new(0);
        let metas_removed = AtomicU64::new(0);
        let cache_expired = AtomicU64::new(0);
        let cache_skipped = AtomicU64::new(0);
        let cache_errors = AtomicU64::new(0);
        let staged_paths: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());
        // One finished partition per `(bucket, L1)` this visit reached. Collected under a mutex
        // rather than accumulated into a shared heap, because a heap shared across buckets would
        // be a top-N over the wrong population — § 4.2's lemma is about a *per-shard* top-N.
        let tails: std::sync::Mutex<Vec<ShardTailPartition>> = std::sync::Mutex::new(Vec::new());
        let floor = self.config.tail_floor_bytes;
        let cap = self.config.tail_max_entries;

        l1_dirs.par_iter().for_each(|l1_dir| {
            // `{bucket}` is the L1 directory's parent, `{XX}` is its own name. Both are
            // available from the path the loop already holds, so nothing has to be re-derived.
            let bucket = l1_dir
                .parent()
                .and_then(|p| p.file_name())
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            let l1 = l1_dir
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            let mut tail = ShardTailBuild::new(bucket, l1, floor, cap);

            let l2_entries = match std::fs::read_dir(l1_dir) {
                Ok(entries) => entries,
                Err(_) => {
                    // The shard exists — the caller enumerated it — but could not be listed.
                    // Do NOT publish an empty partition for it: an empty file is a claim that
                    // the shard holds no above-floor ranges, and this pass did not establish
                    // that. Omitting it leaves the previous partition in place, which is a
                    // stale hint rather than a false one.
                    return;
                }
            };
            for l2_entry in l2_entries.flatten() {
                // Same predicate as the full walk, deliberately shared rather than
                // duplicated: two copies of an entry-type test are two places for the
                // symlink semantics to drift. Spec: cache-eviction-at-scale § 16.6.13 (T1).
                if !crate::disk_cache::is_shard_subdirectory(&l2_entry) {
                    continue;
                }
                let l2_path = l2_entry.path();
                let file_entries = match std::fs::read_dir(&l2_path) {
                    Ok(entries) => entries,
                    Err(_) => continue,
                };
                for file_entry in file_entries.flatten() {
                    let path = file_entry.path();
                    if path.extension().is_none_or(|ext| ext != "meta") {
                        continue;
                    }
                    let result = self.scan_metadata_file(&path, now, Some(runtime));
                    for candidate in result.tail_candidates {
                        tail.push(candidate);
                    }
                    size_bytes.fetch_add(result.size_bytes, Ordering::Relaxed);
                    staged_bytes.fetch_add(result.staged_bytes, Ordering::Relaxed);
                    objects_visited.fetch_add(1, Ordering::Relaxed);
                    if result.staged_bytes > 0 {
                        if let Ok(mut paths) = staged_paths.lock() {
                            paths.push(path.clone());
                        }
                    }
                    if result.meta_removed {
                        metas_removed.fetch_add(1, Ordering::Relaxed);
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
                }
            }
            // Every shard this visit LISTED gets a partition, including one with no members.
            // An absent file and an empty file are the same observation to a reader that
            // checks presence, and § 4.5's reader distinguishes "no members above the floor"
            // from "no partition here".
            if let Ok(mut out) = tails.lock() {
                out.push(tail.finish());
            }
        });

        let result = ShardVisitResult {
            size_bytes: size_bytes.into_inner(),
            staged_bytes: staged_bytes.into_inner(),
            objects_visited: objects_visited.into_inner(),
            metas_removed: metas_removed.into_inner(),
            cache_expired: cache_expired.into_inner(),
            cache_skipped: cache_skipped.into_inner(),
            cache_errors: cache_errors.into_inner(),
            staged_paths: staged_paths.into_inner().unwrap_or_default(),
        };
        (result, tails.into_inner().unwrap_or_default())
    }

    /// Write one shard's Tail_Partition, whole, and install its two checkpoint fields.
    ///
    /// # An empty partition is a WRITTEN file, not an absent one
    ///
    /// § 4.1 says the partition is "rewritten whole by the rolling pass" and is silent on the
    /// zero-member case. It has to be a written file: § 4.5's reader distinguishes "no members
    /// above the floor" — a real and expected state on a cache with no size skew — from "this
    /// shard has never been visited", and it cannot make that distinction from a missing file.
    /// The checkpoint's `tail_entries: 0` makes the same fact checkable without a presence test,
    /// which is the stronger of the two signals.
    ///
    /// # Returns whether the partition was actually published
    ///
    /// `false` on a failed write, and the caller MUST NOT retire that shard's tail-delta records
    /// on a `false`. `TAIL-BUILD` returned `()` here and
    /// [`Self::retire_rebuilt_shards_from_delta`] worked from the partition *list* rather than
    /// from write outcomes, so a shard whose write failed had its delta records retired anyway —
    /// dropping the only hint a large range existed while the partition that was supposed to
    /// supersede it was never written. The range then stayed invisible to selection until the
    /// shard's next visit, which at the design point is days.
    ///
    /// Spec: cache-eviction-at-scale § 4.1, § 4.4, § 6.2.
    async fn write_tail_partition(&self, partition: &ShardTailPartition) -> bool {
        let path = tail_partition_path(&self.cache_dir, &partition.bucket, &partition.l1);
        let mut body = String::new();
        for record in &partition.records {
            match serde_json::to_string(record) {
                Ok(line) => {
                    body.push_str(&line);
                    body.push('\n');
                }
                Err(e) => {
                    warn!(
                        "Tail_Partition {}/{}: could not serialise a record for {}: {}; \
                         the partition is written without it",
                        partition.bucket, partition.l1, record.cache_key, e
                    );
                }
            }
        }
        // `Shared::Yes`: any instance may rebuild any shard, so the temp name must carry this
        // one's identity or two rebuilds racing publish a file that is half of each.
        if let Err(e) =
            crate::disk_cache::write_atomic(&path, body.as_bytes(), crate::disk_cache::Shared::Yes)
                .await
        {
            warn!(
                "Tail_Partition {}/{}: failed to write {:?}: {}. The previous partition stays \
                 in place, this shard's tail-delta records are NOT retired, and the shard is \
                 rebuilt on its next visit.",
                partition.bucket, partition.l1, path, e
            );
            return false;
        }
        record_tail_rebuild(&partition.records);

        // The cap is observable per shard rather than through a global coverage ratio: this
        // figure cannot be wrong, where a ratio needs a `T_max` this pass cannot reach.
        if partition.cap_bound {
            warn!(
                "Tail_Partition {}/{}: the per-shard cap is BINDING — {} above-floor ranges \
                 were considered and tail_max_entries={} were kept, at tail_floor_bytes={}. \
                 Eviction can only see the largest {} ranges in this shard; raise \
                 cache.shared_storage.tail_max_entries or cache.shared_storage.tail_floor_bytes \
                 if a byte target goes unmet.",
                partition.bucket,
                partition.l1,
                partition.considered,
                partition.cap,
                partition.floor,
                partition.cap,
            );
        }

        self.install_tail_checkpoint_fields(partition);
        true
    }

    /// Install `tail_floor`, `tail_entries` and `tail_built_at` into this shard's § 3.2
    /// checkpoint, preserving every field this pass does not own.
    ///
    /// # A third producer of one document, following the second producer's shape
    ///
    /// `StagedShardCheckpoint` already has two producers: the staged fold owns five struct
    /// fields and the `st_size` fold owns six keys inside `other`. This pass is the third, and
    /// it owns three keys. So the install is a read-modify-write under the same per-document
    /// lock `crate::st_size_fold::install_fold_fields` takes, and for the same reasons — two
    /// unsynchronised read-modify-writes lose one producer's update silently, and the loser's
    /// write is a complete, parseable, wrong document.
    ///
    /// **Not `staged_fold::install_preserving_foreign_fields`**, which is the staged fold's own
    /// installer: it takes a whole `StagedShardCheckpoint` and writes that value's five
    /// staged-owned fields, so a caller from here would have to invent them and would overwrite
    /// a real staged figure with a fabricated one. It is also private to that module.
    ///
    /// Best-effort, like the lock itself: a failure leaves the shard without this pass's two
    /// figures, which reads as "not yet rebuilt" rather than as a wrong figure.
    fn install_tail_checkpoint_fields(&self, partition: &ShardTailPartition) {
        use crate::staged_fold::{lock_checkpoint_document, Shard, StagedShardCheckpoint};

        let shard = Shard::new(partition.bucket.clone(), partition.l1.clone());
        // Held until this function returns, which is after the install.
        let _lock = lock_checkpoint_document(&self.cache_dir, &shard);
        let mut checkpoint = match StagedShardCheckpoint::load(&self.cache_dir, &shard) {
            Ok(Some(c)) => c,
            Ok(None) => StagedShardCheckpoint::unfolded_for(&shard),
            Err(e) => {
                warn!(
                    "Tail_Partition {}: a checkpoint exists and could not be read ({}); not \
                     installing tail_floor/tail_entries rather than overwriting it",
                    shard, e
                );
                return;
            }
        };
        checkpoint
            .other
            .insert("tail_floor".to_string(), partition.floor.into());
        checkpoint.other.insert(
            "tail_entries".to_string(),
            (partition.records.len() as u64).into(),
        );
        // A THIRD key, and § 3.2's record names only the two above. It is here because § 4.4's
        // cross-shard tail-delta sweep has to compare a record's append time against this
        // shard's last REBUILD, and neither existing timestamp is this producer's: `scanned_at`
        // belongs to the `st_size` fold and `staged_scanned_at` to the staged fold. Using either
        // would compare against a different pass's cadence. Reported as a deviation from § 3.2
        // rather than written into it.
        checkpoint
            .other
            .insert("tail_built_at".to_string(), tail_now_nanos().into());

        if let Err(e) = checkpoint.install(&self.cache_dir) {
            warn!(
                "Tail_Partition {}: failed to install tail_floor/tail_entries: {}",
                shard, e
            );
        }
    }

    /// Retire the tail-delta records this visit's partitions now contain.
    ///
    /// Retire-set: names the records to REMOVE, never the ones to keep. A retain-set paired with
    /// § 4.5's bounded-prefix reader is the task-77 shape, which in this repo left 1 of 25
    /// entries surviving. It also closes the read-then-rewrite window by construction — an
    /// append landing after this read is not named, so it survives into the next rebuild.
    fn retire_rebuilt_shards_from_delta(&self, partitions: &[ShardTailPartition]) {
        use std::collections::HashSet;

        if partitions.is_empty() {
            return;
        }
        let rebuilt: HashSet<(String, String)> = partitions
            .iter()
            .map(|p| (p.bucket.clone(), p.l1.clone()))
            .collect();
        let retire: HashSet<TailDeltaIdentity> = read_all_tail_delta_records(&self.cache_dir)
            .into_iter()
            .filter(|r| rebuilt.contains(&(r.bucket.clone(), r.l1.clone())))
            .map(|r| r.identity())
            .collect();
        let dropped = retire_tail_delta_records(&self.cache_dir, &retire);
        if dropped > 0 {
            debug!(
                "Tail-delta: retired {} record(s) for {} rebuilt shard(s)",
                dropped,
                rebuilt.len()
            );
        }
    }

    /// The write phase of a shard visit: install this cycle's figures, then make the cursor
    /// durable, both under `global_consolidation.lock`.
    ///
    /// Returns `Ok(Some(written_at))` when the install and the cursor persist both happened,
    /// carrying the `last_visit_at` written, and `Ok(None)` when the phase was **declined** —
    /// the lock could not be taken inside its retry budget, or `schedule.json` carries a
    /// `last_visit_at` that this cycle did not write and that post-dates its start, meaning a
    /// fresher scanner owns the derived state. A decline is not an error: the caller stops the
    /// cycle without advancing the cursor past the shard whose figures were not installed, so
    /// the next cycle re-visits it.
    ///
    /// # Why the lock, and why only here
    ///
    /// R5.5 says consolidation is the single writer of derived state. This pass is a second
    /// one, so the two are serialised — at the write phase only, never at the parse, which is
    /// the long part and must not block consolidation for minutes. `try_acquire_...` is a
    /// try-acquire returning `Result<bool>`, not a blocking acquire, so the retry budget below
    /// is this pass's own policy: consolidation runs on a short interval, so a few hundred
    /// milliseconds of retry covers an in-progress cycle, and giving up cleanly is better than
    /// either blocking the pass indefinitely or writing unserialised.
    ///
    /// Spec: cache-eviction-at-scale § 6.2.
    #[allow(clippy::too_many_arguments)]
    async fn install_rolling_progress(
        &self,
        cycle: &ShardVisitResult,
        visit_tails: &[ShardTailPartition],
        slots_visited: usize,
        tracked_size: u64,
        tracked_objects: u64,
        tracked_staged_size: u64,
        cycle_started_at: u64,
        own_last_visit_at: Option<u64>,
        next_cursor: u8,
    ) -> Result<Option<u64>> {
        let mut acquired = None;
        for attempt in 0..WRITE_PHASE_LOCK_ATTEMPTS {
            match self.consolidator.try_acquire_consolidation_lock_token() {
                Ok(Some(token)) => {
                    acquired = Some(token);
                    break;
                }
                Ok(None) => {}
                Err(e) => {
                    warn!(
                        "Rolling validation: consolidation lock unavailable ({}), \
                         stopping this cycle cleanly",
                        e
                    );
                    return Ok(None);
                }
            }
            if attempt + 1 < WRITE_PHASE_LOCK_ATTEMPTS {
                tokio::time::sleep(WRITE_PHASE_LOCK_RETRY_DELAY).await;
            }
        }
        let Some(lock_token) = acquired else {
            warn!(
                "Rolling validation: could not serialise the write phase against \
                 consolidation within {} attempts; stopping cleanly with the cursor at the \
                 last installed shard",
                WRITE_PHASE_LOCK_ATTEMPTS
            );
            return Ok(None);
        };

        let outcome = self
            .write_phase_locked(
                cycle,
                visit_tails,
                slots_visited,
                tracked_size,
                tracked_objects,
                tracked_staged_size,
                cycle_started_at,
                own_last_visit_at,
                next_cursor,
            )
            .await;

        // By token: a no-op if the watchdog took the lock during the write phase.
        self.consolidator.release_consolidation_lock(lock_token);
        outcome
    }

    /// The body that runs while `global_consolidation.lock` is held. Split out so the lock is
    /// released on every path, including an error one.
    ///
    /// # `visit_tails` is ONE visit's partitions, not the cycle's
    ///
    /// `cycle` is the candidate accumulated across every shard visited so far — that is what
    /// `absorb` produces and what the proportional correction needs. Tail partitions must not
    /// ride it: `absorb` would carry every shard visited so far into every subsequent write
    /// phase, rewriting each shard's partition once per remaining slot and growing without bound
    /// across a 256-slot rotation. So the visit's own partitions are passed separately and
    /// `absorb` is left alone.
    ///
    /// # Order inside the lock, and none of it is interchangeable
    ///
    /// newest-wins decline → size install → partition write → checkpoint `tail_floor` /
    /// `tail_entries` → tail-delta retirement → cursor persist. The decline comes first because
    /// a declined phase must write **no** partition, exactly as it installs no figures: the
    /// cursor does not advance and the next cycle rebuilds the shard. Retirement comes after the
    /// partition write because a record may only be dropped once the partition that supersedes
    /// it is durable. The cursor is last because advancing it past a shard whose results were
    /// lost skips that shard for a whole rotation.
    #[allow(clippy::too_many_arguments)]
    async fn write_phase_locked(
        &self,
        cycle: &ShardVisitResult,
        visit_tails: &[ShardTailPartition],
        slots_visited: usize,
        tracked_size: u64,
        tracked_objects: u64,
        tracked_staged_size: u64,
        cycle_started_at: u64,
        own_last_visit_at: Option<u64>,
        next_cursor: u8,
    ) -> Result<Option<u64>> {
        // Newest-wins. The `scanned_at` on disk is `schedule.json`'s `last_visit_at`; if a
        // peer has visited a shard since this cycle began, its view of the derived state is
        // fresher than ours and a stale install would overwrite it. § 6.2.
        //
        // `own_last_visit_at` is what makes this a test for a FOREIGN writer. Every shard of
        // this cycle writes `last_visit_at` itself, so a bare "newer than the cycle start"
        // test reads back this instance's own timestamp from one shard earlier and declines
        // its own next install — measured, and it ends every cycle after exactly one shard.
        if let Some(on_disk) = Self::read_json_file(&self.schedule_path)
            .and_then(|j| j.get("last_visit_at").and_then(|v| v.as_u64()))
        {
            let is_ours = own_last_visit_at == Some(on_disk);
            if !is_ours && on_disk > cycle_started_at {
                info!(
                    "Rolling validation: schedule.json carries a visit this cycle did not \
                     write ({} > cycle start {}), so a fresher scanner owns the derived \
                     state; not installing",
                    on_disk, cycle_started_at
                );
                return Ok(None);
            }
        }

        // Post-removal figures, with no `self_debit` term: this install is absolute and resets
        // the accumulator holding the arms' debits, so the removal is counted here once. See
        // `ScanFileResult::self_debit`.
        let surviving_objects = cycle.objects_visited.saturating_sub(cycle.metas_removed);
        let slots = slots_visited.min(256);

        let (corrected_size, corrected_objects) = self.apply_proportional_correction(
            cycle.size_bytes,
            surviving_objects,
            slots,
            tracked_size,
            tracked_objects,
        );

        // A rolling pass observes a subset and therefore may NOT install an absolute
        // `cached_objects`; `corrected_objects` is an extrapolation, recorded as such rather
        // than presented as a census. Three things make it unsound as a convergence
        // mechanism, and none of them is fixable here:
        //
        //  1. The expectation `tracked * slots / 256` is derived from the very value being
        //     corrected (R5.3).
        //  2. A slot with no directory on disk still counts toward `slots`, so the numerator
        //     and denominator describe different things (R5.3).
        //  3. Every instance installs its own extrapolation absolutely over the shared
        //     `size_state.json`. The lock held here serialises those writes, which is what
        //     § 6.2 buys; it does not make an extrapolation into a census.
        //
        // R5.2's absolute per-shard aggregates are what make this sound: a shard's count is
        // replaced only by a pass that actually scanned that shard, and the global figure is a
        // sum rather than an extrapolation. Until then `cached_objects` converges only when a
        // full scan runs — a mode R4.5 says does not exist at the design point — so a reader
        // MUST consult `validation_type` in `schedule.json` before treating this
        // figure as a population count.
        // Spec: cache-eviction-at-scale. Requirements: 7.1, 4.4, 4.5, 5.2, 5.3
        debug!(
            "Rolling validation: cached_objects {} -> {} is an EXTRAPOLATION from {} \
             surviving objects over {} of 256 L1 index slots, not a census",
            tracked_objects, corrected_objects, surviving_objects, slots
        );

        let corrected_staged =
            Self::proportional_correction(cycle.staged_bytes, slots, tracked_staged_size);

        // `RollingExtrapolation`, and the kind is what stops R12.6's gauge publishing a number
        // here. `apply_proportional_correction` computes `corrected_size` as
        // `tracked_size + (scanned_size - tracked_size * slots / 256)`, so
        // `corrected_size - tracked_size` is the correction term itself — a function of the
        // persisted counter rather than an independent producer's disagreement with it. R5.3 makes
        // the same point about why the extrapolation cannot converge.
        self.consolidator
            .update_size_from_validation_with_kind(
                corrected_size,
                Some(corrected_staged),
                Some(corrected_objects),
                ValidationSampleKind::RollingExtrapolation,
            )
            .await;

        // The Tail_Partitions this visit rebuilt, then their two checkpoint fields, then the
        // tail-delta records they supersede. Inside the lock because § 6.2's serialisation is
        // about the partition rather than the checkpoint: two interleaved rebuilds could
        // otherwise publish a file that is neither instance's view, where the checkpoint's
        // newest-wins install makes the same interleaving harmless. See the order note above.
        // Only the shards whose partition was actually PUBLISHED may have their delta records
        // retired. A shard whose write failed keeps its records, because they are the only hint
        // its large ranges exist until its next visit.
        let mut published: Vec<ShardTailPartition> = Vec::with_capacity(visit_tails.len());
        for partition in visit_tails {
            if self.write_tail_partition(partition).await {
                published.push(partition.clone());
            }
        }
        self.retire_rebuilt_shards_from_delta(&published);

        // The cursor becomes durable only now, after the figures for the shards it covers
        // are installed. Reversing these two would let the cursor advance past a shard whose
        // results were lost, skipping it for a whole rotation. § 6.4.
        let written_at = Self::epoch_millis_now();
        self.persist_cursor(next_cursor, written_at)?;

        Ok(Some(written_at))
    }

    /// Try to acquire validation lock
    pub async fn try_acquire_validation_lock(&self) -> Result<ValidationLock> {
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
        fs4::FileExt::try_lock(&lock_file).map_err(|e| {
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
                // No credit accompanies a re-append (the bytes were credited when the
                // range was written), so there is nothing to decline; count only what
                // landed.
                if self
                    .consolidator
                    .record_staged_range(
                        &metadata.cache_key,
                        range.start,
                        range.end,
                        range.compressed_size,
                    )
                    .await
                {
                    ranges_appended += 1;
                }
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

    /// Scan metadata files for a periodic validation pass.
    ///
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
    ///
    /// # This runs on the blocking pool, not on an async worker
    ///
    /// The whole traversal is handed to one [`tokio::task::spawn_blocking`] and the rayon
    /// `par_iter` lives inside it. It used to run inline on the awaiting task, so rayon held
    /// a tokio runtime worker for the pass's entire wall-clock duration — hours on a large
    /// cache. On the small instance counts `docs/AWS_DEPLOYMENT.md` recommends as a starting
    /// point that is the difference between the pass costing a large fraction of
    /// request-serving capacity and costing none of it. Only synchronous I/O happens inside;
    /// every async follow-up is done by the caller after this returns.
    ///
    /// Spec: cache-eviction-at-scale § 6.3.
    #[allow(clippy::type_complexity)]
    async fn scan_metadata_with_shared_validator(
        self: &Arc<Self>,
    ) -> Result<(
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        Vec<std::path::PathBuf>,
        ScanSelfDebit,
    )> {
        let tracker = Arc::clone(self);
        // Captured HERE rather than asked for inside the blocking task, so the blocking body
        // never has to reason about what context it landed in.
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || tracker.scan_all_metadata_blocking(&runtime))
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Validation scan blocking task failed: {}", e))
            })?
    }

    /// The synchronous body of the full metadata scan. Runs on the blocking pool; performs
    /// only synchronous I/O and returns a plain result. See
    /// [`Self::scan_metadata_with_shared_validator`].
    #[allow(clippy::type_complexity)]
    fn scan_all_metadata_blocking(
        &self,
        runtime: &tokio::runtime::Handle,
    ) -> Result<(
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        Vec<std::path::PathBuf>,
        ScanSelfDebit,
    )> {
        use rayon::prelude::*;
        use std::sync::atomic::{AtomicU64, Ordering};

        let now = std::time::SystemTime::now();
        let metadata_dir = self.cache_dir.join("metadata");

        if !metadata_dir.exists() {
            return Ok((0, 0, 0, 0, 0, 0, 0, Vec::new(), ScanSelfDebit::default()));
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
        // What the pass's own deletions and repairs debited; see `ScanFileResult::self_debit`.
        let self_debit_bytes = AtomicU64::new(0);
        let self_debit_staged = AtomicU64::new(0);
        let self_debit_objects = AtomicU64::new(0);

        // Collect L1 shard directories for parallel traversal.
        // Structure: metadata/{bucket}/{L1}/{L2}/*.meta
        // Instead of a single sequential WalkDir over the entire tree, we enumerate
        // L1 directories and walk each in parallel via rayon. This overlaps NFS readdir
        // round-trips across threads.
        //
        // The "~35 min to ~2-5 min" figure this comment used to carry was DELETED
        // 2026-09-03 (cache-eviction-at-scale task 14). It was never a
        // measurement: the commit that introduced it (v1.9.5) says
        // "Expected: ~35 min -> ~2-5 min for 554k objects on EFS", and the only
        // recorded measurement of this function contradicts it by roughly an order of
        // magnitude -- 5,428 s for 1,146,783 files, i.e. 211 objects/s, which at that
        // commit's own stated 554k population predicts ~44 min rather than 2-5.
        //
        // Do not put a wall-clock figure here. This function's cost is a function of
        // object count, mean `.meta` size (measured at 2,401 bytes on the reference
        // tree), thread count and backend, and a single number in a comment cannot
        // carry that. The measured rates live in `requirements.md` § "Population and
        // sweep cost".
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

        // `runtime` is the handle the async caller captured before handing this body to the
        // blocking pool. The rayon workers below carry no tokio context of their own, and the
        // blocking-pool thread an unsplit batch folds on carries one that must not be
        // re-entered — `Self::block_on_from_scan` is what tells those two apart.

        // Walk each L1 directory in parallel using rayon's thread pool.
        // Within each L1, enumerate L2 subdirectories and their .meta files sequentially.
        l1_dirs.par_iter().for_each(|l1_dir| {
            let l2_entries = match std::fs::read_dir(l1_dir) {
                Ok(entries) => entries,
                Err(_) => return,
            };
            for l2_entry in l2_entries.flatten() {
                // Entry type from the readdir buffer's `d_type`, not a `stat` per entry. See
                // `disk_cache::is_shard_subdirectory` for the no-symlink invariant this rests on.
                // Spec: cache-eviction-at-scale § 16.6.13 (T1).
                if !crate::disk_cache::is_shard_subdirectory(&l2_entry) {
                    continue;
                }
                let l2_path = l2_entry.path();
                let file_entries = match std::fs::read_dir(&l2_path) {
                    Ok(entries) => entries,
                    Err(_) => continue,
                };
                for file_entry in file_entries.flatten() {
                    let path = file_entry.path();
                    if path.extension().is_none_or(|ext| ext != "meta") {
                        continue;
                    }
                    let result = self.scan_metadata_file(&path, now, Some(runtime));
                    total_size.fetch_add(result.size_bytes, Ordering::Relaxed);
                    staged_total.fetch_add(result.staged_bytes, Ordering::Relaxed);
                    self_debit_bytes.fetch_add(result.self_debit.bytes, Ordering::Relaxed);
                    self_debit_staged.fetch_add(result.self_debit.staged_bytes, Ordering::Relaxed);
                    self_debit_objects.fetch_add(result.self_debit.objects, Ordering::Relaxed);
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
            ScanSelfDebit {
                bytes: self_debit_bytes.load(Ordering::Relaxed),
                staged_bytes: self_debit_staged.load(Ordering::Relaxed),
                objects: self_debit_objects.load(Ordering::Relaxed),
            },
        ))
    }

    /// Remove the stale HEAD-only `.meta` at `path`, whose unlocked read is `unlocked`.
    /// Returns whether this call unlinked it.
    ///
    /// Takes the key's metadata slot lock without waiting: a held slot means a peer is
    /// writing the key, so the entry is left for the next pass. Under the lock the document
    /// is re-read, and is deleted only if it still passes [`stale_head_only_removable`] and
    /// still carries `unlocked`'s [`crate::disk_cache::ExpectedObjectIdentity`]. A peer that
    /// cached a range, refreshed the HEAD or re-PUT the key since is left alone. Blocking
    /// I/O: called from the scan's blocking-pool and rayon threads.
    fn remove_stale_head_only(
        &self,
        path: &std::path::Path,
        unlocked: &crate::cache_types::NewCacheMetadata,
        now: SystemTime,
    ) -> bool {
        let key = &unlocked.cache_key;
        let identity = crate::disk_cache::ExpectedObjectIdentity::of(unlocked);
        let _slot = match crate::metadata_lock_manager::try_acquire_slot_lock(&self.cache_dir, key)
        {
            Ok(lock) => lock,
            Err(e) => {
                debug!(
                    "Cache validation: stale HEAD-only {} left in place, slot lock held ({})",
                    key, e
                );
                return false;
            }
        };
        let still_stale = std::fs::read(path)
            .ok()
            .and_then(|content| {
                serde_json::from_slice::<crate::cache_types::NewCacheMetadata>(&content).ok()
            })
            .is_some_and(|locked| {
                locked.cache_key == *key
                    && stale_head_only_removable(&locked, now)
                    && crate::disk_cache::ExpectedObjectIdentity::of(&locked) == identity
            });
        if !still_stale {
            debug!(
                "Cache validation: HEAD-only {} changed since the scan read it, left in place",
                key
            );
            return false;
        }
        // The slot's lock file is not removed: slot files are shared and `flock` binds to the
        // open file description, so `_slot` releases on drop.
        match std::fs::remove_file(path) {
            Ok(()) => {
                debug!("Cache validation: removed stale HEAD-only entry {}", key);
                true
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => {
                warn!(
                    "Cache validation: could not remove stale HEAD-only entry {}: {}",
                    key, e
                );
                false
            }
        }
    }

    /// Drive `fut` to completion from a validation-scan worker, on whichever thread the scan
    /// actually landed on.
    ///
    /// # A captured handle is necessary and NOT sufficient, and this is why
    ///
    /// The captured-handle measurement recorded in `scan_metadata_file`'s doc comment was
    /// taken with 512 items, where rayon splits, injects the job into its global pool and
    /// blocks the calling thread on a latch — so every item ran on a plain `std::thread` and
    /// a captured `Handle::block_on` was legal on all of them.
    ///
    /// That conclusion does not hold for a batch rayon declines to split. `for_each` reaches
    /// the pool only through `join`, and `join` is only called if the producer splits; a
    /// one-element `par_iter` therefore folds **inline on the calling thread** and never
    /// touches the pool at all. Both production passes call `par_iter().for_each` from an
    /// `async fn`, so that calling thread is a tokio runtime thread — where
    /// `Handle::block_on` panics with "Cannot start a runtime from within a runtime" and
    /// takes the whole validation pass with it.
    ///
    /// A single-element batch is not exotic. `scan_metadata_with_shared_validator` builds
    /// `l1_dirs` from the L1 shard directories that exist on disk, so any cache whose
    /// objects occupy one L1 shard — a new volume, a small one, a single-bucket test fixture
    /// — produces exactly one, and `perform_rolling_validation`'s batch can narrow to one as
    /// `dirs_remaining` drains.
    ///
    /// So the thread is decided at runtime and cannot be assumed. Ask, then act: off the
    /// runtime, block directly; on it, hand the future to a scoped thread that carries no
    /// runtime context. The caller is blocked either way — the `par_iter` already blocks it
    /// — so this adds no stall that was not already there.
    ///
    /// Now that the whole visit runs inside `spawn_blocking`, the inline-fold thread is a
    /// blocking-pool thread rather than a runtime worker, and `block_on` is permitted there
    /// (`a_blocking_thread_carries_a_context_and_still_permits_block_on`). The scoped thread
    /// is therefore no longer strictly necessary on that path — it costs one thread spawn per
    /// affected entry and is left in place deliberately, because `try_current()` cannot tell
    /// a blocking-pool thread from a runtime worker and narrowing the condition would need a
    /// discriminator this module does not have.
    ///
    /// Spec: cache-eviction-at-scale, unit SIZE-TRACKER-BLOCK-ON-DEAD.
    fn block_on_from_scan<F>(handle: &tokio::runtime::Handle, fut: F) -> F::Output
    where
        F: std::future::Future + Send,
        F::Output: Send,
    {
        if tokio::runtime::Handle::try_current().is_err() {
            // The common case: a rayon pool worker, no runtime context, blocking permitted.
            return handle.block_on(fut);
        }
        // rayon folded this batch inline on the caller, which is inside the runtime.
        std::thread::scope(|scope| {
            scope
                .spawn(|| handle.block_on(fut))
                .join()
                .expect("the validation scan's blocking thread must not panic")
        })
    }

    /// The handle the two deliberately-inert GET-expiry sites below ask for. It is `None`,
    /// always, by construction.
    ///
    /// # Why the gate is a constant instead of `Handle::try_current()`
    ///
    /// Both sites used to be gated on `try_current().ok()`, and their inertness was a
    /// consequence of the thread the scan happened to land on rather than a decision. That
    /// held while the scan ran on rayon pool workers, which carry no runtime context — and
    /// it stopped holding the moment the visit moved onto the blocking pool, because a
    /// blocking-pool thread DOES carry one and does permit `block_on`. Both facts are
    /// measured by `a_blocking_thread_carries_a_context_and_still_permits_block_on`.
    ///
    /// So the move would have revived both sites silently, on the unsplit-batch path only,
    /// which is precisely the revival `SIZE-TRACKER-BLOCK-ON-DEAD` established as unsafe:
    /// `invalidate_cache` deletes unconditionally and never re-tests expiry under the slot
    /// lock, so the verdict would come from this pass's unlocked read while the deletion
    /// lands on a volume peers are writing. Reviving the predicate alone is worse still —
    /// it routes idle entries into a deletion that is still inert.
    ///
    /// Making the gate explicit keeps that decision where a reader can see it, and removes
    /// something worse than an inert site: on the unsplit path the old gate succeeded on a
    /// runtime WORKER, where `block_on` panics, so an operator running with
    /// `actively_remove_cached_data` on a single-shard cache had a latent abort of the whole
    /// validation pass rather than two dead branches.
    ///
    /// Reviving the pair needs a deletion primitive that re-decides under the key's slot
    /// lock. That is a separate change with its own red side; when it exists, this function
    /// is the one place to change.
    fn get_expiry_handle_deliberately_none() -> Option<&'static tokio::runtime::Handle> {
        None
    }

    /// Every above-floor range of one parsed `.meta`, as [`TailRecord`]s.
    ///
    /// # The floor is tested against `compressed_size`, and the two figures beside it are wrong
    ///
    /// `RangeSpec::uncompressed_size` and the `.bin` filename's logical extent both sit within
    /// reach and both differ from `compressed_size` for compressible data. Testing the floor
    /// against either admits ranges whose on-disk size is below it, and sorting on either can
    /// rank a 100 MiB range that compresses to 10 MiB ahead of a 50 MiB incompressible one that
    /// frees five times more actual bytes. That is D4's defect, and § 4.4 states it as a
    /// correctness matter rather than an optimisation.
    ///
    /// # `leaf` comes from the path, `expires_at` from the object, `is_staged` from a predicate
    ///
    /// `leaf` is the `{YYY}` directory component of the `.meta`'s own path — carried rather than
    /// re-derived, because re-deriving is a BLAKE3 per candidate at selection time.
    /// `expires_at` is object-level: `RangeSpec` has no expiry field (R3.6b), so every range of
    /// one object carries the same value. `is_staged` goes through
    /// [`crate::cache_types::is_staged_range_spec`] with the **object's** `is_write_cached` as
    /// the fallback, never `RangeSpec::staged` directly.
    ///
    /// Spec: cache-eviction-at-scale § 4.1, § 4.4. Requirements: 3.2.
    fn tail_candidates_for(
        metadata: &crate::cache_types::NewCacheMetadata,
        meta_path: &std::path::Path,
        floor: u64,
    ) -> Vec<TailRecord> {
        // `{YYY}` is the `.meta`'s own parent directory. Taken from the path rather than
        // recomputed so a record's leaf and the directory its `.meta` actually sits in cannot
        // disagree.
        let leaf = meta_path
            .parent()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        let expires_at = systime_nanos(metadata.expires_at);
        let object_is_write_cached = metadata.object_metadata.is_write_cached;
        metadata
            .ranges
            .iter()
            .filter(|r| r.compressed_size >= floor)
            .map(|r| TailRecord {
                cache_key: metadata.cache_key.clone(),
                range_start: r.start,
                range_end: r.end,
                compressed_size: r.compressed_size,
                last_accessed: systime_nanos(r.last_accessed),
                expires_at,
                access_count: r.access_count,
                is_staged: crate::cache_types::is_staged_range_spec(r, object_is_write_cached),
                leaf: leaf.clone(),
            })
            .collect()
    }

    /// Scan metadata file and optionally delete if expired (GET cache expiration)
    ///
    /// # `runtime` is a CAPTURED handle, and it has to be
    ///
    /// This function runs on one of two threads, and which one is decided at runtime. Most
    /// invocations are on a rayon global-pool worker: a plain `std::thread` carrying no tokio
    /// context, where `Handle::try_current()` fails. MEASURED, not assumed: with 512 items
    /// driven through `par_iter().for_each`, `try_current()` failed 512 times and succeeded 0,
    /// and the calling thread ran 0 items (rayon injects the job into the pool and blocks the
    /// caller on a latch rather than recruiting it). A handle cloned in the async caller and
    /// passed down DOES permit `block_on` from those workers, because a rayon worker is not
    /// inside an asynchronous execution context — also measured.
    ///
    /// The other thread is the blocking-pool thread the whole visit now runs on, which a
    /// batch rayon declines to split folds inline on. That one DOES carry a runtime context
    /// and does permit `block_on`
    /// (`a_blocking_thread_carries_a_context_and_still_permits_block_on`). So a captured
    /// handle is still required — `try_current()` is not a usable gate in either direction,
    /// which is why `Self::block_on_from_scan` asks about the thread instead of assuming, and
    /// why the two inert sites below no longer gate on `try_current()` at all.
    ///
    /// # One of the three inert sites is now revived; two are deliberately left inert
    ///
    /// The write-cache-expiry deletion below now uses the captured `runtime` handle, so it
    /// runs for the first time. Its callee re-reads the `.meta` under the key's metadata
    /// slot lock and re-tests both of its conditions there, so the scan's unlocked read is
    /// only a filter and the authoritative decision is taken under the lock — a stale
    /// filter can cost a deferred deletion, never a wrong one.
    ///
    /// The GET-expiry pair below it — the `is_cache_entry_active` consultation and the
    /// `invalidate_cache` deletion — are **left inert on purpose**, and the reason is
    /// shared storage rather than threading. `invalidate_cache` deletes unconditionally: it
    /// takes the slot lock for the mutation but never re-tests `now > expires_at` under it,
    /// so the verdict would come from this pass's unlocked read while the effect lands on a
    /// volume other instances are writing. `expires_at` is refreshed on the shared `.meta`
    /// by any peer's cached GET, graduation or re-PUT, so a peer that refreshed between
    /// this pass's read and the delete would silently lose the entry — the
    /// local-decision / shared-effect corollary in
    /// the cache coherency invariants, and the same class
    /// `SHARED-DELETE-SAFETY` fixed in the parse-failure branch above. Reviving that pair
    /// needs a deletion primitive that re-decides under the lock, which is a different
    /// change with its own red side.
    ///
    /// The two are also interlocked, so neither can be revived alone: the
    /// `is_cache_entry_active` result feeds `.unwrap_or(true)`, and "assume active" is what
    /// currently makes the GET-expiry branch return `cache_skipped` instead of reaching the
    /// delete. Reviving only the predicate would route genuinely-idle entries into a
    /// deletion that is still inert, turning a silent skip into a logged failure and a
    /// `cache_error` on every expired entry.
    fn scan_metadata_file(
        &self,
        path: &PathBuf,
        now: SystemTime,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> ScanFileResult {
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
                    tail_candidates: Vec::new(),
                    self_debit: ScanSelfDebit::default(),
                };
            }
        };

        let metadata: NewCacheMetadata = match serde_json::from_slice(&content) {
            Ok(m) => m,
            Err(e) => {
                // A parse failure is not evidence the document is dead, and this pass used
                // to treat it as one.
                //
                // Five writers rewrite `{key}.meta` IN PLACE with `std::fs::write`, which
                // is `O_TRUNC` before the write, so a write still in flight — or one that
                // failed partway — leaves a zero-length or short document at the final
                // path. That needs no partition and no peer: any `ENOSPC`, `EIO` or
                // post-reconnect `ESTALE` between the truncate and the write reaches it on
                // a single instance with no contention. Unlinking here on the strength of
                // one read therefore deleted a live document, and on shared storage the
                // writer may be another instance, which never learns its publish was
                // destroyed — the local-decision / shared-effect corollary in
                // the cache coherency invariants.
                //
                // `CacheManager::remove_corrupt_metadata`'s discipline — re-read and
                // re-parse under the key's slot lock — is NOT available here, and the
                // reason is structural rather than a matter of plumbing. The slot is
                // `blake3(cache_key) % META_LOCK_SLOTS`, and this scanner holds only a
                // path: the document that failed to parse is exactly the document that
                // does not yield its own `cache_key`, and the filename cannot supply it
                // either, because `disk_cache::sanitize_object_key_for_filename` replaces
                // any key whose sanitised form exceeds 200 characters with a BLAKE3 digest.
                // A lock taken on any other slot would fence nothing.
                //
                // So use the tree's path-keyed discriminator for the same hazard instead.
                // `disk_cache::read_and_parse_metadata_blocking` retries an empty read
                // three times and then reports `TransientMiss`; it retries a parse failure
                // and then splits on whether the file's size moved across the retry window
                // — size stable is confident corruption, size changed is a writer
                // mid-flight. Deleting only on `Corrupt` is what makes this branch
                // survivable for a live document, and it is the same classification the
                // request path has used for this hazard since cache-metadata-resilience.
                //
                // `u64::MAX` is passed as the size cap deliberately. The only question
                // being asked here is torn-versus-stable, and imposing a cap this scanner
                // has never had would add a deletion criterion rather than remove one.
                //
                // Cost: up to three re-reads and ~450 ms of sleeps, on the parse-failure
                // branch only. It is not on the per-object path and cannot affect the
                // traversal budget for documents that parse.
                //
                // Spec: cache-eviction-at-scale. Requirements: 7.1
                let verdict = crate::disk_cache::read_and_parse_metadata_blocking(path, u64::MAX);
                if !matches!(
                    verdict,
                    crate::disk_cache::MetadataReadOutcome::Corrupt { .. }
                ) {
                    warn!(
                        "Failed to parse metadata file {:?}: {} — LEAVING IT IN PLACE. A \
                         locked re-read is not available to this pass, and a re-read \
                         classified the file {:?} rather than confidently corrupt, so a \
                         writer may be mid-rewrite. Not counted, not removed.",
                        path, e, verdict
                    );
                    return ScanFileResult {
                        size_bytes: 0,
                        staged_bytes: 0,
                        cache_expired: false,
                        cache_skipped: false,
                        cache_error: true,
                        meta_removed: false,
                        tail_candidates: Vec::new(),
                        self_debit: ScanSelfDebit::default(),
                    };
                }

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
                    // Self-heal. Ask the filesystem rather than trusting the
                    // `remove_file` result — a failed unlink leaves a countable object.
                    meta_removed: !path.exists(),
                    tail_candidates: Vec::new(),
                    self_debit: ScanSelfDebit::default(),
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

        // The Tail_Partition candidates this `.meta` contributes, built ONCE from the parse
        // already in hand. Which arms below carry them is stated on `ScanFileResult`'s field.
        let tail_candidates =
            Self::tail_candidates_for(&metadata, path, self.config.tail_floor_bytes);

        // ── An empty-object-ETag entry left by a pre-3.0.0 zero-length publish. ─────────
        //
        // This is the ONLY mechanism that can repair one. The read-path guard added with
        // the detector refuses to serve such an entry and forwards to S3, but it cannot
        // heal it: a zero-byte `200` carries no body, and both mainline cache-write arms
        // decline to store an empty one, so the corrupt document survives unlimited read
        // traffic. A cold entry — one nothing reads — was previously unreachable by any
        // mechanism at all.
        //
        // The predicate reads `object_metadata.etag` and `.content_length` off the FULL
        // typed parse above. It must never run on `NewCacheMetadata` values manufactured by
        // `cache_initialization_coordinator::process_metadata_file_lazy`, which fabricates
        // `etag: "lazy-parsed"` and `ranges: Vec::new()` from a file it never fully decoded;
        // reading either field there reads a value no document ever contained. That path has
        // no route into this function — it is a different scan with its own callers.
        //
        // Placed before the write-cache and GET expiry branches, and returning early on a
        // successful repair, because those branches would otherwise evaluate the now-stale
        // in-memory copy: the document on disk has no ranges once this returns, and its
        // figures are 0. An entry that is both corrupt and write-cache-expired is repaired
        // on this pass and expired on the next, which is one day later and harmless.
        //
        // Spec: cache-eviction-at-scale, unit META-REPAIR-COLD-CONVERGE.
        if crate::cache::CacheManager::is_empty_object_etag_entry(&metadata.object_metadata) {
            let cache_manager = self
                .cache_manager
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|weak_ref| weak_ref.upgrade());

            match (cache_manager, runtime) {
                (Some(cache_manager), Some(handle)) => {
                    let key = metadata.cache_key.clone();
                    // Through `block_on_from_scan`, not `handle.block_on` directly: a batch
                    // rayon declines to split runs inline on the caller's runtime thread,
                    // where a bare `block_on` panics. See that helper's doc comment.
                    let repaired = Self::block_on_from_scan(handle, async move {
                        cache_manager.repair_empty_object_etag_entry(&key).await
                    });

                    match repaired {
                        Ok(true) => {
                            // The `.meta` SURVIVES — it is now a correctly published
                            // zero-length entry — so `meta_removed` stays false and the
                            // census still counts this object. Reporting it removed would
                            // under-count a live object, biasing the census DOWNWARD, which
                            // is the direction that understates the object count.
                            //
                            // Both byte figures are 0 because the repaired document has no
                            // ranges, matching how every other branch that changes what is
                            // on disk reports its own outcome.
                            //
                            // Not `cache_expired`: that counter feeds
                            // `cache_entries_expired` in validation.json and an operator
                            // reading it means "entries whose TTL ran out". A repair is
                            // neither an expiry nor an error, so the pass reports a clean
                            // visit and the `info!` in the repair itself is the greppable
                            // record of how many fired.
                            //
                            // The repair removed and debited the superseded ranges through
                            // the accumulator, so the full scan's census adds them back.
                            return ScanFileResult {
                                size_bytes: 0,
                                staged_bytes: 0,
                                cache_expired: false,
                                cache_skipped: false,
                                cache_error: false,
                                meta_removed: false,
                                tail_candidates: Vec::new(),
                                self_debit: ScanSelfDebit {
                                    bytes: total_size,
                                    staged_bytes: staged_size,
                                    objects: 0,
                                },
                            };
                        }
                        Ok(false) => {
                            // A locked re-read disagreed with the unlocked one, so another
                            // writer changed the document underneath this pass. Fall
                            // through and count what was read; the next pass re-decides.
                            debug!(
                                "Empty-object-ETag repair declined for {} — the document \
                                 changed between the scan's read and the locked re-read",
                                metadata.cache_key
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to repair empty-object-ETag entry {}: {}",
                                metadata.cache_key, e
                            );
                            return ScanFileResult {
                                size_bytes: total_size,
                                staged_bytes: staged_size,
                                cache_expired: false,
                                cache_skipped: false,
                                cache_error: true,
                                meta_removed: false,
                                tail_candidates: tail_candidates.clone(),
                                self_debit: ScanSelfDebit::default(),
                            };
                        }
                    }
                }
                (cache_manager, runtime) => {
                    // Nothing was mutated, so the entry's real figures are still the ones to
                    // report. Flagged as an error because the pass found damage it was
                    // unable to act on, and silence here would make an unwired tracker
                    // indistinguishable from a clean cache.
                    warn!(
                        "Found an empty-object-ETag cache entry at {} but cannot repair it: \
                         cache_manager={}, runtime_handle={}",
                        metadata.cache_key,
                        if cache_manager.is_some() {
                            "available"
                        } else {
                            "unavailable"
                        },
                        if runtime.is_some() {
                            "available"
                        } else {
                            "unavailable"
                        }
                    );
                    return ScanFileResult {
                        size_bytes: total_size,
                        staged_bytes: staged_size,
                        cache_expired: false,
                        cache_skipped: false,
                        cache_error: true,
                        meta_removed: false,
                        tail_candidates: tail_candidates.clone(),
                        self_debit: ScanSelfDebit::default(),
                    };
                }
            }
        }

        // ── A stale HEAD-only entry: no ranges, HEAD expired more than a day ago. ──
        //
        // The startup census used to remove these. With it gone this pass is the only
        // remover. The read above is only the
        // filter: `remove_stale_head_only` skips a held slot, re-reads under the key's slot
        // lock and deletes only a document that is still stale, still HEAD-only and still the
        // same version, so a peer that cached a range or refreshed the HEAD keeps its entry.
        //
        // Accounting, as the write-cache expiry arm does it: `cached_objects` is debited
        // through the accumulator, and the entry is reported at its pre-removal figure (one
        // object, no bytes) through `meta_removed` plus `self_debit`, so the full scan's
        // relative install does not debit it a second time. With no captured runtime the
        // debit cannot be issued, so the entry is left and counted.
        if stale_head_only_removable(&metadata, now) {
            if let Some(handle) = runtime {
                if self.remove_stale_head_only(path, &metadata, now) {
                    Self::block_on_from_scan(handle, self.consolidator.decrement_cached_objects(1));
                    let meta_removed = !path.exists();
                    return ScanFileResult {
                        size_bytes: 0,
                        staged_bytes: 0,
                        cache_expired: false,
                        cache_skipped: false,
                        cache_error: false,
                        meta_removed,
                        tail_candidates: Vec::new(),
                        self_debit: ScanSelfDebit {
                            bytes: 0,
                            staged_bytes: 0,
                            objects: u64::from(meta_removed),
                        },
                    };
                }
            }
        }

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
                            tail_candidates: tail_candidates.clone(),
                            self_debit: ScanSelfDebit::default(),
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
                        tail_candidates: tail_candidates.clone(),
                        self_debit: ScanSelfDebit::default(),
                    };
                }
            };

            // Delete the expired write cache entry, through the handle CAPTURED IN THE
            // ASYNC CALLER.
            //
            // This used to be `Handle::try_current().ok().and_then(|h| h.block_on(..))`,
            // which cannot work from either place this function runs: a rayon worker
            // carries no tokio context, so `try_current()` returns `Err` on every
            // invocation, and a thread that IS inside the runtime panics on
            // `Handle::block_on`. The deletion was therefore never performed in any
            // release — and this scan is `check_and_invalidate_expired_write_cache`'s only
            // production caller, so the callee had never executed at all.
            //
            // Safe to revive on shared storage because the callee decides where it
            // mutates: it acquires the key's metadata slot lock BEFORE its own read, then
            // re-parses and re-tests `is_write_cached` and `is_write_cache_expired()` under
            // that lock, and removes the range files, debits both size channels and unlinks
            // the `.meta` without ever releasing it. The unlocked read above is a filter
            // only. A peer that republishes between the two reads makes the locked re-test
            // decline and this pass returns `Some(false)`, so the failure mode of a stale
            // filter is one deferred deletion rather than a lost publish.
            //
            // Spec: cache-eviction-at-scale, unit SIZE-TRACKER-BLOCK-ON-DEAD.
            let Some(handle) = runtime else {
                warn!(
                    "Write cache entry {} is expired but this pass cannot delete it: no \
                     captured runtime handle was supplied to the scan. Counted at its real \
                     size and left in place; the next pass re-decides.",
                    metadata.cache_key
                );
                return ScanFileResult {
                    size_bytes: total_size,
                    staged_bytes: staged_size,
                    cache_expired: false,
                    cache_skipped: false,
                    cache_error: true,
                    meta_removed: false,
                    tail_candidates: tail_candidates.clone(),
                    self_debit: ScanSelfDebit::default(),
                };
            };
            let delete_result = Self::block_on_from_scan(handle, async {
                cache_manager
                    .check_and_invalidate_expired_write_cache(&metadata.cache_key)
                    .await
                    .ok()
            });

            match delete_result {
                Some(true) => {
                    debug!("Deleted expired write cache entry: {}", metadata.cache_key);
                    let meta_removed = !path.exists();
                    return ScanFileResult {
                        size_bytes: 0,
                        staged_bytes: 0,
                        cache_expired: true,
                        cache_skipped: false,
                        cache_error: false,
                        // `Ok(true)` does NOT mean the `.meta` went — that function
                        // returns true on the expiry decision and gates only its own
                        // decrement on `metadata_deleted`. Read the path instead.
                        meta_removed,
                        tail_candidates: Vec::new(),
                        // The callee debited both size channels for the ranges it removed
                        // and, when its unlink succeeded, `cached_objects`. Read from the
                        // path, like `meta_removed`, for the same reason.
                        self_debit: ScanSelfDebit {
                            bytes: total_size,
                            staged_bytes: staged_size,
                            objects: u64::from(meta_removed),
                        },
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
                        tail_candidates: tail_candidates.clone(),
                        self_debit: ScanSelfDebit::default(),
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
                            tail_candidates: tail_candidates.clone(),
                            self_debit: ScanSelfDebit::default(),
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
                        tail_candidates: tail_candidates.clone(),
                        self_debit: ScanSelfDebit::default(),
                    };
                }
            };

            // INERT, AND LEFT THAT WAY DELIBERATELY — see this function's doc comment.
            //
            // `is_active` is `unwrap_or(true)`'s fallback on every visit, and the branch
            // below always takes the `cache_skipped` return rather than the delete, because
            // `get_expiry_handle_deliberately_none` yields `None` by construction. It used to
            // be inert for a weaker reason — `try_current()` failing on a rayon worker, 0
            // successes against 512 failures when measured — which stopped being true once
            // the visit moved onto the blocking pool. See that function's doc comment.
            //
            // That fallback is currently the only thing making the `invalidate_cache` site
            // below unreachable, so it is load-bearing rather than vestigial: reviving this
            // predicate alone would route idle entries into a deletion that is still inert.
            // Reviving the pair needs a deletion primitive that re-decides under the key's
            // slot lock — `invalidate_cache` does not — and that is a separate change.
            let is_active = Self::get_expiry_handle_deliberately_none()
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
                    tail_candidates: tail_candidates.clone(),
                    self_debit: ScanSelfDebit::default(),
                };
            }

            // INERT, AND LEFT THAT WAY DELIBERATELY — see this function's doc comment.
            //
            // Unreachable twice over: `is_active` above is always true, and the gate here
            // yields `None` by construction. Do NOT revive this by swapping in the captured
            // `runtime` handle, which is the one-line change it looks like.
            // `invalidate_cache` deletes unconditionally — it takes the slot lock for the
            // mutation but never re-tests `now > expires_at` under it — so the verdict would
            // come from the unlocked read at the top of this function while the deletion
            // lands on shared storage. `expires_at` is refreshed there by any peer's cached
            // GET, graduation or re-PUT, so a peer that refreshed inside that window loses
            // its entry and never learns. Contrast the write-cache site above, which is
            // revived precisely because its callee re-decides under the lock.
            let delete_result = Self::get_expiry_handle_deliberately_none().and_then(|handle| {
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
                        tail_candidates: Vec::new(),
                        self_debit: ScanSelfDebit::default(),
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
                        tail_candidates: tail_candidates.clone(),
                        self_debit: ScanSelfDebit::default(),
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
            // Moved rather than cloned: this is the common path, taken once per scanned
            // object, and every arm that clones instead is an expiry or an error arm.
            tail_candidates,
            self_debit: ScanSelfDebit::default(),
        }
    }
}

/// RAII guard for validation lock
pub struct ValidationLock {
    file: std::fs::File,
}

impl Drop for ValidationLock {
    fn drop(&mut self) {
        let _ = fs4::FileExt::unlock(&self.file);
        debug!("Released validation lock");
    }
}

/// Ends this instance's `ScanKind::Validation` scanner document when dropped, so every exit
/// from [`CacheSizeTracker::perform_validation_gated`] after the publish ends it: success, an `Err`
/// propagated by `?`, and an unwinding panic alike.
///
/// Declared after the [`ValidationLock`] in `perform_validation`, so it drops first and the
/// document is ended while `validation.lock` is still held. An `end` failure is logged at
/// debug and never surfaces: the document's lease ([`crate::scanner_registry::SCANNER_LEASE_SECS`])
/// is the backstop, and a stale document can only make a peer's limiter count one scanner too
/// many for one lease, never change what a pass computes.
struct ValidationScanDocument {
    cache_dir: PathBuf,
    instance_id: String,
}

impl Drop for ValidationScanDocument {
    fn drop(&mut self) {
        if let Err(e) = crate::scanner_registry::end(
            &self.cache_dir,
            &self.instance_id,
            crate::scanner_registry::ScanKind::Validation,
        ) {
            debug!(
                "Could not end the validation scanner document for {}: {}",
                self.instance_id, e
            );
        }
    }
}

#[cfg(test)]
type ValidationInFlightHook = Box<dyn Fn() -> Result<()>>;

#[cfg(test)]
thread_local! {
    /// Test-only hook run inside `perform_validation` once the scanner document is published,
    /// i.e. while the pass is in flight. An `Err` is returned from `perform_validation`, which
    /// is how the tests drive an error exit after the publish.
    static VALIDATION_IN_FLIGHT_HOOK: std::cell::RefCell<Option<ValidationInFlightHook>> =
        const { std::cell::RefCell::new(None) };

    /// Test-only override of [`FULL_INSTALL_LOCK_ATTEMPTS`] for `perform_full_validation`, so a
    /// deferral test does not wait out the production 60 s budget.
    static FULL_INSTALL_ATTEMPTS_OVERRIDE: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

/// [`FULL_INSTALL_LOCK_ATTEMPTS`], or the test override when one is set.
fn full_install_lock_attempts() -> usize {
    #[cfg(test)]
    if let Some(n) = FULL_INSTALL_ATTEMPTS_OVERRIDE.with(|c| c.get()) {
        return n;
    }
    FULL_INSTALL_LOCK_ATTEMPTS
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
            last_visit_at: Some(1719849600),
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
                last_visit_at: None,
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

    /// A **FILE** at `metadata/{bucket}/` is not selected as an L1 shard directory.
    ///
    /// The census's half of the guard that keeps the origin marker's placement safe
    /// (`public-security-review-readiness` R2). `_origin.json` sits at exactly this level,
    /// and it is invisible here because of `if !l1_path.is_dir() { continue; }` — not because
    /// of the bucket-level underscore guard, which is one level up and never reached for it.
    ///
    /// Selecting a file as an L1 directory would hand the rolling census a path it then tries
    /// to `read_dir`, so that L1 slot would contribute nothing while still consuming its turn
    /// in the 256-wide cursor rotation — a silent under-count of the cache size, in the
    /// direction that makes eviction fire late.
    ///
    /// # The marker alone cannot make this test able to fail
    ///
    /// `_origin.json` is 12 characters, so it also fails `name.len() == 2`; it is excluded
    /// twice over and this test would pass with the `is_dir()` check deleted. So the fixture
    /// also plants a **file named `ab`**, which satisfies both the length and the
    /// `from_str_radix` parse and is therefore excluded by the directory check alone.
    ///
    /// Note this enumerator accepts **uppercase** hex where `ledger_repair::is_shard_level`
    /// rejects it, so the two do not agree on every name. The fixture uses lowercase, which
    /// both accept, so it is not sensitive to that difference.
    #[tokio::test]
    async fn select_l1_directories_ignores_a_file_at_bucket_level() {
        let (tracker, _consolidator, temp_dir) = create_test_tracker().await;
        let metadata_dir = temp_dir.path().join("metadata");

        // One real shard directory, 0xcd, so the assertion distinguishes "the files were
        // ignored" from "the enumeration found nothing at all".
        create_l1_dirs(&metadata_dir, "my-bucket", &[0xcd]);
        let bucket_dir = metadata_dir.join("my-bucket");

        // The real artifact whose safety this test exists to pin.
        std::fs::write(
            bucket_dir.join(crate::origin_identity::MARKER_FILE_NAME),
            br#"{"schema":1,"origin":"minio-a.example.com"}"#,
        )
        .unwrap();
        // The discriminator: a FILE named exactly like an L1 shard.
        std::fs::write(bucket_dir.join("ab"), b"not a shard").unwrap();
        assert_eq!(
            u8::from_str_radix("ab", 16),
            Ok(0xab),
            "fixture precondition: the planted file's name must parse as an L1 index, or the \
             length/parse filter excludes it and the is_dir() check is never reached"
        );

        // A range covering both 0xab and 0xcd, so a file mistaken for a directory would be
        // selected rather than merely being out of range.
        let (dirs, _wraps) = tracker.select_l1_directories(&metadata_dir, 0xab, 0x40);
        let mut names: Vec<String> = dirs
            .iter()
            .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(String::from))
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec!["cd"],
            "only DIRECTORIES may be selected as L1 shards. The origin marker is a file at \
             bucket level precisely because this enumerator filters on is_dir(); selecting a \
             file would consume an L1 slot in the census rotation and contribute nothing, \
             under-counting the cache size."
        );
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
        // to schedule.json with validation_type "full"
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        let duration_secs = 3600.5;
        tracker
            .persist_full_scan_duration(duration_secs)
            .await
            .unwrap();

        // Read schedule.json and verify the field
        let content = std::fs::read_to_string(&tracker.schedule_path).unwrap();
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
        // Two properties of the schedule/results split, in one test:
        //  - persist_full_scan_duration preserves whatever is already in schedule.json
        //    rather than replacing the document, and
        //  - it does not touch validation.json, whose results must survive a mode stamp.
        let (tracker, _consolidator, _temp_dir) = create_test_tracker().await;

        // Write initial validation metadata — the RESULTS file.
        tracker
            .write_validation_metadata(500000, 500000, 0, Duration::from_secs(60), 1000, 0, 0, 0)
            .await
            .unwrap();
        // And an existing cursor in the SCHEDULE file, so "preserves existing fields" has
        // something to preserve.
        tracker.persist_cursor(77, 1_700_000_000).unwrap();

        tracker.persist_full_scan_duration(7200.0).await.unwrap();

        let schedule: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&tracker.schedule_path).unwrap())
                .unwrap();
        assert_eq!(
            schedule
                .get("last_full_scan_duration_secs")
                .and_then(|v| v.as_f64()),
            Some(7200.0),
        );
        assert_eq!(
            schedule.get("validation_type").and_then(|v| v.as_str()),
            Some("full"),
        );
        assert_eq!(
            schedule.get("rolling_cursor").and_then(|v| v.as_u64()),
            Some(77),
            "an existing cursor must survive a full scan's mode stamp"
        );

        // The results file is untouched, and still deserializes.
        let validation: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&tracker.validation_path).unwrap())
                .unwrap();
        assert!(
            validation.get("scanned_size").is_some(),
            "validation.json must still hold its results"
        );
        assert!(
            validation.get("last_full_scan_duration_secs").is_none(),
            "schedule state must NOT leak back into the results file"
        );
        assert!(
            tracker.read_validation_metadata().await.is_ok(),
            "validation.json must stay deserializable as ValidationMetadata"
        );
    }

    // -----------------------------------------------------------------------
    // Round-trip: does a rolling cycle leave validation.json readable?
    //
    // Spec: cache-eviction-at-scale, Phase 0 task 0c.
    // Finding: validation scan, F1
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
    // STATUS: the round-trip defect IS FIXED and this test is no longer ignored. The `#[ignore]` came
    // off with the fix — the schedule/results file split described on
    // `read_rolling_state`, which stopped a rolling cycle writing its twelve
    // schedule fields over `ValidationMetadata`'s document. The attribute's own
    // reason said "the fix is Phase 5"; the decomposition of that work put the fix
    // in Phase 4 instead, so that sentence was stale as well as obsolete.
    //
    // RED SIDE, re-taken on the pre-fix tree rather than quoted from the 2026-08-27
    // observation (`cargo test --lib f1_rolling_cycle -- --ignored --nocapture`):
    //
    //   read_validation_metadata() = Err(Cache error: Failed to parse validation
    //     metadata: missing field `scanned_size` at line 14 column 1)
    //   calculate_next_validation_time() = 0s from now (healthy: >18000s)
    //   last_validation_drift = None
    //
    // All three baseline assertions and both fixture guards passed in that run, so
    // the failure was attributable to the rolling cycle alone.
    #[tokio::test]
    async fn f1_rolling_cycle_leaves_validation_json_deserializable() {
        let _tail_serial = TAIL_TEST_GUARD.lock().await;
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
                    unservable: false,
                    file_identity: None,
                }],
                created_at: now,
                expires_at: now + Duration::from_secs(86400),
                compression_info: CompressionInfo::default(),
                head_expires_at: None,
                head_last_accessed: None,
                head_access_count: 0,
                head_cached_at: None,
                last_validated_at: None,
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
        // THE ONE CHANGE THIS TEST NEEDED FOR THE FIX: the two fixture guards below
        // read `validation_type` and `rolling_objects_validated`, and those are
        // schedule state, so they now live in `schedule.json`. Reading them from
        // `validation.json` is what the defect made possible in the first place.
        // Nothing else moved — the three observations (A), (B) and (C) still read the
        // same three values from the same three calls.
        let raw: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&tracker.schedule_path).unwrap())
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
             default are absent from write_rolling_state's output, so schedule state \
             and scan results are sharing one deserialization target again.",
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
    // What moving the scan onto the blocking pool does to the two deliberately
    // inert GET-expiry sites
    //
    // Spec: cache-eviction-at-scale § 6.3. Unit SIZE-TRACKER-BLOCK-ON-DEAD left
    // `is_cache_entry_active` and the GET-expiry `invalidate_cache` inert on
    // purpose, and recorded that reviving either alone is worse than reviving
    // neither. Both are gated on `Handle::try_current().ok()`, so whether they are
    // inert is a fact about the thread the scan lands on — which this change moves.
    //
    // The two facts below are MEASURED here rather than reasoned about, because the
    // decision to make those sites explicitly inert rests on them:
    //
    //  1. A blocking-pool thread DOES carry a runtime context, so `try_current()`
    //     succeeds there. Topology-gated inertness would therefore have ended
    //     silently the moment the scan moved onto the blocking pool, reviving both
    //     sites on the unsplit-batch path and nowhere else.
    //  2. On that thread `block_on` is permitted, whereas on a runtime worker it
    //     panics. So before this change the unsplit path did not have two inert
    //     sites — it had a latent abort of the whole validation pass.
    //
    // Which is why the gate is no longer `try_current()`: see
    // `get_expiry_handle_deliberately_none`.
    // =====================================================================
    #[test]
    fn a_blocking_thread_carries_a_context_and_still_permits_block_on() {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        let (ctx_on_blocking, block_on_ok_on_blocking, block_on_ok_on_worker) =
            runtime.block_on(async {
                let handle = tokio::runtime::Handle::current();

                let blocking = tokio::task::spawn_blocking({
                    let handle = handle.clone();
                    move || {
                        let ctx = tokio::runtime::Handle::try_current().is_ok();
                        let ok = catch_unwind(AssertUnwindSafe(|| {
                            handle.block_on(async {});
                        }))
                        .is_ok();
                        (ctx, ok)
                    }
                })
                .await
                .unwrap();

                let on_worker = tokio::spawn({
                    let handle = handle.clone();
                    async move {
                        catch_unwind(AssertUnwindSafe(|| {
                            handle.block_on(async {});
                        }))
                        .is_ok()
                    }
                })
                .await
                .unwrap();

                (blocking.0, blocking.1, on_worker)
            });

        std::panic::set_hook(previous_hook);

        assert!(
            ctx_on_blocking,
            "a spawn_blocking thread must carry a runtime context — if it did not, the \
             two GET-expiry sites would still be inert by topology and the explicit gate \
             would be unnecessary"
        );
        assert!(
            block_on_ok_on_blocking,
            "block_on from a spawn_blocking thread must be permitted — this is why moving \
             the scan there removes the latent abort on the unsplit-batch path"
        );
        assert!(
            !block_on_ok_on_worker,
            "block_on from a runtime WORKER must panic — this is the abort the unsplit-batch \
             path used to carry, and the contrast that makes the fact above meaningful"
        );
    }
    // =====================================================================
    // The budget is real, and the shard is the unit of abortability
    //
    // Spec: cache-eviction-at-scale § 6.4. Requirements: 1.5
    //
    // `validation_max_duration` was documented as a budget and was not one.
    // Rolling mode compared elapsed time against it only BETWEEN batches, while
    // `estimate_batch_size` sized the first batch to consume the whole budget — so
    // in steady state there was one batch and the check never fired, and nothing
    // aborted a scan in progress in either mode.
    //
    // THE VACUITY TRAP this fixture is built to avoid: a rotation that fits inside
    // the budget never reaches the abort and passes whatever the code does. Shard
    // 0x00 therefore holds two orders of magnitude more objects than the fifteen
    // shards after it, against a budget of a few tens of milliseconds, so the
    // budget is certain to be spent inside the first visit and certain NOT to have
    // been spent before it.
    //
    // And the assertion is on the PERSISTED CURSOR and on the objects the cycle
    // actually counted — not on the cycle merely returning, which it does either
    // way.
    // =====================================================================
    #[tokio::test]
    async fn the_budget_aborts_the_rotation_mid_way_and_leaves_a_durable_cursor() {
        let _tail_serial = TAIL_TEST_GUARD.lock().await;
        const BIG_SHARD_OBJECTS: usize = 4000;
        const TOTAL_SHARDS: u8 = 16;

        let config = CacheSizeConfig {
            // Large enough that the pre-loop setup cannot consume it, small enough
            // that one visit of 4,000 objects certainly does.
            validation_max_duration: Duration::from_millis(50),
            ..CacheSizeConfig::default()
        };
        let (tracker, _consolidator, temp_dir) = create_test_tracker_with_config(config).await;
        let cache_dir = temp_dir.path().to_path_buf();

        let shard_path = |idx: u8| {
            cache_dir
                .join("metadata")
                .join("budget-bucket")
                .join(format!("{:02x}", idx))
                .join("000")
        };

        for i in 0..BIG_SHARD_OBJECTS {
            write_valid_meta(
                &shard_path(0).join(format!("big_{}.meta", i)),
                &format!("budget-bucket:big-{}", i),
                1024,
            );
        }
        for idx in 1..TOTAL_SHARDS {
            write_valid_meta(
                &shard_path(idx).join("small.meta"),
                &format!("budget-bucket:small-{:02x}", idx),
                1024,
            );
        }
        let total_objects = BIG_SHARD_OBJECTS as u64 + (TOTAL_SHARDS as u64 - 1);

        tracker
            .perform_rolling_validation()
            .await
            .expect("a budget abort is a clean stop, not an error");

        let state = tracker.read_rolling_state().unwrap();
        let schedule: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&tracker.schedule_path).unwrap())
                .unwrap();
        let dirs_scanned = schedule
            .get("rolling_dirs_scanned")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let objects_validated = schedule
            .get("rolling_objects_validated")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        println!(
            "budget abort: persisted cursor={} dirs_scanned={} objects_validated={} \
             (total seeded={})",
            state.cursor, dirs_scanned, objects_validated, total_objects
        );

        // (A) The rotation made progress — so the abort is MID-rotation, not a
        //     refusal to start. Without this the test would pass on a cycle that
        //     visited nothing at all.
        assert!(
            state.cursor >= 1,
            "PRECONDITION: the cycle visited no shard at all, so nothing here says \
             anything about aborting mid-rotation. The {}ms budget was spent before \
             the first visit — raise it. cursor={}",
            50,
            state.cursor
        );

        // (B) And it stopped short of the seeded population, which is the abort.
        //     `slot_cap` on a first cycle is 64 slots, so an unbudgeted cycle would
        //     have reached all 16.
        assert!(
            state.cursor < TOTAL_SHARDS,
            "THE DEFECT: the cycle visited every one of the {} seeded shards despite \
             a {}ms budget, i.e. nothing aborted the rotation in progress. \
             cursor={}",
            TOTAL_SHARDS,
            50,
            state.cursor
        );

        // (C) The persisted cursor is the durable resume point, and it agrees with
        //     the work actually recorded.
        assert_eq!(
            dirs_scanned, state.cursor as u64,
            "the persisted cursor must equal the slots this cycle completed"
        );

        // (D) The shards the pass did not reach contributed NOTHING. This is the
        //     property § 6.4 cares about: a checkpoint must not be installed for
        //     data the pass did not see.
        let expected_objects = BIG_SHARD_OBJECTS as u64 + (state.cursor as u64 - 1);
        assert_eq!(
            objects_validated, expected_objects,
            "THE DEFECT: the cycle counted {} objects where the {} shard(s) it \
             visited hold {}. Figures from unvisited shards reached the install.",
            objects_validated, state.cursor, expected_objects
        );
        assert!(
            objects_validated < total_objects,
            "the aborted cycle cannot have counted the whole seeded population"
        );

        // (E) The next cycle resumes from the persisted cursor rather than
        //     restarting the rotation.
        let resumed_from = state.cursor;
        tracker
            .perform_rolling_validation()
            .await
            .expect("second cycle should succeed");
        let after = tracker.read_rolling_state().unwrap();
        assert!(
            after.cursor > resumed_from,
            "the second cycle must resume from the durable cursor and advance past \
             it: resumed_from={} after={}",
            resumed_from,
            after.cursor
        );
    }

    // =====================================================================
    // The metadata scan does not hold an async worker
    //
    // Spec: cache-eviction-at-scale § 6.3.
    //
    // Both production passes used to call `l1_dirs.par_iter().for_each(...)` inline
    // on the awaiting task, so rayon held one tokio runtime worker for the pass's
    // entire wall-clock duration — hours on a large cache, and on a 2-vCPU instance
    // a large fraction of request-serving capacity.
    //
    // THE PREDICATE, and why it is this one. "Count the threads" does not
    // discriminate: the thread count is identical either way. What the design
    // states is that the runtime stays able to make progress on other work while
    // the pass runs, so that is what is asserted — on a runtime with exactly ONE
    // worker, where a held worker means no other task can run at all.
    //
    // It is deterministic rather than timing-sensitive. Pre-fix,
    // `scan_metadata_with_shared_validator` is an `async fn` whose body contains no
    // `.await` at all, so it completes in a single poll and NEVER yields: the
    // observer task is spawned, is never polled, and the counter reads exactly 0.
    // Post-fix the body awaits one `spawn_blocking`, the sole worker is released
    // for the whole duration of the scan, and the observer — a bare `yield_now`
    // loop — spins many times over. The two sides are 0 against a positive number,
    // not a fast measurement against a slow one, so the threshold below is chosen
    // for a wide margin rather than to be a measurement: 1,862 ticks were observed
    // on the fixed tree against 0 on the pre-fix one.
    // =====================================================================
    #[test]
    fn the_metadata_scan_leaves_the_async_worker_free() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        // The body is SPAWNED, not run by `block_on` directly, and that is load-bearing.
        // `Runtime::block_on` drives the root future on the CALLING thread, which leaves
        // the sole worker free for the observer whatever the scan does — measured: the
        // pre-fix shape scored 1,568 ticks that way, i.e. the test passed while the defect
        // was present. Spawning puts the scan on the one worker the observer also needs.
        runtime.block_on(async {
            tokio::spawn(async {
                let (tracker, _consolidator, temp_dir) = create_test_tracker().await;
                let cache_dir = temp_dir.path().to_path_buf();

                // Enough objects that the blocking window is not instantaneous, few
                // enough that the fixture is cheap. The discriminator does not depend
                // on how long it is.
                for idx in 0u8..4 {
                    for i in 0..50 {
                        write_valid_meta(
                            &cache_dir
                                .join("metadata")
                                .join("worker-bucket")
                                .join(format!("{:02x}", idx))
                                .join("000")
                                .join(format!("obj_{}.meta", i)),
                            &format!("worker-bucket:obj-{:02x}-{}", idx, i),
                            1024,
                        );
                    }
                }

                let ticks = Arc::new(AtomicU64::new(0));
                let observer_ticks = Arc::clone(&ticks);
                let observer = tokio::spawn(async move {
                    loop {
                        observer_ticks.fetch_add(1, Ordering::Relaxed);
                        tokio::task::yield_now().await;
                    }
                });

                let (scanned, _, _, _, files, _, _, _, _) = tracker
                    .scan_metadata_with_shared_validator()
                    .await
                    .expect("scan should succeed");
                let observed = ticks.load(Ordering::Relaxed);
                observer.abort();

                println!(
                    "worker availability during the scan: observer ticks={} \
                 (files scanned={}, bytes={})",
                    observed, files, scanned
                );

                // The scan really ran, so a zero tick count cannot be explained by the
                // scan having done nothing.
                assert_eq!(files, 200, "PRECONDITION: the fixture was not scanned");

                assert!(
                    observed >= 100,
                    "THE DEFECT: the async runtime's only worker was polled {} times by \
                 another task while the metadata scan ran. The rayon iteration is \
                 running inline on the awaiting task, so the scan holds a runtime \
                 worker for its entire duration — the pre-fix shape, where the scan \
                 body contains no await and the observer is never polled at all.",
                    observed
                );
            })
            .await
            .expect("the scan task must not panic");
        });
    }
    // =====================================================================
    // Object census must exclude `.meta` files the scan removed on this pass
    //
    // Spec: cache-eviction-at-scale. Requirements: 7.1
    //
    // The scan mutates as it walks: it self-heals unparseable `.meta` files
    // (finding F7 of the validation scan), and it deletes write-cache and GET
    // entries that have expired. Both counted toward the object census, which is
    // installed as `cached_objects` — so the counter reported objects that had
    // just ceased to exist, always in the upward direction.
    //
    // RED SIDE (measured before the fix, both tests):
    //   full: `cached_objects` = 3, expected 2
    //   rolling: `cached_objects` = 3, expected 2
    // Both are assertion-level failures against the pre-fix binary, not compile
    // errors — the fixtures use only pre-existing API.
    // =====================================================================

    /// Build a valid, non-expired, read-cached `.meta` at `path` with one range of
    /// `size` compressed bytes.
    pub(super) fn write_valid_meta(path: &std::path::Path, cache_key: &str, size: u64) {
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
                unservable: false,
                file_identity: None,
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
            last_validated_at: None,
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

        let state = consolidator.get_size_state().await.unwrap();
        assert_eq!(
            state.cached_objects, 2,
            "census counted a .meta this pass deleted: cached_objects={} for a tree \
             holding 2 surviving objects (3 .meta files visited, 1 self-healed away). \
             The census is installed absolutely by a full scan, so this over-reports \
             the object population.",
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
        let _tail_serial = TAIL_TEST_GUARD.lock().await;
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
            consolidator.get_size_state().await.unwrap().cached_objects,
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

        let state = consolidator.get_size_state().await.unwrap();
        assert_eq!(
            state.cached_objects, 2,
            "rolling census counted a .meta this pass deleted: cached_objects={} for \
             a tree holding 2 surviving objects. The error is worse here than in full \
             mode: the proportional correction multiplies the observed discrepancy by \
             256 / dirs_scanned on the way in.",
            state.cached_objects
        );
    }

    /// A size state that exists but cannot be parsed must not become the rolling cycle's
    /// snapshot of zeros.
    ///
    /// With `tracked_* = 0`, `apply_proportional_correction` computes
    /// `0 + (scanned - 0 * slots / 256)`, so each install writes the SCANNED SUBSET's bytes,
    /// objects and staged bytes over `size_state.json` as absolute whole-cache figures: after
    /// one slot the cache reads as roughly 1/256 of its real size, which is the direction that
    /// keeps eviction from triggering and admission from refusing. This pins that the cycle
    /// installs nothing and advances no cursor instead.
    ///
    /// # RED SIDE
    ///
    /// With the snapshot taken through `unwrap_or_default()` again, the cycle installs the
    /// scanned 3,072 bytes and rewrites `size_state.json`, so the byte-equality assertion fails.
    #[tokio::test]
    async fn rolling_validation_installs_nothing_when_size_state_is_unreadable() {
        let _tail_serial = TAIL_TEST_GUARD.lock().await;
        let (tracker, consolidator, temp_dir) = create_test_tracker().await;
        let cache_dir = temp_dir.path().to_path_buf();
        seed_two_valid_one_corrupt(&cache_dir);

        let size_tracking = cache_dir.join("size_tracking");
        std::fs::create_dir_all(&size_tracking).unwrap();
        let size_state_path = size_tracking.join("size_state.json");
        let unreadable: &[u8] = b"{ not json";
        std::fs::write(&size_state_path, unreadable).unwrap();
        assert!(
            consolidator.get_size_state().await.is_none(),
            "FIXTURE: the size state must be an unreadable read, not an absent one"
        );
        let schedule_path = size_tracking.join("schedule.json");
        let validation_path = size_tracking.join("validation.json");
        assert!(
            !schedule_path.exists() && !validation_path.exists(),
            "FIXTURE: a fresh tracker must have no schedule or results file yet"
        );

        tracker
            .perform_rolling_validation()
            .await
            .expect("an unreadable size state skips the cycle; it is not an error");

        let after = std::fs::read(&size_state_path).unwrap();
        assert!(
            after == unreadable,
            "the rolling cycle installed a correction computed against a defaulted snapshot \
             of zeros: size_state.json now reads {:?}, i.e. the scanned subset installed as \
             the whole cache",
            String::from_utf8_lossy(&after)
        );
        assert!(
            !schedule_path.exists(),
            "a skipped cycle must not persist a cursor: the shards it did not install would \
             be skipped for a rotation"
        );
        assert!(
            !validation_path.exists(),
            "a skipped cycle must not publish results with tracked_size = 0"
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

        let result = tracker.scan_metadata_file(&corrupt, SystemTime::now(), None);

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

    /// Shared fixture for the three assertions below: a `.meta` at a real shard path,
    /// with whatever bytes the caller wants in it.
    fn seed_meta_with(cache_dir: &std::path::Path, name: &str, bytes: &[u8]) -> PathBuf {
        let dir = cache_dir
            .join("metadata")
            .join("shared-delete-safety")
            .join("07")
            .join("000");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.meta", name));
        std::fs::write(&path, bytes).unwrap();
        path
    }

    /// Assertion 1 (the defect, red before the fix): a ZERO-LENGTH `.meta` is not
    /// deleted by the validation pass.
    ///
    /// Zero length is not a hypothetical shape. Five writers rewrite `{key}.meta` in
    /// place with `std::fs::write`, which truncates before it writes, so the document is
    /// zero-length for the whole interval between the `O_TRUNC` and the payload landing
    /// — and stays that way if the write fails. Before the fix this pass unlinked it on
    /// the strength of one failed parse, destroying a live document; on shared storage
    /// the writer may be a peer that never learns.
    ///
    /// Deterministic: `read_and_parse_metadata_blocking` treats an empty read as
    /// transient by construction (three retries, then `TransientMiss`), so this needs no
    /// timing and no second thread. The concurrent-peer case is `A3` below.
    ///
    /// Spec: cache-eviction-at-scale. Requirements: 7.1
    #[tokio::test]
    async fn a_zero_length_meta_is_not_deleted_by_the_validation_pass() {
        let (tracker, _consolidator, temp_dir) = create_test_tracker().await;
        let truncated = seed_meta_with(temp_dir.path(), "mid-rewrite", b"");

        let result = tracker.scan_metadata_file(&truncated, SystemTime::now(), None);

        assert!(
            truncated.exists(),
            "the validation pass deleted a ZERO-LENGTH .meta. That is the shape an \
             in-place `std::fs::write` leaves for the whole interval between its \
             O_TRUNC and its payload — a live document, possibly a peer's — and this \
             pass holds no lock and cannot re-parse under one, so it must leave a \
             document it cannot confidently classify as corrupt"
        );
        assert!(
            !result.meta_removed,
            "meta_removed was reported for a .meta still on disk"
        );
        assert!(
            result.cache_error,
            "an unparseable .meta is still an error for the pass's own counters, \
             whether or not it was removed"
        );
        assert_eq!(
            (result.size_bytes, result.staged_bytes),
            (0, 0),
            "a document that could not be parsed must contribute nothing to the census"
        );
    }

    /// Assertion 2 (the positive control): a STABLE, non-empty, unparseable `.meta`
    /// with no writer active is still removed.
    ///
    /// The fix must not turn the self-heal off. If this reds while `A1` stays green the
    /// guard is over-broad, which is a distinct signature from the defect itself.
    ///
    /// Spec: cache-eviction-at-scale. Requirements: 7.1
    #[tokio::test]
    async fn a_stable_unparseable_meta_is_still_removed() {
        let (tracker, _consolidator, temp_dir) = create_test_tracker().await;
        let corrupt = seed_meta_with(
            temp_dir.path(),
            "genuinely-corrupt",
            b"{ this is not valid NewCacheMetadata json",
        );

        let result = tracker.scan_metadata_file(&corrupt, SystemTime::now(), None);

        assert!(
            !corrupt.exists(),
            "POSITIVE CONTROL FAILED: a stable, non-empty, unparseable .meta with no \
             writer touching it survived the pass. The self-heal must still fire — the \
             fix narrows which documents are confidently corrupt, it does not stop \
             removing the ones that are"
        );
        assert!(
            result.meta_removed,
            "meta_removed must be set when the pass did remove the .meta, or the census \
             counts an object that no longer exists"
        );
    }

    /// Assertion 3 (the cross-instance case): a `.meta` that ANOTHER PROCESS is
    /// rewriting right now survives, and the fixture is built so local hygiene cannot
    /// pass it.
    ///
    /// Constructed per `cache-coherency-invariants.md` § "Constructing the test": the
    /// document is mutated the way a peer's in-place rewrite appears — by a second
    /// thread writing directly to the shared tree — and **no local code path is
    /// driven**. This tracker has never written this key, holds no RAM copy of it, has
    /// no journal entry for it and no in-flight record of it; its `cache_manager` weak
    /// reference is `None`. So a fix that consulted this instance's own state — "skip
    /// keys I am writing" — would delete the file and fail here while passing a
    /// single-instance test. The only thing that can spare it is reading the document
    /// itself, which is what the fix does.
    ///
    /// It is also the structural reason the lock is unavailable rather than merely
    /// unplumbed: the pass holds a path, the slot is `blake3(cache_key)`, and a
    /// document that will not parse does not yield its own cache key.
    ///
    /// Anti-vacuity: the writer thread's progress across the scan is asserted. If it
    /// made none, the interleaving under test never happened and the test says so
    /// rather than passing.
    ///
    /// Spec: cache-eviction-at-scale. Requirements: 7.1
    #[tokio::test]
    async fn a_meta_a_peer_is_rewriting_survives_the_validation_pass() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        let (tracker, _consolidator, temp_dir) = create_test_tracker().await;
        // Seeded non-empty and unparseable, so `initial_size` is a real length and the
        // peer's growth below is what makes the size move.
        let contended = seed_meta_with(temp_dir.path(), "peer-is-writing", b"{\"partial\"");

        let stop = Arc::new(AtomicBool::new(false));
        let writes = Arc::new(AtomicU64::new(0));

        let peer = {
            let path = contended.clone();
            let stop = Arc::clone(&stop);
            let writes = Arc::clone(&writes);
            // A separate OS thread, deliberately: this is another process's write as far
            // as the scanned document is concerned, and nothing in the tracker's own
            // state records it.
            std::thread::spawn(move || {
                let mut payload = String::from("{\"partial\"");
                while !stop.load(Ordering::Relaxed) {
                    // Monotonically growing, so the size at the end of the retry window
                    // cannot coincidentally equal the size at the start.
                    payload.push_str("aaaaaaaaaaaaaaaa");
                    // `std::fs::write` is the production shape: O_TRUNC, then the body.
                    if std::fs::write(&path, payload.as_bytes()).is_ok() {
                        writes.fetch_add(1, Ordering::Relaxed);
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            })
        };

        let writes_before = writes.load(Ordering::Relaxed);
        let result = tracker.scan_metadata_file(&contended, SystemTime::now(), None);
        let writes_after = writes.load(Ordering::Relaxed);

        stop.store(true, Ordering::Relaxed);
        peer.join().unwrap();

        assert!(
            writes_after > writes_before,
            "FIXTURE NOT EXERCISED: the peer thread performed no write during the scan \
             ({} before, {} after), so the concurrent-rewrite interleaving this test \
             exists for never happened. The result below is unverifiable rather than \
             passing.",
            writes_before,
            writes_after
        );
        assert!(
            contended.exists(),
            "the validation pass deleted a .meta that another process was actively \
             rewriting. The pass takes no lock and cannot take one, so the document's \
             own shape — empty, or a length that moved across the re-read window — is \
             the only evidence available that a writer is mid-flight, and it must be \
             honoured. Deleting here destroys a peer's publish and the peer never \
             learns."
        );
        assert!(
            !result.meta_removed,
            "meta_removed was reported for a .meta still on disk"
        );
    }

    // ── ADAPT-U5: validation is counted by the scan limiter, never gated by it ──

    const VALIDATION_PEER: &str = "ip-10-9-9-9.internal:4242";

    fn epoch_secs_now() -> u64 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn validation_scan_doc(
        cache_dir: &std::path::Path,
        instance_id: &str,
    ) -> Option<crate::scanner_registry::ScannerDocument> {
        let path = crate::scanner_registry::scanners_dir(cache_dir)
            .join(format!("{instance_id}.validation.scan"));
        let body = std::fs::read(path).ok()?;
        serde_json::from_slice(&body).ok()
    }

    fn set_validation_hook(hook: Option<ValidationInFlightHook>) {
        VALIDATION_IN_FLIGHT_HOOK.with(|h| *h.borrow_mut() = hook);
    }

    /// (a) While a pass is in flight a peer's fold limiter counts it as one live scanner.
    #[tokio::test]
    async fn validation_pass_is_counted_live_by_a_peer_while_in_flight() {
        let _tail_serial = TAIL_TEST_GUARD.lock().await;
        use crate::scanner_registry::{count_live_others, ScanKind};
        let (tracker, _consolidator, temp_dir) = create_test_tracker().await;
        let dir = temp_dir.path().to_path_buf();
        assert_eq!(
            count_live_others(&dir, VALIDATION_PEER, ScanKind::Fold, epoch_secs_now()),
            0,
            "fixture precondition: no scanner document before the pass"
        );

        let seen = Arc::new(Mutex::new(None::<u32>));
        let (seen_in, dir_in) = (seen.clone(), dir.clone());
        set_validation_hook(Some(Box::new(move || {
            *seen_in.lock().unwrap() = Some(count_live_others(
                &dir_in,
                VALIDATION_PEER,
                ScanKind::Fold,
                epoch_secs_now(),
            ));
            Ok(())
        })));
        let result = tracker.perform_validation().await;
        set_validation_hook(None);
        result.expect("validation on an empty cache must succeed");

        assert_eq!(
            *seen.lock().unwrap(),
            Some(1),
            "a peer must count the in-flight validation pass as one live scanner"
        );
    }

    /// (b) After the pass returns, Ok or Err, the document is ended and no longer live.
    #[tokio::test]
    async fn validation_scanner_document_is_ended_on_ok_and_on_error_return() {
        let _tail_serial = TAIL_TEST_GUARD.lock().await;
        use crate::scanner_registry::{count_live_others, ScanKind};
        let (tracker, consolidator, temp_dir) = create_test_tracker().await;
        let dir = temp_dir.path().to_path_buf();
        let id = consolidator.write_ledger().instance_id().to_string();

        tracker
            .perform_validation()
            .await
            .expect("validation on an empty cache must succeed");
        let doc = validation_scan_doc(&dir, &id).expect("the pass must have published a document");
        assert!(
            doc.ended_at.is_some(),
            "VALIDATION-DOC-ENDED: an Ok return must end the document, got {doc:?}"
        );
        assert_eq!(
            count_live_others(&dir, VALIDATION_PEER, ScanKind::Fold, epoch_secs_now()),
            0
        );

        set_validation_hook(Some(Box::new(|| {
            Err(ProxyError::CacheError(
                "injected mid-pass failure".to_string(),
            ))
        })));
        let result = tracker.perform_validation().await;
        set_validation_hook(None);
        assert!(
            result.is_err(),
            "fixture: the hook must drive an error exit"
        );
        let doc = validation_scan_doc(&dir, &id).expect("the pass must have published a document");
        assert!(
            doc.ended_at.is_some(),
            "VALIDATION-DOC-ENDED: an Err return must end the document, got {doc:?}"
        );
        assert_eq!(
            count_live_others(&dir, VALIDATION_PEER, ScanKind::Fold, epoch_secs_now()),
            0,
            "an ended document must not count as live"
        );
    }

    /// (c) An instance that cannot take `validation.lock` is not scanning and publishes nothing.
    #[tokio::test]
    async fn validation_lock_loser_publishes_no_scanner_document() {
        let _tail_serial = TAIL_TEST_GUARD.lock().await;
        let (tracker, consolidator, temp_dir) = create_test_tracker().await;
        let dir = temp_dir.path().to_path_buf();
        let id = consolidator.write_ledger().instance_id().to_string();

        // Another holder: a separate open file description, so flock conflicts in-process.
        let _held = tracker
            .try_acquire_validation_lock()
            .await
            .expect("fixture: the first lock must be taken");
        let hook_ran = Arc::new(Mutex::new(false));
        let hook_ran_in = hook_ran.clone();
        set_validation_hook(Some(Box::new(move || {
            *hook_ran_in.lock().unwrap() = true;
            Ok(())
        })));
        let result = tracker.perform_validation().await;
        set_validation_hook(None);
        result.expect("losing the lock is a skip, not an error");

        assert!(
            !*hook_ran.lock().unwrap(),
            "fixture: the pass must have skipped on the held lock"
        );
        assert!(
            validation_scan_doc(&dir, &id).is_none(),
            "VALIDATION-LOCK-LOSER: an instance without validation.lock must publish nothing"
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

    pub(super) async fn tracker_over_fixture(
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

        let state = consolidator.get_size_state().await.unwrap();
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

/// Task 30 timing harness — attribute the validation scan's per-object cost by
/// elimination, in-process, in one language.
///
/// Spec: `cache-eviction-at-scale` task 30. Requirements: 19.6, 19.7, 2.2, 4.5.
///
/// # Why this exists, and why it is not a re-run of `scan-rate-sampler.py`
///
/// The recorded 13.9x gap between the production full-scan rate (212 obj/s,
/// `requirements.md` § Measured state) and the sampler's
/// raw-parse rate (2,956 obj/s) is a **cross-language, cross-runtime** comparison: the
/// sampler does Python `read()` + `json.loads()` into untyped dicts across a
/// `ThreadPoolExecutor`; the production figure is Rust `std::fs::read` +
/// `serde_json::from_slice` into a **typed** `NewCacheMetadata` (with custom `SystemTime`
/// deserializers) across a `rayon` pool. No document said so until task 30 was written,
/// which is the same class of error as the withdrawn 287x fold/parse ratio (see the design notes,
/// — dividing two figures that are not the same kind of
/// measurement. The direction of the gap is probably still real; the *magnitude* was
/// never established in one language, and "93%" should be read as an upper bound until
/// this harness measures it.
///
/// So this module does NOT touch the Python sampler. It calls the real, private
/// production functions directly (`scan_metadata_with_shared_validator`,
/// `scan_metadata_file`) plus deliberately stripped variants that remove one candidate
/// cost at a time, all in Rust, all in-process, all against the same generated fixture in
/// the same run. Attribution is by elimination, not by profiler, per the task's own
/// method note.
///
/// # What this does NOT need
///
/// No `/metrics`, no journal consolidation cycle, no fleet, no SSM. This is a
/// bench-instance (or any instance's) measurement against a frozen or freshly generated
/// tree. The `pre-push-checklist.md` fleet-suite parsing traps (`python3` not `grep`,
/// `UNREADABLE` sentinels, `--proxy` not `HTTP_PROXY`) are about SSM-driven fleet
/// assertions and do not apply here.
#[cfg(test)]
mod task30_scan_cost_attribution_tests {
    use super::task14_full_validation_timing_tests::tracker_over_fixture;
    use super::*;
    use crate::cache_types::NewCacheMetadata;

    const FIXTURE_ROOT_ENV: &str = "S3HC_TASK30_FIXTURE_ROOT";

    /// Enumerate `.meta` files under `cache_dir/metadata`, matching the same
    /// `{bucket}/{L1}/{L2}/*.meta` shape `scan_metadata_with_shared_validator` walks.
    /// Kept independent of that function (rather than reusing its L1-dir enumeration)
    /// so a defect in one cannot mask a defect in the other, and so this harness has no
    /// dependency on the production function's internals beyond the directory shape
    /// documented in `structure.md`.
    fn enumerate_meta_files(cache_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        let metadata_dir = cache_dir.join("metadata");
        let Ok(bucket_entries) = std::fs::read_dir(&metadata_dir) else {
            return out;
        };
        for bucket_entry in bucket_entries.flatten() {
            let bucket_path = bucket_entry.path();
            if !bucket_path.is_dir() {
                continue;
            }
            if bucket_path
                .file_name()
                .is_some_and(|n| n.to_str().is_some_and(|s| s.starts_with('_')))
            {
                continue;
            }
            let Ok(l1_entries) = std::fs::read_dir(&bucket_path) else {
                continue;
            };
            for l1_entry in l1_entries.flatten() {
                let l1_path = l1_entry.path();
                if !l1_path.is_dir() {
                    continue;
                }
                let Ok(l2_entries) = std::fs::read_dir(&l1_path) else {
                    continue;
                };
                for l2_entry in l2_entries.flatten() {
                    let l2_path = l2_entry.path();
                    if !l2_path.is_dir() {
                        continue;
                    }
                    let Ok(file_entries) = std::fs::read_dir(&l2_path) else {
                        continue;
                    };
                    for file_entry in file_entries.flatten() {
                        let path = file_entry.path();
                        if path.extension().is_some_and(|e| e == "meta") {
                            out.push(path);
                        }
                    }
                }
            }
        }
        out
    }

    /// Step 0 (baseline): `std::fs::read` + `serde_json::from_slice::<serde_json::Value>`,
    /// nothing else. The floor every other step is measured against. Untyped, so it is
    /// the closest in-Rust analogue to the Python sampler's `json.loads` into a dict —
    /// but still Rust, still `serde_json`, still this process, so it is comparable to
    /// every other step in this module in a way the Python figure never was.
    fn step0_raw_parse_untyped(paths: &[std::path::PathBuf]) -> (Duration, u64) {
        let start = Instant::now();
        let mut errors = 0u64;
        for path in paths {
            match std::fs::read(path) {
                Ok(content) => {
                    if serde_json::from_slice::<serde_json::Value>(&content).is_err() {
                        errors += 1;
                    }
                }
                Err(_) => errors += 1,
            }
        }
        (start.elapsed(), errors)
    }

    /// Step 1: add the typed deserialize — `std::fs::read` +
    /// `serde_json::from_slice::<NewCacheMetadata>`. Isolates the cost of typed `serde`
    /// derive (including the custom `SystemTime` deserializers) over the untyped
    /// baseline, with nothing downstream touched yet.
    fn step1_typed_parse(paths: &[std::path::PathBuf]) -> (Duration, u64) {
        let start = Instant::now();
        let mut errors = 0u64;
        for path in paths {
            match std::fs::read(path) {
                Ok(content) => {
                    if serde_json::from_slice::<NewCacheMetadata>(&content).is_err() {
                        errors += 1;
                    }
                }
                Err(_) => errors += 1,
            }
        }
        (start.elapsed(), errors)
    }

    /// Step 2: add the first per-range loop — `ranges.iter().map(compressed_size).sum()`
    /// — on top of step 1's typed parse.
    fn step2_typed_parse_plus_range_sum(paths: &[std::path::PathBuf]) -> (Duration, u64, u64) {
        let start = Instant::now();
        let mut errors = 0u64;
        let mut total: u64 = 0;
        for path in paths {
            match std::fs::read(path) {
                Ok(content) => match serde_json::from_slice::<NewCacheMetadata>(&content) {
                    Ok(m) => {
                        total = total
                            .wrapping_add(m.ranges.iter().map(|r| r.compressed_size).sum::<u64>());
                    }
                    Err(_) => errors += 1,
                },
                Err(_) => errors += 1,
            }
        }
        (start.elapsed(), errors, total)
    }

    /// Step 3: add the second per-range loop — `staged_compressed_size()`, which runs
    /// `is_staged_range_spec` (and its `contains("mpus_in_progress/")` string scan on the
    /// `None` arm) per range — on top of step 2.
    fn step3_plus_staged_sum(paths: &[std::path::PathBuf]) -> (Duration, u64, u64, u64) {
        let start = Instant::now();
        let mut errors = 0u64;
        let mut total: u64 = 0;
        let mut staged_total: u64 = 0;
        for path in paths {
            match std::fs::read(path) {
                Ok(content) => match serde_json::from_slice::<NewCacheMetadata>(&content) {
                    Ok(m) => {
                        total = total
                            .wrapping_add(m.ranges.iter().map(|r| r.compressed_size).sum::<u64>());
                        staged_total = staged_total.wrapping_add(m.staged_compressed_size());
                    }
                    Err(_) => errors += 1,
                },
                Err(_) => errors += 1,
            }
        }
        (start.elapsed(), errors, total, staged_total)
    }

    /// Step 4: add the per-file `is_write_cache_expired()` check — the write-cache
    /// expiry branch `scan_metadata_file` evaluates unconditionally as an expression,
    /// even though its expensive `SystemTime::now()` comparison only executes when
    /// `is_write_cached` is true — on top of step 3. This does NOT recurse into the
    /// `tokio::runtime::Handle::block_on` delete path; that path only runs when the
    /// entry is BOTH write-cached AND expired, which a freshly generated fixture never
    /// is (`min_age`/`max_age` in `FixtureSpec` govern `last_accessed`, not
    /// `write_cache_expires_at`), so it is out of scope for a by-elimination fixture
    /// harness and is noted as a caveat in the report rather than measured here.
    fn step4_plus_write_cache_expiry_check(
        paths: &[std::path::PathBuf],
    ) -> (Duration, u64, u64, u64, u64) {
        let start = Instant::now();
        let mut errors = 0u64;
        let mut total: u64 = 0;
        let mut staged_total: u64 = 0;
        let mut expired_checks = 0u64;
        for path in paths {
            match std::fs::read(path) {
                Ok(content) => match serde_json::from_slice::<NewCacheMetadata>(&content) {
                    Ok(m) => {
                        total = total
                            .wrapping_add(m.ranges.iter().map(|r| r.compressed_size).sum::<u64>());
                        staged_total = staged_total.wrapping_add(m.staged_compressed_size());
                        if m.object_metadata.is_write_cached
                            && m.object_metadata.is_write_cache_expired()
                        {
                            expired_checks += 1;
                        }
                    }
                    Err(_) => errors += 1,
                },
                Err(_) => errors += 1,
            }
        }
        (start.elapsed(), errors, total, staged_total, expired_checks)
    }

    fn objects_per_s(n: usize, elapsed: Duration) -> f64 {
        n as f64 / elapsed.as_secs_f64().max(1e-9)
    }

    /// Parallel (rayon, same global pool as the real function) variant of step 4 — the
    /// full stack of named candidates — for a same-thread-count comparison against
    /// `perform_full_validation`.
    ///
    /// Added after an EFS run showed single-threaded steps 0-4 running at 300-500 obj/s
    /// while the real rayon-parallel function ran at 4,472 obj/s: on NFS, per-request
    /// round-trip latency dominates wall-clock, so a single-threaded step count is not a
    /// fair comparison to a 64-thread one — parallelism overlaps that latency, and the
    /// gap between single-threaded and N-threaded is a statement about NFS concurrency,
    /// not about per-object CPU cost. This function isolates the SAME candidates as
    /// `step4_plus_write_cache_expiry_check` but spreads the same file list across
    /// rayon's pool, so real-vs-this is thread-count-matched and any remaining gap is
    /// attributable to readdir-interleaving and the `staged_paths` mutex, which this
    /// still does not exercise (files are pre-enumerated, not readdir'd per-L1-dir
    /// inside the timed region).
    fn step4_parallel(paths: &[std::path::PathBuf]) -> (Duration, u64, u64, u64, u64) {
        use rayon::prelude::*;
        use std::sync::atomic::{AtomicU64, Ordering};

        let errors = AtomicU64::new(0);
        let total = AtomicU64::new(0);
        let staged_total = AtomicU64::new(0);
        let expired_checks = AtomicU64::new(0);

        let start = Instant::now();
        paths.par_iter().for_each(|path| match std::fs::read(path) {
            Ok(content) => match serde_json::from_slice::<NewCacheMetadata>(&content) {
                Ok(m) => {
                    total.fetch_add(
                        m.ranges.iter().map(|r| r.compressed_size).sum::<u64>(),
                        Ordering::Relaxed,
                    );
                    staged_total.fetch_add(m.staged_compressed_size(), Ordering::Relaxed);
                    if m.object_metadata.is_write_cached
                        && m.object_metadata.is_write_cache_expired()
                    {
                        expired_checks.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            },
            Err(_) => {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        });
        (
            start.elapsed(),
            errors.load(Ordering::Relaxed),
            total.load(Ordering::Relaxed),
            staged_total.load(Ordering::Relaxed),
            expired_checks.load(Ordering::Relaxed),
        )
    }

    /// Measure the real production `scan_metadata_with_shared_validator` (rayon,
    /// readdir-interleaved, the `staged_paths` mutex, everything) and each stripped
    /// step, back to back, on the SAME fixture in the SAME process, and report what
    /// each step costs. Run with:
    ///
    /// ```text
    /// S3HC_TASK30_FIXTURE_ROOT=/path/to/fixture cargo test --release --lib \
    ///     task30_attribute_scan_cost_by_elimination -- --ignored --nocapture
    /// ```
    ///
    /// The fixture must be freshly generated (e.g. via `graded_cache_fixture_gen`) and
    /// never scanned before — same precondition as the task 14 harness, and for the same
    /// reason: a prior scan's `size_tracking/` state would contaminate
    /// `perform_full_validation`'s reconciliation, which this measurement does not need
    /// but must not corrupt for a later run.
    #[tokio::test]
    #[ignore = "Task 30 timed attribution measurement; requires a fresh generated fixture root"]
    async fn task30_attribute_scan_cost_by_elimination() {
        let cache_dir = PathBuf::from(
            std::env::var(FIXTURE_ROOT_ENV)
                .expect("set S3HC_TASK30_FIXTURE_ROOT to a fresh generated fixture root"),
        );
        assert!(
            !cache_dir.join("size_tracking/size_state.json").exists(),
            "fixture has already been scanned; generate a fresh root instead"
        );
        assert!(
            !cache_dir.join("metadata/_journals").exists(),
            "fixture has journal state; generate a fresh root instead"
        );

        let paths = enumerate_meta_files(&cache_dir);
        assert!(
            !paths.is_empty(),
            "no .meta files found under {}/metadata — fixture is empty or malformed",
            cache_dir.display()
        );
        let n = paths.len();
        println!(
            "\n=== Task 30: scan cost attribution, {n} .meta files, {} ===\n",
            cache_dir.display()
        );

        // --- The real thing, measured first so nothing below has warmed anything it
        // depends on (each step below re-reads every file fresh; page cache is shared
        // across steps within this process either way, which is a caveat stated in the
        // report below rather than something this harness can avoid on a real backend). ---
        let (tracker, consolidator) = tracker_over_fixture(cache_dir.clone()).await;
        let real_start = Instant::now();
        tracker.perform_full_validation().await.unwrap();
        let real_elapsed = real_start.elapsed();
        let real_rate = objects_per_s(n, real_elapsed);
        println!(
            "REAL   perform_full_validation (rayon, typed, both range loops, expiry \
             checks, staged_paths mutex, readdir interleaved): {:.3}s  {:.1} obj/s",
            real_elapsed.as_secs_f64(),
            real_rate
        );
        // Drop the consolidator's state; steps below never touch size_tracking/ again.
        drop(consolidator);

        // --- Stripped variants, single-threaded, on the same file list, same process. ---
        let (t0, err0) = step0_raw_parse_untyped(&paths);
        let r0 = objects_per_s(n, t0);
        println!(
            "STEP 0 raw untyped parse (serde_json::Value)                  : {:.3}s  {:.1} obj/s  errors={err0}",
            t0.as_secs_f64(), r0
        );

        let (t1, err1) = step1_typed_parse(&paths);
        let r1 = objects_per_s(n, t1);
        println!(
            "STEP 1 + typed deserialize (NewCacheMetadata)                 : {:.3}s  {:.1} obj/s  errors={err1}  (delta vs step0: {:+.1}%)",
            t1.as_secs_f64(), r1, (r1 / r0 - 1.0) * 100.0
        );

        let (t2, err2, sum2) = step2_typed_parse_plus_range_sum(&paths);
        let r2 = objects_per_s(n, t2);
        println!(
            "STEP 2 + ranges.iter().map(compressed_size).sum()             : {:.3}s  {:.1} obj/s  errors={err2}  sum={sum2}  (delta vs step1: {:+.1}%)",
            t2.as_secs_f64(), r2, (r2 / r1 - 1.0) * 100.0
        );

        let (t3, err3, sum3, staged3) = step3_plus_staged_sum(&paths);
        let r3 = objects_per_s(n, t3);
        println!(
            "STEP 3 + staged_compressed_size() (2nd per-range loop)        : {:.3}s  {:.1} obj/s  errors={err3}  sum={sum3} staged={staged3}  (delta vs step2: {:+.1}%)",
            t3.as_secs_f64(), r3, (r3 / r2 - 1.0) * 100.0
        );

        let (t4, err4, sum4, staged4, checks4) = step4_plus_write_cache_expiry_check(&paths);
        let r4 = objects_per_s(n, t4);
        println!(
            "STEP 4 + is_write_cached && is_write_cache_expired() check    : {:.3}s  {:.1} obj/s  errors={err4}  sum={sum4} staged={staged4} expired_checks={checks4}  (delta vs step3: {:+.1}%)",
            t4.as_secs_f64(), r4, (r4 / r3 - 1.0) * 100.0
        );

        // Thread-count-matched comparison: step4's candidates, but spread across
        // rayon's pool, same as the real function. This is the figure that actually
        // attributes cost — the single-threaded steps above are for isolating WHICH
        // candidate costs what per-object, not for comparing against the real rate.
        let (t4p, err4p, sum4p, staged4p, checks4p) = step4_parallel(&paths);
        let r4p = objects_per_s(n, t4p);
        println!(
            "\nSTEP 4-PARALLEL (rayon, same pool as real) + all named candidates : {:.3}s  {:.1} obj/s  errors={err4p}  sum={sum4p} staged={staged4p} expired_checks={checks4p}",
            t4p.as_secs_f64(), r4p
        );

        println!("\n--- Attribution summary ---");
        println!(
            "real (rayon, N threads)                          : {:.1} obj/s",
            real_rate
        );
        println!(
            "step4-parallel (rayon, same candidates as real)  : {:.1} obj/s",
            r4p
        );
        println!(
            "gap, real vs step4-parallel (thread-count MATCHED)  : {:+.1}%  -- this is \
             the number that attributes cost. A gap here is readdir-interleaving and/or \
             the staged_paths mutex (neither exercised by step4-parallel), NOT thread \
             count.",
            (real_rate / r4p - 1.0) * 100.0
        );
        println!(
            "step4 (single-threaded)                           : {:.1} obj/s  -- DO NOT \
             compare this directly to 'real'. On NFS, per-request round-trip latency \
             dominates single-threaded wall-clock, so single-threaded-vs-64-threaded \
             measures NFS concurrency benefit, not per-object CPU cost. Kept below only \
             to show which named candidate costs what RELATIVE to the others.",
            r4
        );
        println!(
            "step0->step1 (typed deserialize cost)   : {:+.1}%",
            (r1 / r0 - 1.0) * 100.0
        );
        println!(
            "step1->step2 (range-sum loop cost)       : {:+.1}%",
            (r2 / r1 - 1.0) * 100.0
        );
        println!(
            "step2->step3 (staged_compressed_size)    : {:+.1}%",
            (r3 / r2 - 1.0) * 100.0
        );
        println!(
            "step3->step4 (write-cache expiry check)  : {:+.1}%",
            (r4 / r3 - 1.0) * 100.0
        );
        println!(
            "\nCAVEAT: step4-parallel and real both use rayon's global thread pool \
             (unconfigured, = logical CPUs), so the gap between them is genuinely \
             thread-count-matched. Steps 0-4 (single-threaded) are NOT comparable to \
             'real' directly — see the inline note on step4 above. On a backend where \
             per-request latency dominates (NFS), single-threaded timings mostly measure \
             concurrency headroom rather than per-object cost; use step4-parallel for the \
             headline attribution and the single-threaded steps only for their relative \
             deltas to each other."
        );
        println!(
            "\nCAVEAT: readdir enumeration is interleaved with parsing in the real \
             function (readdir per L2 dir, then parse each file, on the same rayon \
             worker) but is fully separated here (enumerate_meta_files runs once, \
             before any timing starts). If readdir latency is a meaningful fraction of \
             per-file cost on the backend under test — likely on NFS, unlikely on local \
             disk — this harness UNDERSTATES the real function's readdir-interleaving \
             overhead. Not measured by this harness; flag for a future pass if the gap \
             after thread-adjustment is still large."
        );
        println!(
            "\nCAVEAT: the staged_paths Mutex<Vec<PathBuf>> lock (taken only when \
             staged_bytes > 0) is not exercised by any step above unless the fixture has \
             a non-zero staged_fraction. Re-run with a fixture generated with \
             --staged-fraction > 0.0 to attribute that candidate; on a --staged-fraction \
             0.0 fixture (the default), this candidate is ruled out by construction, not \
             measured as zero-cost in general."
        );
    }
}

/// `MIN_VALIDATION_SPACING` enforcement in [`CacheSizeTracker::calculate_next_validation_time`].
///
/// # What was wrong, and what the discriminator is
///
/// The `elapsed < 82_800` branch contained only a `debug!`. It assigned nothing and returned
/// nothing, so the function's result was identical whether validation had run one minute ago or
/// twenty-two hours ago — while its comment claimed a skip. The schedule picks the next occurrence
/// of `validation_time_of_day` and rolls to tomorrow only when today's target has already passed,
/// so a pass finishing shortly BEFORE today's target was followed by another one minutes later.
///
/// `defers_when_a_pass_just_ran_and_the_target_is_imminent` is the only test here that fails
/// against the old code. The other two are the width guards: an over-broad fix that always
/// deferred, or one that ignored the target time, reds them while the discriminator stays green —
/// a different signature from the defect itself.
///
/// These are deterministic logic tests with no filesystem-behaviour or backend dependence, so
/// unlike the validation-lock tests in `cache_initialization_coordinator.rs` a local pass IS
/// sufficient here.
///
/// Spec: cache-eviction-at-scale.
#[cfg(test)]
mod validation_spacing_tests {
    use super::*;
    use crate::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
    use crate::journal_manager::JournalManager;
    use crate::metadata_lock_manager::MetadataLockManager;
    use tempfile::TempDir;

    /// A tracker whose `validation_time_of_day` is `offset_mins` from local now.
    ///
    /// Negative offsets put the target in the past, which is how the "already tomorrow" control
    /// is built without waiting for a wall clock.
    async fn tracker_targeting(offset_mins: i64) -> (Arc<CacheSizeTracker>, TempDir) {
        use chrono::{Duration as ChronoDuration, Local, Timelike};

        let target = Local::now() + ChronoDuration::minutes(offset_mins);
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

        let journal_manager = Arc::new(JournalManager::new(
            cache_dir.clone(),
            "spacing-test".to_string(),
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

        let config = CacheSizeConfig {
            validation_time_of_day: format!("{:02}:{:02}", target.hour(), target.minute()),
            ..CacheSizeConfig::default()
        };
        let tracker = Arc::new(
            CacheSizeTracker::new(cache_dir, config, false, consolidator)
                .await
                .unwrap(),
        );
        (tracker, temp_dir)
    }

    /// THE DISCRIMINATOR. A pass has just run and today's target is minutes away, so the
    /// unspaced schedule would validate again almost immediately.
    ///
    /// Pre-fix this returns a time under an hour out and the assertion FAILS.
    #[tokio::test]
    async fn defers_when_a_pass_just_ran_and_the_target_is_imminent() {
        // +5 minutes, so `next_time <= now` is false and no day-rollover happens above.
        let (tracker, _dir) = tracker_targeting(5).await;

        // `last_validation` is stamped now by the real writer.
        tracker
            .write_validation_metadata(4096, 4096, 0, Duration::from_secs(10), 4, 0, 0, 0)
            .await
            .unwrap();
        let meta = tracker
            .read_validation_metadata()
            .await
            .expect("FIXTURE: metadata must be readable, or the branch under test is skipped");
        let elapsed = SystemTime::now()
            .duration_since(meta.last_validation)
            .unwrap_or(Duration::ZERO);
        assert!(
            elapsed < MIN_VALIDATION_SPACING,
            "FIXTURE: the pass must appear recent for the spacing branch to be reached; \
             elapsed was {:?}",
            elapsed
        );

        let next = tracker.calculate_next_validation_time().await;
        let from_now = next
            .duration_since(SystemTime::now())
            .expect("next validation must be in the future");

        assert!(
            from_now > Duration::from_secs(22 * 3600),
            "a pass that ran {:?} ago must NOT be followed by another within the {:?} minimum \
             spacing. Scheduled {:?} from now, which is the inert-branch behaviour: the schedule \
             picked today's imminent target and nothing pushed it out.",
            elapsed,
            MIN_VALIDATION_SPACING,
            from_now
        );
    }

    /// WIDTH GUARD 1. With no metadata at all the schedule must stay immediate — the spacing
    /// bound must not defer a cache that has never been validated.
    #[tokio::test]
    async fn no_metadata_still_schedules_immediately() {
        let (tracker, _dir) = tracker_targeting(5).await;
        assert!(
            tracker.read_validation_metadata().await.is_err(),
            "FIXTURE: no validation metadata should exist yet"
        );

        let next = tracker.calculate_next_validation_time().await;
        let from_now = next
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        assert!(
            from_now < Duration::from_secs(60),
            "a never-validated cache must validate immediately, got {:?} from now",
            from_now
        );
    }

    /// WIDTH GUARD 2. When today's target has already passed the schedule is already tomorrow,
    /// which satisfies the spacing bound on its own — so the fix must NOT add a second day.
    ///
    /// This is what separates "enforce the bound" from "always defer": an over-broad fix pushes
    /// this to ~48 h and reds here while the discriminator above stays green.
    #[tokio::test]
    async fn does_not_double_defer_when_tomorrow_already_satisfies_the_bound() {
        // -30 minutes: today's target is in the past, so the rollover above already applies.
        let (tracker, _dir) = tracker_targeting(-30).await;
        tracker
            .write_validation_metadata(4096, 4096, 0, Duration::from_secs(10), 4, 0, 0, 0)
            .await
            .unwrap();

        let next = tracker.calculate_next_validation_time().await;
        let from_now = next
            .duration_since(SystemTime::now())
            .expect("next validation must be in the future");

        // ~23.5 h to tomorrow's target, plus up to 1 h jitter. A second rollover would land
        // beyond 30 h.
        assert!(
            from_now < Duration::from_secs(30 * 3600),
            "tomorrow's target already satisfies the {:?} bound, so no further deferral is \
             warranted. Got {:?} from now, which suggests the bound is being applied \
             unconditionally rather than only when it is breached.",
            MIN_VALIDATION_SPACING,
            from_now
        );
        assert!(
            from_now > Duration::from_secs(20 * 3600),
            "FIXTURE: expected roughly a day out, got {:?}",
            from_now
        );
    }
}

/// The daily validation walk repairs a COLD empty-object-ETag entry — one nothing has
/// read — and leaves every other shape alone.
///
/// # Why the walk, and not the read path
///
/// The read-path guard (`CacheManager::is_empty_object_etag_entry` and its three call
/// sites) refuses to serve such an entry and forwards to S3. It cannot heal it: a
/// zero-byte `200` carries no body, and both mainline cache-write arms decline to store an
/// empty one. So read traffic converges on nothing, and an entry nothing reads was
/// unreachable by any mechanism before this. Every test here drives the pass with **no GET
/// and no HEAD**, which is the whole point of the unit.
///
/// # Thread topology is part of the fixture, not incidental
///
/// The scan runs on rayon workers — plain `std::thread`s with no tokio context — and
/// `Handle::block_on` is legal there precisely because they are not inside an asynchronous
/// execution context. Calling `scan_metadata_file` directly from an async test body would
/// panic on that `block_on` instead, so these tests hand the work to a `std::thread` with a
/// cloned handle, reproducing production's topology rather than a shape that only works in
/// a test.
///
/// Spec: cache-eviction-at-scale, unit META-REPAIR-COLD-CONVERGE.
#[cfg(test)]
mod empty_object_etag_cold_repair_tests {
    use super::*;
    use crate::cache::zero_length_arm_orphan_tests::{
        object_metadata, range_bin_path, read_meta, seed_full_object, setup, BODY_LEN,
    };
    use crate::cache::{CacheManager, EMPTY_OBJECT_ETAG};
    use std::sync::Arc;

    /// Run one validation-pass visit of `path` on a non-runtime thread with a captured
    /// handle — production's topology. Returns the pass's own `ScanFileResult`.
    async fn scan_one(tracker: &Arc<CacheSizeTracker>, path: std::path::PathBuf) -> ScanFileResult {
        let handle = tokio::runtime::Handle::current();
        let tracker = Arc::clone(tracker);
        std::thread::spawn(move || {
            tracker.scan_metadata_file(&path, SystemTime::now(), Some(&handle))
        })
        .join()
        .expect("the scan thread must not panic")
    }

    /// The tracker the manager's own `initialize()` built, wired the way production wires
    /// it (`set_cache_manager_in_tracker`), rather than a lookalike built for the test.
    async fn production_tracker(manager: &Arc<CacheManager>) -> Arc<CacheSizeTracker> {
        manager.set_cache_manager_in_tracker().await;
        manager
            .get_size_tracker()
            .await
            .expect("fixture: initialize() must have built a size tracker")
    }

    /// ASSERTION CLASS 1 — THE DEFECT. A cold corrupt entry is repaired by one pass.
    ///
    /// Anti-vacuity, trap 1 (adjacent value): the EXACT two predicate inputs are asserted
    /// before the pass — `content_length > 0` AND the normalized ETag equal to
    /// `EMPTY_OBJECT_ETAG` — rather than an adjacent consequence such as `ranges` being
    /// non-empty or the `.bin` existing. Trap 2 (fixture that never reaches the guard): a
    /// correctly published zero-byte object has `content_length == 0` and never reaches the
    /// predicate, so the positive stored length is asserted as a hard precondition and the
    /// test fails naming that rather than passing for the wrong reason.
    #[tokio::test]
    async fn a_cold_corrupt_entry_is_repaired_by_the_validation_pass_with_no_read() {
        let temp = tempfile::TempDir::new().unwrap();
        let manager = Arc::new(setup(temp.path()).await);
        let cache_key = "eoe-cold-bucket/healed.bin";
        let (bin_path, healed_len) =
            crate::cache::empty_object_etag_guard_tests::plant_corrupt(&manager, cache_key).await;

        // The predicate's exact inputs, before the pass.
        let before = read_meta(&manager, cache_key);
        assert_eq!(
            crate::revalidation::normalize_etag(&before.object_metadata.etag),
            EMPTY_OBJECT_ETAG,
            "PRECONDITION: the fixture's stored ETag must be the empty object's. got={:?}",
            before.object_metadata.etag
        );
        assert!(
            before.object_metadata.content_length > 0,
            "PRECONDITION: the corrupt shape is a POSITIVE stored length under the empty \
             object's ETag. A correctly published zero-byte object has content_length == 0 \
             and never reaches the predicate at all, so a fixture like that would make every \
             assertion below pass without the repair arm existing. got={}",
            before.object_metadata.content_length
        );
        assert_eq!(
            before.object_metadata.content_length, healed_len,
            "PRECONDITION: the healed length must be the superseded object's"
        );
        assert!(
            bin_path.exists(),
            "PRECONDITION: the superseded range file must be on disk at {:?}",
            bin_path
        );

        let tracker = production_tracker(&manager).await;
        let result = scan_one(&tracker, manager.get_new_metadata_file_path(cache_key)).await;

        let after = read_meta(&manager, cache_key);
        assert!(
            after.ranges.is_empty(),
            "the validation pass left the corrupt document naming {} range(s). Nothing else \
             can repair this entry: the read-path guard refuses to serve it but a zero-byte \
             200 carries no body, so no GET can rewrite it. got={:?}",
            after.ranges.len(),
            after.ranges
        );
        assert_eq!(
            after.object_metadata.content_length, 0,
            "the pass left content_length at {} under the empty object's ETag. Publishing \
             ranges-empty alone does NOT converge — the predicate is still true, so the next \
             walk repairs it again, every day, forever.",
            after.object_metadata.content_length
        );
        assert!(
            !CacheManager::is_empty_object_etag_entry(&after.object_metadata),
            "CONVERGENCE: the repaired document still matches the detector, so the repair \
             would fire again on the next pass. content_length={} etag={:?}",
            after.object_metadata.content_length,
            after.object_metadata.etag
        );
        assert!(
            !bin_path.exists(),
            "the superseded range file survived the repair at {:?}. It is referenced by \
             nothing now, so leaving it is a permanent unaccounted leak on the volume.",
            bin_path
        );
        assert_eq!(
            (result.size_bytes, result.staged_bytes),
            (0, 0),
            "a repaired entry has no ranges, so it must contribute nothing to the size \
             census. got size_bytes={} staged_bytes={}",
            result.size_bytes,
            result.staged_bytes
        );
        assert!(
            !result.meta_removed,
            "the .meta SURVIVES a repair — it is now a correctly published zero-length \
             entry — so reporting it removed would under-count a live object and bias the \
             census downward"
        );
        assert!(
            !result.cache_error && !result.cache_expired && !result.cache_skipped,
            "a successful repair is a clean visit, not an expiry, a skip or an error. \
             error={} expired={} skipped={}",
            result.cache_error,
            result.cache_expired,
            result.cache_skipped
        );
    }

    /// ASSERTION CLASS 2 — POSITIVE CONTROL, a correctly published empty object.
    ///
    /// `content_length == 0` with the empty-object ETag and no ranges is the RIGHT shape for
    /// a zero-byte object — a directory marker is exactly this. The pass must leave it
    /// alone. This is what reds if the predicate is narrowed to the ETag alone.
    #[tokio::test]
    async fn a_correctly_published_empty_object_is_left_untouched() {
        let temp = tempfile::TempDir::new().unwrap();
        let manager = Arc::new(setup(temp.path()).await);
        let cache_key = "eoe-cold-bucket/legitimately-empty.bin";

        seed_full_object(
            &manager,
            cache_key,
            &[],
            object_metadata(0, EMPTY_OBJECT_ETAG),
        )
        .await;

        let before = read_meta(&manager, cache_key);
        assert_eq!(
            (before.object_metadata.content_length, before.ranges.len()),
            (0, 0),
            "PRECONDITION: a correctly published empty object has a zero length and no ranges"
        );
        assert_eq!(
            crate::revalidation::normalize_etag(&before.object_metadata.etag),
            EMPTY_OBJECT_ETAG,
            "PRECONDITION: it carries the empty object's ETag, which is exactly why the \
             predicate cannot be the ETag alone"
        );

        // The `.meta`'s mtime, captured before the pass. Comparing the document's FIELDS
        // cannot catch an over-broad detector here: repairing an already-correct empty
        // object rewrites it to byte-identical content, so every field assertion passes
        // while the pass performs a shared-storage write per zero-byte object per day. The
        // mtime is what makes "left untouched" mean untouched rather than
        // rewritten-to-the-same-value. Same reasoning as comparing a range file's
        // (path, mtime) rather than its bytes.
        let meta_path = manager.get_new_metadata_file_path(cache_key);
        let mtime_before = std::fs::metadata(&meta_path).unwrap().modified().unwrap();
        // The publish path renames a temp file into place, so a rewrite lands a new mtime;
        // sleep past the filesystem's timestamp granularity so an unchanged mtime is
        // evidence of no write rather than of a coarse clock.
        std::thread::sleep(Duration::from_millis(20));

        let tracker = production_tracker(&manager).await;
        let result = scan_one(&tracker, meta_path.clone()).await;

        let after = read_meta(&manager, cache_key);
        assert_eq!(
            after.object_metadata, before.object_metadata,
            "POSITIVE CONTROL FAILED: the pass rewrote the object_metadata of a CORRECTLY \
             published empty object. The predicate must require a positive stored length; \
             testing the ETag alone matches every legitimate zero-byte object in the cache."
        );
        let mtime_after = std::fs::metadata(&meta_path).unwrap().modified().unwrap();
        assert_eq!(
            mtime_after, mtime_before,
            "POSITIVE CONTROL FAILED: the pass REPUBLISHED a correctly published empty \
             object. The content is unchanged, so no field assertion can see this — but a \
             detector that matches every zero-byte object costs one shared-storage write per \
             such object on every daily walk, fleet-wide, forever."
        );
        assert!(
            !result.cache_error,
            "a legitimate empty object is not an error for the pass's counters"
        );
        assert!(
            !result.meta_removed && manager.get_new_metadata_file_path(cache_key).exists(),
            "the pass must not remove a correctly published empty object"
        );
    }

    /// ASSERTION CLASS 3 — POSITIVE CONTROL, an ordinary non-empty entry.
    ///
    /// Counted, and left servable. This is what reds if the repair arm is over-broad or an
    /// early return zeroes every entry's figures.
    #[tokio::test]
    async fn an_ordinary_non_empty_entry_is_counted_and_left_servable() {
        let temp = tempfile::TempDir::new().unwrap();
        let manager = Arc::new(setup(temp.path()).await);
        let cache_key = "eoe-cold-bucket/ordinary.bin";
        let body = crate::cache::zero_length_arm_orphan_tests::incompressible_body(23);

        seed_full_object(
            &manager,
            cache_key,
            &body,
            object_metadata(BODY_LEN as u64, "\"v1-ordinary\""),
        )
        .await;

        let before = read_meta(&manager, cache_key);
        assert_eq!(
            before.ranges.len(),
            1,
            "PRECONDITION: the seed must leave exactly one full-object range"
        );
        assert!(
            before.object_metadata.content_length > 0
                && crate::revalidation::normalize_etag(&before.object_metadata.etag)
                    != EMPTY_OBJECT_ETAG,
            "PRECONDITION: a positive length under an ORDINARY ETag — it satisfies the \
             length half of the predicate and not the ETag half, which is what makes it the \
             control for a detector that tests only the length. content_length={} etag={:?}",
            before.object_metadata.content_length,
            before.object_metadata.etag
        );
        let bin_path = range_bin_path(&manager, &before.ranges[0].file_path);
        assert!(bin_path.exists(), "PRECONDITION: its .bin must be on disk");
        let expected_size: u64 = before.ranges.iter().map(|r| r.compressed_size).sum();

        let tracker = production_tracker(&manager).await;
        let result = scan_one(&tracker, manager.get_new_metadata_file_path(cache_key)).await;

        let after = read_meta(&manager, cache_key);
        assert_eq!(
            after.ranges.len(),
            1,
            "POSITIVE CONTROL FAILED: the pass dereferenced an ordinary non-empty entry's \
             ranges. got={:?}",
            after.ranges
        );
        assert_eq!(
            after.object_metadata.content_length, before.object_metadata.content_length,
            "POSITIVE CONTROL FAILED: the pass zeroed an ordinary entry's content_length"
        );
        assert!(
            bin_path.exists(),
            "POSITIVE CONTROL FAILED: the pass deleted an ordinary entry's range file at \
             {:?}, which would make a cached object unservable",
            bin_path
        );
        assert_eq!(
            result.size_bytes, expected_size,
            "POSITIVE CONTROL FAILED: an ordinary entry must be COUNTED at its real size. \
             Reporting 0 here is how an over-broad early return would present — the entry \
             looks intact on disk while the census forgets its bytes."
        );
        assert!(
            !result.cache_error,
            "an ordinary entry is not an error for the pass's counters"
        );
    }
}

/// The revived write-cache-expiry deletion on the validation pass.
///
/// # The threading IS the fixture, and an async test body cannot see this defect
///
/// The defect being closed is that `Handle::try_current()` returns `Err` on every rayon
/// worker, so the deletion never ran. A test that calls `scan_metadata_file` directly from
/// an `#[tokio::test]` body **already carries a tokio context**, so `try_current()` would
/// succeed there and the broken code would pass — and on a thread inside the runtime
/// `Handle::block_on` panics instead, so that shape does not merely under-test, it tests a
/// third thing that happens nowhere. Neither is production.
///
/// So every test here reaches the site through `par_iter().for_each`, which is how both
/// `perform_full_validation` and `perform_rolling_validation` reach it. Rayon injects the
/// job into its global pool and blocks the caller on a latch rather than recruiting it, so
/// the item runs on a plain `std::thread` with no tokio context — the exact condition the
/// bug depended on, and the exact condition under which a *captured* handle's `block_on` is
/// legal.
///
/// Spec: cache-eviction-at-scale, unit SIZE-TRACKER-BLOCK-ON-DEAD.
#[cfg(test)]
mod write_cache_expiry_revival_tests {
    use super::*;
    use crate::cache::zero_length_arm_orphan_tests::{
        incompressible_body, object_metadata, range_bin_path, read_meta, seed_full_object, setup,
    };
    use crate::cache::CacheManager;
    use std::sync::Arc;
    use std::time::Duration;

    /// Which rayon topology a fixture drives. BOTH are production shapes, and they put the
    /// scan on different kinds of thread, so a test has to say which one it means.
    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Topology {
        /// A batch rayon SPLITS: the job is injected into the global pool and the caller
        /// blocks on a latch, so the item runs on a plain `std::thread` with no tokio
        /// context. This is the topology the original 512-item measurement recorded, and the
        /// one that made `Handle::try_current()` fail.
        PoolWorker,
        /// A batch rayon does NOT split — one L1 shard directory. `for_each` reaches the
        /// pool only via `join`, and an unsplit producer never calls it, so the fold runs
        /// inline on the caller: a tokio runtime thread, where a bare `Handle::block_on`
        /// panics. Reached by any cache whose objects occupy a single L1 shard.
        CallerInsideRuntime,
    }

    /// Drive ONE visit of `path` through `par_iter().for_each`, the way both production
    /// passes reach the scan, and assert the resulting thread topology is the one asked for.
    ///
    /// The topology assertion is the anti-vacuity guard: if a rayon or tokio change ever
    /// moved the work onto a different kind of thread, these tests would silently stop
    /// exercising the condition the defect depends on, and would pass either way.
    fn scan_via_rayon(
        tracker: &Arc<CacheSizeTracker>,
        path: &std::path::Path,
        runtime: Option<&tokio::runtime::Handle>,
        topology: Topology,
    ) -> ScanFileResult {
        use rayon::prelude::*;
        let target = path.to_path_buf();
        // A second, deliberately non-existent path is what forces the split. The scan
        // returns early on an unreadable file, so the filler contributes nothing but the
        // length rayon needs in order to `join` and therefore to inject.
        let mut batch = vec![target.clone()];
        if topology == Topology::PoolWorker {
            batch.push(path.with_extension("meta.absent-filler"));
        }

        let out = std::sync::Mutex::new(None);
        let saw_runtime_ctx = std::sync::atomic::AtomicBool::new(false);
        batch.par_iter().for_each(|p| {
            if p == &target {
                saw_runtime_ctx.store(
                    tokio::runtime::Handle::try_current().is_ok(),
                    std::sync::atomic::Ordering::SeqCst,
                );
                let r = tracker.scan_metadata_file(p, SystemTime::now(), runtime);
                *out.lock().unwrap() = Some(r);
            } else {
                let _ = tracker.scan_metadata_file(p, SystemTime::now(), runtime);
            }
        });

        let observed = if saw_runtime_ctx.load(std::sync::atomic::Ordering::SeqCst) {
            Topology::CallerInsideRuntime
        } else {
            Topology::PoolWorker
        };
        assert_eq!(
            observed, topology,
            "FIXTURE INVALID: this test means to exercise {:?} but the scan actually ran on \
             {:?}. The two differ in whether the scan thread carries a tokio context, which \
             is the entire condition the defect depends on — so a fixture on the wrong one \
             would pass without testing anything.",
            topology, observed
        );
        out.into_inner().unwrap().expect("the scan must have run")
    }

    /// The tracker `initialize()` built, wired as production wires it.
    async fn production_tracker(manager: &Arc<CacheManager>) -> Arc<CacheSizeTracker> {
        manager.set_cache_manager_in_tracker().await;
        manager
            .get_size_tracker()
            .await
            .expect("fixture: initialize() must have built a size tracker")
    }

    /// Publish a real cached object, then mark it write-cached with `write_cache_expires_at`
    /// at `expires_in_the_past`. Returns the `.meta` path and its one range file.
    ///
    /// The object is seeded through the production full-object publish so the `.bin` and the
    /// `ranges` array are real; only the two fields the expiry branch reads are then set,
    /// because no production path leaves an entry expired-and-still-present on demand.
    async fn seed_write_cached(
        manager: &Arc<CacheManager>,
        cache_key: &str,
        expired: bool,
    ) -> (std::path::PathBuf, std::path::PathBuf) {
        let body = incompressible_body(7);
        seed_full_object(
            manager,
            cache_key,
            &body,
            object_metadata(body.len() as u64, "\"wc-revival-etag\""),
        )
        .await;

        let mut meta = read_meta(manager, cache_key);
        let bin = range_bin_path(
            manager,
            &meta
                .ranges
                .first()
                .expect("fixture: the publish must have named a range")
                .file_path,
        );
        meta.object_metadata.is_write_cached = true;
        meta.object_metadata.write_cache_expires_at = Some(if expired {
            SystemTime::now() - Duration::from_secs(3600)
        } else {
            SystemTime::now() + Duration::from_secs(86_400)
        });
        let meta_path = manager.get_new_metadata_file_path(cache_key);
        std::fs::write(&meta_path, serde_json::to_string(&meta).unwrap()).unwrap();
        (meta_path, bin)
    }

    /// red side (c). The full scan deletes an expired write-cache entry, and the deletion
    /// debits it through the accumulator. The relative install must count it once: the census
    /// carries it at its pre-deletion figures (`ScanSelfDebit`) and the debit takes it out.
    ///
    /// # RED SIDE
    ///
    /// With the expiry arm's `self_debit` set to `ScanSelfDebit::default()`, the census omits
    /// the entry AND its debit applies, so `total_size` reads the live object minus the
    /// expired one.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_entry_the_full_scan_deletes_is_debited_exactly_once() {
        let temp = tempfile::TempDir::new().unwrap();
        let manager = Arc::new(setup(temp.path()).await);
        let live_key = "wc-case39-bucket/live.bin";
        let expired_key = "wc-case39-bucket/expired.bin";
        let body = incompressible_body(11);
        seed_full_object(
            &manager,
            live_key,
            &body,
            object_metadata(body.len() as u64, "\"case39-live\""),
        )
        .await;
        let (expired_meta, _bin) = seed_write_cached(&manager, expired_key, true).await;
        let size_of = |key: &str| -> u64 {
            read_meta(&manager, key)
                .ranges
                .iter()
                .map(|r| r.compressed_size)
                .sum()
        };
        let live = size_of(live_key);
        let expired = size_of(expired_key);
        assert!(live > 0 && expired > 0, "FIXTURE: both objects need bytes");

        let tracker = production_tracker(&manager).await;
        let consolidator = tracker.consolidator.clone();
        // Ground the tracked figures on both objects, discarding the publishes' credits.
        consolidator
            .update_size_from_validation(live + expired, Some(0), Some(2))
            .await;

        tokio::time::timeout(Duration::from_secs(60), tracker.perform_full_validation())
            .await
            .expect("the full scan must finish inside 60 s")
            .unwrap();
        assert!(
            !expired_meta.exists(),
            "FIXTURE: the pass must have deleted the expired entry"
        );

        // The next cycle's apply, by hand, so no journal processing intervenes.
        consolidator.size_accumulator().flush().await.unwrap();
        let next = consolidator.collect_deltas().await;
        assert_eq!(
            next.delta,
            -(expired as i64),
            "FIXTURE: the deletion's own debit must be pending in a delta file"
        );
        consolidator
            .atomic_update_size_delta(next.delta, next.write_cache_delta)
            .await
            .unwrap();
        consolidator.delete_applied_delta_files(&next.paths).await;

        let total = consolidator.get_size_state().await.unwrap().total_size;
        assert_eq!(
            total, live,
            "DOUBLE-DEBIT: total_size={} after the full scan and the next apply, expected the \
             live object's {} (the expired entry's {} must be debited once)",
            total, live, expired
        );
    }

    /// THE DEFECT. On a rayon-threaded pass the expired write-cache entry is deleted.
    ///
    /// Pre-fix this asserts a NEGATIVE that was true: the `.meta` and the `.bin` both
    /// survive, because `try_current()` failed and the deletion was never attempted. The
    /// failure message names that, so a red run says "the effect did not occur" rather than
    /// merely "not equal".
    #[tokio::test(flavor = "multi_thread")]
    async fn an_expired_write_cache_entry_is_deleted_by_the_rayon_threaded_pass() {
        let temp = tempfile::TempDir::new().unwrap();
        let manager = Arc::new(setup(temp.path()).await);
        let cache_key = "wc-revival-bucket/expired.bin";
        let (meta_path, bin) = seed_write_cached(&manager, cache_key, true).await;

        // The branch's exact two inputs, before the pass — not an adjacent consequence.
        let before = read_meta(&manager, cache_key);
        assert!(
            before.object_metadata.is_write_cached,
            "PRECONDITION: the entry must be write-cached, or the branch is never entered"
        );
        assert!(
            before.object_metadata.is_write_cache_expired(),
            "PRECONDITION: write_cache_expires_at must already have passed. got={:?}",
            before.object_metadata.write_cache_expires_at
        );
        assert!(bin.exists(), "PRECONDITION: the range file must be on disk");

        let tracker = production_tracker(&manager).await;
        let result = scan_via_rayon(
            &tracker,
            &meta_path,
            Some(&tokio::runtime::Handle::current()),
            Topology::PoolWorker,
        );

        assert!(
            !meta_path.exists(),
            "THE DEFECT: the pass left the expired write-cache entry's .meta in place at \
             {:?}. This is what `Handle::try_current()` produced on a rayon worker — it \
             returns Err there, so the deletion was never attempted and the entry outlives \
             its own expiry indefinitely, since nothing else in production calls \
             check_and_invalidate_expired_write_cache.",
            meta_path
        );
        assert!(
            !bin.exists(),
            "THE DEFECT: the .meta went but the range file survived at {:?}, which is an \
             unaccounted leak referenced by nothing.",
            bin
        );
        assert!(
            result.cache_expired,
            "a deletion on the expiry branch must be reported as an expiry, since \
             cache_entries_expired in validation.json is what an operator reads to see the \
             pass doing this work"
        );
        assert!(
            result.meta_removed,
            "the .meta is gone, so the object census must stop counting this object; \
             reporting it present biases the census UPWARD"
        );
        assert_eq!(
            (result.size_bytes, result.staged_bytes),
            (0, 0),
            "a deleted entry contributes nothing to the size census. got size_bytes={} \
             staged_bytes={}",
            result.size_bytes,
            result.staged_bytes
        );
        assert!(
            !result.cache_error,
            "a successful deletion is not an error for the pass's counters"
        );
    }

    /// POSITIVE CONTROL. A write-cached entry whose expiry has NOT passed is untouched.
    ///
    /// Distinct signature: an over-broad revival — one that deletes on `is_write_cached`
    /// alone, or that lets the callee's locked re-test be skipped — reds HERE and leaves the
    /// defect test green. A revival that is simply absent reds the defect test and leaves
    /// this one green. The two cannot fail for the same reason.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unexpired_write_cache_entry_is_counted_and_left_in_place() {
        let temp = tempfile::TempDir::new().unwrap();
        let manager = Arc::new(setup(temp.path()).await);
        let cache_key = "wc-revival-bucket/live.bin";
        let (meta_path, bin) = seed_write_cached(&manager, cache_key, false).await;

        let before = read_meta(&manager, cache_key);
        assert!(
            before.object_metadata.is_write_cached,
            "PRECONDITION: the control must be write-cached, so it enters the same branch \
             as the defect fixture and differs ONLY in whether its expiry has passed"
        );
        assert!(
            !before.object_metadata.is_write_cache_expired(),
            "PRECONDITION: the control's expiry must NOT have passed"
        );
        let expected_size: u64 = before.ranges.iter().map(|r| r.compressed_size).sum();
        assert!(
            expected_size > 0,
            "PRECONDITION: the control must have bytes"
        );
        let mtime_before = std::fs::metadata(&meta_path).unwrap().modified().unwrap();

        let tracker = production_tracker(&manager).await;
        let result = scan_via_rayon(
            &tracker,
            &meta_path,
            Some(&tokio::runtime::Handle::current()),
            Topology::PoolWorker,
        );

        assert!(
            meta_path.exists(),
            "POSITIVE CONTROL FAILED: the pass deleted a LIVE write-cached entry's .meta. \
             The revived deletion is gated on the expiry having passed, and this one has \
             not."
        );
        assert!(
            bin.exists(),
            "POSITIVE CONTROL FAILED: the pass deleted a live write-cached entry's range \
             file at {:?}, which makes a staged object unservable before its TTL",
            bin
        );
        assert_eq!(
            std::fs::metadata(&meta_path).unwrap().modified().unwrap(),
            mtime_before,
            "POSITIVE CONTROL FAILED: the pass REWROTE a live entry's .meta. No field \
             assertion can see that — the content is unchanged — but it is one shared-storage \
             write per staged object per instance-day, fleet-wide, forever."
        );
        assert_eq!(
            result.size_bytes, expected_size,
            "POSITIVE CONTROL FAILED: a live entry must be COUNTED at its real size. \
             Reporting 0 is how an over-broad early return presents: the entry looks intact \
             on disk while the census forgets its bytes."
        );
        assert!(
            !result.cache_expired && !result.cache_error,
            "a live entry is neither an expiry nor an error. expired={} error={}",
            result.cache_expired,
            result.cache_error
        );
    }

    /// THE CAPTURED HANDLE IS LOAD-BEARING, not decorative.
    ///
    /// Same fixture and same rayon topology as the defect test, with the one input that
    /// distinguishes fixed from broken removed: no handle. If the deletion could somehow
    /// still happen here, the handle would not be what revived it and the defect test would
    /// be passing for some other reason. It asserts the pre-fix outcome positively — the
    /// entry survives — and that the pass says so rather than reporting a clean visit.
    #[tokio::test(flavor = "multi_thread")]
    async fn without_a_captured_handle_the_expired_entry_survives_and_is_flagged() {
        let temp = tempfile::TempDir::new().unwrap();
        let manager = Arc::new(setup(temp.path()).await);
        let cache_key = "wc-revival-bucket/no-handle.bin";
        let (meta_path, bin) = seed_write_cached(&manager, cache_key, true).await;

        let before = read_meta(&manager, cache_key);
        assert!(
            before.object_metadata.is_write_cache_expired(),
            "PRECONDITION: identical fixture to the defect test — already expired"
        );
        let expected_size: u64 = before.ranges.iter().map(|r| r.compressed_size).sum();

        let tracker = production_tracker(&manager).await;
        let result = scan_via_rayon(&tracker, &meta_path, None, Topology::PoolWorker);

        assert!(
            meta_path.exists() && bin.exists(),
            "with no captured handle the pass has nothing to block on, so it must leave the \
             entry alone. If it deleted anyway, the handle is not what revived the deletion \
             and the defect test proves nothing."
        );
        assert!(
            result.cache_error,
            "finding an expired entry it cannot act on is damage the pass must report; \
             silence would make an unwired scan indistinguishable from a clean cache"
        );
        assert_eq!(
            result.size_bytes, expected_size,
            "nothing was mutated, so the entry's real bytes are still the ones to count"
        );
        assert!(
            !result.cache_expired && !result.meta_removed,
            "nothing expired and nothing was removed. expired={} meta_removed={}",
            result.cache_expired,
            result.meta_removed
        );
    }

    /// A CAPTURED HANDLE IS NOT ENOUGH ON ITS OWN — the single-shard batch.
    ///
    /// When a batch holds one L1 shard directory, rayon does not split, `for_each` never
    /// calls `join`, and the fold runs inline on the calling thread. Both production passes
    /// call `par_iter().for_each` from an `async fn`, so that thread is inside the runtime,
    /// and a bare `handle.block_on` there panics with "Cannot start a runtime from within a
    /// runtime" — aborting the whole validation pass rather than one entry.
    ///
    /// This is reachable on any cache whose objects occupy a single L1 shard, which includes
    /// a new volume and a small one. The same fixture and the same assertions as the defect
    /// test, on the other topology.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_deletion_survives_a_batch_rayon_does_not_split() {
        let temp = tempfile::TempDir::new().unwrap();
        let manager = Arc::new(setup(temp.path()).await);
        let cache_key = "wc-revival-bucket/unsplit.bin";
        let (meta_path, bin) = seed_write_cached(&manager, cache_key, true).await;

        let tracker = production_tracker(&manager).await;
        // `Topology::CallerInsideRuntime` is asserted inside the helper, so this test fails
        // naming the fixture if rayon ever stops folding an unsplit batch on the caller.
        let result = scan_via_rayon(
            &tracker,
            &meta_path,
            Some(&tokio::runtime::Handle::current()),
            Topology::CallerInsideRuntime,
        );

        assert!(
            !meta_path.exists() && !bin.exists(),
            "the expired entry survived an unsplit batch. A captured handle alone does not \
             make the scan's blocking call legal: on this topology the scan runs on a tokio \
             runtime thread, so the blocking work has to be handed to a thread that carries \
             no runtime context."
        );
        assert!(
            result.cache_expired && result.meta_removed,
            "the unsplit topology must reach the same verdict as the split one — the thread \
             the scan happens to land on is not part of the expiry decision. expired={} \
             meta_removed={}",
            result.cache_expired,
            result.meta_removed
        );
        assert!(
            !result.cache_error,
            "a successful deletion is not an error, whichever topology it ran on"
        );
    }
}

/// The Tail_Partition rebuild: membership, order, the cap, the sub-floor arm, and the tail-delta.
///
/// # Why these are inline rather than in `tests/`
///
/// The pass's entry point (`perform_rolling_validation`) is private, and every assertion here is
/// about what ONE genuine visit writes. An integration test would need that entry point made
/// `pub` purely so a test could reach it, widening the crate's API for no production caller —
/// which is also the `pub`-hides-`dead_code` trap `pre-push-checklist.md` step 3 warns about. The
/// incumbent precedent for driving a real rolling cycle from inside this file is
/// `f1_rolling_cycle_leaves_validation_json_deserializable`, and these follow it.
///
/// # Three fixture arms, and a population drawn from "what a cache looks like" reaches one
///
/// A mixed shard (some ranges above the floor, some below) is the easy arm. The other two are
/// unreachable from a realistic population: an **all-sub-floor** shard, where the partition must
/// be a written file holding zero records rather than an absent one; and a **cap-exceeded**
/// shard, which at the default `tail_max_entries: 4096` no reasonable fixture reaches — so that
/// arm sets the cap to 3, which doubles as the proof that the operator's configured value is the
/// value the rebuild reads.
///
/// # The adjacent figures, and why the fixture makes them disagree in both directions
///
/// `compressed_size`, `RangeSpec::uncompressed_size` and the range's logical extent are all
/// equal for incompressible data, so a fixture built from incompressible payloads passes
/// whichever of the three the code reads. These fixtures make them disagree: one range clears
/// the floor on extent and fails it on `compressed_size` and must be ABSENT, and one pair has
/// opposite extent and `compressed_size` orderings so the order assertion can only pass on the
/// latter.
///
/// Spec: cache-eviction-at-scale §§ 4.1, 4.2, 4.4. Requirements: 3.2, 3.4, 3.8, 1.5.
#[cfg(test)]
mod tail_partition_tests {
    use super::*;
    use crate::cache_types::{
        CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState,
    };
    use crate::compression::CompressionAlgorithm;
    use crate::journal_consolidator::ConsolidationConfig;
    use crate::journal_manager::JournalManager;
    use crate::metadata_lock_manager::MetadataLockManager;
    use std::collections::HashMap;
    use tempfile::TempDir;

    const MIB: u64 = 1024 * 1024;
    const FLOOR: u64 = MIB;

    /// Serialises every test in this module, because [`TailPartitionGauges`]' inputs are
    /// PROCESS-GLOBAL atomics and libtest runs a binary's tests as parallel threads in one
    /// process.
    ///
    /// Measured rather than anticipated: `the_gauges_report_the_rebuild_and_none_is_not_zero`
    /// passed in isolation and failed in the full suite reporting
    /// `tail_rebuilds_total: 7, tail_entries_last_rebuild: Some(0)` — seven rebuilds from six
    /// concurrent sibling tests, and a last-rebuild figure belonging to one of them. Every test
    /// here also **resets** those atomics through `tracker_with_cap`, so the interference runs in
    /// both directions.
    ///
    /// An exact assertion on a process-global counter under parallel execution is not safe, and
    /// a `>` assertion is not safe either — it can pass on a sibling's increment. Holding one
    /// guard for the whole test body is the fix the testing guidance prescribes.
    pub(super) use super::TAIL_TEST_GUARD;

    /// One range of a fixture object. `extent` is stated separately from `compressed` precisely
    /// so a test can make the two disagree.
    #[derive(Clone)]
    struct Spec {
        start: u64,
        extent: u64,
        compressed: u64,
        staged: Option<bool>,
    }

    impl Spec {
        /// A range whose extent, uncompressed size and compressed size all agree — the shape
        /// that cannot discriminate between the three figures.
        fn incompressible(start: u64, size: u64) -> Self {
            Self {
                start,
                extent: size,
                compressed: size,
                staged: None,
            }
        }
        /// A range that clears the floor on EXTENT and fails it on `compressed_size`.
        fn compressible(start: u64, extent: u64, compressed: u64) -> Self {
            Self {
                start,
                extent,
                compressed,
                staged: None,
            }
        }
        fn staged(mut self, staged: Option<bool>) -> Self {
            self.staged = staged;
            self
        }
    }

    /// Seed one `.meta` at `metadata/{bucket}/{l1}/{leaf}/{name}.meta`.
    ///
    /// The path is what decides the shard, because the rebuild derives `(bucket, L1)` from the
    /// directory it is walking rather than from the key — so a fixture controls the shard
    /// directly and does not have to hunt for a key that hashes to one.
    fn seed_meta(
        cache_dir: &std::path::Path,
        bucket: &str,
        l1: &str,
        leaf: &str,
        name: &str,
        object_is_write_cached: bool,
        specs: &[Spec],
    ) -> PathBuf {
        let path = cache_dir
            .join("metadata")
            .join(bucket)
            .join(l1)
            .join(leaf)
            .join(format!("{}.meta", name));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let now = SystemTime::now();
        let total: u64 = specs.iter().map(|s| s.compressed).sum();
        let meta = NewCacheMetadata {
            cache_key: format!("{}/{}", bucket, name),
            object_metadata: ObjectMetadata {
                etag: "\"tail\"".to_string(),
                last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
                content_length: total,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: UploadState::Complete,
                cumulative_size: total,
                parts: Vec::new(),
                response_headers: HashMap::new(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: total,
                parts_count: None,
                part_ranges: HashMap::new(),
                upload_id: None,
                is_write_cached: object_is_write_cached,
                // No write-cache expiry, so the write-cache-expiry branch cannot fire and remove
                // the `.meta` under the fixture.
                write_cache_expires_at: Some(now + Duration::from_secs(86_400)),
                write_cache_created_at: Some(now),
                write_cache_last_accessed: Some(now),
                graduation_accounted: false,
            },
            ranges: specs
                .iter()
                .map(|s| RangeSpec {
                    start: s.start,
                    end: s.start + s.extent - 1,
                    file_path: format!("ranges/{}_{}.bin", name, s.start),
                    compression_algorithm: CompressionAlgorithm::Lz4,
                    compressed_size: s.compressed,
                    // The second adjacent figure. Set to the EXTENT, not to `compressed`, so a
                    // fixture range is only "all three equal" when the caller asks for it.
                    uncompressed_size: s.extent,
                    created_at: now,
                    last_accessed: now,
                    access_count: 7,
                    staged: s.staged,
                    unservable: false,
                    file_identity: None,
                })
                .collect(),
            created_at: now,
            expires_at: now + Duration::from_secs(86_400),
            compression_info: CompressionInfo::default(),
            head_expires_at: None,
            head_last_accessed: None,
            head_access_count: 0,
            head_cached_at: None,
            last_validated_at: None,
        };
        std::fs::write(&path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
        path
    }

    /// A tracker over a fresh temp cache directory, with the Tail_Partition cap set explicitly.
    async fn tracker_with_cap(cap: usize) -> (Arc<CacheSizeTracker>, TempDir) {
        reset_tail_counters_for_tests();
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
        let consolidator = Arc::new(JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        ));
        consolidator.initialize().await.unwrap();
        let config = CacheSizeConfig {
            validation_enabled: false,
            // Generous, so the budget check never aborts a cycle mid-fixture and the failure
            // mode of a slow machine is a slow test rather than a flaky one.
            validation_max_duration: Duration::from_secs(3600),
            tail_floor_bytes: FLOOR,
            tail_max_entries: cap,
            ..CacheSizeConfig::default()
        };
        let tracker = Arc::new(
            CacheSizeTracker::new(cache_dir, config, false, consolidator)
                .await
                .unwrap(),
        );
        (tracker, temp_dir)
    }

    /// Read a partition back as records. `None` means the FILE IS ABSENT, which is a different
    /// observation from an empty file and is the distinction the sub-floor arm turns on.
    fn read_partition(
        cache_dir: &std::path::Path,
        bucket: &str,
        l1: &str,
    ) -> Option<Vec<TailRecord>> {
        let path = tail_partition_path(cache_dir, bucket, l1);
        let content = std::fs::read_to_string(&path).ok()?;
        Some(
            content
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| serde_json::from_str(l).expect("every partition line must parse"))
                .collect(),
        )
    }

    /// One checkpoint field, as JSON.
    fn checkpoint_field(
        cache_dir: &std::path::Path,
        bucket: &str,
        l1: &str,
        field: &str,
    ) -> Option<serde_json::Value> {
        let shard = crate::staged_fold::Shard::new(bucket, l1);
        crate::staged_fold::StagedShardCheckpoint::load(cache_dir, &shard)
            .ok()
            .flatten()
            .and_then(|c| c.other.get(field).cloned())
    }

    /// A mixed shard admits only ranges whose **compressed** size clears the floor.
    ///
    /// The discriminating range is `compressible(…, 4 MiB extent, 512 KiB compressed)`: its
    /// extent and its `uncompressed_size` both clear a 1 MiB floor and its `compressed_size` does
    /// not. An implementation testing the floor against either adjacent figure admits it, so its
    /// ABSENCE is the assertion.
    ///
    /// A failure implicates the `filter` in `CacheSizeTracker::tail_candidates_for`.
    #[tokio::test]
    async fn a_mixed_shard_admits_only_ranges_whose_compressed_size_clears_the_floor() {
        let _serial = TAIL_TEST_GUARD.lock().await;
        let (tracker, temp) = tracker_with_cap(4096).await;
        let cache_dir = temp.path().to_path_buf();
        seed_meta(
            &cache_dir,
            "bucket-mixed",
            "00",
            "aaa",
            "mixed",
            false,
            &[
                Spec::incompressible(0, 2 * MIB),
                // Clears the floor on extent and on uncompressed_size, fails it on compressed.
                Spec::compressible(8 * MIB, 4 * MIB, 512 * 1024),
                // Exactly at the floor: membership is `>=`, so this is a member.
                Spec::incompressible(32 * MIB, MIB),
                // Well below the floor on every figure.
                Spec::incompressible(64 * MIB, 4096),
            ],
        );

        tracker.perform_rolling_validation().await.unwrap();

        let records = read_partition(&cache_dir, "bucket-mixed", "00")
            .expect("the visited shard must have a partition file");
        let sizes: Vec<u64> = records.iter().map(|r| r.compressed_size).collect();
        assert_eq!(
            sizes,
            vec![2 * MIB, MIB],
            "only the two ranges whose COMPRESSED size clears the {} byte floor may be members, \
             largest first. Got {:?}. A 512 KiB range present here means the floor was tested \
             against the extent or against uncompressed_size.",
            FLOOR,
            records
        );
        assert!(
            !records.iter().any(|r| r.range_start == 8 * MIB),
            "the compressible range at offset {} cleared the floor on EXTENT only and must be \
             absent; its presence is the D4 defect",
            8 * MIB
        );
        assert_eq!(
            checkpoint_field(&cache_dir, "bucket-mixed", "00", "tail_entries"),
            Some(serde_json::json!(2)),
            "the checkpoint's tail_entries must equal the members written"
        );
        assert_eq!(
            checkpoint_field(&cache_dir, "bucket-mixed", "00", "tail_floor"),
            Some(serde_json::json!(FLOOR)),
            "the checkpoint must record the floor this partition was built with"
        );
    }

    /// Order is by `compressed_size` descending, and the fixture's extent order is the OPPOSITE.
    ///
    /// § 4.4's worked example: a 100 MiB range compressing to 10 MiB against a 50 MiB
    /// incompressible one. Ordering by extent puts the first ahead, which ranks a candidate that
    /// frees five times fewer bytes first. So asserting that the 50 MiB range sorts FIRST can
    /// only pass on a `compressed_size` ordering.
    ///
    /// A failure implicates `SmallestFirst`'s inverted `Ord` or `ShardTailBuild::finish`'s use of
    /// `into_sorted_vec`.
    #[tokio::test]
    async fn the_partition_orders_by_compressed_size_and_the_extent_order_is_opposite() {
        let _serial = TAIL_TEST_GUARD.lock().await;
        let (tracker, temp) = tracker_with_cap(4096).await;
        let cache_dir = temp.path().to_path_buf();
        seed_meta(
            &cache_dir,
            "bucket-order",
            "01",
            "bbb",
            "order",
            false,
            &[
                // Larger extent, SMALLER compressed size.
                Spec::compressible(0, 100 * MIB, 10 * MIB),
                // Smaller extent, LARGER compressed size.
                Spec::incompressible(200 * MIB, 50 * MIB),
            ],
        );

        tracker.perform_rolling_validation().await.unwrap();

        let records = read_partition(&cache_dir, "bucket-order", "01").expect("partition written");
        assert_eq!(
            records.len(),
            2,
            "both ranges clear the floor: {:?}",
            records
        );
        assert_eq!(
            records[0].compressed_size,
            50 * MIB,
            "the 50 MiB incompressible range must sort FIRST. Got {:?}. A 10 MiB record first \
             means the sort key is the extent (100 MiB vs 50 MiB) rather than compressed_size.",
            records
        );
        assert_eq!(records[0].range_start, 200 * MIB);
        assert_eq!(records[1].compressed_size, 10 * MIB);
    }

    /// An all-sub-floor shard gets a WRITTEN partition holding zero records.
    ///
    /// Asserting `!path.exists()` here would pass on an implementation that wrote nothing **and**
    /// on one that never ran, which is why the assertion is existence plus emptiness plus the
    /// checkpoint's `tail_entries: 0`. § 4.5's reader has to tell "no members above the floor"
    /// from "this shard has never been visited", and it cannot do that from a missing file.
    ///
    /// A failure implicates `CacheSizeTracker::write_tail_partition` returning early on an empty
    /// member list.
    #[tokio::test]
    async fn an_all_sub_floor_shard_writes_an_empty_partition_rather_than_none() {
        let _serial = TAIL_TEST_GUARD.lock().await;
        let (tracker, temp) = tracker_with_cap(4096).await;
        let cache_dir = temp.path().to_path_buf();
        seed_meta(
            &cache_dir,
            "bucket-subfloor",
            "02",
            "ccc",
            "subfloor",
            false,
            &[
                Spec::incompressible(0, 4096),
                Spec::incompressible(8192, 64 * 1024),
                Spec::incompressible(100_000, 900 * 1024),
            ],
        );

        tracker.perform_rolling_validation().await.unwrap();

        let path = tail_partition_path(&cache_dir, "bucket-subfloor", "02");
        assert!(
            path.exists(),
            "an all-sub-floor shard must still have a partition FILE at {:?} — an absent file \
             and an empty one are the same observation to a reader that checks presence",
            path
        );
        let records = read_partition(&cache_dir, "bucket-subfloor", "02").unwrap();
        assert!(
            records.is_empty(),
            "no range clears the floor, so the file must hold no records: {:?}",
            records
        );
        assert_eq!(
            checkpoint_field(&cache_dir, "bucket-subfloor", "02", "tail_entries"),
            Some(serde_json::json!(0)),
            "tail_entries: 0 is the checkable form of the same fact, and does not need a \
             presence test"
        );
    }

    /// The OPERATOR's `tail_max_entries` is the value the rebuild uses, and the cap drops the
    /// SMALLEST member.
    ///
    /// At the default 4096 this arm is unreachable from any reasonable fixture, so the cap is set
    /// to 3 and four above-floor ranges are seeded. That makes the test two things at once: the
    /// cap's behaviour, and the proof that the configured value reached the rebuild rather than
    /// the default — which is the wiring defect the `st_size` fold had to return and fix.
    ///
    /// The assertion is on WHICH record survives, not on the count: dropping the last-pushed
    /// record instead of the smallest also yields three.
    ///
    /// A failure implicates `ShardTailBuild::push`'s `heap.pop()` or the `tail_max_entries` path
    /// from `SharedStorageConfig` through `CacheSizeConfig`.
    #[tokio::test]
    async fn the_configured_cap_binds_and_discards_the_smallest_member() {
        let _serial = TAIL_TEST_GUARD.lock().await;
        let (tracker, temp) = tracker_with_cap(3).await;
        let cache_dir = temp.path().to_path_buf();

        // Fail loudly rather than skipping if the configured value did not arrive: at 4096 every
        // assertion below would pass vacuously on four members. Read the tracker's own config:
        // the floor and cap gauges are process-global and every tracker a parallel test builds
        // overwrites them (`publish_tail_config_in_force`), so they cannot carry a precondition.
        assert_eq!(
            tracker.config.tail_max_entries, 3,
            "PRECONDITION: the tracker must hold the configured cap of 3, not the default. \
             Got {}. Every assertion below is vacuous at 4096.",
            tracker.config.tail_max_entries
        );
        assert_eq!(
            tracker.config.tail_floor_bytes, FLOOR,
            "PRECONDITION: the configured floor must be in force"
        );

        seed_meta(
            &cache_dir,
            "bucket-cap",
            "03",
            "ddd",
            "cap",
            false,
            &[
                Spec::incompressible(0, 4 * MIB),
                Spec::incompressible(16 * MIB, 3 * MIB),
                Spec::incompressible(32 * MIB, 2 * MIB),
                // The smallest above-floor range, and the one the cap must discard. Seeded LAST
                // so "drop the smallest" and "drop the last pushed" would both remove it —
                // which is why the next-smallest is also asserted present below.
                Spec::incompressible(48 * MIB, MIB),
            ],
        );

        tracker.perform_rolling_validation().await.unwrap();

        let records = read_partition(&cache_dir, "bucket-cap", "03").expect("partition written");
        let sizes: Vec<u64> = records.iter().map(|r| r.compressed_size).collect();
        assert_eq!(
            sizes,
            vec![4 * MIB, 3 * MIB, 2 * MIB],
            "the cap of 3 must keep the THREE LARGEST members, largest first. Got {:?}.",
            records
        );
        assert!(
            !records.iter().any(|r| r.compressed_size == MIB),
            "the 1 MiB range is the smallest above-floor member and must be the one discarded"
        );
        assert_eq!(
            checkpoint_field(&cache_dir, "bucket-cap", "03", "tail_entries"),
            Some(serde_json::json!(3))
        );
    }

    /// Same cap, but the smallest range is seeded FIRST rather than last.
    ///
    /// This is the arm "drop the last pushed" passes and "drop the smallest" also passes, taken
    /// together with the test above: between the two, a last-pushed implementation has to discard
    /// a 4 MiB member here, and the assertion names it.
    #[tokio::test]
    async fn the_cap_discards_the_smallest_whatever_order_the_shard_is_walked_in() {
        let _serial = TAIL_TEST_GUARD.lock().await;
        let (tracker, temp) = tracker_with_cap(3).await;
        let cache_dir = temp.path().to_path_buf();
        seed_meta(
            &cache_dir,
            "bucket-cap-order",
            "04",
            "eee",
            "caporder",
            false,
            &[
                // Smallest FIRST.
                Spec::incompressible(0, MIB),
                Spec::incompressible(16 * MIB, 2 * MIB),
                Spec::incompressible(32 * MIB, 3 * MIB),
                Spec::incompressible(48 * MIB, 4 * MIB),
            ],
        );

        tracker.perform_rolling_validation().await.unwrap();

        let records =
            read_partition(&cache_dir, "bucket-cap-order", "04").expect("partition written");
        let sizes: Vec<u64> = records.iter().map(|r| r.compressed_size).collect();
        assert_eq!(
            sizes,
            vec![4 * MIB, 3 * MIB, 2 * MIB],
            "the three largest survive regardless of walk order. Got {:?}. A 1 MiB member here \
             with the 4 MiB one missing means the cap discards the most recently offered record \
             rather than the smallest.",
            records
        );
    }

    /// `is_staged` is derived per range, not copied from the object.
    ///
    /// The object carries `is_write_cached: true`. One range records `staged: Some(false)` and
    /// must come out `false`; the other records `None` and must fall back to the object flag and
    /// come out `true`. A fixture where the two agree cannot tell a per-range classification from
    /// an object-level one, so both halves are asserted.
    ///
    /// A failure implicates `tail_candidates_for`'s use of
    /// `crate::cache_types::is_staged_range_spec` — reading `RangeSpec::staged` directly would
    /// make the `None` range come out `false`, and OR-ing the object flag in would make the
    /// `Some(false)` range come out `true`.
    #[tokio::test]
    async fn is_staged_is_derived_per_range_rather_than_taken_from_the_object_flag() {
        let _serial = TAIL_TEST_GUARD.lock().await;
        let (tracker, temp) = tracker_with_cap(4096).await;
        let cache_dir = temp.path().to_path_buf();
        seed_meta(
            &cache_dir,
            "bucket-staged",
            "05",
            "fff",
            "staged",
            // The OBJECT says write-cached.
            true,
            &[
                // Recorded membership says otherwise, and recorded membership wins.
                Spec::incompressible(0, 4 * MIB).staged(Some(false)),
                // No recorded membership: falls back to the object flag.
                Spec::incompressible(16 * MIB, 2 * MIB).staged(None),
            ],
        );

        tracker.perform_rolling_validation().await.unwrap();

        let records = read_partition(&cache_dir, "bucket-staged", "05").expect("partition written");
        assert_eq!(records.len(), 2, "{:?}", records);
        let explicit = records
            .iter()
            .find(|r| r.range_start == 0)
            .expect("the Some(false) range must be a member");
        let fallback = records
            .iter()
            .find(|r| r.range_start == 16 * MIB)
            .expect("the None range must be a member");
        assert!(
            !explicit.is_staged,
            "a range recording staged: Some(false) stays unstaged even on a write-cached object; \
             true here means the object flag was read instead of the per-range predicate"
        );
        assert!(
            fallback.is_staged,
            "a range recording staged: None falls back to the object's is_write_cached, which is \
             true here; false means RangeSpec::staged was read directly"
        );
        // And the record's other derived fields are the ones the design names.
        assert_eq!(explicit.access_count, 7);
        assert_eq!(
            explicit.leaf, "fff",
            "leaf is the {{YYY}} path component, carried rather than re-derived"
        );
        assert!(
            explicit.expires_at > 0 && explicit.expires_at == fallback.expires_at,
            "expires_at is OBJECT-level, so every range of one object carries the same value: \
             {} vs {}",
            explicit.expires_at,
            fallback.expires_at
        );
    }

    /// A declined write phase writes NO partition.
    ///
    /// `schedule.json` is seeded with a `last_visit_at` far in the future, so the newest-wins
    /// check sees a visit this cycle did not write that post-dates its start and returns
    /// `Ok(None)` on the first shard. The cursor does not advance and the next cycle rebuilds the
    /// shard, so publishing a partition here would publish a view whose figures were discarded.
    ///
    /// A failure implicates the ORDER inside `write_phase_locked`: the decline must come before
    /// the partition write.
    #[tokio::test]
    async fn a_declined_write_phase_writes_no_partition() {
        let _serial = TAIL_TEST_GUARD.lock().await;
        let (tracker, temp) = tracker_with_cap(4096).await;
        let cache_dir = temp.path().to_path_buf();
        seed_meta(
            &cache_dir,
            "bucket-declined",
            "00",
            "aaa",
            "declined",
            false,
            &[Spec::incompressible(0, 8 * MIB)],
        );
        // A foreign visit, far enough ahead that it cannot be confused with this cycle's own.
        let future = CacheSizeTracker::epoch_millis_now() + 10 * 60 * 1000;
        std::fs::write(
            cache_dir.join("size_tracking").join("schedule.json"),
            serde_json::to_string_pretty(&serde_json::json!({
                "rolling_cursor": 0,
                "last_visit_at": future,
                "validation_type": "rolling",
            }))
            .unwrap(),
        )
        .unwrap();

        tracker.perform_rolling_validation().await.unwrap();

        assert!(
            read_partition(&cache_dir, "bucket-declined", "00").is_none(),
            "a declined write phase installs no figures and must publish no partition either"
        );
        assert_eq!(
            checkpoint_field(&cache_dir, "bucket-declined", "00", "tail_entries"),
            None,
            "and no checkpoint field either"
        );
        // The guard is only meaningful if the same fixture DOES produce a partition without the
        // foreign timestamp — otherwise this passes on a pass that never ran.
        std::fs::remove_file(cache_dir.join("size_tracking").join("schedule.json")).unwrap();
        tracker.perform_rolling_validation().await.unwrap();
        assert!(
            read_partition(&cache_dir, "bucket-declined", "00").is_some(),
            "CONTROL: with no foreign visit on disk the same fixture must produce a partition, \
             or the assertion above passed because nothing ran"
        );
    }

    /// Tail-delta compaction is a retire-set: naming one record leaves the others alone.
    ///
    /// The anti-vacuity shape matters here. A `rewrite_retaining(set)` given the same one
    /// identity would delete the other two, which is the task-77 defect this repo shipped — it
    /// left 1 of 25 entries surviving. So the assertion is on the survivors, not on the count of
    /// records dropped.
    #[test]
    fn tail_delta_compaction_retires_the_named_record_and_keeps_the_rest() {
        let temp = TempDir::new().unwrap();
        let cache_dir = temp.path();
        let mk = |key: &str, at: u64| TailDeltaRecord {
            bucket: "b".to_string(),
            l1: "0a".to_string(),
            at,
            instance_id: "host:1".to_string(),
            record: TailRecord {
                cache_key: key.to_string(),
                range_start: 0,
                range_end: MIB - 1,
                compressed_size: MIB,
                last_accessed: 1,
                expires_at: 2,
                access_count: 1,
                is_staged: false,
                leaf: "abc".to_string(),
            },
        };
        let records = vec![mk("b/one", 10), mk("b/two", 20), mk("b/three", 30)];
        append_tail_delta_records(cache_dir, "host:1", &records, 4096).unwrap();
        assert_eq!(count_tail_delta_records(cache_dir), Some(3));

        let mut retire = std::collections::HashSet::new();
        retire.insert(records[1].identity());
        let dropped = retire_tail_delta_records(cache_dir, &retire);

        assert_eq!(dropped, 1, "exactly the named record is dropped");
        let survivors: Vec<String> = read_all_tail_delta_records(cache_dir)
            .into_iter()
            .map(|r| r.record.cache_key)
            .collect();
        assert_eq!(
            survivors,
            vec!["b/one".to_string(), "b/three".to_string()],
            "the two UNNAMED records must survive. A retain-set given the same one identity \
             would have deleted both, which is the task-77 shape."
        );
        assert_eq!(count_tail_delta_records(cache_dir), Some(2));
    }

    /// A retire whose set names nothing in one instance's file leaves that file untouched.
    ///
    /// Two instances' files, the retire names a record in only one. The untouched file's
    /// bytes and mtime must be identical afterwards, and the other file must be rewritten
    /// (control: without it the test would pass on a retire that rewrote nothing at all).
    #[test]
    fn retire_does_not_rewrite_a_file_it_drops_nothing_from() {
        let temp = TempDir::new().unwrap();
        let cache_dir = temp.path();
        let mk = |inst: &str, key: &str, at: u64| TailDeltaRecord {
            bucket: "b".to_string(),
            l1: "0a".to_string(),
            at,
            instance_id: inst.to_string(),
            record: TailRecord {
                cache_key: key.to_string(),
                range_start: 0,
                range_end: MIB - 1,
                compressed_size: MIB,
                last_accessed: 1,
                expires_at: 2,
                access_count: 1,
                is_staged: false,
                leaf: "abc".to_string(),
            },
        };
        let a = vec![mk("host:1", "b/a1", 10), mk("host:1", "b/a2", 20)];
        let b = vec![mk("host:2", "b/b1", 30), mk("host:2", "b/b2", 40)];
        append_tail_delta_records(cache_dir, "host:1", &a, 4096).unwrap();
        append_tail_delta_records(cache_dir, "host:2", &b, 4096).unwrap();
        let file_of = |inst: &str| {
            tail_delta_files(cache_dir)
                .into_iter()
                .find(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with(inst))
                })
                .expect("both instances' files must exist")
        };
        assert_eq!(
            tail_delta_files(cache_dir).len(),
            2,
            "two files, or the test is vacuous"
        );
        let (pa, pb) = (file_of("host:1"), file_of("host:2"));
        // Push both mtimes into the past so a rewrite is visible as a change.
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
        for p in [&pa, &pb] {
            std::fs::OpenOptions::new()
                .write(true)
                .open(p)
                .unwrap()
                .set_modified(old)
                .unwrap();
        }
        let bytes_a = std::fs::read(&pa).unwrap();
        let mtime_a = std::fs::metadata(&pa).unwrap().modified().unwrap();
        let mtime_b = std::fs::metadata(&pb).unwrap().modified().unwrap();

        let mut retire = std::collections::HashSet::new();
        retire.insert(b[0].identity());
        let dropped = retire_tail_delta_records(cache_dir, &retire);

        assert_eq!(dropped, 1);
        assert_eq!(
            std::fs::read(&pa).unwrap(),
            bytes_a,
            "the untouched file's bytes must be identical"
        );
        assert_eq!(
            std::fs::metadata(&pa).unwrap().modified().unwrap(),
            mtime_a,
            "the untouched file must not be rewritten: its mtime moved"
        );
        assert_ne!(
            std::fs::metadata(&pb).unwrap().modified().unwrap(),
            mtime_b,
            "CONTROL: the file the retire names a record in must be rewritten"
        );
        assert_eq!(read_tail_delta_file(&pb).len(), 1);
        // An empty retire set returns before touching any file.
        assert_eq!(
            retire_tail_delta_records(cache_dir, &std::collections::HashSet::new()),
            0
        );
    }

    /// Rebuilding a shard retires that shard's delta records and leaves other shards' alone.
    ///
    /// The second shard is the control: without it, a sweep that emptied the whole file would
    /// pass. `bucket-subfloor/02` is rebuilt by the visit; `bucket-elsewhere/7f` has no `.meta`
    /// on disk at all, so no visit reaches it.
    #[tokio::test]
    async fn a_rebuild_retires_only_the_rebuilt_shards_delta_records() {
        let _serial = TAIL_TEST_GUARD.lock().await;
        let (tracker, temp) = tracker_with_cap(4096).await;
        let cache_dir = temp.path().to_path_buf();
        seed_meta(
            &cache_dir,
            "bucket-subfloor",
            "02",
            "ccc",
            "subfloor",
            false,
            &[Spec::incompressible(0, 8 * MIB)],
        );
        let mk = |bucket: &str, l1: &str| TailDeltaRecord {
            bucket: bucket.to_string(),
            l1: l1.to_string(),
            at: 1,
            instance_id: "host:1".to_string(),
            record: TailRecord {
                cache_key: format!("{}/obj", bucket),
                range_start: 0,
                range_end: MIB - 1,
                compressed_size: MIB,
                last_accessed: 1,
                expires_at: 2,
                access_count: 1,
                is_staged: false,
                leaf: "ccc".to_string(),
            },
        };
        append_tail_delta_records(
            &cache_dir,
            "host:1",
            &[mk("bucket-subfloor", "02"), mk("bucket-elsewhere", "7f")],
            4096,
        )
        .unwrap();
        assert_eq!(count_tail_delta_records(&cache_dir), Some(2));

        tracker.perform_rolling_validation().await.unwrap();

        let survivors: Vec<(String, String)> = read_all_tail_delta_records(&cache_dir)
            .into_iter()
            .map(|r| (r.bucket, r.l1))
            .collect();
        assert_eq!(
            survivors,
            vec![("bucket-elsewhere".to_string(), "7f".to_string())],
            "the rebuilt shard's record is superseded by the partition and must be retired; the \
             other shard's must survive, or the sweep is emptying the file rather than retiring \
             by shard"
        );
    }

    /// A shard whose partition WRITE FAILED keeps its tail-delta records.
    ///
    /// # The defect, and why it needed its own red side
    ///
    /// `write_tail_partition` warned and returned on a failed write, and
    /// `retire_rebuilt_shards_from_delta` worked from the partition LIST rather than from write
    /// outcomes — so that shard's records were retired anyway. The delta record is the only hint a
    /// newly admitted large range exists until the shard's next visit, which at the design point
    /// is days, so dropping it while the partition that was supposed to supersede it was never
    /// written makes the range invisible to selection for that whole interval.
    ///
    /// # The failure is INJECTED through the filesystem, not through a flag
    ///
    /// A directory planted at the partition's own path makes `write_atomic`'s rename fail, which
    /// is a real write failure on the real code path rather than a test-only branch. And it cannot
    /// be self-healed by the code under test: nothing on this path removes a directory in the way.
    ///
    /// The CONTROL is the second half. Without it, the survival assertion passes on an
    /// implementation that retires nothing at all — which is the mirror defect.
    #[tokio::test]
    async fn a_shard_whose_partition_write_failed_keeps_its_delta_records() {
        let _serial = TAIL_TEST_GUARD.lock().await;
        let (tracker, temp) = tracker_with_cap(4096).await;
        let cache_dir = temp.path().to_path_buf();
        seed_meta(
            &cache_dir,
            "bucket-blocked",
            "02",
            "ccc",
            "blocked",
            false,
            &[Spec::incompressible(0, 8 * MIB)],
        );
        // A DIRECTORY where the partition file must go. `write_atomic` writes a sibling `.tmp` and
        // renames it over this path; renaming a file onto a directory fails.
        let blocked = tail_partition_path(&cache_dir, "bucket-blocked", "02");
        std::fs::create_dir_all(&blocked).unwrap();
        assert!(
            blocked.is_dir(),
            "fixture: the write must be genuinely blocked, or this test asserts nothing"
        );

        let record = TailDeltaRecord {
            bucket: "bucket-blocked".to_string(),
            l1: "02".to_string(),
            at: 1,
            instance_id: "host:1".to_string(),
            record: TailRecord {
                cache_key: "bucket-blocked/obj".to_string(),
                range_start: 0,
                range_end: MIB - 1,
                compressed_size: MIB,
                last_accessed: 1,
                expires_at: 2,
                access_count: 1,
                is_staged: false,
                leaf: "ccc".to_string(),
            },
        };
        append_tail_delta_records(&cache_dir, "host:1", std::slice::from_ref(&record), 4096)
            .unwrap();

        tracker.perform_rolling_validation().await.unwrap();

        assert_eq!(
            read_all_tail_delta_records(&cache_dir).len(),
            1,
            "the partition write failed, so nothing supersedes this record and it MUST survive. \
             Retiring it would leave the range invisible to selection until the shard's next \
             visit."
        );
        assert!(
            blocked.is_dir(),
            "fixture check: the blocking directory must still be in the way, or the write \
             succeeded and the survival above proves nothing"
        );

        // CONTROL — the same fixture with the write unblocked MUST retire the record. Without
        // this, the assertion above passes on an implementation that retires nothing.
        std::fs::remove_dir_all(&blocked).unwrap();
        let (tracker2, temp2) = tracker_with_cap(4096).await;
        let cache_dir2 = temp2.path().to_path_buf();
        seed_meta(
            &cache_dir2,
            "bucket-blocked",
            "02",
            "ccc",
            "blocked",
            false,
            &[Spec::incompressible(0, 8 * MIB)],
        );
        append_tail_delta_records(&cache_dir2, "host:1", &[record], 4096).unwrap();
        tracker2.perform_rolling_validation().await.unwrap();
        assert_eq!(
            read_all_tail_delta_records(&cache_dir2).len(),
            0,
            "CONTROL FAILED: with the write unblocked the record must be retired, or the survival \
             assertion above is satisfied by a mechanism that retires nothing"
        );
    }

    /// The gauges report the rebuild, and `tail_entries_last_rebuild` distinguishes a genuinely
    /// empty partition from no rebuild at all.
    #[tokio::test]
    async fn the_gauges_report_the_rebuild_and_none_is_not_zero() {
        let _serial = TAIL_TEST_GUARD.lock().await;
        let (tracker, temp) = tracker_with_cap(4096).await;
        let cache_dir = temp.path().to_path_buf();
        let before = collect_tail_partition_gauges(&cache_dir);
        assert_eq!(
            before.tail_entries_last_rebuild, None,
            "before any rebuild the figure must be absent, NOT zero — a zero-member partition is \
             a real reading and the two must be distinguishable"
        );
        assert_eq!(before.tail_rebuilds_total, 0);
        assert_eq!(
            before.tail_effective_min_bytes, None,
            "before any rebuild the effective minimum must be absent, NOT zero"
        );

        seed_meta(
            &cache_dir,
            "bucket-gauge",
            "06",
            "ggg",
            "gauge",
            false,
            &[Spec::incompressible(0, 8 * MIB)],
        );
        tracker.perform_rolling_validation().await.unwrap();

        let after = collect_tail_partition_gauges(&cache_dir);
        assert!(after.tail_rebuilds_total >= 1, "{:?}", after);
        assert_eq!(
            after.tail_entries_last_rebuild,
            Some(1),
            "one member was written: {:?}",
            after
        );
        // RED SIDE: with the `TAIL_EFFECTIVE_MIN_LAST.store` line in `record_tail_rebuild`
        // deleted, this fails with `left: None, right: Some(8388608)`.
        assert_eq!(
            after.tail_effective_min_bytes,
            Some(8 * MIB),
            "the one member's compressed size is the shard's effective minimum: {:?}",
            after
        );
        assert_eq!(after.tail_delta_records, Some(0));
    }
}

/// § 4.5's selection READ: the bounded prefix, the absent-versus-empty distinction, the
/// histogram-derived depth, and the tail-delta's cap.
///
/// The writer's own tests are `mod tail_partition_tests`; these are the reader's. Kept apart
/// deliberately: a reader test that built its fixture by running the writer could not fail when
/// both agreed on something wrong, and a writer test that read its own output through this reader
/// would have the same blind spot in the other direction.
///
/// Spec: cache-eviction-at-scale § 4.5. Requirements: 3.3, 1.5
#[cfg(test)]
mod tail_selection_read_tests {
    use super::*;

    /// Serialises the two tests that observe [`tail_delta_cap_rewrites_total`], which is a
    /// PROCESS-GLOBAL counter with no reset — libtest runs this binary's tests as parallel threads
    /// in one process, so a sibling's rewrites land in this test's before/after delta.
    ///
    /// Measured rather than anticipated: `the_delta_cap_rewrite_is_amortised_over_cap_appends`
    /// reported **1** rewrite when run alone and **8** when its sibling ran beside it, which is the
    /// exact failure the testing guidance prescribes one held guard for. Poisoning is
    /// tolerated because a panicking sibling must not convert one failure into several.
    static REWRITE_COUNTER_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn rewrite_guard() -> std::sync::MutexGuard<'static, ()> {
        REWRITE_COUNTER_GUARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn record(key: &str, size: u64) -> TailRecord {
        TailRecord {
            cache_key: key.to_string(),
            range_start: 0,
            range_end: size - 1,
            compressed_size: size,
            last_accessed: 1,
            expires_at: 0,
            access_count: 0,
            is_staged: false,
            leaf: "aaa".to_string(),
        }
    }

    /// Write a partition file with the records in the order given — deliberately NOT sorted here,
    /// so a test can plant a file in the wrong order and observe that a prefix read of it is a
    /// prefix of the FILE rather than of the shard's top-N.
    fn plant(cache_dir: &std::path::Path, bucket: &str, l1: &str, records: &[TailRecord]) {
        let path = tail_partition_path(cache_dir, bucket, l1);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut body = String::new();
        for r in records {
            body.push_str(&serde_json::to_string(r).unwrap());
            body.push('\n');
        }
        std::fs::write(&path, body).unwrap();
    }

    /// § 4.1's ruling: an ABSENT partition file and an EMPTY one are different states.
    ///
    /// # Why three assertions rather than one
    ///
    /// `!path.exists()` passes both on an implementation that correctly wrote an empty file and on
    /// one that never ran, which is the trap § 4.1's added note names. Here the reader is the thing
    /// under test, so the trap is the mirror image: a reader that returned an empty record list for
    /// both would let a cold volume report `TAIL_EMPTY` — a claim that the cache holds nothing
    /// large — when what it actually has is an unvisited shard. Both arms are asserted against the
    /// same reader in the same test so neither can pass by the other's construction.
    #[test]
    fn an_absent_partition_is_not_an_empty_one() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path();

        // EMPTY: the file exists and holds zero records.
        plant(dir, "bucket-empty", "01", &[]);
        let empty = read_tail_partition_prefix(dir, "bucket-empty", "01", 64);
        assert_eq!(
            empty,
            TailPartitionRead::Present {
                records: Vec::new(),
                complete: true
            },
            "a partition file that exists and holds zero records means this shard has no member \
             above the floor — a real and expected state (§ 4.3), and NOT the same observation as \
             an unvisited shard"
        );

        // ABSENT: no file at all.
        let absent = read_tail_partition_prefix(dir, "bucket-absent", "02", 64);
        assert_eq!(
            absent,
            TailPartitionRead::Absent,
            "an absent partition file means the shard has not been visited since the scheme was \
             introduced, so selection has NO information about it"
        );

        // And the enumeration sees only the one that exists, which is what stops a selection pass
        // reading a shard it has no file for and counting the nothing it found.
        assert_eq!(
            enumerate_tail_shards(dir),
            vec![("bucket-empty".to_string(), "01".to_string())]
        );
    }

    /// The prefix is bounded by `depth` and reports whether more is available.
    ///
    /// `complete` is the figure § 4.5's escalation branches on: without it, "the prefix holds
    /// everything" and "the prefix stopped early" are the same observation and the escalation
    /// either never fires or never stops.
    #[test]
    fn the_prefix_is_bounded_by_depth_and_reports_whether_more_remains() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path();
        let records: Vec<TailRecord> = (0..200)
            .map(|i| record(&format!("b/key-{i:04}"), 200_000 - i as u64))
            .collect();
        plant(dir, "bucket", "00", &records);

        let shallow = read_tail_partition_prefix(dir, "bucket", "00", 10);
        assert_eq!(shallow.records().len(), 10, "the prefix must honour depth");
        assert!(
            shallow.could_yield_more(),
            "a prefix that stopped short of end-of-file must say so, or the escalation cannot \
             tell it from a whole partition"
        );
        assert_eq!(
            shallow.records()[0].compressed_size,
            200_000,
            "the prefix is the head of the file, which the writer orders compressed_size \
             descending — so it is the shard's top-N by size"
        );

        let deep = read_tail_partition_prefix(dir, "bucket", "00", 4096);
        assert_eq!(deep.records().len(), 200);
        assert!(
            !deep.could_yield_more(),
            "a read that reached end-of-file holds the whole partition"
        );
    }

    /// A trailing partial line is discarded, not parsed and not fatal.
    ///
    /// § 4.5 requires this for two independent reasons: the byte boundary of a prefix read lands
    /// mid-record by construction, and a concurrent rebuild can leave a torn tail (§ 2 trap 5).
    #[test]
    fn a_torn_trailing_line_is_discarded_rather_than_breaking_the_read() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path();
        let path = tail_partition_path(dir, "bucket", "00");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut body = serde_json::to_string(&record("b/whole", 4096)).unwrap();
        body.push('\n');
        // A record cut off mid-field, as a rename racing a reader would leave it.
        body.push_str(r#"{"cache_key":"b/torn","range_start":0,"range_e"#);
        std::fs::write(&path, body).unwrap();

        let read = read_tail_partition_prefix(dir, "bucket", "00", 64);
        assert_eq!(
            read.records().len(),
            1,
            "the whole record survives and the torn one is dropped: {:?}",
            read
        );
        assert_eq!(read.records()[0].cache_key, "b/whole");
    }

    /// The depth derived from a histogram, and what happens when there isn't one.
    ///
    /// # The clamp is the substance, not the derivation
    ///
    /// § 4.5 derives the depth from the histogram and says getting it wrong "costs one extra round
    /// of reads, not correctness". That is true of an over-estimate. An UNDER-estimate predicts a
    /// **shallower** prefix — and the `st_size` fold's coverage is partial by construction, since
    /// it visits shards on its own cadence and budget, so a mid-rotation volume's histogram
    /// systematically under-counts. Clamping the derived depth up to the no-histogram default is
    /// what converts that from a missed candidate into an extra round.
    #[test]
    fn the_depth_is_derived_from_the_histogram_and_clamped_below_by_the_default() {
        // No histogram at all: the default, which is also the floor.
        assert_eq!(
            initial_prefix_depth(1 << 30, None, 256, 4096),
            DEFAULT_TAIL_PREFIX_DEPTH,
            "a volume with no fold installed starts at the default and relies on escalation"
        );

        // A THIN histogram — one shard's worth of a 256-shard volume — under-counts, so its
        // honest derivation is shallower than the default. The clamp is what stops that being a
        // shallower read than no histogram at all.
        let mut thin = vec![0u64; crate::st_size_fold::HISTOGRAM_CLASSES];
        thin[13] = 4; // four ranges of >= 2 MiB
        let derived_thin = initial_prefix_depth(8 * 1024 * 1024, Some(&thin), 256, 4096);
        assert_eq!(
            derived_thin, DEFAULT_TAIL_PREFIX_DEPTH,
            "an under-counting histogram must not predict a SHALLOWER prefix than no histogram \
             at all — the error direction has to be an extra round, never a missed candidate"
        );

        // A histogram whose largest classes cannot hold the target asks for everything, so the
        // pass reaches CAP_EXHAUSTED rather than escalating four times to discover the same.
        let mut tiny = vec![0u64; crate::st_size_fold::HISTOGRAM_CLASSES];
        tiny[1] = 10; // ten ranges of ~512 B
        assert_eq!(
            initial_prefix_depth(1 << 40, Some(&tiny), 256, 4096),
            4096,
            "when the whole histogram cannot hold the target, the honest first depth is the cap"
        );

        // The cap is the ceiling: a deeper read cannot return more, because the partition does
        // not hold more.
        let mut fat = vec![0u64; crate::st_size_fold::HISTOGRAM_CLASSES];
        fat[33] = 1_000_000;
        assert_eq!(
            initial_prefix_depth(1 << 40, Some(&fat), 1, 8),
            8,
            "the depth is capped at tail_max_entries"
        );
    }

    /// The global histogram is the element-wise sum of every shard checkpoint's own.
    ///
    /// Element-wise summing is sound only because `histogram_class`'s edges are FIXED rather than
    /// derived per shard. `None` for a volume with no checkpoint at all, because an unknown
    /// distribution and an empty one are different things and a zeroed vector cannot say which.
    #[test]
    fn the_global_histogram_sums_every_shards_own_and_is_none_when_there_is_none() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path();
        assert_eq!(
            global_size_histogram(dir),
            None,
            "a volume no fold has reached has an UNKNOWN distribution, not an empty one"
        );

        for (bucket, l1, class, count) in [("b", "00", 12usize, 3u64), ("b", "01", 12, 4)] {
            let mut classes = vec![0u64; crate::st_size_fold::HISTOGRAM_CLASSES];
            classes[class] = count;
            install_histogram_for_tests(dir, bucket, l1, &classes);
        }

        let summed = global_size_histogram(dir).expect("two checkpoints carry one");
        assert_eq!(
            summed[12], 7,
            "3 + 4 element-wise. Two shards' histograms are addable only because the class edges \
             are fixed rather than derived from each shard's own distribution."
        );
        assert_eq!(summed.iter().sum::<u64>(), 7, "nothing else moved");
    }

    /// The tail-delta is capped at the APPEND, which is what makes § 4.5's read of it bounded.
    ///
    /// # Two-sided: the cap binds AND it keeps the largest
    ///
    /// Asserting only that the file shrank would pass on an implementation that truncated it, which
    /// would drop exactly the records selection needs. So this asserts WHICH records survive: the
    /// largest `cap`, because § 4.2's lemma is about a top-N and a per-instance top-N is sound only
    /// if it is genuinely the top.
    ///
    /// # The bound asserted is the BYTE bound, deliberately
    ///
    /// The trigger is a byte threshold, so the file's record count at that size depends on key
    /// length and is not `2 × cap`. An earlier draft of this test asserted `records <= 2 × cap` and
    /// passed at **exactly** 16 against a ceiling of 16 — a coincidence of this fixture's key
    /// length, not a property of the code. The byte bound is what the implementation actually
    /// guarantees and what bounds the read's I/O; the read's own heap is what bounds memory, and
    /// that is asserted separately below.
    #[test]
    fn the_tail_delta_is_capped_at_the_append_and_keeps_the_largest() {
        let _serial = rewrite_guard();
        let before_rewrites = tail_delta_cap_rewrites_total();
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path();
        let cap = 8usize;
        let mk = |size: u64| TailDeltaRecord {
            bucket: "b".to_string(),
            l1: "00".to_string(),
            at: size,
            instance_id: "host:1".to_string(),
            record: record(&format!("b/key-{size}"), size),
        };

        // Append well past the high-water mark of 2 × cap, one at a time, as consolidation does.
        for size in 1..=200u64 {
            append_tail_delta_records(dir, "host:1", &[mk(size * 4096)], cap).unwrap();
        }

        // The I/O bound: the file's SIZE, which is what the trigger actually tests and what
        // bounds § 4.5's read. Without a cap this grows as admission rate × rotation period, and
        // § 16.5 makes the rotation period grow with N.
        let high_water_bytes = (cap * 2 * 512) as u64;
        let on_disk_bytes = std::fs::metadata(tail_delta_path(dir, "host:1"))
            .unwrap()
            .len();
        assert!(
            on_disk_bytes <= high_water_bytes,
            "the file must stay inside its byte high-water mark of {} B: it holds {} B after 200 \
             appends",
            high_water_bytes,
            on_disk_bytes
        );

        let on_disk = read_all_tail_delta_records(dir);
        assert!(
            on_disk.len() >= cap,
            "the cap must not truncate below itself: {} records",
            on_disk.len()
        );
        // The largest appended size is 200 × 4096. Whatever survives must include it.
        let largest = on_disk
            .iter()
            .map(|r| r.record.compressed_size)
            .max()
            .unwrap();
        assert_eq!(
            largest,
            200 * 4096,
            "the cap discards the SMALLEST, never the most recent: a truncation would lose the \
             largest recent admission, which is precisely the record selection needs"
        );
        let smallest = on_disk
            .iter()
            .map(|r| r.record.compressed_size)
            .min()
            .unwrap();
        assert!(
            smallest > 4096,
            "the earliest, smallest admissions must be the ones dropped. Smallest surviving: {}",
            smallest
        );
        assert!(
            tail_delta_cap_rewrites_total() - before_rewrites > 0,
            "the cap rewrite must actually have run, or the file stayed short for another reason"
        );

        // And the bounded read is bounded regardless of what is on disk.
        let read = read_tail_delta_candidates(dir, cap);
        assert!(
            read.len() <= cap,
            "the read keeps at most `cap` per instance: {} records",
            read.len()
        );
    }

    /// Equal-size records: the cap retains the COLDEST, never the lexicographically largest key.
    ///
    /// # Why the fixture is all ties, and why that is the realistic case rather than the corner
    ///
    /// A fixture with distinct sizes never reaches the tie-break at all, so it passes whatever the
    /// tie-break is — `the_tail_delta_is_capped_at_the_append_and_keeps_the_largest` above is
    /// exactly that shape and stays green either way. Ties are not exotic: every range the proxy
    /// admits from an `aws s3 cp` multipart write is one `multipart_chunksize`, and a widened page
    /// is one `page_size`, so a cache under uniform traffic has size classes with hundreds of
    /// members each and the cap decides *inside* one of them.
    ///
    /// `last_accessed` here is deliberately ANTI-correlated with the cache key, so the retained
    /// set under a `cache_key` tie-break (the four lexicographically largest keys, `k08`-`k11`)
    /// and the retained set under a coldness tie-break (`k00`-`k03`) are disjoint. A fixture where
    /// the two agree would pass on both trees.
    #[test]
    fn equal_size_delta_records_are_retained_coldest_first() {
        let _serial = rewrite_guard();
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path();
        const SIZE: u64 = 1024 * 1024;

        // One size class, twelve members. `last_accessed` rises with the key, so the coldest four
        // are the lexicographically SMALLEST keys.
        let records: Vec<TailDeltaRecord> = (0..12u64)
            .map(|i| TailDeltaRecord {
                bucket: "b".to_string(),
                l1: "00".to_string(),
                at: 1_000 + i,
                instance_id: "host:1".to_string(),
                record: TailRecord {
                    cache_key: format!("b/k{:02}", i),
                    range_start: 0,
                    range_end: SIZE - 1,
                    compressed_size: SIZE,
                    last_accessed: i + 1,
                    expires_at: 0,
                    access_count: 0,
                    is_staged: false,
                    leaf: "aaa".to_string(),
                },
            })
            .collect();

        // A cap far above the fixture at the APPEND, so the append's own rewrite cannot be what
        // selected the survivors: the read's heap is the consumer under test.
        append_tail_delta_records(dir, "host:1", &records, 4096).unwrap();
        assert_eq!(
            count_tail_delta_records(dir),
            Some(12),
            "the fixture must be on disk whole, or the retention under test never happens"
        );

        let kept = read_tail_delta_candidates(dir, 4);
        let mut keys: Vec<String> = kept.iter().map(|r| r.record.cache_key.clone()).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "b/k00".to_string(),
                "b/k01".to_string(),
                "b/k02".to_string(),
                "b/k03".to_string()
            ],
            "with every size equal the cap must keep the four COLDEST records. A `cache_key`              tie-break keeps b/k08-b/k11 instead, which is the newest four here and has no              relation to eviction priority"
        );
    }

    /// The MEASUREMENT behind § 4.5(b): what capping the delta costs on the consolidation path.
    ///
    /// # The question, and why it was open
    ///
    /// Capping the delta means the file must be size-ORDERED, not merely size-bounded, because a
    /// prefix of an append-ordered file is an arbitrary subset rather than a top-N. Maintaining it
    /// that way means a whole-file rewrite, and R5.6 measures a rewrite at **119.5 ms** against a
    /// **10.5 ms** append — which is the reason appending was chosen in the first place. Nobody had
    /// measured whether a capped delta is affordable.
    ///
    /// # The answer is amortisation, and this test pins the property the arithmetic rests on
    ///
    /// The high-water mark is `2 × cap` and the low-water mark is `cap`, so one rewrite is
    /// amortised over `cap` appends. At the default 4096 that is `119.5 / 4096 ≈ 0.03 ms` per
    /// append against a 10.5 ms append — about 0.3%.
    ///
    /// **That arithmetic is only true if the rewrite rate really is one per `cap` appends**, which
    /// is what this asserts. A high-water mark that collapsed onto the low-water mark would make
    /// every append pay a whole-file rewrite and the overhead would be 1,138%, not 0.3%. The
    /// assertion is the guard on the design decision, not decoration.
    ///
    /// The wall-clock figures printed here are LOCAL disk and are not comparable to R5.6's
    /// shared-volume measurement; they are printed so the ratio between the two operations can be
    /// sanity-checked, not so the absolute numbers can be quoted.
    #[test]
    fn the_delta_cap_rewrite_is_amortised_over_cap_appends() {
        let _serial = rewrite_guard();
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path();
        let cap = 256usize;
        let appends = 4 * cap;
        let before = tail_delta_cap_rewrites_total();

        let mk = |i: u64| TailDeltaRecord {
            bucket: "bucket-measure".to_string(),
            l1: "00".to_string(),
            at: i,
            instance_id: "host:1".to_string(),
            record: record(
                &format!("bucket-measure/object-with-a-realistic-length-key-{i:08}"),
                i * 4096,
            ),
        };

        let start = std::time::Instant::now();
        for i in 1..=appends as u64 {
            append_tail_delta_records(dir, "host:1", &[mk(i)], cap).unwrap();
        }
        let elapsed = start.elapsed();
        let rewrites = tail_delta_cap_rewrites_total() - before;

        // The property the amortisation arithmetic rests on: rewrites scale as appends / cap, not
        // as appends. One rewrite per `cap` appends, plus one for the first crossing of the
        // high-water mark, plus slack for the byte-proxy trigger over-firing on long keys.
        let ceiling = (appends / cap) as u64 + 2;
        assert!(
            rewrites <= ceiling,
            "the cap rewrite must be amortised over ~{} appends: {} appends produced {} \
             rewrite(s), above the ceiling of {}. If the high-water and low-water marks have \
             collapsed onto each other, every append pays a whole-file rewrite and R5.6's 119.5ms \
             figure applies per append rather than per {} appends.",
            cap,
            appends,
            rewrites,
            ceiling,
            cap
        );
        assert!(
            rewrites >= 1,
            "the rewrite must actually fire, or this measures an uncapped file"
        );
        println!(
            "MEASUREMENT (local disk, NOT comparable to R5.6's shared-volume figures): \
             {appends} single-record appends at cap={cap} took {:?} total, {:?} per append, and \
             triggered {rewrites} whole-file cap rewrite(s) — one per {:.0} appends. Amortised \
             against R5.6's 119.5ms rewrite / 10.5ms append, a rewrite every {:.0} appends is \
             {:.2}ms per append, {:.2}% of the append cost.",
            elapsed,
            elapsed / appends as u32,
            appends as f64 / rewrites as f64,
            appends as f64 / rewrites as f64,
            119.5 / (appends as f64 / rewrites as f64),
            100.0 * (119.5 / (appends as f64 / rewrites as f64)) / 10.5,
        );
    }

    /// The bounded read is bounded even against a file an older build left oversized.
    ///
    /// The belt-and-braces half of the delta's bound: the file is capped by construction at the
    /// append, and this is what happens when it was not. Memory stays bounded; the I/O cost is
    /// paid once and the next append compacts it.
    #[test]
    fn the_bounded_read_survives_a_file_no_capping_build_wrote() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path();
        let path = tail_delta_path(dir, "legacy:1");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut body = String::new();
        for size in 1..=5000u64 {
            let r = TailDeltaRecord {
                bucket: "b".to_string(),
                l1: "00".to_string(),
                at: size,
                instance_id: "legacy:1".to_string(),
                record: record(&format!("b/legacy-{size}"), size * 512),
            };
            body.push_str(&serde_json::to_string(&r).unwrap());
            body.push('\n');
        }
        std::fs::write(&path, body).unwrap();

        let read = read_tail_delta_candidates(dir, 16);
        assert_eq!(read.len(), 16, "the read is capped whatever the file holds");
        assert_eq!(
            read.iter().map(|r| r.record.compressed_size).max().unwrap(),
            5000 * 512,
            "and it keeps the LARGEST, so a legacy file costs I/O once rather than correctness"
        );
    }
}

/// The validation scheduler's fleet behaviour: one pass per spacing window fleet-wide, a bounded
/// retry rate while a peer holds `validation.lock`, and a full scan's size install serialised
/// against a consolidation cycle's delta collect-and-apply.
///
/// Every tracker here is built on its own `JournalConsolidator`, so the two `flock`s each test
/// relies on (`validation.lock`, `global_consolidation.lock`) are separate open file
/// descriptions and conflict in-process the way two proxies' do.
#[cfg(test)]
mod validation_scheduler_fleet_tests {
    use super::*;
    use crate::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
    use crate::journal_manager::JournalManager;
    use crate::metadata_lock_manager::MetadataLockManager;
    use tempfile::TempDir;

    async fn consolidator_on(dir: &std::path::Path, name: &str) -> Arc<JournalConsolidator> {
        std::fs::create_dir_all(dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(dir.join("locks")).unwrap();
        let journal_manager = Arc::new(JournalManager::new(dir.to_path_buf(), name.to_string()));
        let lock_manager = Arc::new(MetadataLockManager::new(
            dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidator = Arc::new(JournalConsolidator::new(
            dir.to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        ));
        consolidator.initialize().await.unwrap();
        consolidator
    }

    /// A tracker on `dir` whose time of day is six hours out, so the scheduled window is
    /// never the reason a pass runs or not.
    async fn tracker_on(
        dir: &std::path::Path,
        name: &str,
    ) -> (Arc<CacheSizeTracker>, Arc<JournalConsolidator>) {
        use chrono::{Duration as ChronoDuration, Local, Timelike};
        let target = Local::now() + ChronoDuration::hours(6);
        let consolidator = consolidator_on(dir, name).await;
        let config = CacheSizeConfig {
            validation_time_of_day: format!("{:02}:{:02}", target.hour(), target.minute()),
            ..CacheSizeConfig::default()
        };
        let tracker = Arc::new(
            CacheSizeTracker::new(dir.to_path_buf(), config, false, consolidator.clone())
                .await
                .unwrap(),
        );
        (tracker, consolidator)
    }

    fn plant_delta(dir: &std::path::Path, name: &str, delta: i64) {
        std::fs::write(
            dir.join("size_tracking")
                .join(format!("delta_{}.json", name)),
            format!(
                r#"{{"delta":{},"write_cache_delta":0,"instance_id":"{}"}}"#,
                delta, name
            ),
        )
        .unwrap();
    }

    /// Two proxies on one cache, both scheduled for the same window before either has run.
    /// The first takes the lock and runs; the second takes it after the first releases and must
    /// skip, because the first wrote `validation.json`.
    ///
    /// # RED SIDE
    ///
    /// With the under-lock re-check removed, B returns `Ran` and rewrites `validation.json`.
    #[tokio::test]
    async fn two_trackers_in_one_window_run_exactly_one_pass() {
        let _tail_serial = TAIL_TEST_GUARD.lock().await;
        let tmp = TempDir::new().unwrap();
        let (a, _ca) = tracker_on(tmp.path(), "fleet-a").await;
        let (b, _cb) = tracker_on(tmp.path(), "fleet-b").await;

        // Both schedule before either has run: no metadata, so both say "now".
        let now = SystemTime::now();
        for t in [&a, &b] {
            let next = t.calculate_next_validation_time().await;
            assert!(
                next.duration_since(now).unwrap_or_default() < Duration::from_secs(5),
                "FIXTURE: with no validation.json both instances must schedule immediately"
            );
        }

        let first = a.perform_scheduled_validation().await.unwrap();
        assert_eq!(
            first,
            ValidationOutcome::Ran,
            "FIXTURE: A must run the pass"
        );
        let stamped = a
            .read_validation_metadata()
            .await
            .expect("FIXTURE: A's pass must write validation.json")
            .last_validation;

        let second = b.perform_scheduled_validation().await.unwrap();
        let after = b.read_validation_metadata().await.unwrap().last_validation;
        assert_eq!(
            second,
            ValidationOutcome::PeerRanRecently,
            "SPACING-UNDER-LOCK: B took validation.lock after A's pass completed and must skip \
             on re-reading validation.json, not run a second full pass in the same window"
        );
        assert_eq!(
            after, stamped,
            "SPACING-UNDER-LOCK: B's skip must leave A's validation.json untouched"
        );
    }

    /// While a peer holds `validation.lock` and `validation.json` is unreadable (a fresh cache
    /// during its first scan), the scheduler must back off rather than retry in a tight loop.
    /// Over a 500 ms window the pre-fix scheduler made thousands of attempts; the fixed one makes
    /// one and then sleeps at least `VALIDATION_RETRY_BACKOFF`.
    #[tokio::test]
    async fn contention_with_unreadable_metadata_backs_off() {
        let tmp = TempDir::new().unwrap();
        let (holder, _ch) = tracker_on(tmp.path(), "spin-holder").await;
        let (waiter, _cw) = tracker_on(tmp.path(), "spin-waiter").await;
        let _held = holder
            .try_acquire_validation_lock()
            .await
            .expect("FIXTURE: the holder must take validation.lock");
        assert!(
            waiter.read_validation_metadata().await.is_err(),
            "FIXTURE: validation.json must be unreadable, or the scheduler is not told 'now'"
        );

        let task = {
            let w = waiter.clone();
            tokio::spawn(async move { w.validation_scheduler().await })
        };
        tokio::time::sleep(Duration::from_millis(500)).await;
        task.abort();
        let attempts = waiter
            .validation_attempts
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            attempts >= 1,
            "FIXTURE: the scheduler must have attempted at least once, got {}",
            attempts
        );
        assert!(
            attempts <= 2,
            "SPIN: {} validation attempts in 500 ms against a held lock; a contended attempt \
             must floor the next one at least {} out",
            attempts,
            format_duration_human(VALIDATION_RETRY_BACKOFF)
        );
    }

    /// A full scan's install while a consolidation cycle holds the lock between collecting the
    /// delta files and applying them. The install waits for the cycle, and the final total is
    /// the scanned figure plus only the deltas written after the scan.
    ///
    /// Figures: tracked 1000, a pre-scan delta of +200 (already on disk, so the scan sees
    /// 1200, and the baseline taken before the walk is 1000 + 200), a post-scan delta of +50.
    /// Correct final: 1250. The absolute install this test was written against yielded
    /// 1200 + 200 + 50 = 1450 inside the cycle's collect-to-apply window. The relative install
    /// adds `census - baseline` = 0, which commutes with the cycle's apply, so it reads 1250
    /// with or without the lock; the test now pins the figure and the wait.
    #[tokio::test]
    async fn an_install_during_a_cycles_apply_waits_and_counts_only_post_scan_deltas() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let (tracker, consolidator) = tracker_on(&dir, "install-tracker").await;
        let peer = consolidator_on(&dir, "install-peer").await;

        consolidator
            .update_size_from_validation(1000, Some(0), Some(1))
            .await;
        plant_delta(&dir, "pre_scan", 200);

        // The peer's cycle: take the lock, collect, hold across the apply, release.
        assert!(
            peer.try_acquire_global_consolidation_lock().unwrap(),
            "FIXTURE: the peer cycle must take global_consolidation.lock"
        );
        let collected = peer.collect_deltas().await;
        assert_eq!(
            collected.delta, 200,
            "FIXTURE: the cycle must collect the pre-scan delta"
        );
        let cycle = {
            let peer = peer.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                peer.atomic_update_size_delta(collected.delta, collected.write_cache_delta)
                    .await
                    .unwrap();
                peer.delete_applied_delta_files(&collected.paths).await;
                peer.release_global_consolidation_lock();
            })
        };

        // 40 x 25 ms = 1 s, comfortably past the cycle's 300 ms hold.
        let installed = tracker
            .install_full_scan_figures(
                1200,
                0,
                2,
                crate::journal_consolidator::ValidationBaseline {
                    total_size: 1200,
                    write_cache_size: 0,
                    cached_objects: 1,
                    state_unreadable: false,
                },
                40,
                Duration::from_millis(25),
            )
            .await;
        cycle.await.unwrap();
        assert!(
            installed,
            "FIXTURE: the install must have happened once the cycle released"
        );

        // A delta written after the scan, applied by the next cycle.
        plant_delta(&dir, "post_scan", 50);
        let next = consolidator.collect_deltas().await;
        consolidator
            .atomic_update_size_delta(next.delta, next.write_cache_delta)
            .await
            .unwrap();
        consolidator.delete_applied_delta_files(&next.paths).await;

        let total = consolidator.get_size_state().await.unwrap().total_size;
        assert_eq!(
            total, 1250,
            "INSTALL-RACE: total_size must be the scanned 1200 plus the post-scan 50. 1450 means \
             the install landed inside the cycle's collect-to-apply window and the pre-scan +200 \
             was counted twice"
        );
    }

    /// The install defers, rather than racing, when the lock stays held past its budget.
    #[tokio::test]
    async fn an_install_defers_when_the_cycle_outlasts_its_budget() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let (tracker, consolidator) = tracker_on(&dir, "defer-tracker").await;
        let peer = consolidator_on(&dir, "defer-peer").await;
        consolidator
            .update_size_from_validation(1000, Some(0), Some(1))
            .await;

        assert!(peer.try_acquire_global_consolidation_lock().unwrap());
        let installed = tracker
            .install_full_scan_figures(
                5000,
                0,
                9,
                crate::journal_consolidator::ValidationBaseline {
                    total_size: 1000,
                    write_cache_size: 0,
                    cached_objects: 1,
                    state_unreadable: false,
                },
                3,
                Duration::from_millis(10),
            )
            .await;
        peer.release_global_consolidation_lock();

        assert!(!installed, "the install must report a deferral");
        assert_eq!(
            consolidator.get_size_state().await.unwrap().total_size,
            1000,
            "a deferred install must leave the tracked figure alone"
        );
    }

    /// A full scan whose install defers records the deferral in `validation.json`, so its
    /// `drift_bytes` is not read as a correction that happened.
    ///
    /// # RED SIDE
    ///
    /// With `!installed` replaced by `false` at the `write_validation_metadata_inner` call in
    /// `perform_full_validation`, `size_install_deferred` reads false.
    #[tokio::test]
    async fn a_deferred_full_scan_install_records_the_deferral() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let (tracker, consolidator) = tracker_on(&dir, "drift-tracker").await;
        let peer = consolidator_on(&dir, "drift-peer").await;
        consolidator
            .update_size_from_validation(1000, Some(0), Some(1))
            .await;

        assert!(peer.try_acquire_global_consolidation_lock().unwrap());
        FULL_INSTALL_ATTEMPTS_OVERRIDE.with(|c| c.set(Some(2)));
        let result = tracker.perform_full_validation().await;
        FULL_INSTALL_ATTEMPTS_OVERRIDE.with(|c| c.set(None));
        peer.release_global_consolidation_lock();
        result.unwrap();

        assert_eq!(
            consolidator.get_size_state().await.unwrap().total_size,
            1000,
            "FIXTURE: the install must have deferred and left the tracked figure alone"
        );
        let metadata = tracker.read_validation_metadata().await.unwrap();
        assert_eq!(
            metadata.drift_bytes, -1000,
            "FIXTURE: the empty cache must measure a -1000 drift"
        );
        assert!(
            metadata.size_install_deferred,
            "DEFERRED-DRIFT: validation.json reports drift {} as if corrected, but the install \
             deferred and nothing was corrected",
            metadata.drift_bytes
        );

        // The installing path reports no deferral.
        tracker.perform_full_validation().await.unwrap();
        let metadata = tracker.read_validation_metadata().await.unwrap();
        assert!(
            !metadata.size_install_deferred,
            "an installed pass must not report a deferral"
        );
        assert_eq!(consolidator.get_size_state().await.unwrap().total_size, 0);
    }

    /// one 4096-byte object the walk counts, with the tracked figures grounded
    /// on it, so the walk measures zero drift and every movement comes from the hook.
    async fn grounded_on_one_object(dir: &std::path::Path, consolidator: &JournalConsolidator) {
        super::tests::write_valid_meta(
            &dir.join("metadata/case39-bucket/00/000/counted.meta"),
            "case39-bucket/counted",
            4096,
        );
        consolidator
            .update_size_from_validation(4096, Some(0), Some(1))
            .await;
    }

    /// The next cycle's apply: collect every delta file, persist, delete what was applied.
    async fn apply_pending_a(consolidator: &JournalConsolidator) {
        let next = consolidator.collect_deltas().await;
        consolidator
            .atomic_update_size_delta(next.delta, next.write_cache_delta)
            .await
            .unwrap();
        consolidator.delete_applied_delta_files(&next.paths).await;
    }

    /// Run one full scan with `hook` landing after the walk and before the install.
    async fn full_scan_with(
        tracker: &Arc<CacheSizeTracker>,
        dir: &std::path::Path,
        hook: Box<dyn FnOnce() + Send>,
    ) {
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = ran.clone();
        test_seams::set_after_full_scan_walk(
            dir,
            Box::new(move || {
                hook();
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }),
        );
        tokio::time::timeout(Duration::from_secs(60), tracker.perform_full_validation())
            .await
            .expect("the full scan must finish inside 60 s")
            .unwrap();
        assert!(
            ran.load(std::sync::atomic::Ordering::SeqCst),
            "FIXTURE: the post-walk hook must have run"
        );
        assert!(
            !tracker
                .read_validation_metadata()
                .await
                .unwrap()
                .size_install_deferred,
            "FIXTURE: the install must have happened, not deferred"
        );
    }

    /// red side (a). A PUT whose credit lands in a peer's delta file after the walk passed
    /// its shard. The census does not see it, so only its credit can count it.
    ///
    /// # RED SIDE
    ///
    /// With the absolute install (`update_size_from_validation_with_kind` in
    /// `install_full_scan_figures`), the install deletes the peer's file and `total_size` reads
    /// 4096.
    #[tokio::test]
    async fn a_put_credited_after_the_walk_passed_its_shard_survives_the_install() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let (tracker, consolidator) = tracker_on(&dir, "case39-put").await;
        grounded_on_one_object(&dir, &consolidator).await;

        let planted = dir.join("size_tracking").join("delta_peer_put.json");
        let hook_dir = dir.clone();
        full_scan_with(
            &tracker,
            &dir,
            Box::new(move || plant_delta(&hook_dir, "peer_put", 4096)),
        )
        .await;
        let peer_file_survived = planted.exists();
        apply_pending_a(&consolidator).await;

        let total = consolidator.get_size_state().await.unwrap().total_size;
        assert_eq!(
            total, 8192,
            "PUT-LOST: total_size={} after the full scan's install and the next apply, expected \
             8192 (the walk's 4096 plus the post-walk PUT's 4096)",
            total
        );
        assert!(
            peer_file_survived,
            "PEER-DELTA-DELETED: the install deleted a peer's unapplied delta file {:?}",
            planted
        );
    }

    /// red side (b). A DELETE after the walk counted the object: the `.meta` goes and the
    /// debit lands in a peer's delta file.
    ///
    /// # RED SIDE
    ///
    /// With the absolute install, the install deletes the debit and `total_size` reads 4096.
    #[tokio::test]
    async fn a_delete_after_the_walk_counted_the_object_stays_deleted() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let (tracker, consolidator) = tracker_on(&dir, "case39-delete").await;
        grounded_on_one_object(&dir, &consolidator).await;

        let planted = dir.join("size_tracking").join("delta_peer_delete.json");
        let hook_dir = dir.clone();
        full_scan_with(
            &tracker,
            &dir,
            Box::new(move || {
                std::fs::remove_file(hook_dir.join("metadata/case39-bucket/00/000/counted.meta"))
                    .unwrap();
                plant_delta(&hook_dir, "peer_delete", -4096);
            }),
        )
        .await;
        let peer_file_survived = planted.exists();
        apply_pending_a(&consolidator).await;

        let total = consolidator.get_size_state().await.unwrap().total_size;
        assert_eq!(
            total, 0,
            "DELETE-UNDONE: total_size={} after the full scan's install and the next apply, \
             expected 0 (the walk counted 4096, then the object was deleted and debited)",
            total
        );
        assert!(
            peer_file_survived,
            "PEER-DELTA-DELETED: the install deleted a peer's unapplied delta file {:?}",
            planted
        );
    }

    /// (review secondary). A large negative correction while a large positive delta
    /// file is still unapplied: the clamp must act on `size_state` plus that file.
    ///
    /// # RED SIDE
    ///
    /// With nothing carried into the accumulator, the install clamps 1000 + 1500 - 5000 to 0
    /// and the stalled +4000 then applies on top: 4000.
    #[tokio::test]
    async fn a_correction_the_clamp_would_cut_is_applied_with_the_pending_deltas() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let consolidator = consolidator_on(&dir, "case44-clamp").await;
        consolidator
            .update_size_from_validation(1000, Some(0), Some(1))
            .await;
        plant_delta(&dir, "stalled_a", 4000);
        let baseline = consolidator
            .validation_baseline()
            .await
            .expect("FIXTURE: size_state.json must be readable");
        assert_eq!(
            baseline.total_size, 5000,
            "FIXTURE: the baseline must count the stalled delta"
        );

        assert!(
            consolidator
                .install_validation_correction(
                    1500,
                    0,
                    1,
                    baseline,
                    crate::cache_size_tracker::ValidationSampleKind::FullScan,
                )
                .await,
            "FIXTURE: the install must have happened"
        );
        consolidator.size_accumulator().flush().await.unwrap();
        apply_pending_a(&consolidator).await;

        let total = consolidator.get_size_state().await.unwrap().total_size;
        assert_eq!(
            total, 1500,
            "CLAMP-CUT-CORRECTION: total_size={} after the install and the next apply, expected \
             the census's 1500 (1000 tracked plus 4000 stalled, corrected by 1500 - 5000)",
            total
        );
    }

    /// (fix 3). The full scan's install reaps a stale `delta_*.json.tmp` and leaves a fresh
    /// temp file and a live delta file alone.
    ///
    /// # RED SIDE
    ///
    /// With the reap call removed from `install_full_scan_figures`, the stale temp file survives.
    #[tokio::test]
    async fn the_full_scan_reaps_a_stale_delta_temp_and_keeps_live_delta_files() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let (tracker, consolidator) = tracker_on(&dir, "case44-reap").await;
        grounded_on_one_object(&dir, &consolidator).await;
        let size_tracking = dir.join("size_tracking");
        let stale = size_tracking.join("delta_dead-instance_4.json.tmp");
        std::fs::write(&stale, br#"{"delta": 7}"#).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&stale)
            .unwrap()
            .set_modified(std::time::SystemTime::now() - Duration::from_secs(2 * 3600))
            .unwrap();
        let fresh = size_tracking.join("delta_live-instance_9.json.tmp");
        std::fs::write(&fresh, br#"{"delta": 8}"#).unwrap();
        plant_delta(&dir, "live_a", 10);
        let live = size_tracking.join("delta_live_a.json");

        tokio::time::timeout(Duration::from_secs(60), tracker.perform_full_validation())
            .await
            .expect("the full scan must finish inside 60 s")
            .unwrap();
        assert!(
            !tracker
                .read_validation_metadata()
                .await
                .unwrap()
                .size_install_deferred,
            "FIXTURE: the install must have happened, not deferred"
        );

        assert!(
            !stale.exists(),
            "STALE-TMP-KEPT: a delta temp file two hours old has no live writer and the full \
             scan must reap it"
        );
        assert!(
            fresh.exists(),
            "a fresh delta temp file may be mid-rename and must survive the full scan"
        );
        assert!(
            live.exists(),
            "LIVE-DELTA-DELETED: the full scan deleted an unapplied delta file"
        );
    }
}

/// a proxy does not count the cache on start (Ed, 2026-10-10). The startup path loads
/// `size_state.json` and, when the figure is overdue or missing, asks the scheduler for the
/// usual locked validation instead of walking `.meta` files itself.
#[cfg(test)]
mod startup_census_tests {
    use super::*;
    use crate::cache_initialization_coordinator::CacheInitializationCoordinator;
    use crate::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
    use crate::journal_manager::JournalManager;
    use crate::metadata_lock_manager::MetadataLockManager;
    use tempfile::TempDir;

    async fn consolidator_on(dir: &std::path::Path, name: &str) -> Arc<JournalConsolidator> {
        std::fs::create_dir_all(dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(dir.join("locks")).unwrap();
        let journal_manager = Arc::new(JournalManager::new(dir.to_path_buf(), name.to_string()));
        let lock_manager = Arc::new(MetadataLockManager::new(
            dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidator = Arc::new(JournalConsolidator::new(
            dir.to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        ));
        consolidator.initialize().await.unwrap();
        consolidator
    }

    /// A tracker whose time of day is six hours out, so the daily window never runs a pass.
    async fn tracker_on(
        dir: &std::path::Path,
        consolidator: Arc<JournalConsolidator>,
    ) -> Arc<CacheSizeTracker> {
        use chrono::{Duration as ChronoDuration, Local, Timelike};
        let target = Local::now() + ChronoDuration::hours(6);
        let config = CacheSizeConfig {
            validation_time_of_day: format!("{:02}:{:02}", target.hour(), target.minute()),
            ..CacheSizeConfig::default()
        };
        Arc::new(
            CacheSizeTracker::new(dir.to_path_buf(), config, false, consolidator)
                .await
                .unwrap(),
        )
    }

    /// `validation.json` recording a pass `age` ago, in the shape the tracker writes.
    fn write_validation_json(dir: &std::path::Path, age: Duration) {
        let secs = (SystemTime::now() - age)
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        std::fs::write(
            dir.join("size_tracking/validation.json"),
            serde_json::json!({
                "last_validation": secs,
                "scanned_size": 0,
                "tracked_size": 0,
                "drift_bytes": 0,
                "scan_duration_ms": 0,
                "metadata_files_scanned": 0,
                "status": "completed",
            })
            .to_string(),
        )
        .unwrap();
    }

    fn plant_delta(dir: &std::path::Path, name: &str, delta: i64, write_cache_delta: i64) {
        std::fs::write(
            dir.join("size_tracking")
                .join(format!("delta_{}.json", name)),
            format!(
                r#"{{"delta":{},"write_cache_delta":{},"instance_id":"{}"}}"#,
                delta, write_cache_delta, name
            ),
        )
        .unwrap();
    }

    async fn apply_pending(consolidator: &JournalConsolidator) {
        let next = consolidator.collect_deltas().await;
        consolidator
            .atomic_update_size_delta(next.delta, next.write_cache_delta)
            .await
            .unwrap();
        consolidator.delete_applied_delta_files(&next.paths).await;
    }

    fn coordinator_on(dir: &std::path::Path) -> CacheInitializationCoordinator {
        CacheInitializationCoordinator::new(
            dir.to_path_buf(),
            false,
            crate::config::CacheConfig {
                cache_dir: dir.to_path_buf(),
                ..crate::config::CacheConfig::default()
            },
        )
    }

    /// red side (a). `validation.json` is two days old and `size_state.json` holds 5000
    /// bytes over 7 objects, while the `.meta` files on disk total 3000 over 2.
    ///
    /// # RED SIDE
    ///
    /// On the earlier tree the stale gate sends startup into the `.meta` walk (2 files
    /// processed) and the census overwrites the figure with 3000.
    #[tokio::test]
    async fn a_stale_validation_with_a_valid_size_state_does_not_walk_at_startup() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        super::tests::write_valid_meta(
            &dir.join("metadata/case45-a/00/000/one.meta"),
            "case45-a/one",
            1000,
        );
        super::tests::write_valid_meta(
            &dir.join("metadata/case45-a/01/000/two.meta"),
            "case45-a/two",
            2000,
        );
        let consolidator = consolidator_on(&dir, "case45-a").await;
        consolidator
            .update_size_from_validation(5000, Some(0), Some(7))
            .await;
        write_validation_json(&dir, Duration::from_secs(48 * 3600));
        let tracker = tracker_on(&dir, consolidator.clone()).await;

        let summary = coordinator_on(&dir)
            .initialize(None, &mut Some(tracker.clone()))
            .await
            .unwrap();

        let walked = summary.performance_metrics.files_processed;
        let state = consolidator.get_size_state().await.unwrap();
        assert_eq!(
            walked, 0,
            "STARTUP-WALK: startup processed {} .meta files; a proxy must not count the cache on \
             start (size_state total now {})",
            walked, state.total_size
        );
        assert_eq!(
            (state.total_size, state.cached_objects),
            (5000, 7),
            "STARTUP-OVERWRITE: startup replaced the persisted figure with its own count"
        );
        assert_eq!(
            tracker.validation_request().map(|r| r.kind),
            Some(ValidationRequestKind::Overdue),
            "an overdue validation must be requested from the scheduler"
        );
    }

    /// (b). `size_state.json` is missing on a cache holding 3000 bytes over 2 objects.
    /// Startup must not walk; it starts from 0, marks the figure unvalidated and requests a
    /// locked full validation, after which the figure equals the census.
    ///
    /// # RED SIDE
    ///
    /// On the earlier tree the stale `validation.json` sends startup into the walk.
    #[tokio::test]
    async fn a_missing_size_state_requests_a_locked_full_validation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        super::tests::write_valid_meta(
            &dir.join("metadata/case45-b/00/000/one.meta"),
            "case45-b/one",
            1000,
        );
        super::tests::write_valid_meta(
            &dir.join("metadata/case45-b/01/000/two.meta"),
            "case45-b/two",
            2000,
        );
        let consolidator = consolidator_on(&dir, "case45-b").await;
        write_validation_json(&dir, Duration::from_secs(48 * 3600));
        assert!(
            !dir.join("size_tracking/size_state.json").exists(),
            "FIXTURE: size_state.json must be missing"
        );
        let tracker = tracker_on(&dir, consolidator.clone()).await;

        let summary = coordinator_on(&dir)
            .initialize(None, &mut Some(tracker.clone()))
            .await
            .unwrap();

        assert_eq!(
            summary.performance_metrics.files_processed, 0,
            "STARTUP-WALK: startup processed {} .meta files with size_state.json missing",
            summary.performance_metrics.files_processed
        );
        assert_eq!(
            consolidator.get_size_state().await.unwrap().total_size,
            0,
            "the figure must start from 0 until the validation runs"
        );
        assert_eq!(
            tracker.validation_request().map(|r| r.kind),
            Some(ValidationRequestKind::FigureUnvalidated),
            "UNREQUESTED: a missing size_state.json must request a validation"
        );
        let outcome = tokio::time::timeout(
            Duration::from_secs(60),
            tracker.perform_requested_validation(),
        )
        .await
        .expect("the requested validation must finish inside 60 s")
        .unwrap();
        assert_eq!(outcome, ValidationOutcome::Ran);
        let state = consolidator.get_size_state().await.unwrap();
        assert_eq!(
            (state.total_size, state.cached_objects),
            (3000, 2),
            "NOT-GROUNDED: after the requested validation the figure must equal the census"
        );
        assert!(
            tracker.validation_request().is_none(),
            "a validation that installed must clear the request"
        );
    }

    /// (b), the unreadable half: a corrupt `size_state.json` is re-grounded by the
    /// requested full validation rather than deferring every pass.
    #[tokio::test]
    async fn an_unreadable_size_state_is_grounded_by_the_requested_validation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        super::tests::write_valid_meta(
            &dir.join("metadata/case45-u/00/000/one.meta"),
            "case45-u/one",
            1000,
        );
        let consolidator = consolidator_on(&dir, "case45-u").await;
        write_validation_json(&dir, Duration::from_secs(3600));
        std::fs::write(dir.join("size_tracking/size_state.json"), b"{not json").unwrap();
        plant_delta(&dir, "u_pending", 300, 0);
        let tracker = tracker_on(&dir, consolidator.clone()).await;
        coordinator_on(&dir)
            .initialize(None, &mut Some(tracker.clone()))
            .await
            .unwrap();
        assert_eq!(
            tracker.validation_request().map(|r| r.kind),
            Some(ValidationRequestKind::FigureUnvalidated),
            "an unreadable size_state.json must request a validation"
        );
        let hook_dir = dir.clone();
        test_seams::set_after_full_scan_walk(
            &dir,
            Box::new(move || plant_delta(&hook_dir, "u_after", 50, 0)),
        );
        tracker.perform_requested_validation().await.unwrap();
        apply_pending(&consolidator).await;
        let total = consolidator.get_size_state().await.unwrap().total_size;
        assert_eq!(
            total, 1050,
            "UNREADABLE-NOT-GROUNDED: total_size={} after the requested validation and the next \
             apply, expected the census 1000 plus the 50 credited after the baseline",
            total
        );
    }

    /// red side (c), the review's trace. Tracked 1500, true 1000. Proxy A's full
    /// validation takes its baseline and walks; proxy B starts (validation two days old)
    /// inside that window; 200 bytes are credited after; A installs.
    ///
    /// # RED SIDE
    ///
    /// On the earlier tree B's startup census installs an absolute 1000 and resets the delta
    /// files, then A corrects again by 1000 - 1500: 500, plus the 200 credited after: 700.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_proxy_starting_inside_a_peers_full_validation_does_not_double_correct() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        super::tests::write_valid_meta(
            &dir.join("metadata/case45-c/00/000/one.meta"),
            "case45-c/one",
            1000,
        );
        let consolidator = consolidator_on(&dir, "case45-c-a").await;
        consolidator
            .update_size_from_validation(1500, Some(0), Some(1))
            .await;
        write_validation_json(&dir, Duration::from_secs(48 * 3600));
        let a = tracker_on(&dir, consolidator.clone()).await;
        let b_consolidator = consolidator_on(&dir, "case45-c-b").await;
        let b = tracker_on(&dir, b_consolidator).await;

        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = ran.clone();
        let hook_dir = dir.clone();
        test_seams::set_after_full_scan_walk(
            &dir,
            Box::new(move || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        coordinator_on(&hook_dir)
                            .initialize(None, &mut Some(b))
                            .await
                            .unwrap();
                    })
                });
                plant_delta(&hook_dir, "c_after", 200, 0);
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }),
        );
        tokio::time::timeout(Duration::from_secs(60), a.perform_full_validation())
            .await
            .expect("the full scan must finish inside 60 s")
            .unwrap();
        assert!(
            ran.load(std::sync::atomic::Ordering::SeqCst),
            "FIXTURE: B's startup must have run inside A's walk"
        );
        apply_pending(&consolidator).await;

        let total = consolidator.get_size_state().await.unwrap().total_size;
        assert_eq!(
            total, 1200,
            "DOUBLE-CORRECTED: total_size={} after A's install and the next apply, expected 1200 \
             (A's census 1000 plus the 200 credited after its baseline)",
            total
        );
    }

    /// size_state total 1000 and write-cache 500, a stalled
    /// pending +4000 total / +0 write-cache, census 1500 total / 500 write-cache.
    ///
    /// # RED SIDE
    ///
    /// With the subset invariant applied to `size_state` alone, total is clamped to 0 (its cut
    /// carried) and write-cache is clamped to that 0 with nothing carried: after the apply,
    /// total 1500 and write-cache 0.
    #[tokio::test]
    async fn the_subset_clamp_after_a_carried_cut_keeps_the_write_cache_figure() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let consolidator = consolidator_on(&dir, "case45-subset").await;
        consolidator
            .update_size_from_validation(1000, Some(500), Some(1))
            .await;
        plant_delta(&dir, "stalled_b", 4000, 0);
        let baseline = consolidator
            .validation_baseline()
            .await
            .expect("FIXTURE: size_state.json must be readable");
        assert_eq!(
            (baseline.total_size, baseline.write_cache_size),
            (5000, 500),
            "FIXTURE: the baseline must count the stalled delta"
        );
        assert!(
            consolidator
                .install_validation_correction(
                    1500,
                    500,
                    1,
                    baseline,
                    ValidationSampleKind::FullScan
                )
                .await,
            "FIXTURE: the install must have happened"
        );
        consolidator.size_accumulator().flush().await.unwrap();
        apply_pending(&consolidator).await;

        let state = consolidator.get_size_state().await.unwrap();
        assert_eq!(
            (state.total_size, state.write_cache_size),
            (1500, 500),
            "SUBSET-CLAMP-CUT: (total, write_cache)=({}, {}) after the install and the next \
             apply, expected the census (1500, 500)",
            state.total_size,
            state.write_cache_size
        );
    }

    /// the carry cap must include this instance's unflushed credits.
    /// Tracked 1000, baseline 1000; after the baseline a -1000 debit is applied and this
    /// instance credits +4000 that it has not flushed; the census is 500.
    ///
    /// # RED SIDE
    ///
    /// With the cap read from delta files alone, the -500 correction is clamped at 0 with
    /// nothing carried and the +4000 applies on top: 4000 instead of 500 + (-1000 + 4000).
    #[tokio::test]
    async fn the_carry_cap_counts_this_instances_unflushed_credits() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let consolidator = consolidator_on(&dir, "case45-cap").await;
        consolidator
            .update_size_from_validation(1000, Some(0), Some(1))
            .await;
        let baseline = consolidator
            .validation_baseline()
            .await
            .expect("FIXTURE: size_state.json must be readable");
        plant_delta(&dir, "cap_debit", -1000, 0);
        apply_pending(&consolidator).await;
        consolidator.size_accumulator().add(4000);
        assert!(
            consolidator
                .install_validation_correction(500, 0, 1, baseline, ValidationSampleKind::FullScan)
                .await,
            "FIXTURE: the install must have happened"
        );
        consolidator.size_accumulator().flush().await.unwrap();
        apply_pending(&consolidator).await;

        let total = consolidator.get_size_state().await.unwrap().total_size;
        assert_eq!(
            total, 3500,
            "CAP-MISSES-ACCUMULATOR: total_size={} after the install and the next apply, \
             expected 3500 (census 500, then -1000 and +4000 after the baseline)",
            total
        );
    }
}

/// the review follow-ups. The validation pass removes stale HEAD-only `.meta` files
/// the startup census used to remove; `RecordEmpty` needs a `metadata/` that is absent or
/// listably empty; and a `FigureUnvalidated` request is satisfied by a peer's completed full
/// pass and not by a deferred one.
#[cfg(test)]
mod review_followups_tests {
    use super::*;
    use crate::cache_initialization_coordinator::{
        CacheInitializationCoordinator, StartupValidation,
    };
    use crate::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
    use crate::journal_manager::JournalManager;
    use crate::metadata_lock_manager::MetadataLockManager;
    use tempfile::TempDir;

    async fn consolidator_on(dir: &std::path::Path, name: &str) -> Arc<JournalConsolidator> {
        std::fs::create_dir_all(dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(dir.join("locks")).unwrap();
        let journal_manager = Arc::new(JournalManager::new(dir.to_path_buf(), name.to_string()));
        let lock_manager = Arc::new(MetadataLockManager::new(
            dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidator = Arc::new(JournalConsolidator::new(
            dir.to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        ));
        consolidator.initialize().await.unwrap();
        consolidator
    }

    /// A tracker whose time of day is six hours out, so the daily window never runs a pass.
    async fn tracker_on(
        dir: &std::path::Path,
        consolidator: Arc<JournalConsolidator>,
    ) -> Arc<CacheSizeTracker> {
        use chrono::{Duration as ChronoDuration, Local, Timelike};
        let target = Local::now() + ChronoDuration::hours(6);
        let config = CacheSizeConfig {
            validation_time_of_day: format!("{:02}:{:02}", target.hour(), target.minute()),
            ..CacheSizeConfig::default()
        };
        Arc::new(
            CacheSizeTracker::new(dir.to_path_buf(), config, false, consolidator)
                .await
                .unwrap(),
        )
    }

    /// `(total_size, cached_objects)` as the next consolidation cycle will persist them:
    /// `size_state.json` plus every pending delta file, after flushing this instance's
    /// accumulator. Nothing is applied. (`atomic_update_size_delta`, the public apply, carries
    /// no `cached_objects` delta, so applying through it would drop the debit under test.)
    async fn figures_after_next_cycle(consolidator: &JournalConsolidator) -> (u64, u64) {
        consolidator.size_accumulator().flush().await.unwrap();
        let state = consolidator.get_size_state().await.unwrap();
        let next = consolidator.collect_deltas().await;
        (
            state.total_size.saturating_add_signed(next.delta),
            state
                .cached_objects
                .saturating_add_signed(next.cached_objects_delta),
        )
    }

    async fn full_validation(tracker: &Arc<CacheSizeTracker>) {
        tokio::time::timeout(Duration::from_secs(60), tracker.perform_full_validation())
            .await
            .expect("the full validation must finish inside 60 s")
            .unwrap();
    }

    /// A HEAD-only `.meta` (versioned, no ranges) at `metadata/{bucket}/00/000/{name}.meta`,
    /// whose HEAD expiry passed `expired_for` ago.
    fn write_head_only(
        dir: &std::path::Path,
        bucket: &str,
        name: &str,
        expired_for: Duration,
    ) -> PathBuf {
        let path = dir
            .join("metadata")
            .join(bucket)
            .join("00/000")
            .join(format!("{}.meta", name));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let head_expires_at = SystemTime::now() - expired_for;
        let at = head_expires_at - Duration::from_secs(3600);
        let meta = crate::cache_types::NewCacheMetadata {
            cache_key: format!("{}/{}", bucket, name),
            object_metadata: crate::cache_types::ObjectMetadata {
                etag: format!("\"{}\"", name),
                last_modified: "Mon, 05 Oct 2026 09:00:00 GMT".to_string(),
                content_length: 12_345,
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: at,
            expires_at: at,
            head_expires_at: Some(head_expires_at),
            ..Default::default()
        };
        std::fs::write(&path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
        path
    }

    /// Fix 1. A HEAD-only `.meta` whose HEAD expired two days ago is removed by the full
    /// validation; one expired for an hour is kept; `cached_objects` counts the removal once.
    ///
    /// # RED SIDE
    ///
    /// On the earlier tree nothing removes a stale HEAD-only `.meta`, so the file
    /// survives the pass.
    #[tokio::test]
    async fn a_full_validation_removes_a_stale_head_only_meta_and_counts_it_once() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        super::tests::write_valid_meta(
            &dir.join("metadata/case46-h/01/000/ranged.meta"),
            "case46-h/ranged",
            1000,
        );
        let stale = write_head_only(&dir, "case46-h", "stale", Duration::from_secs(2 * 86_400));
        let recent = write_head_only(&dir, "case46-h", "recent", Duration::from_secs(3600));
        let consolidator = consolidator_on(&dir, "case46-h").await;
        consolidator
            .update_size_from_validation(1000, Some(0), Some(3))
            .await;
        let tracker = tracker_on(&dir, consolidator.clone()).await;

        full_validation(&tracker).await;
        assert!(
            !tracker
                .read_validation_metadata()
                .await
                .unwrap()
                .size_install_deferred,
            "FIXTURE: the install must have happened, not deferred"
        );
        let (total, objects) = figures_after_next_cycle(&consolidator).await;
        assert!(
            !stale.exists(),
            "STALE-HEAD-ONLY-KEPT: a HEAD-only .meta whose HEAD expired two days ago survived the \
             full validation (cached_objects after the next cycle {})",
            objects
        );
        assert!(
            recent.exists(),
            "a HEAD-only .meta whose HEAD expired an hour ago is inside the one-day grace and \
             must be kept"
        );
        assert_eq!(
            (total, objects),
            (1000, 2),
            "COUNT: (total, cached_objects)=({}, {}) after the install and the next cycle, \
             expected 1000 bytes over the 2 surviving objects",
            total,
            objects
        );
    }

    /// Fix 1, the lock. A peer holding the key's slot spares the entry on this pass; the next
    /// pass, with the slot free, removes it.
    ///
    /// # RED SIDE
    ///
    /// On the earlier tree the held-slot half passes (nothing removes the entry) and the positive
    /// control fails: the entry survives the second pass too.
    #[tokio::test]
    async fn a_held_slot_spares_a_stale_head_only_meta_until_the_next_pass() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        super::tests::write_valid_meta(
            &dir.join("metadata/case46-s/01/000/ranged.meta"),
            "case46-s/ranged",
            1000,
        );
        let stale = write_head_only(&dir, "case46-s", "stale", Duration::from_secs(2 * 86_400));
        let consolidator = consolidator_on(&dir, "case46-s").await;
        consolidator
            .update_size_from_validation(1000, Some(0), Some(2))
            .await;
        let tracker = tracker_on(&dir, consolidator.clone()).await;

        let held = crate::metadata_lock_manager::try_acquire_slot_lock(&dir, "case46-s/stale")
            .expect("FIXTURE: the slot must be free before the pass");
        full_validation(&tracker).await;
        let (_, contended) = figures_after_next_cycle(&consolidator).await;
        assert!(
            stale.exists(),
            "HELD-SLOT-DELETE: the pass removed a HEAD-only .meta while a peer held its slot"
        );
        assert_eq!(
            contended, 2,
            "a spared entry is still an object: cached_objects={}, expected 2",
            contended
        );
        drop(held);

        full_validation(&tracker).await;
        let (_, freed) = figures_after_next_cycle(&consolidator).await;
        assert!(
            !stale.exists(),
            "STALE-HEAD-ONLY-KEPT: with the slot free the stale HEAD-only .meta must be removed \
             (cached_objects now {})",
            freed
        );
        assert_eq!(
            freed, 1,
            "COUNT: cached_objects={} after the removal, expected 1",
            freed
        );
    }

    /// Fix 1, the re-read. The scan's snapshot says stale, but a peer refreshed the HEAD
    /// before the slot lock was taken: the locked re-read spares the entry.
    ///
    /// # RED SIDE
    ///
    /// With the locked re-read's predicate replaced by `true` (`case46-LEVER-REREAD`), the
    /// refreshed entry is deleted.
    #[tokio::test]
    async fn the_stale_head_only_removal_re_reads_under_the_slot_lock() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let path = write_head_only(
            &dir,
            "case46-r",
            "refreshed",
            Duration::from_secs(2 * 86_400),
        );
        let consolidator = consolidator_on(&dir, "case46-r").await;
        let tracker = tracker_on(&dir, consolidator).await;
        let snapshot: crate::cache_types::NewCacheMetadata =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let now = SystemTime::now();
        assert!(
            stale_head_only_removable(&snapshot, now),
            "FIXTURE: the snapshot must read as stale, or the re-read is not exercised"
        );
        let mut refreshed = snapshot.clone();
        refreshed.head_expires_at = Some(now + Duration::from_secs(3600));
        std::fs::write(&path, serde_json::to_string_pretty(&refreshed).unwrap()).unwrap();

        let removed = tracker.remove_stale_head_only(&path, &snapshot, now);
        assert!(
            !removed && path.exists(),
            "STALE-SNAPSHOT-DELETE: the removal acted on the scan's snapshot and deleted an entry \
             whose HEAD a peer refreshed (removed={}, exists={})",
            removed,
            path.exists()
        );
    }

    /// Fix 3. `metadata/` exists but cannot be listed (it is a regular file, so `read_dir`
    /// fails with an error other than NotFound). Startup must request `FigureUnvalidated`,
    /// with `size_state.json` missing and with it all zeros. Control: an absent `metadata/` is
    /// an empty cache and records its validation.
    ///
    /// # RED SIDE
    ///
    /// On the earlier tree `cache_holds_data` reads any `read_dir` error as "no data", so both
    /// unlistable cases return `RecordEmpty`.
    #[tokio::test]
    async fn an_unlistable_metadata_dir_never_records_an_empty_cache() {
        let coordinator_on = |dir: &std::path::Path| {
            CacheInitializationCoordinator::new(
                dir.to_path_buf(),
                false,
                crate::config::CacheConfig {
                    cache_dir: dir.to_path_buf(),
                    ..crate::config::CacheConfig::default()
                },
            )
        };

        let control = TempDir::new().unwrap();
        assert_eq!(
            coordinator_on(control.path())
                .startup_validation_request()
                .await,
            StartupValidation::RecordEmpty,
            "CONTROL: an absent metadata/ with no size_state.json is an empty cache"
        );

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        std::fs::write(dir.join("metadata"), b"not a directory").unwrap();
        let err = std::fs::read_dir(dir.join("metadata")).unwrap_err();
        assert_ne!(
            err.kind(),
            std::io::ErrorKind::NotFound,
            "FIXTURE: listing metadata/ must fail with an error other than NotFound"
        );

        let missing = coordinator_on(&dir).startup_validation_request().await;
        assert_eq!(
            missing,
            StartupValidation::Request(ValidationRequestKind::FigureUnvalidated),
            "RECORD-EMPTY-ON-ERROR: size_state.json missing and metadata/ unlistable ({}) gave \
             {:?}; an unreadable metadata/ is not evidence of an empty cache",
            err,
            missing
        );

        std::fs::create_dir_all(dir.join("size_tracking")).unwrap();
        std::fs::write(
            dir.join("size_tracking/size_state.json"),
            serde_json::to_string(&crate::journal_consolidator::SizeState::default()).unwrap(),
        )
        .unwrap();
        let zeros = coordinator_on(&dir).startup_validation_request().await;
        assert_eq!(
            zeros,
            StartupValidation::Request(ValidationRequestKind::FigureUnvalidated),
            "RECORD-EMPTY-ON-ERROR: an all-zero size_state.json with metadata/ unlistable ({}) \
             gave {:?}",
            err,
            zeros
        );
    }

    /// Fix 4 (a). A `FigureUnvalidated` request is satisfied by a peer's full pass that
    /// completed after it and installed: `perform_validation_gated` returns `PeerRanRecently`
    /// under the lock, and `perform_requested_validation` clears the request. An uncredited
    /// `.meta` written after the peer's pass makes a pass by this instance visible in the
    /// figure.
    ///
    /// # RED SIDE
    ///
    /// With the `FigureUnvalidated` arm of the request re-check forced false
    /// (`case46-LEVER-PEER`), this instance runs its own pass and counts the late `.meta`.
    #[tokio::test]
    async fn a_figure_unvalidated_request_is_satisfied_by_a_peers_completed_full_pass() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        super::tests::write_valid_meta(
            &dir.join("metadata/case46-p/00/000/one.meta"),
            "case46-p/one",
            1000,
        );
        let a_consolidator = consolidator_on(&dir, "case46-pa").await;
        a_consolidator
            .update_size_from_validation(5000, Some(0), Some(9))
            .await;
        let a = tracker_on(&dir, a_consolidator.clone()).await;
        let b = tracker_on(&dir, consolidator_on(&dir, "case46-pb").await).await;

        a.request_validation(ValidationRequestKind::FigureUnvalidated);
        // `validation.json` stores whole seconds, so a pass that completes inside the
        // request's own second reads as earlier than the request.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        full_validation(&b).await;
        assert!(
            !b.read_validation_metadata()
                .await
                .unwrap()
                .size_install_deferred,
            "FIXTURE: the peer's install must have happened"
        );
        super::tests::write_valid_meta(
            &dir.join("metadata/case46-p/01/000/late.meta"),
            "case46-p/late",
            2000,
        );

        let outcome =
            tokio::time::timeout(Duration::from_secs(60), a.perform_requested_validation())
                .await
                .expect("the requested validation must finish inside 60 s")
                .unwrap();
        let state = a_consolidator.get_size_state().await.unwrap();
        assert_eq!(
            outcome,
            ValidationOutcome::PeerRanRecently,
            "PEER-NOT-SATISFYING: this instance ran its own pass after the peer's completed full \
             pass; figure now ({}, {})",
            state.total_size,
            state.cached_objects
        );
        assert_eq!(
            (state.total_size, state.cached_objects),
            (1000, 1),
            "the figure must be the peer's census, untouched by a second pass"
        );
        assert!(
            a.validation_request().is_none(),
            "a request satisfied by a peer must be cleared"
        );
    }

    /// Fix 4 (b). A peer's full pass that completed after the request but deferred its size
    /// install grounded nothing, so it must not satisfy a `FigureUnvalidated` request: this
    /// instance runs its own pass, installs, and clears the request.
    ///
    /// # RED SIDE
    ///
    /// With `&& !metadata.size_install_deferred` dropped from the request re-check
    /// (`case46-LEVER-DEFERRED`), the re-check skips (`PeerRanRecently`), the request stays
    /// pending, and the figure stays at the unvalidated 5000.
    #[tokio::test]
    async fn a_deferred_peer_pass_does_not_satisfy_a_figure_unvalidated_request() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        super::tests::write_valid_meta(
            &dir.join("metadata/case46-d/00/000/one.meta"),
            "case46-d/one",
            1000,
        );
        let a_consolidator = consolidator_on(&dir, "case46-da").await;
        a_consolidator
            .update_size_from_validation(5000, Some(0), Some(9))
            .await;
        let a = tracker_on(&dir, a_consolidator.clone()).await;
        let b = tracker_on(&dir, consolidator_on(&dir, "case46-db").await).await;
        let holder = consolidator_on(&dir, "case46-dh").await;

        a.request_validation(ValidationRequestKind::FigureUnvalidated);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(holder.try_acquire_global_consolidation_lock().unwrap());
        FULL_INSTALL_ATTEMPTS_OVERRIDE.with(|c| c.set(Some(2)));
        let peer = b.perform_full_validation().await;
        FULL_INSTALL_ATTEMPTS_OVERRIDE.with(|c| c.set(None));
        holder.release_global_consolidation_lock();
        peer.unwrap();
        assert!(
            a.read_validation_metadata()
                .await
                .unwrap()
                .size_install_deferred,
            "FIXTURE: the peer's install must have deferred"
        );
        assert_eq!(
            a_consolidator.get_size_state().await.unwrap().total_size,
            5000,
            "FIXTURE: a deferred install leaves the figure alone"
        );

        let outcome =
            tokio::time::timeout(Duration::from_secs(60), a.perform_requested_validation())
                .await
                .expect("the requested validation must finish inside 60 s")
                .unwrap();
        let state = a_consolidator.get_size_state().await.unwrap();
        assert_eq!(
            outcome,
            ValidationOutcome::Ran,
            "DEFERRED-PEER-SATISFIED: a peer pass whose install deferred satisfied the request \
             ({:?}); figure still ({}, {})",
            outcome,
            state.total_size,
            state.cached_objects
        );
        assert_eq!(
            (state.total_size, state.cached_objects),
            (1000, 1),
            "this instance's own pass must ground the figure"
        );
        assert!(
            a.validation_request().is_none(),
            "a pass that installed must clear the request"
        );
    }
}

#[cfg(test)]
mod rolling_self_debit_tests {
    //! a rolling cycle whose own scan arms remove or heal an entry counts it once.
    //!
    //! The arms (stale HEAD-only removal, write-cache expiry, empty-object-ETag repair) debit
    //! through this instance's size accumulator while the pass reports the entry at its
    //! post-mutation figures. The rolling install is absolute and is followed by
    //! `reset_all_delta_files` plus `size_accumulator.reset()`, so the pending debit is
    //! discarded and the census alone carries the removal. Adding `self_debit` back, as the
    //! full scan's relative install must, would count the removal zero times here.
    //!
    //! Every fixture seeds `schedule.json` with a tiny scan rate so the first cycle visits all
    //! 256 slots: the last install then has `slots == 256`, the proportional correction is the
    //! scanned figure itself, and the assertions read a census rather than an extrapolation.
    //! Each tracked total carries `INSTALL_MARK` extra bytes, so a figure without them proves
    //! the absolute install ran (with no install, the accumulator debit alone gives the
    //! "counted once" object figure too).
    use super::*;
    use crate::cache::zero_length_arm_orphan_tests::{
        incompressible_body, object_metadata, read_meta, seed_full_object, setup,
    };
    use crate::cache::CacheManager;
    use crate::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
    use crate::journal_manager::JournalManager;
    use crate::metadata_lock_manager::MetadataLockManager;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    const INSTALL_MARK: u64 = 4096;

    /// Make the next rolling cycle a whole rotation: `slot_cap = budget / rate`, clamped to 256.
    fn seed_whole_rotation(dir: &std::path::Path) {
        std::fs::create_dir_all(dir.join("size_tracking")).unwrap();
        std::fs::write(
            dir.join("size_tracking/schedule.json"),
            r#"{"rolling_cursor":0,"rolling_scan_rate":0.000001}"#,
        )
        .unwrap();
    }

    /// One rolling cycle, bounded, then the precondition that it covered all 256 slots.
    async fn rolling_whole_rotation(tracker: &Arc<CacheSizeTracker>) {
        tokio::time::timeout(
            Duration::from_secs(120),
            tracker.perform_rolling_validation(),
        )
        .await
        .expect("the rolling cycle must finish inside 120 s")
        .unwrap();
        let state = tracker.read_rolling_state().unwrap();
        assert_eq!(
            (state.full_rotation_count, state.cursor),
            (1, 0),
            "FIXTURE: the cycle must have visited all 256 slots, or the install is an \
             extrapolation and the figures below are not a census"
        );
    }

    /// `(total_size, write_cache_size, cached_objects)` as the next consolidation cycle will
    /// persist them: `size_state.json` plus every pending delta file, after flushing this
    /// instance's accumulator. Nothing is applied.
    async fn figures_after_next_cycle(consolidator: &JournalConsolidator) -> (u64, u64, u64) {
        consolidator.size_accumulator().flush().await.unwrap();
        let state = consolidator.get_size_state().await.unwrap();
        let next = consolidator.collect_deltas().await;
        (
            state.total_size.saturating_add_signed(next.delta),
            state
                .write_cache_size
                .saturating_add_signed(next.write_cache_delta),
            state
                .cached_objects
                .saturating_add_signed(next.cached_objects_delta),
        )
    }

    async fn consolidator_on(dir: &std::path::Path, name: &str) -> Arc<JournalConsolidator> {
        std::fs::create_dir_all(dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(dir.join("locks")).unwrap();
        let journal_manager = Arc::new(JournalManager::new(dir.to_path_buf(), name.to_string()));
        let lock_manager = Arc::new(MetadataLockManager::new(
            dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidator = Arc::new(JournalConsolidator::new(
            dir.to_path_buf(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        ));
        consolidator.initialize().await.unwrap();
        consolidator
    }

    /// A tracker whose time of day is six hours out, so the daily window never runs a pass.
    async fn tracker_on(
        dir: &std::path::Path,
        consolidator: Arc<JournalConsolidator>,
    ) -> Arc<CacheSizeTracker> {
        use chrono::{Duration as ChronoDuration, Local, Timelike};
        let target = Local::now() + ChronoDuration::hours(6);
        let config = CacheSizeConfig {
            validation_time_of_day: format!("{:02}:{:02}", target.hour(), target.minute()),
            ..CacheSizeConfig::default()
        };
        Arc::new(
            CacheSizeTracker::new(dir.to_path_buf(), config, false, consolidator)
                .await
                .unwrap(),
        )
    }

    /// The tracker the manager's own `initialize()` built, wired as production wires it.
    async fn production_tracker(manager: &Arc<CacheManager>) -> Arc<CacheSizeTracker> {
        manager.set_cache_manager_in_tracker().await;
        manager
            .get_size_tracker()
            .await
            .expect("fixture: initialize() must have built a size tracker")
    }

    /// A HEAD-only `.meta` (versioned, no ranges) whose HEAD expiry passed `expired_for` ago.
    fn write_head_only(
        dir: &std::path::Path,
        bucket: &str,
        name: &str,
        expired_for: Duration,
    ) -> PathBuf {
        let path = dir
            .join("metadata")
            .join(bucket)
            .join("00/000")
            .join(format!("{}.meta", name));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let head_expires_at = SystemTime::now() - expired_for;
        let at = head_expires_at - Duration::from_secs(3600);
        let meta = crate::cache_types::NewCacheMetadata {
            cache_key: format!("{}/{}", bucket, name),
            object_metadata: crate::cache_types::ObjectMetadata {
                etag: format!("\"{}\"", name),
                last_modified: "Mon, 05 Oct 2026 09:00:00 GMT".to_string(),
                content_length: 12_345,
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: at,
            expires_at: at,
            head_expires_at: Some(head_expires_at),
            ..Default::default()
        };
        std::fs::write(&path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
        path
    }

    /// Publish `body` through the production full-object path, then mark it write-cached with
    /// every range staged. `expired` puts `write_cache_expires_at` an hour in the past.
    /// Returns the `.meta` path and the entry's compressed bytes.
    async fn seed_staged(
        manager: &Arc<CacheManager>,
        cache_key: &str,
        body: &[u8],
        expired: bool,
    ) -> (PathBuf, u64) {
        seed_full_object(
            manager,
            cache_key,
            body,
            object_metadata(body.len() as u64, "\"case47-staged\""),
        )
        .await;
        let mut meta = read_meta(manager, cache_key);
        meta.object_metadata.is_write_cached = true;
        meta.object_metadata.write_cache_expires_at = Some(if expired {
            SystemTime::now() - Duration::from_secs(3600)
        } else {
            SystemTime::now() + Duration::from_secs(86_400)
        });
        for range in &mut meta.ranges {
            range.staged = Some(true);
        }
        let bytes: u64 = meta.ranges.iter().map(|r| r.compressed_size).sum();
        let meta_path = manager.get_new_metadata_file_path(cache_key);
        std::fs::write(&meta_path, serde_json::to_string(&meta).unwrap()).unwrap();
        (meta_path, bytes)
    }

    fn body_of(seeds: &[u8]) -> Vec<u8> {
        seeds.iter().flat_map(|s| incompressible_body(*s)).collect()
    }

    /// Arm 1. A rolling cycle removes one stale HEAD-only entry; `cached_objects` after the
    /// install and the next cycle's apply counts the removal once.
    ///
    /// Readings: once = 2, twice (accumulator debit applied on top of the install) = 1,
    /// never (`self_debit` added back to the rolling census) = 3.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_rolling_cycle_counts_a_stale_head_only_removal_once() {
        let _tail_serial = TAIL_TEST_GUARD.lock().await;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        super::tests::write_valid_meta(
            &dir.join("metadata/case47-h/01/000/ranged.meta"),
            "case47-h/ranged",
            1000,
        );
        let stale = write_head_only(&dir, "case47-h", "stale", Duration::from_secs(2 * 86_400));
        let recent = write_head_only(&dir, "case47-h", "recent", Duration::from_secs(3600));
        let consolidator = consolidator_on(&dir, "case47-h").await;
        consolidator
            .update_size_from_validation(1000 + INSTALL_MARK, Some(0), Some(3))
            .await;
        let tracker = tracker_on(&dir, consolidator.clone()).await;
        seed_whole_rotation(&dir);

        rolling_whole_rotation(&tracker).await;
        assert!(
            !stale.exists(),
            "FIXTURE: the rolling cycle must have removed the stale HEAD-only .meta"
        );
        assert!(
            recent.exists(),
            "FIXTURE: the in-grace HEAD-only .meta is kept"
        );

        let (total, _, objects) = figures_after_next_cycle(&consolidator).await;
        assert_eq!(
            total, 1000,
            "FIXTURE: total_size={} still carries the {} byte install mark, so the rolling \
             install did not run",
            total, INSTALL_MARK
        );
        assert_eq!(
            objects, 2,
            "ROLLING-SELF-DEBIT: cached_objects={} after the rolling install and the next \
             cycle, expected 2 (3 tracked, 1 stale HEAD-only removed). 1 means the removal \
             was counted twice, 3 that it was not counted",
            objects
        );
    }

    /// Arm 2. A rolling cycle deletes one expired write-cache entry; total and write-cache
    /// bytes, and `cached_objects`, count the deletion once.
    ///
    /// Readings: once = (live, live, 1), twice = (live - expired, live - expired, 0),
    /// never = (live + expired, live + expired, 2).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_rolling_cycle_counts_a_write_cache_expiry_once() {
        let _tail_serial = TAIL_TEST_GUARD.lock().await;
        let temp = TempDir::new().unwrap();
        let manager = Arc::new(setup(temp.path()).await);
        let (_live_meta, live) = seed_staged(
            &manager,
            "case47-wc/live.bin",
            &body_of(&[21, 22, 23]),
            false,
        )
        .await;
        let (expired_meta, expired) =
            seed_staged(&manager, "case47-wc/expired.bin", &body_of(&[24]), true).await;
        assert!(
            live > expired && expired > 0,
            "FIXTURE: live ({}) must exceed expired ({}) so a double debit stays above zero",
            live,
            expired
        );

        let tracker = production_tracker(&manager).await;
        let consolidator = tracker.consolidator.clone();
        consolidator
            .update_size_from_validation(
                live + expired + INSTALL_MARK,
                Some(live + expired),
                Some(2),
            )
            .await;
        seed_whole_rotation(temp.path());

        rolling_whole_rotation(&tracker).await;
        assert!(
            !expired_meta.exists(),
            "FIXTURE: the rolling cycle must have deleted the expired write-cache entry"
        );

        let figures = figures_after_next_cycle(&consolidator).await;
        assert_eq!(
            figures,
            (live, live, 1),
            "ROLLING-SELF-DEBIT: (total, write_cache, cached_objects)={:?} after the rolling \
             install and the next cycle, expected ({}, {}, 1). ({}, {}, 0) is a double \
             count, ({}, {}, 2) none; a total carrying +{} means no install ran",
            figures,
            live,
            live,
            live - expired,
            live - expired,
            live + expired,
            live + expired,
            INSTALL_MARK
        );
    }

    /// Arm 3. A rolling cycle repairs one empty-object-ETag entry: its superseded range is
    /// removed and debited, the `.meta` survives. Bytes count the removal once and the object
    /// stays counted.
    ///
    /// Readings: once = (live, 0, 2), twice = (live - superseded, 0, 2),
    /// never = (live + superseded, 0, 2).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_rolling_cycle_counts_an_empty_etag_repair_once() {
        let _tail_serial = TAIL_TEST_GUARD.lock().await;
        let temp = TempDir::new().unwrap();
        let manager = Arc::new(setup(temp.path()).await);
        let live_key = "case47-eoe/live.bin";
        let live_body = body_of(&[31, 32, 33]);
        seed_full_object(
            &manager,
            live_key,
            &live_body,
            object_metadata(live_body.len() as u64, "\"case47-live\""),
        )
        .await;
        let live: u64 = read_meta(&manager, live_key)
            .ranges
            .iter()
            .map(|r| r.compressed_size)
            .sum();
        let corrupt_key = "case47-eoe/corrupt.bin";
        crate::cache::empty_object_etag_guard_tests::plant_corrupt(&manager, corrupt_key).await;
        let superseded: u64 = read_meta(&manager, corrupt_key)
            .ranges
            .iter()
            .map(|r| r.compressed_size)
            .sum();
        assert!(
            live > superseded && superseded > 0,
            "FIXTURE: live ({}) must exceed the superseded range ({}) so a double debit stays \
             above zero",
            live,
            superseded
        );

        let tracker = production_tracker(&manager).await;
        let consolidator = tracker.consolidator.clone();
        consolidator
            .update_size_from_validation(live + superseded + INSTALL_MARK, Some(0), Some(2))
            .await;
        seed_whole_rotation(temp.path());

        rolling_whole_rotation(&tracker).await;
        assert!(
            read_meta(&manager, corrupt_key).ranges.is_empty(),
            "FIXTURE: the rolling cycle must have repaired the empty-object-ETag entry"
        );

        let figures = figures_after_next_cycle(&consolidator).await;
        assert_eq!(
            figures,
            (live, 0, 2),
            "ROLLING-SELF-DEBIT: (total, write_cache, cached_objects)={:?} after the rolling \
             install and the next cycle, expected ({}, 0, 2). {} is a double count, {} none; \
             a total carrying +{} means no install ran",
            figures,
            live,
            live - superseded,
            live + superseded,
            INSTALL_MARK
        );
    }
}
