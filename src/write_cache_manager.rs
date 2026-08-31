//! Write Cache Manager Module
//!
//! Provides capacity management and eviction for write-through caching of PUT operations.
//! Write cache is limited to a configurable percentage of total disk cache and uses
//! the same eviction algorithm as the read cache (LRU or TinyLFU).
//!
//! # Requirements
//! - Requirement 6.1: Write cache capacity limited to percentage of disk cache
//! - Requirement 6.5: Eviction uses configured algorithm (LRU or TinyLFU)
//! - Requirement 4.2, 4.3: Incomplete upload eviction after TTL
//! - Requirement 9.1: Atomic CAS-loop reservation (no separate check + reserve)
//! - Requirement 9.2: Single entry point returns reservation handle or "no capacity"
//! - Requirement 9.3: Saturating release on drop (never underflows)
//! - Requirement 9.4: Concurrent releases produce correct final value
//! - Requirement 9.5: Rate-limited warn on underflow detection

use crate::cache::CacheEvictionAlgorithm;

use crate::{ProxyError, Result};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tracing::{debug, error, info, warn};

/// Rate-limit interval for underflow warnings (seconds).
/// At most one warning per this interval to avoid log spam.
const UNDERFLOW_WARN_INTERVAL_SECS: u64 = 60;

/// Global atomic timestamp (epoch seconds) for rate-limiting underflow warnings.
/// Shared across all `WriteReservation` drops within the process.
static LAST_UNDERFLOW_WARN: AtomicU64 = AtomicU64::new(0);

/// RAII handle representing a successful capacity reservation in the write cache.
///
/// Holds `size` bytes reserved against the shared `current_size` counter.
/// On drop, releases the reservation using saturating subtraction to prevent underflow.
/// If an underflow condition is detected (current < size before saturation), a
/// rate-limited `warn!` is emitted.
///
/// # Requirements
/// Implements Requirements 9.1, 9.3, 9.4, 9.5
pub struct WriteReservation {
    size: u64,
    current_size: Arc<AtomicU64>,
}

impl WriteReservation {
    /// Get the reserved size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Create a no-op reservation that does nothing on drop.
    ///
    /// Used as a fallback when `WriteCacheManager` is not initialized (e.g., during
    /// early startup or in tests that don't call `initialize()`). The reservation
    /// signals "proceed with the operation" without tracking capacity.
    pub fn noop() -> Self {
        Self {
            size: 0,
            current_size: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Drop for WriteReservation {
    fn drop(&mut self) {
        if self.size == 0 {
            return;
        }

        // Use fetch_update with saturating subtraction to prevent underflow.
        // This is atomic: the CAS loop ensures correctness under concurrent drops.
        let result =
            self.current_size
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    if current < self.size {
                        // Underflow detected — emit rate-limited warning
                        let now_secs = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let last = LAST_UNDERFLOW_WARN.load(Ordering::Relaxed);
                        if now_secs.saturating_sub(last) >= UNDERFLOW_WARN_INTERVAL_SECS
                            && LAST_UNDERFLOW_WARN
                                .compare_exchange(
                                    last,
                                    now_secs,
                                    Ordering::Relaxed,
                                    Ordering::Relaxed,
                                )
                                .is_ok()
                        {
                            warn!(
                                release_size = self.size,
                                current_size = current,
                                "Write cache capacity underflow detected: \
                             release_size ({}) > current_size ({}), saturating to 0",
                                self.size,
                                current
                            );
                        }
                        Some(0)
                    } else {
                        Some(current - self.size)
                    }
                });

        // fetch_update with Some(...) always succeeds eventually, but log if it somehow fails
        if let Err(val) = result {
            debug!(
                "WriteReservation drop: unexpected fetch_update failure, current_size={}",
                val
            );
        }
    }
}

/// Write cache manager for capacity tracking and eviction
///
/// Manages write cache capacity separately from read cache, ensuring write operations
/// don't starve the read cache. Uses the same eviction algorithm as read cache for
/// consistency.
///
/// # Design Notes
/// - All capacity tracking uses compressed size (actual disk usage)
/// - Eviction is triggered when capacity would be exceeded
/// - Incomplete multipart uploads are evicted after TTL expiration
/// - Capacity reservation uses a single atomic CAS-loop entry point (`try_reserve`)
///   that returns an RAII `WriteReservation` handle (Requirement 9.1, 9.2)
pub struct WriteCacheManager {
    /// Maximum write cache size in bytes (calculated from percentage of total cache)
    max_size: u64,

    /// Current write cache usage in bytes (compressed size on disk).
    /// Shared with `WriteReservation` handles via `Arc`.
    current_size: Arc<AtomicU64>,

    /// Live (best-effort) count of staged (write-cached, ungraduated) entries.
    /// Incremented on a successful write-cache commit and decremented on
    /// graduation (first read) or eviction. This is an approximate gauge for
    /// observability (`/metrics` `write_cache.staged_entries`) — it is not the
    /// accounting authority and is not itself journaled; later tasks (R1
    /// graduation accounting, R5 eviction accounting) make the underlying
    /// tier transitions precise, at which point this counter's inputs become
    /// precise too.
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.2, 8.3
    staged_entries: Arc<AtomicU64>,

    /// Cumulative count of objects evicted from the write/staging tier by
    /// `evict_write_cached_object`. Deliberately separate from the read
    /// cache's `cache.evictions` counter (Requirement 8.4) — before this,
    /// `cache.evictions` reported 0 regardless of how much staging eviction
    /// had run, which read as "no eviction happened" when the write tier's
    /// own eviction path was in fact deleting entries.
    ///
    /// Not itself the accounting authority (that is Size_State, updated via
    /// the journal per R5); this is an observability counter only.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.4
    staging_evictions_total: Arc<AtomicU64>,

    /// Cumulative compressed bytes freed by staging eviction
    /// (`evict_write_cached_object`'s `total_freed` per call, summed).
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.4
    staging_eviction_bytes_total: Arc<AtomicU64>,

    /// Cumulative count of entries that graduated out of the staging tier on their
    /// first read, counted per instance at the point the graduation is journaled.
    ///
    /// Pairs with `staged_entries` to make "the tier is draining" observable: a
    /// `staged_entries` gauge sitting flat is ambiguous between an idle proxy and a
    /// broken graduation path, and this counter separates them. It is the natural
    /// companion to `staging_evictions_total` — together they account for the two ways
    /// an entry can leave the tier, one of which reclaims disk and one of which does not.
    ///
    /// Counts graduations this instance performed, so it is **not** a fleet-wide total
    /// and is not the accounting authority (that is Size_State's `write_cache_size`,
    /// moved by the `Graduation` journal entry under the consolidation lock). Two
    /// proxies racing the same key can both count here while only one decrement is
    /// applied; that is expected and is why this is an observability counter.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.3
    graduations_total: Arc<AtomicU64>,

    /// Write cache TTL (default: 1 day), refreshed on read access
    write_ttl: Duration,

    /// Incomplete upload TTL (default: 1 day)
    incomplete_upload_ttl: Duration,

    /// Eviction algorithm (same as read cache: LRU or TinyLFU)
    eviction_algorithm: CacheEvictionAlgorithm,

    /// Cache directory for file operations
    cache_dir: PathBuf,

    /// Maximum object size for write caching (objects larger than this bypass cache)
    max_object_size: u64,

    /// Journal consolidator, wired after construction via
    /// [`Self::set_journal_consolidator`] (the consolidator does not exist yet when
    /// this struct is built in `CacheManager::initialize`).
    ///
    /// Staging eviction needs it for two separate things, and conflating them is the
    /// mistake R5 exists to fix:
    ///
    /// - the **size accumulator**, which is the sole authority for
    ///   `SizeState::{total_size, write_cache_size}`. Eviction debits it and the
    ///   consolidator folds the debit into Size_State under the global lock. Nothing
    ///   here writes `size_state.json` directly.
    /// - the **Remove journal entries**, which are metadata convergence only: they
    ///   prune the evicted ranges from the shared `.meta` so the other instances
    ///   observe the removal, and carry the `cached_objects` decrement.
    ///
    /// `None` in unit tests that construct a bare manager; eviction then still
    /// deletes files and moves the local gauges, but performs no shared accounting
    /// and logs a WARN saying so.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 5.1, 5.2, 5.3
    journal_consolidator: Option<Arc<crate::journal_consolidator::JournalConsolidator>>,
}

impl WriteCacheManager {
    /// Create a new WriteCacheManager
    ///
    /// # Arguments
    /// * `cache_dir` - Base cache directory
    /// * `total_cache_size` - Total disk cache size in bytes
    /// * `write_cache_percent` - Percentage of total cache for write cache (1-50%)
    /// * `write_ttl` - TTL for write-cached objects
    /// * `incomplete_upload_ttl` - TTL for incomplete multipart uploads
    /// * `eviction_algorithm` - Eviction algorithm to use (LRU or TinyLFU)
    /// * `max_object_size` - Maximum object size for write caching
    ///
    /// # Requirements
    /// Implements Requirements 6.1, 6.5
    pub fn new(
        cache_dir: PathBuf,
        total_cache_size: u64,
        write_cache_percent: f32,
        write_ttl: Duration,
        incomplete_upload_ttl: Duration,
        eviction_algorithm: CacheEvictionAlgorithm,
        max_object_size: u64,
    ) -> Self {
        // Clamp percentage to valid range (1-50%)
        let clamped_percent = write_cache_percent.clamp(1.0, 50.0);
        let max_size = ((total_cache_size as f64) * (clamped_percent as f64 / 100.0)) as u64;

        info!(
            "WriteCacheManager initialized: max_size={} bytes ({:.1}% of {} total), \
             write_ttl={:?}, incomplete_upload_ttl={:?}, eviction_algorithm={:?}",
            max_size,
            clamped_percent,
            total_cache_size,
            write_ttl,
            incomplete_upload_ttl,
            eviction_algorithm
        );

        Self {
            max_size,
            current_size: Arc::new(AtomicU64::new(0)),
            staged_entries: Arc::new(AtomicU64::new(0)),
            staging_evictions_total: Arc::new(AtomicU64::new(0)),
            staging_eviction_bytes_total: Arc::new(AtomicU64::new(0)),
            graduations_total: Arc::new(AtomicU64::new(0)),
            write_ttl,
            incomplete_upload_ttl,
            eviction_algorithm,
            cache_dir,
            max_object_size,
            journal_consolidator: None,
        }
    }

    /// Wire the journal consolidator so staging eviction can perform shared
    /// accounting. Called from `CacheManager::initialize` once the consolidator
    /// exists; mirrors `DiskCacheManager::set_journal_consolidator`.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 5.1, 5.2
    pub fn set_journal_consolidator(
        &mut self,
        consolidator: Arc<crate::journal_consolidator::JournalConsolidator>,
    ) {
        self.journal_consolidator = Some(consolidator);
    }

    /// Create a new WriteCacheManager with default settings
    ///
    /// Uses:
    /// - 10% of total cache for write cache
    /// - 1 day write TTL
    /// - 1 day incomplete upload TTL
    /// - LRU eviction algorithm
    /// - 256MB max object size
    pub fn new_with_defaults(cache_dir: PathBuf, total_cache_size: u64) -> Self {
        Self::new(
            cache_dir,
            total_cache_size,
            10.0,                       // 10% default
            Duration::from_secs(86400), // 1 day
            Duration::from_secs(86400), // 1 day
            CacheEvictionAlgorithm::default(),
            256 * 1024 * 1024, // 256MB
        )
    }

    /// Get the maximum write cache size
    pub fn max_size(&self) -> u64 {
        self.max_size
    }

    /// Get the write TTL
    pub fn write_ttl(&self) -> Duration {
        self.write_ttl
    }

    /// Get the incomplete upload TTL
    pub fn incomplete_upload_ttl(&self) -> Duration {
        self.incomplete_upload_ttl
    }

    /// Get the eviction algorithm
    pub fn eviction_algorithm(&self) -> &CacheEvictionAlgorithm {
        &self.eviction_algorithm
    }

    /// Get the maximum object size for write caching
    pub fn max_object_size(&self) -> u64 {
        self.max_object_size
    }

    /// Get the cache directory
    pub fn cache_dir(&self) -> &PathBuf {
        &self.cache_dir
    }

    // =========================================================================
    // Capacity Management Methods (Requirements 9.1, 9.2, 9.3, 9.4, 9.5)
    // =========================================================================

    /// Get current write cache usage (compressed bytes on disk)
    ///
    /// # Requirements
    /// Implements Requirement 6.3
    pub fn current_usage(&self) -> u64 {
        self.current_size.load(Ordering::SeqCst)
    }

    /// Get the live (approximate) count of staged (write-cached, ungraduated)
    /// entries. Best-available data source for `/metrics` `write_cache.staged_entries`
    /// until graduation (R1) and eviction (R5) accounting land.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.2, 8.3
    pub fn staged_entries(&self) -> u64 {
        self.staged_entries.load(Ordering::Relaxed)
    }

    /// Increment the live staged-entry counter. Called on a successful
    /// write-cache commit (a new `.meta` written with `is_write_cached: true`).
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.3
    pub fn increment_staged_entries(&self) {
        self.staged_entries.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the live staged-entry counter, saturating at zero. Called on
    /// graduation (first read transitions the entry out of the write tier) or
    /// on staging eviction.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.3
    pub fn decrement_staged_entries(&self) {
        self.staged_entries
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            })
            .ok();
    }

    /// Cumulative count of objects evicted from the write/staging tier.
    /// Distinct from `cache.evictions`, which reflects read-cache eviction
    /// only — see the field doc for why the split matters.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.4
    pub fn staging_evictions_total(&self) -> u64 {
        self.staging_evictions_total.load(Ordering::Relaxed)
    }

    /// Cumulative compressed bytes freed by staging eviction.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.4
    pub fn staging_eviction_bytes_total(&self) -> u64 {
        self.staging_eviction_bytes_total.load(Ordering::Relaxed)
    }

    /// Cumulative count of entries this instance graduated out of the staging tier.
    /// See the field doc for why this is per-instance and not the accounting authority.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.3
    pub fn graduations_total(&self) -> u64 {
        self.graduations_total.load(Ordering::Relaxed)
    }

    /// Increment the cumulative graduation counter. Called once per graduation this
    /// instance performs, at the point the `Graduation` journal entry is written.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.3
    pub fn increment_graduations(&self) {
        self.graduations_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Account a write operation's bytes as in-flight, returning an RAII handle that
    /// releases them on drop (including on cancellation and panic).
    ///
    /// # This is no longer an admission gate
    ///
    /// It reads as one, and it was one until 2.7.0, which is why the distinction is
    /// spelled out here. Today the **only** thing it refuses is an object larger than
    /// `max_object_size` (R3.5). It does not consult the Staging_Bound at all — see
    /// [`Self::reserve_for_sizing`] for why the bound became a target rather than a gate,
    /// and `CacheManager::disk_safety_refusal` for the one bound that may still decline
    /// caching.
    ///
    /// Two behaviours were removed on the way here, and both are worth knowing about
    /// because both were measured on the fleet rather than argued:
    ///
    /// **Inline eviction (task 8).** A refusal used to run `evict_to_target` and retry.
    /// That put a full `metadata/` walk on the request path of every refused PUT — with
    /// 1,779 entries it dominated PUT latency at 7-9s independent of the object's own
    /// size, where a 64 MiB body added under 0.3s against a 1.8s run-to-run variance. It
    /// also deleted *other instances'* freshly-staged entries, because the candidate walk
    /// scanned shared storage with no ownership filter while the decision to run it came
    /// from this instance's private counter. Observed 2026-08-24: an instance with a
    /// correct counter cached a 32 MiB object and an instance with an inflated one deleted
    /// it seconds later, freeing 33.5 MB against a 6.19 GB shortfall and then refusing its
    /// own reservation anyway. See `cache-coherency-invariants.md`, "Invariant 2's
    /// dangerous corollary".
    ///
    /// **The capacity refusal itself (task 28).** Staging eviction is now driven by the
    /// Write_Ledger from `CacheManager::evict_staging_tier`, asynchronously and under the
    /// global eviction lock, so there is nothing left for a synchronous refusal to
    /// protect.
    ///
    /// # Returns
    /// * `Some(WriteReservation)` — admitted; the bytes are counted as in-flight until dropped
    /// * `None` — the object exceeds `max_object_size`, or the in-flight total would
    ///   overflow `u64` (an arithmetic impossibility at real object sizes, not a capacity
    ///   decision)
    ///
    /// # Requirements
    /// Implements Requirements 9.1, 9.2
    pub async fn try_reserve(&self, size: u64) -> Option<WriteReservation> {
        // R3.5 / task 30: the ONLY size-based refusal left, and it is deliberately
        // ahead of everything else so eviction is never considered for an upload that
        // could not be cached at any capacity. A regression here would be expensive and
        // silent — it would make the evictor delete live entries to make room for an
        // object that is then refused anyway — so it is covered by its own test rather
        // than resting on this comment.
        if size > self.max_object_size {
            debug!(
                "Write cache bypass: object size {} exceeds max_object_size {}",
                size, self.max_object_size
            );
            return None;
        }

        self.reserve_for_sizing(size)
    }

    /// Account `size` as in-flight and hand back the RAII release handle.
    ///
    /// # This no longer refuses on Staging_Bound grounds (R3.1, R3.3)
    ///
    /// It used to: a CAS loop returned `None` whenever `current_size + size` exceeded
    /// `max_size`, which made `write_cache_percent` a hard admission gate.
    ///
    /// # Why a gate was the wrong shape — and NOT because refusing a PUT costs more
    ///
    /// The spec (requirements.md R3, design.md §1) justified this with a cost asymmetry:
    /// refusing a GET costs one future fetch, whereas refusing a PUT is a "one-shot loss"
    /// because the write-through opportunity exists only once. **That reasoning is wrong
    /// and is not why this changed.** The costs are the same. Refuse a GET and the next
    /// request for that key pays an S3 fetch where it would have hit, then caches; refuse
    /// a PUT and the next read pays an S3 fetch where it would have hit, then caches. One
    /// extra fetch either way, and the cache self-heals either way — the write-through
    /// opportunity being once-only does not matter, because the read path repopulates the
    /// entry regardless.
    ///
    /// If anything the asymmetry runs the other way: a staged entry nobody has read yet is
    /// speculative, a bet that a read is coming (the spec's own "read-after-write bet"),
    /// whereas a cached GET reflects demonstrated demand. So the write tier has the
    /// *weaker* claim on space, not the stronger one.
    ///
    /// The actual reasons a gate was wrong here, none of which need an asymmetry:
    ///
    /// 1. **Caching is worth doing whenever it is affordable, and refusing buys nothing a
    ///    target does not.** The bytes are already in hand, so caching costs no extra
    ///    bandwidth, and the space is reclaimable. Declining is only justified when the
    ///    space genuinely is not there — which is the Disk_Safety_Bound's job, not this
    ///    one's.
    /// 2. **The gate was enforced against a figure that was wrong.** `current_size` was
    ///    seeded at startup from resident bytes and never drained, so it sat near or above
    ///    the bound from the moment a proxy started. Refusals were therefore uncorrelated
    ///    with real capacity pressure. A hard gate on an unreliable number is strictly
    ///    worse than a soft target with reclamation behind it, whatever the cost model.
    /// 3. **The read tier already had the right shape** — admit freely, reclaim
    ///    asynchronously with trigger/target hysteresis — and there was no reason for the
    ///    write tier to differ. This is a consistency argument, not a cost one.
    ///
    /// The bound still exists, and still means something: it is the point at which
    /// reclamation starts. What changed is that it is enforced by removing staged entries
    /// rather than by refusing new ones.
    ///
    /// What the reservation is still for, and why it was not simply deleted:
    ///
    /// - **Sizing.** `open_write_cache_sink` needs `content_length` up front to size the
    ///   sink, so something has to carry it.
    /// - **Inflight_Bytes.** A genuine per-instance quantity worth reporting
    ///   (`/metrics write_cache.inflight_bytes`), and deliberately per-instance rather
    ///   than shared (R7.2) — uploads in flight on another proxy are not this proxy's
    ///   concern.
    ///
    /// Going over the Staging_Bound now triggers asynchronous eviction toward a target
    /// below it instead (R3.1), driven from the consolidation cycle. The only bound that
    /// may still decline caching is the Disk_Safety_Bound, checked by the caller before
    /// this is reached (R4.1).
    ///
    /// `None` is still possible, but only for arithmetic overflow — `current_size + size`
    /// exceeding `u64` — which is not a capacity decision and cannot happen with real
    /// object sizes.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 3.1, 3.3, 7.2
    fn reserve_for_sizing(&self, size: u64) -> Option<WriteReservation> {
        loop {
            let current = self.current_size.load(Ordering::SeqCst);
            let new = current.checked_add(size)?;
            match self.current_size.compare_exchange_weak(
                current,
                new,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    debug!(
                        "Write cache reserved for sizing: size={}, old_inflight={}, new_inflight={}",
                        size, current, new
                    );
                    return Some(WriteReservation {
                        size,
                        current_size: self.current_size.clone(),
                    });
                }
                Err(_) => continue, // CAS failed, retry
            }
        }
    }

    /// Directly release capacity (for internal eviction use only).
    ///
    /// Uses saturating subtraction to prevent underflow. Emits a rate-limited
    /// warning if underflow would have occurred.
    ///
    /// This method is NOT part of the public API for callers performing uploads.
    /// Upload callers use `WriteReservation` (RAII drop) for release.
    /// This is used internally by eviction paths that track sizes independently.
    fn release_capacity_internal(&self, compressed_size: u64) {
        if compressed_size == 0 {
            return;
        }

        self.current_size
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if current < compressed_size {
                    // Underflow — rate-limited warning
                    let now_secs = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let last = LAST_UNDERFLOW_WARN.load(Ordering::Relaxed);
                    if now_secs.saturating_sub(last) >= UNDERFLOW_WARN_INTERVAL_SECS
                        && LAST_UNDERFLOW_WARN
                            .compare_exchange(last, now_secs, Ordering::Relaxed, Ordering::Relaxed)
                            .is_ok()
                    {
                        warn!(
                            release_size = compressed_size,
                            current_size = current,
                            "Write cache capacity underflow detected in eviction: \
                             release_size ({}) > current_size ({}), saturating to 0",
                            compressed_size,
                            current
                        );
                    }
                    Some(0)
                } else {
                    Some(current - compressed_size)
                }
            })
            .ok();

        debug!("Write cache released (internal): size={}", compressed_size);
    }

    // =========================================================================
    // Eviction Methods
    //
    // Staging eviction is driven by the Write_Ledger from
    // `CacheManager::evict_staging_tier`, not from here. What remains in this
    // module is the per-object eviction *mechanism* — `evict_write_cached_object`
    // below, which owns the R5 accounting — plus incomplete-upload cleanup, which
    // is a separate TTL-driven concern.
    //
    // Removed in task 25, deliberately rather than left alongside: `evict_to_target`,
    // `collect_eviction_candidates`, `calculate_eviction_score` and
    // `sort_candidates_for_eviction`. Two implementations of eviction is how one of
    // them rots, and this one had already rotted in three separate ways —
    // a `WalkDir` over all of `metadata/` to select O(evicted) victims, a decision
    // taken from a private counter but applied to shared storage, and a TinyLFU
    // score that could not express anything useful about a tier whose entries have
    // by definition never been read (`access_count` is 1 per range, so the score
    // collapsed to `n_ranges >> (idle/3600)` — a coarse bucket with no tiebreak and
    // a bias against single-part objects).
    //
    // The staging tier needs no eviction score at all: its candidate order is
    // insertion order, which the ledger expresses directly.
    // =========================================================================

    /// Evict a single write-cached object, decrementing the shared accounting.
    ///
    /// # Accounting contract (R5)
    ///
    /// Before this change the function deleted range files and the `.meta` and then
    /// adjusted only `self.current_size` — a per-instance, in-memory counter. It wrote
    /// no journal entries and touched neither the size accumulator nor Size_State, so
    /// `write_cache_size` ratcheted upward forever while data was being deleted. That
    /// is the R5 leak; read-tier eviction did debit both channels
    /// (`CacheManager::perform_eviction_with_lock`, Step 5), which is the evidence
    /// that the omission was an oversight rather than a definition difference.
    ///
    /// **Corrected 2026-08-28.** This paragraph used to say read-tier eviction "has
    /// always done this correctly", and R5.4 below cited it as the model. That was
    /// true of *which figures* it debited and false of *which ranges*: it built its
    /// debit list from the candidate list rather than from what was deleted, so a
    /// range whose `.bin` unlink failed was debited with its bytes still on disk.
    /// Fixed under `cache-eviction-at-scale` R7.2, which copied the
    /// `deleted_ranges` pattern below in the opposite direction. Do not restore the
    /// stronger wording — it is the reason the read-tier defect went unexamined
    /// while this one was being fixed.
    ///
    /// Three rules, and each maps to an acceptance criterion:
    ///
    /// - **R5.1** — debit `subtract` unconditionally and `subtract_write_cache` only for
    ///   ranges still classified as staged by `cache_types::is_staged_range_spec` —
    ///   the range's own recorded membership, with the object flag from the `.meta`
    ///   **this call** loaded as the fallback for an unrecorded range. A staged range
    ///   counts in `total_size` *and* in
    ///   `write_cache_size`; a graduated one counts only in `total_size`. Both use
    ///   `compressed_size`, for symmetry with the add sites
    ///   (`DiskCacheManager::store_range` and
    ///   `JournalConsolidator::write_multipart_journal_entries` both credit
    ///   `compressed_size`); debiting an on-disk `len()` instead would drift silently.
    /// - **R5.4** — debit only for range files that **existed and deleted cleanly**.
    ///   The accumulator debits and the journal entries are both built from
    ///   `deleted_ranges`, the filtered list — never from `metadata.ranges` — so a
    ///   missing or undeletable file cannot produce a phantom debit.
    /// - **R5.3** — do NOT also subtract directly. Nothing here writes
    ///   `size_state.json`; the debit reaches it only via the accumulator's delta file
    ///   and `collect_and_apply_deltas` under the global consolidation lock. See the
    ///   equivalent warning in `CacheManager::enforce_disk_cache_limits_internal`.
    ///
    /// `release_capacity_internal` is still called and is **not** a direct subtraction
    /// in the R5.3 sense: it moves this instance's in-flight admission counter
    /// (`current_size`, exposed as `/metrics write_cache.inflight_bytes`), which is a
    /// different quantity from `SizeState::write_cache_size` (exposed as
    /// `resident_bytes`). The two are updated by different mechanisms with different
    /// visibility and are expected to be able to diverge — see the steering note on
    /// treating two figures that agree to the byte as a symptom rather than as
    /// corroboration.
    ///
    /// # The graduation race, and why the write-cache debit is conditional
    ///
    /// Candidate selection filters on `is_write_cached`, so it is
    /// tempting to conclude that anything reaching this function is staged by definition
    /// and to debit `write_cache_size` unconditionally. **That was the first version of
    /// this code and it was wrong.** The filter runs at *collection* time; this function
    /// re-reads the `.meta` and the entry may have graduated in between — a first GET on
    /// any instance clears the flag and appends a `Graduation` entry, which debits the
    /// write-cache figure by these same bytes.
    ///
    /// An unconditional debit therefore removes those bytes **twice**, driving
    /// `write_cache_size` toward undershoot. Undershoot is the more dangerous direction
    /// because it silently over-admits rather than refusing, so the symptom is a write
    /// cache quietly exceeding its allocation rather than an error.
    ///
    /// Classifying per range from the `.meta` this call read closes it, and does so
    /// **independently of the caller**: Phase E's task 21 is also specified to skip
    /// graduated candidates, but relying on that would make correctness here depend on
    /// how a not-yet-written check in another phase is worded.
    ///
    /// # Cross-instance hazard, unchanged by this task
    ///
    /// The removed candidate walk scanned the **shared**
    /// `metadata/` tree with no instance-ownership filter, while the decision to run it
    /// comes from this instance's private counter. That is the "local decision, shared
    /// effect" defect in `cache-coherency-invariants.md`, demonstrated live on
    /// 2026-08-24 when one proxy deleted a 32 MiB object another had just cached. Task
    /// 8 removed the only production caller for exactly that reason. R5 makes its
    /// accounting correct so that Phase E's ledger-driven evictor — which fixes the
    /// decision input, not the accounting — inherits a correct debit path.
    ///
    /// **Phase E has since given it that caller, so this is a live production path
    /// again**: `CacheManager::evict_staging_tier_locked` → `evict_staged_object` →
    /// here. The decision input is now the Write_Ledger read under the global eviction
    /// lock rather than a private counter, which is what makes that safe — the
    /// "currently reachable only from this module's tests" note this comment used to
    /// carry was true of task 8's state and is no longer. Corrected 2026-08-27 while
    /// fixing task 76, where the stale note had led a security scan to under-rate a
    /// double-debit reaching this site.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 5.1, 5.2, 5.3, 5.4
    pub(crate) async fn evict_write_cached_object(&self, cache_key: &str) -> Result<u64> {
        // Read metadata to get range files
        let metadata_path = self.get_metadata_path(cache_key)?;

        if !metadata_path.exists() {
            return Err(ProxyError::CacheError(format!(
                "Metadata file not found for eviction: {}",
                cache_key
            )));
        }

        let content = tokio::fs::read_to_string(&metadata_path)
            .await
            .map_err(|e| ProxyError::CacheError(format!("Failed to read metadata: {}", e)))?;

        let metadata: crate::cache_types::NewCacheMetadata = serde_json::from_str(&content)
            .map_err(|e| ProxyError::CacheError(format!("Failed to parse metadata: {}", e)))?;

        let mut total_freed: u64 = 0;
        // Ranges whose `.bin` existed AND was removed without error. R5.4: this is the
        // only list the accounting below may be built from — `metadata.ranges` would
        // include files that were already gone, producing a phantom debit.
        // Tuple: (range_start, range_end, compressed_size, bin_file_path, counts_as_staged)
        let mut deleted_ranges: Vec<(u64, u64, u64, String, bool)> = Vec::new();

        // Delete all range files
        for range in &metadata.ranges {
            let range_path = self.cache_dir.join("ranges").join(&range.file_path);
            if range_path.exists() {
                if let Err(e) = tokio::fs::remove_file(&range_path).await {
                    warn!("Failed to remove range file {:?}: {}", range_path, e);
                } else {
                    total_freed += range.compressed_size;
                    deleted_ranges.push((
                        range.start,
                        range.end,
                        range.compressed_size,
                        range_path.to_string_lossy().to_string(),
                        // Classified from the `.meta` THIS call read, not from the state
                        // the candidate walk saw — see the graduation-race note on this
                        // function. Per range, from the membership the range recorded at
                        // credit time, so evicting a mixed object does not debit
                        // `write_cache_size` for a read-tier range.
                        //
                        // No new tuple element was needed here: `counts_as_staged` was
                        // already carried, so only the predicate changed. That is why
                        // this site keeps its tuple while read-tier eviction's Step 5
                        // moved to a named struct — that one genuinely needed an eighth
                        // element.
                        // Requirements: 12.3, 12.4
                        crate::cache_types::is_staged_range_spec(
                            range,
                            metadata.object_metadata.is_write_cached,
                        ),
                    ));
                }
            }
        }

        // Delete metadata file
        let metadata_deleted = match tokio::fs::remove_file(&metadata_path).await {
            Ok(()) => true,
            Err(e) => {
                warn!("Failed to remove metadata file {:?}: {}", metadata_path, e);
                false
            }
        };

        // R5.1 / R5.2 / R5.3: shared accounting. Debit both accumulators for the
        // ranges actually deleted, then write Remove journal entries so the other
        // instances converge. Nothing below writes `size_state.json`.
        if !deleted_ranges.is_empty() || metadata_deleted {
            if let Some(consolidator) = &self.journal_consolidator {
                // R5.1: by compressed_size, for symmetry with the adds. `subtract` is
                // unconditional — the bytes left the disk either way — but the
                // write-cache debit is conditional on the range still being staged,
                // exactly as read-tier eviction's Step 5 does it. A graduated entry has
                // already had its write-cache bytes debited by its `Graduation` entry, so
                // debiting again here would remove them twice.
                for (start, end, compressed_size, _path, counts_as_staged) in &deleted_ranges {
                    // `subtract_range` rather than `subtract`, so the range's dedup
                    // entry is released along with its bytes and a later re-cache of
                    // the same range can be credited again. See
                    // `SizeAccumulator::subtract_range`.
                    consolidator.size_accumulator().subtract_range(
                        cache_key,
                        *start,
                        *end,
                        *compressed_size,
                    );
                    if *counts_as_staged {
                        consolidator
                            .size_accumulator()
                            .subtract_write_cache(*compressed_size);
                    }
                }

                // R5.2: Remove journal entries, so the shared `.meta` converges and
                // the other instances observe the removal. Metadata only — these do
                // not move any size figure (journal-derived size deltas were retired
                // in favour of the accumulator; see `consolidate_key`).
                if !deleted_ranges.is_empty() {
                    let journal_entries: Vec<(String, u64, u64, u64, String)> = deleted_ranges
                        .iter()
                        .map(|(start, end, compressed_size, path, _counts_as_staged)| {
                            (
                                cache_key.to_string(),
                                *start,
                                *end,
                                *compressed_size,
                                path.clone(),
                            )
                        })
                        .collect();
                    consolidator
                        .write_eviction_journal_entries(journal_entries)
                        .await;
                }

                // R5.2: `cached_objects` converges only when the `.meta` is gone, which
                // is the same condition read-tier eviction uses (it counts a key as
                // evicted when a `.meta` appears among the deleted paths).
                if metadata_deleted {
                    consolidator.decrement_cached_objects(1).await;
                }

                // Flush the debit to a delta file now rather than waiting for the
                // periodic flush. The files are already gone; leaving the subtraction
                // in memory means losing it entirely on a crash, with the bytes
                // unrecoverable until the next full validation scan. Read-tier
                // eviction flushes for the same reason before releasing its lock.
                if let Err(e) = consolidator.size_accumulator().flush().await {
                    warn!(
                        "Failed to flush accumulator after staging eviction: cache_key={}, error={}",
                        cache_key, e
                    );
                }
            } else {
                warn!(
                    "Journal consolidator not wired into WriteCacheManager: staging eviction \
                     of cache_key={} freed {} bytes that will NOT be reflected in Size_State \
                     until the next full validation scan",
                    cache_key, total_freed
                );
            }
        }

        // Update current size via internal saturating release
        self.release_capacity_internal(total_freed);
        // Best-effort staged-entry gauge: this entry is no longer staged — but only
        // if it was staged to begin with. This call has no production caller today
        // (see the doc comment above), but a caller evicting an already-graduated
        // read-cache entry must not decrement a gauge it was never counted in.
        // Spec: write-cache-accounting-and-eviction. Requirements: 8.2, 8.3
        if metadata.object_metadata.is_write_cached {
            self.decrement_staged_entries();
        }

        // Staging-eviction observability, deliberately separate from
        // `cache.evictions` (Requirement 8.4). Counted here regardless of
        // whether any bytes were actually freed (a metadata-only entry with
        // no surviving range files still counts as one evicted object), but
        // `staging_eviction_bytes_total` only accumulates what was actually
        // freed on disk.
        self.staging_evictions_total.fetch_add(1, Ordering::Relaxed);
        if total_freed > 0 {
            self.staging_eviction_bytes_total
                .fetch_add(total_freed, Ordering::Relaxed);
        }

        Ok(total_freed)
    }

    /// Get metadata file path for a cache key
    ///
    /// Returns an error if `cache_key` is malformed (missing bucket/object separator).
    fn get_metadata_path(&self, cache_key: &str) -> Result<PathBuf> {
        use crate::disk_cache::get_sharded_path;

        let base_dir = self.cache_dir.join("metadata");

        get_sharded_path(&base_dir, cache_key, ".meta").map_err(|e| {
            ProxyError::CacheError(format!(
                "Malformed cache key '{}': {}. Cache keys must be in 'bucket/object' format.",
                cache_key, e
            ))
        })
    }

    /// Evict incomplete multipart uploads older than TTL
    ///
    /// Scans mpus_in_progress/ directory for uploads that have exceeded
    /// the incomplete_upload_ttl and removes them.
    ///
    /// # Returns
    /// * `Ok(bytes_freed)` - Number of compressed bytes freed
    /// * `Err` - If eviction fails
    ///
    /// # Requirements
    /// Implements Requirements 4.2, 4.3
    pub async fn evict_incomplete_uploads(&self) -> Result<u64> {
        let mpus_dir = self.cache_dir.join("mpus_in_progress");

        if !mpus_dir.exists() {
            debug!("No mpus_in_progress directory, nothing to evict");
            return Ok(0);
        }

        let mut total_freed: u64 = 0;
        let mut evicted_count: u64 = 0;
        let now = SystemTime::now();

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

            if !upload_meta_path.exists() {
                // No metadata file, check directory mtime
                if let Ok(metadata) = tokio::fs::metadata(&upload_dir).await {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(age) = now.duration_since(modified) {
                            if age > self.incomplete_upload_ttl {
                                // Evict this incomplete upload
                                match self.evict_incomplete_upload(&upload_dir).await {
                                    Ok(freed) => {
                                        total_freed += freed;
                                        evicted_count += 1;
                                        info!(
                                            "Evicted incomplete upload (no metadata): dir={:?}, age={:?}, freed={} bytes",
                                            upload_dir, age, freed
                                        );
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to evict incomplete upload {:?}: {}",
                                            upload_dir, e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                continue;
            }

            // Read upload metadata to check age
            if let Ok(content) = tokio::fs::read_to_string(&upload_meta_path).await {
                if let Ok(tracker) =
                    serde_json::from_str::<crate::cache_types::MultipartUploadTracker>(&content)
                {
                    // Age is measured from the most recent sign of activity, which is
                    // the NEWEST of the upload directory's mtime and `upload.meta`'s.
                    //
                    // The directory is what matters now. `upload.meta` is written once,
                    // at CreateMultipartUpload, and never touched again — parts write
                    // their own `part{N}.json` records rather than rewriting it. So its
                    // mtime is the upload's START time, and using it alone would evict a
                    // long-running upload out from under itself the moment it passed the
                    // TTL, however recently a part had landed. The directory's mtime does
                    // move, because each part creates files in it.
                    let newest_mtime = {
                        let mut newest: Option<std::time::SystemTime> = None;
                        for path in [&upload_dir, &upload_meta_path] {
                            if let Ok(md) = tokio::fs::metadata(path).await {
                                if let Ok(modified) = md.modified() {
                                    if newest.is_none_or(|current| modified > current) {
                                        newest = Some(modified);
                                    }
                                }
                            }
                        }
                        newest
                    };
                    let age = match newest_mtime {
                        Some(modified) => now.duration_since(modified).unwrap_or_default(),
                        None => now.duration_since(tracker.started_at).unwrap_or_default(),
                    };

                    if age > self.incomplete_upload_ttl {
                        // Evict this incomplete upload
                        match self
                            .evict_incomplete_upload_with_tracker(&upload_dir, &tracker)
                            .await
                        {
                            Ok(freed) => {
                                total_freed += freed;
                                evicted_count += 1;
                                info!(
                                    "Evicted incomplete upload: upload_id={}, age={:?}, parts={}, freed={} bytes",
                                    tracker.upload_id, age, tracker.parts.len(), freed
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to evict incomplete upload {}: {}",
                                    tracker.upload_id, e
                                );
                            }
                        }
                    }
                }
            }
        }

        if evicted_count > 0 {
            info!(
                "Incomplete upload eviction complete: evicted={} uploads, freed={} bytes",
                evicted_count, total_freed
            );
        }

        Ok(total_freed)
    }

    /// Evict a single incomplete upload directory
    async fn evict_incomplete_upload(&self, upload_dir: &PathBuf) -> Result<u64> {
        let mut total_freed: u64 = 0;

        // Remove all files in the directory
        let mut entries = tokio::fs::read_dir(upload_dir)
            .await
            .map_err(|e| ProxyError::CacheError(format!("Failed to read upload dir: {}", e)))?;

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if let Ok(metadata) = tokio::fs::metadata(&path).await {
                total_freed += metadata.len();
            }
            if let Err(e) = tokio::fs::remove_file(&path).await {
                warn!("Failed to remove file {:?}: {}", path, e);
            }
        }

        // Remove the directory
        if let Err(e) = tokio::fs::remove_dir(upload_dir).await {
            warn!("Failed to remove upload directory {:?}: {}", upload_dir, e);
        }

        self.release_capacity_internal(total_freed);
        Ok(total_freed)
    }

    /// Evict an incomplete upload with tracker information
    ///
    /// Acquires per-upload lock before deletion to prevent race conditions
    /// with active uploads in shared cache scenarios.
    ///
    /// # Requirements
    /// Implements Requirements 4.2, 4.3, 8a.4
    async fn evict_incomplete_upload_with_tracker(
        &self,
        upload_dir: &PathBuf,
        tracker: &crate::cache_types::MultipartUploadTracker,
    ) -> Result<u64> {
        use fs2::FileExt;

        // Acquire per-upload lock before deletion (Requirement 8a.4)
        let lock_file_path = upload_dir.join("upload.lock");
        let lock_file = match std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_file_path)
        {
            Ok(f) => f,
            Err(e) => {
                warn!(
                    "Failed to open per-upload lock file for eviction: upload_id={}, error={}",
                    tracker.upload_id, e
                );
                // Continue without lock - directory may have been cleaned up already
                return self.evict_incomplete_upload(upload_dir).await;
            }
        };

        // Try to acquire exclusive lock (non-blocking)
        match lock_file.try_lock_exclusive() {
            Ok(()) => {
                debug!(
                    "Acquired per-upload lock for eviction: upload_id={}",
                    tracker.upload_id
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // Lock is held by another instance - skip this upload
                debug!(
                    "Skipping incomplete upload eviction (lock held by another instance): upload_id={}",
                    tracker.upload_id
                );
                return Ok(0);
            }
            Err(e) => {
                warn!(
                    "Failed to acquire per-upload lock for eviction: upload_id={}, error={}",
                    tracker.upload_id, e
                );
                // Continue without lock - may fail but won't corrupt data
            }
        }

        let mut total_freed: u64 = 0;

        // Parts are stored inside the upload directory (mpus_in_progress/{upload_id}/part{N}.bin)
        // Track sizes of part files before removing the directory
        for part in &tracker.parts {
            let part_path = upload_dir.join(format!("part{}.bin", part.part_number));
            if part_path.exists() {
                if let Ok(metadata) = tokio::fs::metadata(&part_path).await {
                    total_freed += metadata.len();
                }
            }
        }

        // Release lock before removing directory (lock file is inside directory)
        drop(lock_file);

        // Remove the upload directory and its contents
        let freed_from_dir = self.evict_incomplete_upload(upload_dir).await.unwrap_or(0);
        total_freed += freed_from_dir;

        Ok(total_freed)
    }

    /// Run incomplete upload cleanup on startup
    ///
    /// This method should be called during cache manager initialization to
    /// clean up any incomplete uploads that expired while the proxy was down.
    ///
    /// # Requirements
    /// Implements Requirements 4.2, 4.3
    pub async fn cleanup_incomplete_uploads_on_startup(&self) -> Result<()> {
        info!("Running incomplete upload cleanup on startup");

        match self.evict_incomplete_uploads().await {
            Ok(freed) => {
                if freed > 0 {
                    info!("Startup incomplete upload cleanup freed {} bytes", freed);
                } else {
                    debug!("No incomplete uploads to clean up on startup");
                }
                Ok(())
            }
            Err(e) => {
                error!("Startup incomplete upload cleanup failed: {}", e);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_write_cache_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            10 * 1024 * 1024 * 1024, // 10GB total
            10.0,                    // 10%
            Duration::from_secs(86400),
            Duration::from_secs(86400),
            CacheEvictionAlgorithm::LRU,
            256 * 1024 * 1024,
        );

        // 10% of 10GB = 1GB
        assert_eq!(manager.max_size(), 1024 * 1024 * 1024);
        assert_eq!(manager.current_usage(), 0);
        assert_eq!(manager.write_ttl(), Duration::from_secs(86400));
        assert_eq!(manager.incomplete_upload_ttl(), Duration::from_secs(86400));
        assert_eq!(*manager.eviction_algorithm(), CacheEvictionAlgorithm::LRU);
    }

    #[test]
    fn test_write_cache_manager_defaults() {
        let temp_dir = TempDir::new().unwrap();
        let manager = WriteCacheManager::new_with_defaults(
            temp_dir.path().to_path_buf(),
            10 * 1024 * 1024 * 1024, // 10GB total
        );

        // Default 10% of 10GB = 1GB
        assert_eq!(manager.max_size(), 1024 * 1024 * 1024);
        assert_eq!(manager.write_ttl(), Duration::from_secs(86400));
        assert_eq!(manager.incomplete_upload_ttl(), Duration::from_secs(86400));
    }

    /// Seed a staged (write-cached) `.meta` plus one range file for `cache_key`,
    /// so `evict_write_cached_object` has a real, evictable object to act on.
    async fn seed_staged_object(cache_dir: &std::path::Path, cache_key: &str, size: u64) {
        let metadata_dir = cache_dir.join("metadata");
        let meta_path = crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta")
            .expect("valid cache key");
        if let Some(parent) = meta_path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }

        let ranges_dir = cache_dir.join("ranges");
        let range_file_path =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_0-end.bin")
                .expect("valid cache key");
        if let Some(parent) = range_file_path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&range_file_path, vec![0u8; size as usize])
            .await
            .unwrap();
        let range_file_path_str = range_file_path
            .strip_prefix(&ranges_dir)
            .unwrap_or(&range_file_path)
            .to_string_lossy()
            .to_string();

        let base_time = SystemTime::now();
        let object_metadata = crate::cache_types::ObjectMetadata {
            etag: "etag-staged".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: size,
            content_type: Some("application/octet-stream".to_string()),
            is_write_cached: true,
            write_cache_expires_at: Some(base_time + Duration::from_secs(86400)),
            write_cache_created_at: Some(base_time - Duration::from_secs(3600)),
            write_cache_last_accessed: Some(base_time - Duration::from_secs(3600)),
            ..Default::default()
        };
        let range_spec = crate::cache_types::RangeSpec {
            start: 0,
            end: size.saturating_sub(1),
            file_path: range_file_path_str,
            compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
            compressed_size: size,
            uncompressed_size: size,
            created_at: base_time - Duration::from_secs(3600),
            last_accessed: base_time - Duration::from_secs(3600),
            access_count: 1,
            staged: None,
        };
        let metadata = crate::cache_types::NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata,
            ranges: vec![range_spec],
            created_at: base_time - Duration::from_secs(3600),
            expires_at: base_time + Duration::from_secs(86400),
            compression_info: crate::cache_types::CompressionInfo::default(),
            ..Default::default()
        };
        let json = serde_json::to_string_pretty(&metadata).unwrap();
        tokio::fs::write(&meta_path, json).await.unwrap();
    }

    /// Staging eviction must count separately from `cache.evictions`
    /// (Requirement 8.4): eviction deleting a staged object must
    /// increment both `staging_evictions_total` (by one object) and
    /// `staging_eviction_bytes_total` (by the bytes actually freed), and
    /// leave both at zero before any eviction has run.
    #[tokio::test]
    async fn test_staging_eviction_counters_increment_separately_from_read_cache() {
        let temp_dir = TempDir::new().unwrap();
        let manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            1_000_000, // total cache size, irrelevant here
            10.0,
            Duration::from_secs(86400),
            Duration::from_secs(86400),
            CacheEvictionAlgorithm::LRU,
            10 * 1024 * 1024,
        );

        // Nothing evicted yet: both counters start at zero.
        assert_eq!(manager.staging_evictions_total(), 0);
        assert_eq!(manager.staging_eviction_bytes_total(), 0);

        let cache_key = "test-bucket/staged-object";
        let object_size = 4096u64;
        seed_staged_object(temp_dir.path(), cache_key, object_size).await;

        // Historical note: the removed `evict_to_target` only walked candidates when
        // `current_size` exceeded
        // the target — it tracks in-flight/committed reservations, not what's
        // actually on disk. Reserve to match the object we just seeded so the
        // eviction path has something to reduce toward the target=0 below,
        // mirroring how `try_reserve`'s slow path arrives at this call. The
        // reservation is intentionally leaked (never dropped): eviction
        // releases the same bytes via `release_capacity_internal`, and
        // letting the `WriteReservation` also drop would release them a
        // second time, tripping the underflow-warning path for an unrelated
        // reason.
        let reservation = manager.try_reserve(object_size).await.unwrap();
        std::mem::forget(reservation);

        // Evict everything (target 0) via the public entry point, same path
        // `try_reserve`'s slow path uses.
        let freed = manager.evict_write_cached_object(cache_key).await.unwrap();
        assert_eq!(freed, object_size, "eviction should report bytes freed");

        assert_eq!(
            manager.staging_evictions_total(),
            1,
            "one object evicted should increment staging_evictions_total by exactly one"
        );
        assert_eq!(
            manager.staging_eviction_bytes_total(),
            object_size,
            "staging_eviction_bytes_total should equal the bytes actually freed"
        );

        // Evicting again with nothing left to evict must not move either counter.
        // Evicting the same key again: the `.meta` is gone now, so this reports an
        // error rather than a zero-byte success. Either way neither counter may move,
        // which is the property under test.
        let freed_again = manager
            .evict_write_cached_object(cache_key)
            .await
            .unwrap_or(0);
        assert_eq!(freed_again, 0);
        assert_eq!(manager.staging_evictions_total(), 1);
        assert_eq!(manager.staging_eviction_bytes_total(), object_size);
    }

    /// Build a `WriteCacheManager` with a real `JournalConsolidator` wired in, so the
    /// R5 accounting assertions below read the accumulator the production path uses.
    fn manager_with_consolidator(
        temp_dir: &TempDir,
    ) -> (
        WriteCacheManager,
        Arc<crate::journal_consolidator::JournalConsolidator>,
    ) {
        let consolidator = Arc::new(crate::journal_consolidator::JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            Arc::new(crate::journal_manager::JournalManager::new(
                temp_dir.path().to_path_buf(),
                "test-instance".to_string(),
            )),
            Arc::new(crate::metadata_lock_manager::MetadataLockManager::new(
                temp_dir.path().to_path_buf(),
                Duration::from_secs(30),
                3,
            )),
            crate::journal_consolidator::ConsolidationConfig::default(),
        ));
        let mut manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            1_000_000,
            10.0,
            Duration::from_secs(86400),
            Duration::from_secs(86400),
            CacheEvictionAlgorithm::LRU,
            10 * 1024 * 1024,
        );
        manager.set_journal_consolidator(consolidator.clone());
        (manager, consolidator)
    }

    /// R5.1: staging eviction must debit **both** accumulators by the range's
    /// `compressed_size`, so the Journal_Consolidator folds the reduction into
    /// Size_State. Before this, eviction adjusted only the per-instance in-memory
    /// counter, so `write_cache_size` ratcheted upward forever while data was being
    /// deleted — the R5 leak.
    ///
    /// Both totals move because a staged range counts in `total_size` **and** in
    /// `write_cache_size`; `write_cache_size` is a subset, not an addition.
    ///
    /// Asserting the accumulator (rather than `size_state.json`) is deliberate: the
    /// accumulator is the predicate the consolidator actually reads, and R5.3 forbids
    /// this path from writing `size_state.json` itself. `flush()` inside the eviction
    /// path resets the in-memory deltas to zero after writing a delta file, so these
    /// assertions read the delta file's contents rather than `current_delta()`.
    #[tokio::test]
    async fn staging_eviction_debits_both_accumulators() {
        let temp_dir = TempDir::new().unwrap();
        let (manager, consolidator) = manager_with_consolidator(&temp_dir);

        assert_eq!(consolidator.size_accumulator().current_delta(), 0);
        assert_eq!(
            consolidator.size_accumulator().current_write_cache_delta(),
            0
        );

        let cache_key = "test-bucket/staged-object";
        let object_size = 4096u64;
        seed_staged_object(temp_dir.path(), cache_key, object_size).await;

        let reservation = manager.try_reserve(object_size).await.unwrap();
        std::mem::forget(reservation);

        let freed = manager.evict_write_cached_object(cache_key).await.unwrap();
        assert_eq!(freed, object_size);

        // The eviction path flushes, so the deltas live in a delta file rather than in
        // the atomics. Sum what was written.
        let (flushed_delta, flushed_wc_delta) =
            consolidator.collect_and_apply_deltas().await.unwrap();

        assert_eq!(
            flushed_delta,
            -(object_size as i64),
            "R5.1: total_size must be debited by the range's compressed_size"
        );
        assert_eq!(
            flushed_wc_delta,
            -(object_size as i64),
            "R5.1: write_cache_size must be debited too — this is the half that was \
             missing, and its absence is why the figure only ever grew"
        );
    }

    /// R5.4: a range file that is already absent must NOT be debited. The accumulator
    /// debits and the Remove journal entries are both built from the list of files that
    /// existed and deleted cleanly, never from `metadata.ranges` — otherwise an entry
    /// whose `.bin` was already gone (evicted by the read tier, or lost) would be
    /// subtracted a second time, driving `write_cache_size` toward undershoot. Undershoot
    /// is the more dangerous direction because it silently over-admits.
    ///
    /// The fixture is the divergence that makes this observable: a `.meta` claiming a
    /// range whose `.bin` does not exist. A test that seeds both would pass whatever the
    /// code does.
    #[tokio::test]
    async fn staging_eviction_does_not_debit_for_an_absent_range_file() {
        let temp_dir = TempDir::new().unwrap();
        let (manager, consolidator) = manager_with_consolidator(&temp_dir);

        let cache_key = "test-bucket/staged-object";
        let object_size = 4096u64;
        seed_staged_object(temp_dir.path(), cache_key, object_size).await;

        // Delete the `.bin` but leave the `.meta` claiming it — the state R5.4 is about.
        let ranges_dir = temp_dir.path().join("ranges");
        let mut removed = 0;
        for entry in walkdir::WalkDir::new(&ranges_dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.path().extension().is_some_and(|x| x == "bin") {
                std::fs::remove_file(entry.path()).unwrap();
                removed += 1;
            }
        }
        assert_eq!(
            removed, 1,
            "precondition: exactly one .bin must have been removed, or this test is \
             asserting against a fixture that never had the divergence it needs"
        );

        let reservation = manager.try_reserve(object_size).await.unwrap();
        std::mem::forget(reservation);

        let freed = manager.evict_write_cached_object(cache_key).await.unwrap();
        assert_eq!(
            freed, 0,
            "no bytes were freed, because the file was not there"
        );

        let (flushed_delta, flushed_wc_delta) =
            consolidator.collect_and_apply_deltas().await.unwrap();
        assert_eq!(
            flushed_delta, 0,
            "R5.4: no debit for a range file that was already absent"
        );
        assert_eq!(flushed_wc_delta, 0, "R5.4: and none to write_cache_size");
    }

    /// Clear `is_write_cached` on an already-seeded `.meta`, the way a first read on any
    /// instance does. Used to reproduce the graduation race: the candidate was collected
    /// while staged, and graduated before it was deleted.
    async fn graduate_seeded_object(cache_dir: &std::path::Path, cache_key: &str) {
        let meta_path =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .expect("valid cache key");
        let content = tokio::fs::read_to_string(&meta_path).await.unwrap();
        let mut metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&content).unwrap();
        metadata.object_metadata.is_write_cached = false;
        metadata.object_metadata.write_cache_expires_at = None;
        metadata.object_metadata.write_cache_created_at = None;
        metadata.object_metadata.write_cache_last_accessed = None;
        tokio::fs::write(&meta_path, serde_json::to_string_pretty(&metadata).unwrap())
            .await
            .unwrap();
    }

    /// **Evicting an entry that graduated after it was collected must NOT debit
    /// `write_cache_size` a second time.**
    ///
    /// Candidate selection filters on `is_write_cached`, which makes it tempting
    /// to debit the write-cache figure unconditionally here — everything reaching
    /// `evict_write_cached_object` was staged when it was picked. But the filter runs at
    /// *collection* time and this function re-reads the `.meta`, so a first GET on any
    /// instance can clear the flag in between and append a `Graduation` entry that debits
    /// these same bytes. Debiting again is a double subtraction, and it drives
    /// `write_cache_size` toward **undershoot** — the direction that silently over-admits
    /// instead of refusing.
    ///
    /// The fixture is the divergence, and it is the whole test: a `.meta` whose flag was
    /// cleared between seeding and eviction. A test that evicted a still-staged entry
    /// would pass with the debit conditional or unconditional, which is why
    /// `staging_eviction_debits_both_accumulators` above cannot cover this.
    ///
    /// `total_size` must still be debited — the bytes did leave the disk.
    #[tokio::test]
    async fn staging_eviction_of_a_graduated_entry_debits_total_only() {
        let temp_dir = TempDir::new().unwrap();
        let (manager, consolidator) = manager_with_consolidator(&temp_dir);

        let cache_key = "test-bucket/graduated-then-evicted";
        let object_size = 4096u64;
        seed_staged_object(temp_dir.path(), cache_key, object_size).await;

        // The race: graduation lands after the candidate was collected, before deletion.
        graduate_seeded_object(temp_dir.path(), cache_key).await;

        // Call the evictor directly. This was already necessary before task 25 removed
        // the walk-based `evict_to_target`, whose candidate walk filtered on the flag: a
        // graduated entry was never collected and nothing would be deleted. The
        // ledger-driven evictor skips a graduated candidate for the same reason
        // (`StagedCandidateVerdict::Graduated`), so reaching this path still requires
        // calling the per-object evictor directly.
        let freed = manager
            .evict_write_cached_object(cache_key)
            .await
            .expect("eviction succeeds");
        assert_eq!(
            freed, object_size,
            "precondition: the entry really was deleted, so there is a debit to get wrong"
        );

        let (flushed_delta, flushed_wc_delta) =
            consolidator.collect_and_apply_deltas().await.unwrap();

        assert_eq!(
            flushed_delta,
            -(object_size as i64),
            "total_size must still be debited — the bytes left the disk"
        );
        assert_eq!(
            flushed_wc_delta, 0,
            "write_cache_size must NOT be debited: the entry graduated, so its \
             Graduation journal entry already removed these bytes from the figure. \
             Debiting here too is a double subtraction driving the figure toward \
             undershoot, which silently over-admits."
        );
    }

    /// R5.3, stated as an assertion rather than left to the comments: the eviction path
    /// must not subtract the freed bytes from Size_State's **size fields** directly. The
    /// debit must arrive only via the accumulator's delta file, applied under the global
    /// consolidation lock; a direct subtraction here would remove the bytes twice.
    ///
    /// # The predicate here was wrong once — do not "simplify" it back
    ///
    /// The first version of this test asserted that eviction does not create
    /// `size_state.json` at all. It failed, and it *should* have: eviction legitimately
    /// calls `decrement_cached_objects` for R5.2 (so `cached_objects` converges and the
    /// other instances' counts do not drift), and that persists the state file. Read-tier
    /// eviction does exactly the same thing.
    ///
    /// "Does not write the file" and "does not subtract sizes" look equivalent and are
    /// not. Asserting the former forbids behaviour the spec requires; asserting the
    /// latter is the property R5.3 is about. So this reads `total_size` and
    /// `write_cache_size` specifically, and deliberately permits `cached_objects` to move.
    #[tokio::test]
    async fn staging_eviction_does_not_subtract_sizes_from_size_state_directly() {
        let temp_dir = TempDir::new().unwrap();
        let (manager, consolidator) = manager_with_consolidator(&temp_dir);

        // Establish a known non-zero starting point, so a subtraction would be visible
        // as a change rather than being indistinguishable from the default of 0.
        consolidator
            .update_size_from_validation(100_000, Some(50_000), Some(7))
            .await;
        let before = consolidator.get_size_state().await;
        assert_eq!(before.total_size, 100_000, "precondition");
        assert_eq!(before.write_cache_size, 50_000, "precondition");

        let cache_key = "test-bucket/staged-object";
        let object_size = 4096u64;
        seed_staged_object(temp_dir.path(), cache_key, object_size).await;

        let reservation = manager.try_reserve(object_size).await.unwrap();
        std::mem::forget(reservation);
        let freed = manager.evict_write_cached_object(cache_key).await.unwrap();
        assert_eq!(freed, object_size, "precondition: the eviction did happen");

        let after = consolidator.get_size_state().await;
        assert_eq!(
            after.total_size, before.total_size,
            "R5.3: eviction must not subtract from total_size directly — the debit \
             belongs to the accumulator, applied under the global lock"
        );
        assert_eq!(
            after.write_cache_size, before.write_cache_size,
            "R5.3: nor from write_cache_size"
        );
        // And the one field it IS allowed to move, per R5.2.
        assert_eq!(
            after.cached_objects, 6,
            "R5.2: cached_objects converges, so other instances' counts do not drift"
        );
    }

    #[tokio::test]
    async fn test_try_reserve_basic() {
        let temp_dir = TempDir::new().unwrap();
        let manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            1024 * 1024, // 1MB total
            10.0,        // 10% = ~100KB max write cache
            Duration::from_secs(86400),
            Duration::from_secs(86400),
            CacheEvictionAlgorithm::LRU,
            256 * 1024 * 1024,
        );

        // Should succeed for small reservation
        let reservation = manager.try_reserve(1000).await;
        assert!(reservation.is_some());
        assert_eq!(manager.current_usage(), 1000);

        // Second reservation should also succeed
        let reservation2 = manager.try_reserve(1000).await;
        assert!(reservation2.is_some());
        assert_eq!(manager.current_usage(), 2000);

        // Drop first reservation — usage should decrease
        drop(reservation);
        assert_eq!(manager.current_usage(), 1000);

        // Drop second reservation
        drop(reservation2);
        assert_eq!(manager.current_usage(), 0);
    }

    /// The Staging_Bound admits past itself rather than refusing (R3.1, R3.3).
    ///
    /// This test previously asserted the **opposite** — that a reservation taking the
    /// in-flight total past the bound returns `None` — and it was correct to do so until
    /// task 28. It is inverted here rather than deleted, because "going over the bound
    /// still caches" is the headline behaviour change of Phase F and deserves a direct
    /// assertion, not just the absence of the old one.
    ///
    /// Why the inversion is the right way round: caching is worth doing whenever the space
    /// is affordable, and overshoot is now reclaimed by asynchronous eviction toward a
    /// target below the bound rather than prevented by refusing the upload. See
    /// [`WriteCacheManager::reserve_for_sizing`] for the full reasoning — including why the
    /// cost-asymmetry argument the spec gives for this ("refusing a PUT is a one-shot
    /// loss") is wrong, and what the actual justification is.
    #[tokio::test]
    async fn reservation_admits_past_the_staging_bound_instead_of_refusing() {
        let temp_dir = TempDir::new().unwrap();
        let manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            10_000, // 10KB total
            10.0,   // 10% = 1000 bytes Staging_Bound
            Duration::from_secs(86400),
            Duration::from_secs(86400),
            CacheEvictionAlgorithm::LRU,
            256 * 1024 * 1024,
        );
        assert_eq!(
            manager.max_size(),
            1000,
            "precondition: bound is 1000 bytes"
        );

        let r1 = manager.try_reserve(900).await;
        assert!(r1.is_some());
        assert_eq!(manager.current_usage(), 900);

        // 900 + 200 = 1100, past the 1000-byte bound. Admitted anyway.
        let r2 = manager.try_reserve(200).await;
        assert!(
            r2.is_some(),
            "the Staging_Bound must not refuse an upload (R3.1); going over it triggers \
             asynchronous eviction instead"
        );
        assert_eq!(
            manager.current_usage(),
            1100,
            "in-flight bytes are reported honestly even when over the bound, so \
             `over_bound` on /metrics can be true"
        );

        // Release still works and is exact.
        drop(r2);
        assert_eq!(manager.current_usage(), 900);
        drop(r1);
        assert_eq!(manager.current_usage(), 0);
    }

    /// R3.5 / task 30: the per-object cap is the one size-based refusal that remains, and
    /// it must be evaluated **before** anything else, so eviction is never attempted for
    /// an upload that could not be cached at any capacity.
    ///
    /// Asserted at an in-flight total of zero, where no capacity argument could explain a
    /// refusal, so a pass can only mean the object-size check fired.
    #[tokio::test]
    async fn max_object_size_refusal_survives_the_bound_becoming_a_target() {
        let temp_dir = TempDir::new().unwrap();
        let manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            1024 * 1024 * 1024, // 1 GiB total
            50.0,               // 512 MiB Staging_Bound — deliberately far above the cap
            Duration::from_secs(86400),
            Duration::from_secs(86400),
            CacheEvictionAlgorithm::LRU,
            1024, // 1 KiB max object size
        );

        assert_eq!(
            manager.current_usage(),
            0,
            "precondition: nothing in flight"
        );
        assert!(
            manager.try_reserve(1025).await.is_none(),
            "an object above max_object_size must be refused regardless of capacity"
        );
        assert_eq!(
            manager.current_usage(),
            0,
            "a refused object must not be counted as in-flight"
        );
        assert!(
            manager.try_reserve(1024).await.is_some(),
            "exactly at the cap is admissible"
        );
    }

    /// Design test 10: the counter-pinning regression guard.
    ///
    /// The outage this spec was opened for was not a refusal bug, it was a **pinning**
    /// bug. `current_size` was seeded from on-disk residency at startup
    /// (`initialize_from_scan_results`, removed by task 27) and nothing drained it, so
    /// every proxy came up at 157.61% of its allocation and refused every single-part
    /// PUT for the life of the process. Restarting did not help: the same persisted
    /// figure was re-read.
    ///
    /// Three properties, and it is the combination that pins the defect rather than any
    /// one of them:
    ///
    /// 1. A fresh manager holds **zero** in-flight bytes. A process with no uploads in
    ///    flight has no in-flight bytes; there is no state to recover.
    /// 2. Repeated refusals for a non-capacity reason do not accumulate. A refusal that
    ///    charged the counter would degrade the tier a little on every attempt, which is
    ///    the shape of the original defect arrived at incrementally instead of at
    ///    startup.
    /// 3. Admission still works afterwards. Without this the first two are satisfiable
    ///    by a manager that refuses everything and counts nothing.
    ///
    /// Deliberately driven through `max_object_size`, which is evaluated ahead of every
    /// capacity consideration (task 30), so the loop cannot be confused with a capacity
    /// refusal — after Phase F there is no capacity refusal left to confuse it with, and
    /// this test should keep passing precisely because the reason is not capacity.
    ///
    /// Shown failing first by restoring a seeded counter: constructing the manager and
    /// then charging it a non-zero starting figure reds assertion 1 and, because the
    /// figure never drains, assertion 3 as well.
    #[tokio::test]
    async fn refusals_do_not_accumulate_in_flight_bytes_over_repeated_attempts() {
        let temp_dir = TempDir::new().unwrap();
        let manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            1024 * 1024 * 1024, // 1 GiB total
            10.0,               // ~102 MiB Staging_Bound
            Duration::from_secs(86400),
            Duration::from_secs(86400),
            CacheEvictionAlgorithm::LRU,
            1024, // 1 KiB max object size
        );

        // 1. A fresh process starts at zero. This is what task 27 restored by deleting
        //    the scan-results seeding, and it is the whole of the fix for the outage.
        assert_eq!(
            manager.current_usage(),
            0,
            "a fresh manager must hold no in-flight bytes: nothing is in flight"
        );

        // 2. Twenty refusals, all for the same non-capacity reason.
        for attempt in 1..=20 {
            assert!(
                manager.try_reserve(2048).await.is_none(),
                "attempt {attempt}: an object above max_object_size must be refused"
            );
            assert_eq!(
                manager.current_usage(),
                0,
                "attempt {attempt}: a refused object must not be charged. A counter that \
                 crept upward here would reproduce the original defect incrementally \
                 rather than at startup"
            );
        }

        // 3. And the tier is still usable, which is what makes the two assertions above
        //    mean something. A manager that refused everything would satisfy them both.
        let reservation = manager
            .try_reserve(1024)
            .await
            .expect("an admissible object must still be admitted after 20 refusals");
        assert_eq!(manager.current_usage(), 1024);
        drop(reservation);
        assert_eq!(
            manager.current_usage(),
            0,
            "and the admitted bytes must drain on release, or the counter is pinned \
             again by a slower route"
        );
    }

    #[tokio::test]
    async fn test_try_reserve_exceeds_max_object_size() {
        let temp_dir = TempDir::new().unwrap();
        let manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            1024 * 1024 * 1024, // 1GB total
            10.0,               // 10% = 100MB max write cache
            Duration::from_secs(86400),
            Duration::from_secs(86400),
            CacheEvictionAlgorithm::LRU,
            1024 * 1024, // 1MB max object size
        );

        // Object larger than max_object_size should be rejected
        let reservation = manager.try_reserve(2 * 1024 * 1024).await;
        assert!(reservation.is_none());
        assert_eq!(manager.current_usage(), 0);
    }

    #[tokio::test]
    async fn test_reservation_drop_releases_capacity() {
        let temp_dir = TempDir::new().unwrap();
        let manager = WriteCacheManager::new_with_defaults(
            temp_dir.path().to_path_buf(),
            1024 * 1024 * 1024, // 1GB total
        );

        {
            let _r = manager.try_reserve(5000).await.unwrap();
            assert_eq!(manager.current_usage(), 5000);
            // _r drops here
        }

        assert_eq!(manager.current_usage(), 0);
    }

    #[tokio::test]
    async fn test_reservation_saturating_release() {
        // Test that releasing more than current_size saturates to 0
        let current_size = Arc::new(AtomicU64::new(50));

        let reservation = WriteReservation {
            size: 100, // More than current_size
            current_size: current_size.clone(),
        };

        drop(reservation);

        // Should saturate to 0, not underflow
        assert_eq!(current_size.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_capacity_percent_clamping() {
        let temp_dir = TempDir::new().unwrap();

        // Test below minimum (1%)
        let manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            1000,
            0.5, // Below 1%
            Duration::from_secs(86400),
            Duration::from_secs(86400),
            CacheEvictionAlgorithm::LRU,
            256 * 1024 * 1024,
        );
        assert_eq!(manager.max_size(), 10); // 1% of 1000

        // Test above maximum (50%)
        let manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            1000,
            75.0, // Above 50%
            Duration::from_secs(86400),
            Duration::from_secs(86400),
            CacheEvictionAlgorithm::LRU,
            256 * 1024 * 1024,
        );
        assert_eq!(manager.max_size(), 500); // 50% of 1000
    }

    /// Concurrent reservations must account **exactly**, which is the property that
    /// survives the bound becoming a target.
    ///
    /// This test used to be `test_concurrent_reservations_never_exceed_capacity` and its
    /// final assertion was `total_reserved <= max_size`. That assertion is now false by
    /// design (R3.1), so it is replaced rather than relaxed: what the CAS loop is still
    /// responsible for is that every admitted reservation is counted once and no update
    /// is lost, which is a strictly sharper claim than the inequality it replaces —
    /// the old bound could be satisfied by silently dropping reservations.
    #[tokio::test]
    async fn concurrent_reservations_account_exactly() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            10_000, // 10KB total
            100.0,  // Clamped to 50% = 5000 bytes Staging_Bound
            Duration::from_secs(86400),
            Duration::from_secs(86400),
            CacheEvictionAlgorithm::LRU,
            5000,
        ));

        let bound = manager.max_size();
        let mut handles = Vec::new();
        for _ in 0..50 {
            let mgr = manager.clone();
            handles.push(tokio::spawn(async move { mgr.try_reserve(100).await }));
        }

        let results: Vec<_> = futures::future::join_all(handles).await;
        let successful: Vec<_> = results
            .into_iter()
            .filter_map(|r| r.ok().flatten())
            .collect();

        // Every attempt is admitted: 100 bytes is under `max_object_size` and the bound
        // no longer refuses. This is the behaviour change, asserted head-on.
        assert_eq!(
            successful.len(),
            50,
            "no reservation may be refused for capacity now that the bound is a target"
        );

        // No lost updates under concurrency: the counter equals the sum of what was handed out.
        let total_reserved: u64 = successful.iter().map(|r| r.size()).sum();
        assert_eq!(manager.current_usage(), total_reserved);
        assert_eq!(total_reserved, 5000);

        // And it genuinely went past the bound, so this is not passing by accident of a
        // fixture that happened to stay under it.
        assert!(
            total_reserved >= bound,
            "fixture must actually reach the bound to be testing anything: \
             reserved={total_reserved}, bound={bound}"
        );

        drop(successful);
        assert_eq!(manager.current_usage(), 0);
    }
}

// ============================================================================
// Property-Based Tests
// ============================================================================

#[cfg(test)]
mod property_tests {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    /// Property 10: Write cache capacity enforcement with eviction
    ///
    /// *For any* sequence of PUT requests within `max_object_size`, every request is
    /// admitted and the in-flight total equals exactly the sum of what is outstanding.
    ///
    /// **Restated for task 28.** The original property was "if usage plus request size
    /// exceeds the write cache capacity, evict first; if eviction cannot free enough,
    /// bypass caching", asserted as `current_usage() <= max_write_cache` at every step.
    /// Both halves are now wrong by design: the Staging_Bound does not refuse (R3.1) and
    /// eviction is neither inline nor synchronous with admission (R3.2).
    ///
    /// The property that replaces it is about the accounting rather than the gate, and it
    /// is sharper: the old inequality could be satisfied by refusing everything, whereas
    /// exact accounting cannot. Capacity behaviour proper is now a property of
    /// `CacheManager::evict_staging_tier`, which is where the bound is interpreted.
    ///
    /// **Validates: Requirements 3.1, 3.3, 3.5**
    #[quickcheck]
    fn prop_write_cache_capacity_enforcement(
        total_cache_mb: u8,
        write_percent: u8,
        request_sizes_kb: Vec<u16>,
    ) -> TestResult {
        // Filter invalid inputs
        if total_cache_mb == 0 || write_percent == 0 || request_sizes_kb.is_empty() {
            return TestResult::discard();
        }

        // Limit test size
        if request_sizes_kb.len() > 20 {
            return TestResult::discard();
        }

        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();

            // Calculate sizes
            let total_cache_size = (total_cache_mb as u64) * 1024 * 1024;
            let write_percent_clamped = (write_percent as f32).clamp(1.0, 50.0);
            let max_write_cache =
                ((total_cache_size as f64) * (write_percent_clamped as f64 / 100.0)) as u64;

            let manager = WriteCacheManager::new(
                temp_dir.path().to_path_buf(),
                total_cache_size,
                write_percent_clamped,
                Duration::from_secs(86400),
                Duration::from_secs(86400),
                CacheEvictionAlgorithm::LRU,
                max_write_cache, // Max object size = max write cache for this test
            );

            // Track active reservations
            let mut active_reservations: Vec<WriteReservation> = Vec::new();

            // Process each request
            for size_kb in &request_sizes_kb {
                let request_size = (*size_kb as u64) * 1024;

                // Skip if request is larger than max object size
                if request_size > manager.max_object_size() {
                    continue;
                }

                match manager.try_reserve(request_size).await {
                    Some(reservation) => {
                        active_reservations.push(reservation);
                    }
                    None => {
                        // Only reachable for an object above `max_object_size`, which the
                        // loop already skipped, or u64 overflow, which these sizes cannot
                        // reach. A refusal here means the bound is gating again.
                        return TestResult::failed();
                    }
                }

                // Exact accounting: the counter equals the sum of what is outstanding,
                // whether or not that is over the bound.
                let expected: u64 = active_reservations.iter().map(|r| r.size()).sum();
                if manager.current_usage() != expected {
                    return TestResult::failed();
                }
            }

            // After dropping all reservations, usage should be 0
            drop(active_reservations);
            if manager.current_usage() != 0 {
                return TestResult::failed();
            }

            // `max_write_cache` is deliberately not asserted against: exceeding it is
            // permitted now, and eviction rather than admission is what brings it back.
            let _ = max_write_cache;

            TestResult::passed()
        })
    }

    /// Property 9: Monotone non-underflowing capacity
    ///
    /// *For any* generated interleaved reserve/release trace, assert that
    /// `current_size` is always in the range `[0, capacity]` at every step,
    /// that after all reservations are dropped `current_size == 0`, and that
    /// no panics occur regardless of the trace.
    ///
    /// The trace is modeled as a sequence of operations:
    /// - `Reserve(size)` — attempt to reserve `size` bytes
    /// - `Release(index)` — drop the reservation at `index` in the active list
    ///
    /// **Validates: Requirements 9.1, 9.2, 9.3, 9.4**
    #[quickcheck]
    fn prop_write_cache_monotone_capacity(capacity_kb: u16, ops: Vec<(bool, u16)>) -> TestResult {
        // Filter invalid inputs: need a non-zero capacity and at least one operation
        if capacity_kb == 0 || ops.is_empty() {
            return TestResult::discard();
        }

        // Limit trace length to keep tests fast
        if ops.len() > 50 {
            return TestResult::discard();
        }

        let capacity = (capacity_kb as u64) * 1024;
        // max_object_size = capacity (allow any single reservation up to full capacity)
        let max_object_size = capacity;

        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();

            // Create manager with exact capacity (100% write cache of total = capacity)
            // We set total_cache_size = capacity * 2 and write_cache_percent = 50%
            // so max_size = capacity
            let manager = WriteCacheManager::new(
                temp_dir.path().to_path_buf(),
                capacity * 2, // total cache
                50.0,         // 50% → max_size = capacity
                Duration::from_secs(86400),
                Duration::from_secs(86400),
                CacheEvictionAlgorithm::LRU,
                max_object_size,
            );

            // Verify our capacity calculation
            assert_eq!(manager.max_size(), capacity);

            let mut active_reservations: Vec<WriteReservation> = Vec::new();

            for (is_reserve, value) in &ops {
                if *is_reserve {
                    // Reserve operation: attempt to reserve `value` KB
                    let size = (*value as u64) * 1024;
                    if size == 0 {
                        continue; // skip zero-size reserves
                    }
                    if let Some(reservation) = manager.try_reserve(size).await {
                        active_reservations.push(reservation);
                    }
                    // A refusal is possible only for `size > max_object_size`, which this
                    // fixture sets equal to `capacity`, so a large generated value can
                    // still be refused. Not asserted either way here — this property is
                    // about the counter tracking the outstanding set exactly.
                } else {
                    // Release operation: drop the reservation at index `value % len`
                    if !active_reservations.is_empty() {
                        let idx = (*value as usize) % active_reservations.len();
                        active_reservations.swap_remove(idx);
                    }
                }

                // Invariant, restated for task 28: the counter equals the sum of the
                // outstanding reservations at every step. The previous invariant was
                // `current <= capacity`, which is no longer true by design — the
                // Staging_Bound admits past itself (R3.1) — and was in any case weaker,
                // since dropping an update silently would satisfy it.
                let expected: u64 = active_reservations.iter().map(|r| r.size()).sum();
                if manager.current_usage() != expected {
                    return TestResult::failed();
                }
            }
            // `capacity` is no longer an upper bound on in-flight bytes.
            let _ = capacity;

            // After dropping all reservations, current_size must be 0
            drop(active_reservations);
            let final_usage = manager.current_usage();
            if final_usage != 0 {
                return TestResult::failed();
            }

            TestResult::passed()
        })
    }

    /// **Feature: write-through-cache-finalization, Property 8: Incomplete upload eviction**
    /// *For any* multipart upload that exceeds the incomplete upload TTL without completion,
    /// all cached parts and tracking metadata SHALL be actively removed.
    /// **Validates: Requirements 4.2, 4.3**
    #[quickcheck]
    fn prop_incomplete_upload_eviction(part_count: u8, ttl_seconds: u8) -> TestResult {
        // Filter out invalid inputs
        let part_count = (part_count % 5) + 1; // 1-5 parts
        let ttl_seconds = (ttl_seconds % 5) + 1; // 1-5 seconds TTL for testing

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();

            // Create manager with short TTL for testing
            let manager = WriteCacheManager::new(
                temp_dir.path().to_path_buf(),
                100 * 1024 * 1024,                       // 100MB total
                10.0,                                    // 10% write cache
                Duration::from_secs(86400),              // 1 day write TTL
                Duration::from_secs(ttl_seconds as u64), // Short incomplete upload TTL
                crate::cache::CacheEvictionAlgorithm::LRU,
                256 * 1024 * 1024, // 256MB max object
            );

            let upload_id = "test-upload-eviction";
            let cache_key = "test-bucket/test-object-eviction";

            // Create multipart upload directory and tracker
            let mpus_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
            tokio::fs::create_dir_all(&mpus_dir).await.unwrap();

            // Create part files in the upload directory and tracker
            let mut parts = Vec::new();
            for part_num in 1..=part_count {
                let part_data: Vec<u8> = (0..1024).map(|i| (i + part_num as usize) as u8).collect();
                let part_path = mpus_dir.join(format!("part{}.bin", part_num));

                tokio::fs::write(&part_path, &part_data).await.unwrap();

                parts.push(crate::cache_types::CachedPartInfo {
                    part_number: part_num as u32,
                    size: 1024,
                    etag: format!("\"etag-{}\"", part_num),
                    compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
                });
            }

            // Create tracker with old started_at time
            let tracker = crate::cache_types::MultipartUploadTracker {
                upload_id: upload_id.to_string(),
                cache_key: cache_key.to_string(),
                started_at: std::time::SystemTime::now() - Duration::from_secs(3600), // 1 hour ago
                parts,
                total_size: (part_count as u64) * 1024,
                content_type: None,
            };

            let tracker_json = serde_json::to_string_pretty(&tracker).unwrap();
            let upload_meta_path = mpus_dir.join("upload.meta");
            tokio::fs::write(&upload_meta_path, &tracker_json)
                .await
                .unwrap();

            // Set file mtime to be older than TTL
            // We need to wait for the TTL to expire based on file mtime
            // For testing, we'll use filetime crate to set mtime in the past
            let old_time =
                std::time::SystemTime::now() - Duration::from_secs((ttl_seconds as u64) + 10);
            let old_filetime = filetime::FileTime::from_system_time(old_time);
            filetime::set_file_mtime(&upload_meta_path, old_filetime).unwrap();
            // Backdate the DIRECTORY too, not just `upload.meta`.
            //
            // The sweep judges an upload abandoned from the newest of the two, because
            // `upload.meta` is written once at CreateMultipartUpload and is therefore
            // the upload's START time — parts write their own records rather than
            // rewriting it. Backdating only the file would leave the directory looking
            // freshly active, which is what a live long-running upload looks like and
            // is exactly the case the sweep must NOT evict. A genuinely abandoned
            // upload has no recent activity in its directory either, so backdating
            // both is the faithful fixture.
            filetime::set_file_mtime(&mpus_dir, old_filetime).unwrap();

            // Verify parts exist before eviction
            for part in &tracker.parts {
                let part_path = mpus_dir.join(format!("part{}.bin", part.part_number));
                if !part_path.exists() {
                    return TestResult::failed();
                }
            }

            // Verify tracker exists
            if !upload_meta_path.exists() {
                return TestResult::failed();
            }

            // Run eviction
            let freed = manager.evict_incomplete_uploads().await.unwrap();

            // Verify bytes were freed
            if freed == 0 {
                return TestResult::failed();
            }

            // Verify tracking metadata and all parts are deleted (directory removed)
            if mpus_dir.exists() {
                return TestResult::failed();
            }

            TestResult::passed()
        })
    }
}
