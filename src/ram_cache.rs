//! RAM Cache Module
//!
//! Provides in-memory caching with configurable eviction algorithms (LRU, TinyLFU).
//! Integrates with compression system for memory efficiency and serves as first-tier cache.
//!
//! Note: Access tracking for disk metadata updates is now handled by the journal system
//! (CacheHitUpdateBuffer) at the DiskCacheManager level, not in the RAM cache.

use crate::cache::{CacheEvictionAlgorithm, RamCacheEntry, RamCacheRead};
use crate::cache_types::CacheMetadata;
use crate::Result;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Hardcoded RAM cache admission ceiling: 64 MiB.
///
/// A compile-time constant, not a config parameter. `ShardedRamCache::new`
/// clamps the effective shard count so that `per_shard_capacity` is always
/// at least this many bytes, guaranteeing that any single entry up to this
/// size is admitted to the RAM cache rather than silently dropped
/// (`put()` drops an entry when `entry_size > per_shard_capacity`). This
/// applies unconditionally, independent of whether page-aligned range
/// caching is enabled for any key.
///
/// Spec: page-aligned-range-cache Requirement 7.7.
pub const RAM_CACHE_ADMISSION_CEILING: usize = 64 * 1024 * 1024;

/// Compute the shard index for a cache key.
///
/// Uses the first 8 bytes of the BLAKE3 hash as a `u64` (little-endian) and
/// reduces modulo `shard_count`. This gives a uniform, deterministic mapping
/// from key to shard with no additional hashing state.
pub fn shard_index(cache_key: &str, shard_count: usize) -> usize {
    let hash = blake3::hash(cache_key.as_bytes());
    let h = u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap());
    (h % shard_count as u64) as usize
}

/// Per-shard eviction state: holds all LRU/TinyLFU counters for one shard.
///
/// Each `RamCacheShard` owns an independent copy of the eviction bookkeeping
/// with no sharing between shards.
pub(crate) struct EvictionState {
    /// Which algorithm this shard uses.
    pub eviction_algorithm: CacheEvictionAlgorithm,
    /// LRU recency queue (front = least-recently used).
    pub lru_order: VecDeque<String>,
}

impl EvictionState {
    /// Create a new eviction state for the given algorithm.
    ///
    /// TinyLFU no longer needs any window/frequency state here: victim scoring
    /// (`shard_find_tinylfu_victim`) reads `access_count`/`last_accessed`
    /// directly from each entry's atomics at eviction time.
    pub fn new(eviction_algorithm: CacheEvictionAlgorithm) -> Self {
        Self {
            eviction_algorithm,
            lru_order: VecDeque::new(),
        }
    }
}

/// One shard of the sharded RAM cache.
///
/// `ShardedRamCache` owns a `Vec<tokio::sync::RwLock<RamCacheShard>>`.  Each
/// shard is independently locked, so operations on different keys in different
/// shards can proceed concurrently without contention.
///
/// Fields:
/// - `data` — the cache entries stored in this shard.
/// - `eviction` — per-shard LRU/TinyLFU counters.
/// - `current_size` — sum of entry sizes currently stored (bytes).
/// - `capacity` — maximum bytes this shard may hold (= total_capacity / shard_count).
/// - `pending_accesses` — deferred-reorder buffer: keys pushed here by `get()`
///   under a shared read lock; drained and applied to `eviction` by the next
///   `put()` call that holds the write lock.
pub(crate) struct RamCacheShard {
    /// Cache entries for this shard.
    pub data: HashMap<String, RamCacheEntry>,
    /// Eviction algorithm state (LRU order / TinyLFU counters) for this shard.
    pub eviction: EvictionState,
    /// Total byte size of all entries currently in `data`.
    pub current_size: usize,
    /// Maximum byte capacity for this shard (total_capacity / shard_count).
    pub capacity: usize,
    /// Deferred access-reorder buffer drained by the next `put()` write lock.
    pub pending_accesses: Vec<String>,
    /// Number of entries evicted from this shard under capacity pressure.
    /// Incremented under the write lock in `put()`; read under the read lock
    /// in `stats()`. Spec: compression-followup-fixes Requirement 3.
    pub eviction_count: u64,
    /// Unix-millis timestamp of the most recent eviction (`0` = never evicted).
    pub last_eviction_ms: u64,
}

impl RamCacheShard {
    /// Create a new, empty shard with the given capacity and eviction algorithm.
    pub fn new(capacity: usize, eviction_algorithm: CacheEvictionAlgorithm) -> Self {
        Self {
            data: HashMap::new(),
            eviction: EvictionState::new(eviction_algorithm),
            current_size: 0,
            capacity,
            pending_accesses: Vec::new(),
            eviction_count: 0,
            last_eviction_ms: 0,
        }
    }
}

/// Sharded RAM cache with per-shard `tokio::sync::RwLock` and aggregate atomic stats.
///
/// Keys are routed to shards via `shard_index(key, shard_count)` (BLAKE3 hash % shard_count).
/// Concurrent reads for keys in different shards proceed with no contention; concurrent reads
/// of the *same* key in the same shard share a read lock (no write lock required).
///
/// Aggregate hit/miss counters live here (not per-shard) so they can be read with two
/// atomic loads rather than acquiring all shard locks.
pub struct ShardedRamCache {
    /// Per-shard storage and eviction state, each independently locked.
    shards: Vec<RwLock<RamCacheShard>>,
    /// Number of shards (mirrors `shards.len()`; stored for O(1) access).
    shard_count: usize,
    /// Total cache hits across all shards (updated atomically under a read lock).
    hit_count: AtomicU64,
    /// Total cache misses across all shards (updated atomically under a read lock).
    miss_count: AtomicU64,
}

impl ShardedRamCache {
    /// Create a new sharded RAM cache.
    ///
    /// `total_capacity` is divided across an **effective shard count** that is
    /// clamped so that `per_shard_capacity` never falls below
    /// [`RAM_CACHE_ADMISSION_CEILING`] (64 MiB). This guarantees, unconditionally
    /// and regardless of whether page-aligned range caching is enabled for any
    /// key, that any single entry up to 64 MiB is admitted to the RAM cache
    /// rather than silently dropped by `put()` (which drops an entry when
    /// `entry_size > per_shard_capacity`).
    ///
    /// `effective_shard_count = min(shard_count, max(1, total_capacity / RAM_CACHE_ADMISSION_CEILING))`
    ///
    /// When the effective shard count is clamped below the requested
    /// `shard_count`, a warning is logged naming the 64 MiB ceiling and
    /// suggesting a larger `max_ram_cache_size`, since concurrency (shard
    /// count) was reduced to honour the admission guarantee.
    ///
    /// All shards use `eviction_algorithm` for their independent LRU/TinyLFU
    /// bookkeeping.
    ///
    /// Spec: page-aligned-range-cache Requirements 7.7, 7.8.
    pub fn new(
        total_capacity: usize,
        shard_count: usize,
        eviction_algorithm: CacheEvictionAlgorithm,
    ) -> Self {
        let shard_count = shard_count.max(1);
        let effective_shard_count =
            shard_count.min((total_capacity / RAM_CACHE_ADMISSION_CEILING).max(1));

        if effective_shard_count < shard_count {
            warn!(
                "ShardedRamCache: reducing RAM cache concurrency from {} to {} shards to honour \
                 the 64 MiB ({} byte) RAM_CACHE_ADMISSION_CEILING (total_capacity = {} bytes). \
                 Raise max_ram_cache_size to restore the configured shard count.",
                shard_count, effective_shard_count, RAM_CACHE_ADMISSION_CEILING, total_capacity
            );
        }

        let per_shard_capacity = (total_capacity / effective_shard_count).max(1);

        let shards = (0..effective_shard_count)
            .map(|_| {
                RwLock::new(RamCacheShard::new(
                    per_shard_capacity,
                    eviction_algorithm.clone(),
                ))
            })
            .collect();

        Self {
            shards,
            shard_count: effective_shard_count,
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
        }
    }

    /// Test-only constructor that bypasses the [`RAM_CACHE_ADMISSION_CEILING`]
    /// shard clamp, using exactly `shard_count` shards regardless of
    /// `total_capacity`.
    ///
    /// Production code and the admission-ceiling tests must go through
    /// [`ShardedRamCache::new`], which is where the ceiling guarantee (7.7,
    /// 7.8) is enforced. This helper exists only so pre-existing sharding-
    /// mechanics tests (shard routing, per-shard eviction, concurrent reads
    /// across shards) can exercise a specific shard count at the tiny byte-
    /// scale capacities those tests use, which the real clamp would otherwise
    /// always collapse to 1 shard (any capacity under `shard_count * 64 MiB`
    /// clamps).
    #[cfg(test)]
    fn new_unclamped_for_test(
        total_capacity: usize,
        shard_count: usize,
        eviction_algorithm: CacheEvictionAlgorithm,
    ) -> Self {
        let shard_count = shard_count.max(1);
        let per_shard_capacity = (total_capacity / shard_count).max(1);

        let shards = (0..shard_count)
            .map(|_| {
                RwLock::new(RamCacheShard::new(
                    per_shard_capacity,
                    eviction_algorithm.clone(),
                ))
            })
            .collect();

        Self {
            shards,
            shard_count,
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
        }
    }

    /// Return the total byte capacity across all shards.
    ///
    /// Because `per_shard_capacity = total_capacity / shard_count` (integer
    /// division), the sum may be slightly less than the original `total_capacity`
    /// argument.  Callers that need to report cache size should use this method
    /// rather than the original argument.
    pub fn max_size(&self) -> usize {
        // All shards were constructed with the same capacity, so multiplying is
        // equivalent to iterating — and avoids acquiring any shard locks.
        if self.shard_count == 0 {
            return 0;
        }
        // Read capacity from the first shard without holding the lock for long.
        // SAFETY: shard_count ≥ 1 is guaranteed by the constructor.
        //
        // We use try_read() which succeeds immediately when no writer holds the
        // lock.  If a writer is active (extremely unlikely at stats time) we fall
        // back to the shard_count * (total / shard_count) approximation — which
        // is exactly what the constructor computed anyway.
        let first_capacity = self.shards[0].try_read().map(|g| g.capacity).unwrap_or(0);
        first_capacity * self.shard_count
    }

    /// Look up a key in the cache.
    ///
    /// Acquires a **shared** read lock on one shard so concurrent reads of the
    /// same key (or keys in different shards) proceed without contention.
    ///
    /// On hit:
    /// - Updates `last_accessed` and `access_count` via atomic stores (no write
    ///   lock needed — the atomics are reachable through `&RamCacheEntry`).
    /// - Clones the `Arc<Bytes>` handle (O(1), no data copy) and builds a
    ///   `RamCacheRead` view.
    /// - Drops the read lock before returning (the `Arc` keeps the data alive
    ///   independently of the lock).
    /// - Increments the aggregate `hit_count` atomic.
    /// - On a sampled fraction of reads (`access_count % 8 == 0`) attempts a
    ///   non-blocking `try_write()` on the shard to push the key onto
    ///   `pending_accesses` for deferred LRU/TinyLFU reordering.  If the
    ///   write lock is contended the push is skipped — this is best-effort.
    ///
    /// On miss:
    /// - Drops the read lock immediately.
    /// - Increments the aggregate `miss_count` atomic.
    /// - Returns `None`.
    ///
    /// _Requirements: 1.4, 2.2, 3.2, 4.1, 4.3_
    pub async fn get(&self, key: &str) -> Option<RamCacheRead> {
        let idx = shard_index(key, self.shard_count);
        let shard = &self.shards[idx];

        // Shared read lock — multiple concurrent readers of the same shard
        // proceed simultaneously.
        let guard = shard.read().await;

        let entry = guard.data.get(key);

        match entry {
            None => {
                // Release read lock before touching atomics (keeps lock hold minimal).
                drop(guard);
                self.miss_count.fetch_add(1, Ordering::Relaxed);
                debug!("ShardedRamCache miss for key: {}", key);
                None
            }
            Some(entry) => {
                // Update access metadata atomically — no write lock needed.
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                entry.last_accessed.store(now_ms, Ordering::Relaxed);
                let new_count = entry.access_count.fetch_add(1, Ordering::Relaxed) + 1;

                // Clone Arc<Bytes> — O(1) reference-count increment, no data copy.
                let read = RamCacheRead {
                    data: entry.data.clone(),
                    metadata: entry.metadata.clone(),
                    compressed: entry.compressed,
                    compression_algorithm: entry.compression_algorithm.clone(),
                };

                // Read the eviction algorithm before dropping the read guard —
                // needed below to gate the deferred-reorder block to LRU only.
                let eviction_algorithm = guard.eviction.eviction_algorithm.clone();

                // Release the read lock before any write-lock attempt.
                // The Arc clone keeps the data alive independently.
                drop(guard);

                self.hit_count.fetch_add(1, Ordering::Relaxed);
                debug!("ShardedRamCache hit for key: {}", key);

                // Sampled deferred reorder: on every ~8th access attempt a
                // non-blocking write lock to push the key onto pending_accesses.
                // If the lock is already held (contended write), skip silently —
                // best-effort; the next put() will drain whatever was recorded.
                //
                // Only meaningful for LRU: TinyLFU victim scoring reads
                // `access_count`/`last_accessed` atomics directly at eviction
                // time and needs no per-access ordering structure.
                if new_count % 8 == 0 && eviction_algorithm == CacheEvictionAlgorithm::LRU {
                    if let Ok(mut write_guard) = shard.try_write() {
                        write_guard.pending_accesses.push(key.to_string());
                    }
                }

                Some(read)
            }
        }
    }

    /// Store an entry in the sharded cache.
    ///
    /// Acquires an **exclusive write lock** on the shard that owns `entry.cache_key`.
    /// While holding the write lock the method:
    ///
    /// 1. **Drains `pending_accesses`** — applies deferred LRU/TinyLFU reordering for
    ///    all keys that were recorded by recent `get()` calls under a read lock.  This
    ///    keeps eviction ordering approximately up-to-date without requiring every `get()`
    ///    to take the write lock.
    /// 2. **Evicts** entries until the shard has room for the new entry
    ///    (`current_size + entry_size ≤ capacity`).  Eviction uses the same LRU or
    ///    TinyLFU victim-selection logic as `RamCache::evict_entry`.
    /// 3. **Inserts** the new entry, updates `current_size`, and adds the key to the
    ///    eviction tracking structures.
    ///
    /// If an entry with the same key already exists it is replaced: its old size is
    /// subtracted from `current_size` and it is removed from tracking before the new
    /// entry is inserted.
    ///
    /// If the entry alone exceeds the shard capacity it is silently dropped (same
    /// behaviour as `RamCache::put`).
    ///
    /// _Requirements: 1.5, 1.6, 3.3, 4.4, 4.5_
    pub async fn put(&self, entry: RamCacheEntry) -> Result<()> {
        let cache_key = entry.cache_key.clone();
        let idx = shard_index(&cache_key, self.shard_count);
        let mut guard = self.shards[idx].write().await;

        // Step 1: drain pending_accesses and apply deferred LRU/TinyLFU reordering.
        // We collect first to avoid a borrow-split issue while calling
        // shard_update_access_tracking(&mut guard, ...).
        let pending: Vec<String> = guard.pending_accesses.drain(..).collect();
        for key in pending {
            // Only update ordering for keys that still exist in the shard.
            if guard.data.contains_key(&key) {
                shard_update_access_tracking(&mut guard, &key);
            }
        }

        // Step 2: calculate the size of the new entry.
        let entry_size = shard_calculate_entry_size(&entry);

        // If the entry alone is larger than the full shard capacity, drop it silently
        // (consistent with RamCache::put).
        if entry_size > guard.capacity {
            warn!(
                "ShardedRamCache: entry {} too large for shard ({} bytes > {} bytes capacity), dropping",
                cache_key, entry_size, guard.capacity
            );
            return Ok(());
        }

        // Step 3: evict until there is room for the new entry.
        while guard.current_size + entry_size > guard.capacity && !guard.data.is_empty() {
            let victim = match guard.eviction.eviction_algorithm {
                CacheEvictionAlgorithm::LRU => shard_find_lru_victim(&guard),
                CacheEvictionAlgorithm::TinyLFU => shard_find_tinylfu_victim(&guard),
            };

            if let Some(victim_key) = victim {
                if let Some(evicted) = guard.data.remove(&victim_key) {
                    let evicted_size = shard_calculate_entry_size(&evicted);
                    guard.current_size = guard.current_size.saturating_sub(evicted_size);
                    shard_remove_from_tracking(&mut guard, &victim_key);
                    // Track eviction for observability (Spec:
                    // compression-followup-fixes Requirement 3). Under the
                    // write lock, so plain-field updates are race-free.
                    guard.eviction_count += 1;
                    guard.last_eviction_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    debug!(
                        "ShardedRamCache evicted key {} ({} bytes) from shard {}",
                        victim_key, evicted_size, idx
                    );
                } else {
                    // Victim key was in tracking but not in data — remove from tracking
                    // and break to avoid an infinite loop.
                    shard_remove_from_tracking(&mut guard, &victim_key);
                    break;
                }
            } else {
                // No victim found (e.g. data is empty) — stop.
                break;
            }
        }

        // Step 4: remove any existing entry for this key before inserting.
        if let Some(existing) = guard.data.remove(&cache_key) {
            let existing_size = shard_calculate_entry_size(&existing);
            guard.current_size = guard.current_size.saturating_sub(existing_size);
            shard_remove_from_tracking(&mut guard, &cache_key);
        }

        // Step 5: insert the new entry.
        guard.current_size += entry_size;
        guard.data.insert(cache_key.clone(), entry);
        shard_add_to_tracking(&mut guard, &cache_key);

        debug!(
            "ShardedRamCache stored key {} ({} bytes) in shard {} (shard utilization: {}/{})",
            cache_key, entry_size, idx, guard.current_size, guard.capacity
        );

        Ok(())
    }

    /// Invalidate a single key from the cache.
    ///
    /// Acquires a write lock on the one shard that owns `key`, removes the
    /// entry from `shard.data`, updates `current_size`, and removes the key
    /// from LRU/TinyLFU tracking.  If the key is not present the call is a
    /// no-op and returns `Ok(())`.
    ///
    /// _Requirements: 1.4_
    pub async fn invalidate(&self, key: &str) -> Result<()> {
        let idx = shard_index(key, self.shard_count);
        let mut guard = self.shards[idx].write().await;

        if let Some(entry) = guard.data.remove(key) {
            let entry_size = shard_calculate_entry_size(&entry);
            guard.current_size = guard.current_size.saturating_sub(entry_size);
            shard_remove_from_tracking(&mut guard, key);
            debug!("ShardedRamCache invalidated key: {}", key);
        } else {
            debug!("ShardedRamCache invalidate: key not found: {}", key);
        }

        Ok(())
    }

    /// Invalidate all entries whose cache key starts with `prefix`.
    ///
    /// Iterates **all** shards under exclusive write locks (one at a time),
    /// removing matching entries and updating `current_size` and eviction
    /// tracking for each.  Returns the total number of entries removed.
    ///
    /// Used on the object overwrite path (`cache.rs`) to purge all range
    /// entries for an object in one call.
    ///
    /// _Requirements: 1.4_
    pub async fn invalidate_by_prefix(&self, prefix: &str) -> Result<usize> {
        let mut total_removed = 0usize;

        for shard_lock in &self.shards {
            let mut guard = shard_lock.write().await;

            let keys_to_remove: Vec<String> = guard
                .data
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();

            for key in &keys_to_remove {
                if let Some(entry) = guard.data.remove(key) {
                    let entry_size = shard_calculate_entry_size(&entry);
                    guard.current_size = guard.current_size.saturating_sub(entry_size);
                    shard_remove_from_tracking(&mut guard, key);
                }
            }

            total_removed += keys_to_remove.len();
        }

        if total_removed > 0 {
            debug!(
                "ShardedRamCache invalidated {} entries with prefix: {}",
                total_removed, prefix
            );
        }

        Ok(total_removed)
    }

    /// Return aggregate statistics for the whole sharded cache.
    ///
    /// Hit/miss counts are read from atomics (no lock needed).  Entry count,
    /// `current_size`, `eviction_count`, and `last_eviction` are aggregated by
    /// iterating all shards under shared read locks: eviction counts are summed
    /// and the most recent per-shard eviction timestamp is returned (`None` if
    /// nothing has been evicted).  The `eviction_algorithm` is read from the
    /// first shard.
    ///
    /// _Requirements: 1.1; compression-followup-fixes Req 3_
    pub async fn stats(&self) -> RamCacheStats {
        let hit_count = self.hit_count.load(Ordering::Relaxed);
        let miss_count = self.miss_count.load(Ordering::Relaxed);

        let total_requests = hit_count + miss_count;
        let hit_rate = if total_requests > 0 {
            hit_count as f32 / total_requests as f32
        } else {
            0.0
        };

        let max_size = self.max_size() as u64;

        let mut current_size: u64 = 0;
        let mut entries_count: u64 = 0;
        let mut eviction_count: u64 = 0;
        let mut last_eviction_ms: u64 = 0;
        let mut eviction_algorithm = CacheEvictionAlgorithm::LRU;

        for (i, shard_lock) in self.shards.iter().enumerate() {
            let guard = shard_lock.read().await;
            current_size += guard.current_size as u64;
            entries_count += guard.data.len() as u64;
            eviction_count += guard.eviction_count;
            last_eviction_ms = last_eviction_ms.max(guard.last_eviction_ms);
            if i == 0 {
                eviction_algorithm = guard.eviction.eviction_algorithm.clone();
            }
        }

        let last_eviction = if last_eviction_ms == 0 {
            None
        } else {
            Some(UNIX_EPOCH + std::time::Duration::from_millis(last_eviction_ms))
        };

        RamCacheStats {
            current_size,
            max_size,
            entries_count,
            hit_count,
            miss_count,
            hit_rate,
            eviction_count,
            last_eviction,
            eviction_algorithm,
        }
    }
}

// ---------------------------------------------------------------------------
// Shard-level helper functions (free functions operating on RamCacheShard)
// ---------------------------------------------------------------------------

/// Calculate the byte size of a single `RamCacheEntry` as used for per-shard
/// capacity accounting.  Mirrors `RamCache::calculate_entry_size` but returns
/// `usize` because `RamCacheShard::current_size` / `capacity` are `usize`.
fn shard_calculate_entry_size(entry: &RamCacheEntry) -> usize {
    let base_size = std::mem::size_of::<RamCacheEntry>();
    let key_size = entry.cache_key.len();
    let data_size = entry.data.len();
    let metadata_size = std::mem::size_of::<CacheMetadata>();
    base_size + key_size + data_size + metadata_size
}

/// Add `key` to the shard's LRU eviction tracking.
///
/// TinyLFU is a no-op here: victim scoring (`shard_find_tinylfu_victim`) reads
/// `access_count`/`last_accessed` directly from each entry's atomics at
/// eviction time, so no separate window/frequency bookkeeping is needed.
/// Equivalent to `RamCache::add_to_tracking`.
fn shard_add_to_tracking(shard: &mut RamCacheShard, key: &str) {
    match shard.eviction.eviction_algorithm {
        CacheEvictionAlgorithm::LRU => {
            shard.eviction.lru_order.push_back(key.to_string());
        }
        CacheEvictionAlgorithm::TinyLFU => {}
    }
}

/// Remove `key` from the shard's LRU eviction tracking.
///
/// TinyLFU is a no-op (see `shard_add_to_tracking`).
/// Equivalent to `RamCache::remove_from_tracking`.
fn shard_remove_from_tracking(shard: &mut RamCacheShard, key: &str) {
    match shard.eviction.eviction_algorithm {
        CacheEvictionAlgorithm::LRU => {
            if let Some(pos) = shard.eviction.lru_order.iter().position(|k| k == key) {
                shard.eviction.lru_order.remove(pos);
            }
        }
        CacheEvictionAlgorithm::TinyLFU => {}
    }
}

/// Update LRU ordering for `key` on an access.
///
/// TinyLFU is a no-op (see `shard_add_to_tracking`).
/// Equivalent to `RamCache::update_access_tracking`.
fn shard_update_access_tracking(shard: &mut RamCacheShard, key: &str) {
    match shard.eviction.eviction_algorithm {
        CacheEvictionAlgorithm::LRU => {
            // Move to back of LRU queue (most-recently used)
            if let Some(pos) = shard.eviction.lru_order.iter().position(|k| k == key) {
                shard.eviction.lru_order.remove(pos);
            }
            shard.eviction.lru_order.push_back(key.to_string());
        }
        CacheEvictionAlgorithm::TinyLFU => {}
    }
}

/// Find the LRU victim key in a shard.
/// Equivalent to `RamCache::find_lru_victim`.
fn shard_find_lru_victim(shard: &RamCacheShard) -> Option<String> {
    shard.eviction.lru_order.front().cloned()
}

/// Find the TinyLFU victim key in a shard.
///
/// Selects the entry minimizing `(decayed_frequency(access_count, idle_secs),
/// last_accessed_ms)` — lowest decayed frequency first, oldest `last_accessed` as
/// tiebreak. Reads `access_count`/`last_accessed` directly from the entry's atomics
/// rather than the (superseded) windowed-frequency tracking, so a genuinely hot but
/// idle entry is not evicted before a fresh one-hit-wonder.
/// Equivalent to `RamCache::find_tinylfu_victim`.
fn shard_find_tinylfu_victim(shard: &RamCacheShard) -> Option<String> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    shard
        .data
        .iter()
        .min_by_key(|(_key, entry)| {
            let last_accessed_ms = entry.last_accessed.load(Ordering::Relaxed);
            let idle_secs = now_ms.saturating_sub(last_accessed_ms) / 1000;
            let access_count = entry.access_count.load(Ordering::Relaxed);
            (
                crate::cache::decayed_frequency(access_count, idle_secs),
                last_accessed_ms,
            )
        })
        .map(|(key, _)| key.clone())
}

/// RAM cache statistics
#[derive(Debug, Clone)]
pub struct RamCacheStats {
    pub current_size: u64,
    pub max_size: u64,
    pub entries_count: u64,
    pub hit_count: u64,
    pub miss_count: u64,
    pub hit_rate: f32,
    pub eviction_count: u64,
    pub last_eviction: Option<SystemTime>,
    pub eviction_algorithm: CacheEvictionAlgorithm,
}

#[cfg(test)]
mod sharded_tests {
    use super::*;
    use crate::cache::CacheEvictionAlgorithm;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    /// Helper mirroring tests::create_test_entry, local to this module.
    fn make_entry(key: &str, data: &[u8]) -> RamCacheEntry {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        RamCacheEntry {
            cache_key: key.to_string(),
            data: Arc::new(bytes::Bytes::from(data.to_vec())),
            metadata: crate::cache_types::CacheMetadata {
                etag: "test-etag".to_string(),
                last_modified: "test-modified".to_string(),
                content_length: data.len() as u64,
                part_number: None,
                cache_control: None,
                access_count: 0,
                last_accessed: SystemTime::now(),
            },
            created_at: SystemTime::now(),
            last_accessed: AtomicU64::new(now_ms),
            access_count: AtomicU64::new(0),
            compressed: false,
            compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
        }
    }

    // -----------------------------------------------------------------
    // Test 1: shard_assignment_deterministic
    // Same key always maps to the same shard; calling multiple times gives
    // identical results.  Different keys *may* map to different shards.
    // Validates: Requirement 1.3, Property 1
    // -----------------------------------------------------------------
    #[test]
    fn test_shard_assignment_deterministic() {
        // Calling shard_index with the same key and count must always return
        // the same value.
        let key = "bucket/object:range:0:65536";
        let idx1 = shard_index(key, 64);
        let idx2 = shard_index(key, 64);
        let idx3 = shard_index(key, 64);
        assert_eq!(idx1, idx2);
        assert_eq!(idx2, idx3);
        assert!(idx1 < 64);

        // Verify the boundary: shard_count = 1 must always give index 0.
        assert_eq!(shard_index(key, 1), 0);
        assert_eq!(shard_index("anything", 1), 0);

        // At least two distinct keys must exist that map to different shards
        // when shard_count is large enough (64 shards, BLAKE3 is uniform).
        let mut found_different = false;
        let base_idx = shard_index("key_a", 64);
        for i in 0u32..128 {
            let k = format!("key_{}", i);
            if shard_index(&k, 64) != base_idx {
                found_different = true;
                break;
            }
        }
        assert!(
            found_different,
            "All 128 test keys mapped to the same shard — hash function is degenerate"
        );
    }

    // -----------------------------------------------------------------
    // Test 2: test_get_put_basic
    // put then get returns an Arc<Bytes> with identical contents.
    // Validates: Requirement 1.4, 2.2, Property 2
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_get_put_basic() {
        let cache = ShardedRamCache::new(64 * 1024, 4, CacheEvictionAlgorithm::LRU);

        let data = b"hello world";
        let entry = make_entry("test:basic:key", data);
        cache.put(entry).await.unwrap();

        let read = cache.get("test:basic:key").await;
        assert!(read.is_some(), "get() should return Some after put()");

        let read = read.unwrap();
        assert_eq!(
            read.data.as_ref(),
            &data[..],
            "Returned bytes must match stored bytes"
        );
        assert_eq!(read.metadata.etag, "test-etag");

        // Miss for an absent key.
        assert!(cache.get("does:not:exist").await.is_none());
    }

    // -----------------------------------------------------------------
    // Test 3: test_arc_clone_outlives_eviction
    // Arc<Bytes> obtained from get() remains valid after the entry is
    // evicted from the shard.
    // Validates: Requirement 2.2, 2.3, Property 2
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_arc_clone_outlives_eviction() {
        // One shard, tiny capacity — just enough for one entry of ~100 bytes.
        // shard_calculate_entry_size = size_of::<RamCacheEntry>() + key.len() +
        //   data.len() + size_of::<CacheMetadata>()
        // We give the cache slightly more than one entry to store "A" then evict it.
        let small_data = b"key_A_data";
        let entry_a = make_entry("evict:key:A", small_data);
        // Compute the approximate size of one entry so we can set capacity to fit
        // exactly one such entry.
        let one_entry_size = std::mem::size_of::<RamCacheEntry>()
            + "evict:key:A".len()
            + small_data.len()
            + std::mem::size_of::<crate::cache_types::CacheMetadata>();
        // capacity = 1 entry + a small margin (not enough for two).
        let capacity = one_entry_size + 16;

        let cache = ShardedRamCache::new(capacity, 1, CacheEvictionAlgorithm::LRU);
        cache.put(entry_a).await.unwrap();

        // Grab an Arc clone while key A is still live.
        let arc_clone: Arc<bytes::Bytes> = cache
            .get("evict:key:A")
            .await
            .expect("A must be present before eviction")
            .data;

        // Now put a second entry that is large enough to force eviction of A.
        let _big_data = vec![0u8; capacity + 1];
        // This entry is too large for the shard — it will be silently dropped.
        // So use an entry exactly as large as "A" to trigger LRU eviction of A.
        let entry_b = make_entry("evict:key:B", small_data);
        cache.put(entry_b).await.unwrap();

        // A should now be evicted (LRU: A was added first, B was added second,
        // the shard can hold at most one entry).
        assert!(
            cache.get("evict:key:A").await.is_none(),
            "A must be evicted after B was inserted into the full shard"
        );

        // The previously obtained Arc must still hold valid data.
        assert_eq!(
            arc_clone.as_ref(),
            &small_data[..],
            "Arc clone must remain valid after eviction"
        );
        // Arc ref-count should still be 1 (we hold the only clone).
        assert_eq!(Arc::strong_count(&arc_clone), 1);
    }

    /// Spec: compression-followup-fixes Requirement 3.
    /// Evictions under capacity pressure must be counted and surfaced through
    /// `stats()` — the previously hardcoded `0` / `None` was a permanently-zero
    /// exported metric.
    #[tokio::test]
    async fn test_eviction_stats_are_counted() {
        let small_data = b"evict_stats_data";
        let one_entry_size = std::mem::size_of::<RamCacheEntry>()
            + "evict:stats:A".len()
            + small_data.len()
            + std::mem::size_of::<crate::cache_types::CacheMetadata>();
        // Capacity for exactly one entry (plus a small margin), one shard.
        let cache = ShardedRamCache::new(one_entry_size + 16, 1, CacheEvictionAlgorithm::LRU);

        // Before any eviction: metric is zero / never.
        let before = cache.stats().await;
        assert_eq!(before.eviction_count, 0);
        assert!(before.last_eviction.is_none());

        // Insert three same-sized entries into the single one-entry shard,
        // forcing two evictions.
        cache
            .put(make_entry("evict:stats:A", small_data))
            .await
            .unwrap();
        cache
            .put(make_entry("evict:stats:B", small_data))
            .await
            .unwrap();
        cache
            .put(make_entry("evict:stats:C", small_data))
            .await
            .unwrap();

        let after = cache.stats().await;
        assert_eq!(
            after.eviction_count, 2,
            "two evictions should be counted (A and B evicted for B and C)"
        );
        assert!(
            after.last_eviction.is_some(),
            "last_eviction timestamp must be set after an eviction"
        );
    }

    // -----------------------------------------------------------------
    // Test 4: test_capacity_per_shard
    // Eviction fires when a shard reaches its per-shard capacity limit.
    // Validates: Requirement 1.7, Property 3
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_capacity_per_shard() {
        // 2 shards; we need to find two keys that both land on shard 0
        // so we can fill *one* shard past its capacity.
        let shard_count = 2;

        // Find keys that hash to shard 0.
        let mut shard0_keys: Vec<String> = Vec::new();
        for i in 0u32..10_000 {
            let k = format!("cap:test:key:{}", i);
            if shard_index(&k, shard_count) == 0 {
                shard0_keys.push(k);
                if shard0_keys.len() == 5 {
                    break;
                }
            }
        }
        assert!(
            shard0_keys.len() >= 3,
            "Could not find 3 keys hashing to shard 0"
        );

        // Each entry holds ~50 bytes of payload.
        let payload = vec![b'Z'; 50];
        let one_entry_size = std::mem::size_of::<RamCacheEntry>()
            + shard0_keys[0].len()
            + payload.len()
            + std::mem::size_of::<crate::cache_types::CacheMetadata>();

        // Total capacity = 2 × per_shard_capacity.
        // We want the shard to fit exactly 2 entries; the 3rd triggers eviction.
        let per_shard_capacity = one_entry_size * 2 + 16;
        let total_capacity = per_shard_capacity * shard_count;

        // This test exercises per-shard eviction mechanics at a tiny byte scale,
        // which the real admission-ceiling clamp would otherwise collapse to a
        // single shard — use the unclamped test constructor to keep 2 shards.
        let cache = ShardedRamCache::new_unclamped_for_test(
            total_capacity,
            shard_count,
            CacheEvictionAlgorithm::LRU,
        );

        // Insert 3 entries into shard 0.
        for key in shard0_keys.iter().take(3) {
            cache.put(make_entry(key, &payload)).await.unwrap();
        }

        // Read shard 0 state directly to verify eviction fired.
        let shard_guard = cache.shards[0].read().await;
        let entry_count = shard_guard.data.len();
        drop(shard_guard);

        assert!(
            entry_count <= 2,
            "Shard 0 should hold at most 2 entries after eviction, but holds {}",
            entry_count
        );
    }

    // -----------------------------------------------------------------
    // Test: test_admission_ceiling_clamp_arithmetic
    //
    // Verifies the effective_shard_count clamp formula directly against the
    // scenarios called out in the task: 512 MiB/8 -> 8 shards (no clamp);
    // 256 MiB/8 -> 4 shards (clamp fires); 1 GiB/8 -> 8 (configured count,
    // never *increased* by the clamp — max() only ever raises the floor when
    // capacity is scarce, min() never expands beyond what was configured);
    // 64 MiB/8 -> 1 shard (fully clamped to the floor).
    // Validates: Requirements 7.7, 7.8
    // -----------------------------------------------------------------
    #[test]
    fn test_admission_ceiling_clamp_arithmetic() {
        const MIB: usize = 1024 * 1024;

        // 512 MiB / 8 configured shards -> 8 effective shards (512/64 = 8, no clamp).
        let cache = ShardedRamCache::new(512 * MIB, 8, CacheEvictionAlgorithm::LRU);
        assert_eq!(
            cache.shard_count, 8,
            "512 MiB / 8 shards must yield 8 effective shards"
        );

        // 256 MiB / 8 configured shards -> clamps to 4 effective shards (256/64 = 4).
        let cache = ShardedRamCache::new(256 * MIB, 8, CacheEvictionAlgorithm::LRU);
        assert_eq!(
            cache.shard_count, 4,
            "256 MiB / 8 shards must clamp to 4 effective shards"
        );

        // 1 GiB / 8 configured shards -> capped AT the configured count (min() never
        // raises above what was configured, even though 1024/64 = 16 > 8).
        let cache = ShardedRamCache::new(1024 * MIB, 8, CacheEvictionAlgorithm::LRU);
        assert_eq!(
            cache.shard_count, 8,
            "1 GiB / 8 configured shards must stay at the configured 8 (never increased)"
        );

        // 64 MiB / 8 configured shards -> clamps all the way down to 1 effective shard.
        let cache = ShardedRamCache::new(64 * MIB, 8, CacheEvictionAlgorithm::LRU);
        assert_eq!(
            cache.shard_count, 1,
            "64 MiB / 8 shards must clamp to 1 effective shard"
        );
    }

    // -----------------------------------------------------------------
    // Test: test_64mib_entry_admitted_at_256mib_and_512mib
    //
    // A single entry of exactly RAM_CACHE_ADMISSION_CEILING (64 MiB) bytes
    // must be admitted (not silently dropped) both at the old 256 MiB default
    // (which clamps to 4 shards -> 64 MiB per shard) and at the new 512 MiB
    // default (which clamps to 8 shards -> 64 MiB per shard). This is the
    // core admission guarantee: an entry exactly at the ceiling always fits
    // in a shard sized to the ceiling.
    // Validates: Requirement 7.7
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_64mib_entry_admitted_at_256mib_and_512mib() {
        const MIB: usize = 1024 * 1024;
        // Payload sized so that the total entry size (struct overhead + key +
        // data + metadata) is exactly RAM_CACHE_ADMISSION_CEILING bytes — this
        // is the worst case that must still fit in a 64 MiB shard.
        let key = "admission:ceiling:entry";
        let overhead = std::mem::size_of::<RamCacheEntry>()
            + key.len()
            + std::mem::size_of::<crate::cache_types::CacheMetadata>();
        let payload_len = RAM_CACHE_ADMISSION_CEILING - overhead;
        let payload = vec![0xABu8; payload_len];

        for total_capacity in [256 * MIB, 512 * MIB] {
            let cache = ShardedRamCache::new(total_capacity, 8, CacheEvictionAlgorithm::LRU);
            cache.put(make_entry(key, &payload)).await.unwrap();

            let read = cache.get(key).await;
            assert!(
                read.is_some(),
                "a {}-byte entry (exactly the admission ceiling) must be admitted at \
                 total_capacity={} bytes, not silently dropped",
                RAM_CACHE_ADMISSION_CEILING,
                total_capacity
            );
            assert_eq!(read.unwrap().data.len(), payload_len);
        }
    }

    // -----------------------------------------------------------------
    // Test: test_shard_clamp_warning_path_exercised
    //
    // When the configured shard_count is reduced by the admission-ceiling
    // clamp, ShardedRamCache::new must still construct a working cache with
    // the reduced (effective) shard count — exercising the warn! branch
    // in `new` without asserting on log output (which the test harness does
    // not capture), while confirming the resulting cache is fully usable.
    // Validates: Requirement 7.8
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_shard_clamp_warning_path_exercised() {
        const MIB: usize = 1024 * 1024;
        // 128 MiB / 8 configured -> clamps to 2 effective shards (128/64=2),
        // which is strictly less than the configured 8, so the warning path
        // in `new()` fires.
        let cache = ShardedRamCache::new(128 * MIB, 8, CacheEvictionAlgorithm::LRU);
        assert_eq!(cache.shard_count, 2, "128 MiB / 8 shards must clamp to 2");

        // The clamped cache must still be fully functional.
        cache
            .put(make_entry("clamp:warn:key", b"still works"))
            .await
            .unwrap();
        let read = cache.get("clamp:warn:key").await;
        assert!(read.is_some());
        assert_eq!(read.unwrap().data.as_ref(), b"still works" as &[u8]);
    }

    // -----------------------------------------------------------------
    // Test: test_default_max_ram_cache_size_is_512mib
    //
    // The default `max_ram_cache_size` (config.rs `CacheConfig::default()`)
    // must be 512 MiB, per the measured decision (Resolved Question 4) so
    // that the unconditional 64 MiB shard clamp yields 8 effective shards
    // out of the box, preserving pre-clamp concurrency.
    // Validates: Resolved Question 4
    // -----------------------------------------------------------------
    #[test]
    fn test_default_max_ram_cache_size_is_512mib() {
        let default_cache_config = crate::config::CacheConfig::default();
        assert_eq!(
            default_cache_config.max_ram_cache_size,
            512 * 1024 * 1024,
            "default max_ram_cache_size must be 512 MiB"
        );
    }

    // -----------------------------------------------------------------
    // Test 5: test_shard_count_1_behaves_as_single_shard
    // A ShardedRamCache with 1 shard works correctly as a plain cache.
    // Validates: Requirement 1.2 boundary, Property 1
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_shard_count_1_behaves_as_single_shard() {
        let cache = ShardedRamCache::new(64 * 1024, 1, CacheEvictionAlgorithm::LRU);
        assert_eq!(cache.shard_count, 1);

        let keys = ["single:a", "single:b", "single:c"];
        let values: &[&[u8]] = &[b"alpha", b"bravo", b"charlie"];

        for (k, v) in keys.iter().zip(values.iter()) {
            cache.put(make_entry(k, v)).await.unwrap();
        }

        for (k, v) in keys.iter().zip(values.iter()) {
            let read = cache.get(k).await.expect("every key must be retrievable");
            assert_eq!(read.data.as_ref(), *v, "data mismatch for key {}", k);
        }

        // All entries live in the single shard.
        let shard_guard = cache.shards[0].read().await;
        assert_eq!(shard_guard.data.len(), 3);
        drop(shard_guard);
    }

    // -----------------------------------------------------------------
    // Test: test_sub_page_hit_updates_page_access_count
    //
    // Spec: page-aligned-range-cache, Task 5 (RAM cache as the Page unit).
    // When widening is enabled, `fill_page` (http_proxy.rs) looks up the RAM
    // cache keyed by the *containing Page's* bounds — never by the client's
    // requested sub-range — so a sub-page hit is, at the RAM-cache level,
    // indistinguishable from any other hit against that single Page entry.
    // No new heat-tracking code is required for this (Requirement 7.4): the
    // existing `access_count` atomic increment in `get()` already applies
    // per-entry, and the entry IS the whole Page by construction. This test
    // promotes a whole Page-sized entry and asserts that two independent
    // page-keyed lookups (standing in for two different sub-page client
    // reads landing in the same Page) each increment the Page's single
    // `access_count`, proving heat is tracked at Page granularity.
    // Validates: Requirement 7.4.
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_sub_page_hit_updates_page_access_count() {
        let cache =
            ShardedRamCache::new_unclamped_for_test(1024 * 1024, 4, CacheEvictionAlgorithm::LRU);

        let page_key = "bucket/object:range:0:4095";
        let page_data: Vec<u8> = (0u32..4096).map(|b| (b % 251) as u8).collect();
        cache.put(make_entry(page_key, &page_data)).await.unwrap();

        let access_count_before = {
            let idx = shard_index(page_key, cache.shard_count);
            let guard = cache.shards[idx].read().await;
            guard
                .data
                .get(page_key)
                .expect("page entry must be present after promotion")
                .access_count
                .load(Ordering::Relaxed)
        };

        // Two "sub-page" reads: both resolve to the same page-keyed lookup,
        // exactly as `fill_page` does for any sub-range within the Page —
        // there is no per-sub-range RAM key to look up separately.
        let read1 = cache.get(page_key).await;
        assert!(read1.is_some(), "first sub-page hit must find the Page");
        let read2 = cache.get(page_key).await;
        assert!(
            read2.is_some(),
            "second sub-page hit must find the same Page"
        );

        let access_count_after = {
            let idx = shard_index(page_key, cache.shard_count);
            let guard = cache.shards[idx].read().await;
            guard
                .data
                .get(page_key)
                .expect("page entry must still be present")
                .access_count
                .load(Ordering::Relaxed)
        };

        assert_eq!(
            access_count_after,
            access_count_before + 2,
            "each sub-page hit must increment the whole Page's access_count \
             by one, confirming heat tracking is page-granular by construction"
        );
    }

    // -----------------------------------------------------------------
    // Test 6: test_get_uses_read_lock
    // Two concurrent tasks that call get() on the same key both succeed
    // and each increments access_count (atomic).
    // Validates: Requirement 4.1, 4.3, Property 5
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_get_uses_read_lock() {
        // This test exercises concurrent shard-lock access at a tiny byte scale,
        // which the real admission-ceiling clamp would otherwise collapse to a
        // single shard — use the unclamped test constructor to keep 4 shards.
        let cache = Arc::new(ShardedRamCache::new_unclamped_for_test(
            64 * 1024,
            4,
            CacheEvictionAlgorithm::LRU,
        ));

        cache
            .put(make_entry("concurrent:key", b"concurrent data"))
            .await
            .unwrap();

        // Capture the access_count before the concurrent reads.
        let before: u64 = {
            let idx = shard_index("concurrent:key", 4);
            let guard = cache.shards[idx].read().await;
            guard
                .data
                .get("concurrent:key")
                .unwrap()
                .access_count
                .load(Ordering::Relaxed)
        };

        // Spawn two tasks that concurrently call get().
        let c1 = Arc::clone(&cache);
        let c2 = Arc::clone(&cache);
        let t1 = tokio::spawn(async move { c1.get("concurrent:key").await });
        let t2 = tokio::spawn(async move { c2.get("concurrent:key").await });

        let r1 = t1.await.unwrap();
        let r2 = t2.await.unwrap();

        assert!(r1.is_some(), "task 1 must get a hit");
        assert!(r2.is_some(), "task 2 must get a hit");
        assert_eq!(r1.unwrap().data.as_ref(), b"concurrent data" as &[u8]);
        assert_eq!(r2.unwrap().data.as_ref(), b"concurrent data" as &[u8]);

        // access_count must have been incremented at least once per task.
        let after: u64 = {
            let idx = shard_index("concurrent:key", 4);
            let guard = cache.shards[idx].read().await;
            guard
                .data
                .get("concurrent:key")
                .unwrap()
                .access_count
                .load(Ordering::Relaxed)
        };

        assert!(
            after >= before + 2,
            "access_count must increase by at least 2 (one per concurrent get), \
             before={} after={}",
            before,
            after
        );
    }

    // -----------------------------------------------------------------
    // Test 7: test_deferred_reorder_evicts_cold_first
    // A hot key (accessed many times, triggering sampled pending_accesses
    // recording) survives while a cold key (never re-accessed) is evicted
    // when a new entry is inserted.
    //
    // The deferred-reorder mechanism: every ~8th access (access_count % 8 == 0)
    // the get() method tries a non-blocking try_write() to push the key onto
    // pending_accesses.  The *next* put() drains pending_accesses to move the
    // key to the back of the LRU queue.  So after ≥8 accesses and one put(),
    // the hot key is at the back of LRU; cold keys remain at the front and are
    // evicted first.
    // Validates: Requirement 4.4, 4.6, Property 6
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_deferred_reorder_evicts_cold_first() {
        // One shard, capacity for exactly 2 entries of this size.
        let payload = vec![b'X'; 20];
        let one_entry_size = std::mem::size_of::<RamCacheEntry>()
            + "deferred:hot".len()
            + payload.len()
            + std::mem::size_of::<crate::cache_types::CacheMetadata>();

        // Capacity holds 2 entries; inserting a 3rd forces eviction of the LRU entry.
        let capacity = one_entry_size * 2 + 16;
        let cache = ShardedRamCache::new(capacity, 1, CacheEvictionAlgorithm::LRU);

        // Insert "cold" first (oldest in LRU queue) then "hot".
        cache
            .put(make_entry("deferred:cold", &payload))
            .await
            .unwrap();
        cache
            .put(make_entry("deferred:hot", &payload))
            .await
            .unwrap();

        // Access "hot" at least 8 times so that on access_count == 8 the sampled
        // deferred-reorder path fires and records "deferred:hot" in pending_accesses.
        for _ in 0..9 {
            cache.get("deferred:hot").await;
        }

        // Now insert a new entry.  put() drains pending_accesses first (moving
        // "deferred:hot" to the back of the LRU queue), then evicts the entry at the
        // front — which must be "deferred:cold".
        cache
            .put(make_entry("deferred:new", &payload))
            .await
            .unwrap();

        let hot_present = cache.get("deferred:hot").await.is_some();
        let cold_present = cache.get("deferred:cold").await.is_some();
        let new_present = cache.get("deferred:new").await.is_some();

        assert!(
            hot_present,
            "hot key must survive — deferred LRU reorder should keep it at the back"
        );
        assert!(new_present, "newly inserted key must be present");
        assert!(
            !cold_present,
            "cold key must be evicted first (it was at the front of LRU queue)"
        );
    }

    /// Back-date `entry`'s `last_accessed` atomic to `idle_secs` ago, so eviction
    /// scoring sees it as idle without needing to actually sleep in the test.
    fn backdate(entry: &RamCacheEntry, idle_secs: u64) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let old_ms = now_ms.saturating_sub(idle_secs * 1000);
        entry.last_accessed.store(old_ms, Ordering::Relaxed);
    }

    // -----------------------------------------------------------------
    // Test 8: test_tinylfu_inversion_regression
    //
    // One shard sized for exactly 2 entries. A "hot" entry has a very high
    // access_count but has been idle for 2 half-lives (2 * 3600s); a "fresh"
    // one-hit entry has access_count == 1 and last_accessed == now. Inserting
    // a 3rd entry forces exactly one eviction; the fresh one-hit-wonder must be
    // evicted and the idle-hot entry must survive.
    //
    // Sanity check against the OLD buggy formula (frequency * 1000 / recency),
    // which this test would have failed against pre-fix:
    //   hot:   frequency=100_000, recency ~= idle_secs = 7200s
    //          old_score = 100_000 * 1000 / 7200 ≈ 13_888
    //   fresh: frequency=1, recency ~= 1s (just inserted)
    //          old_score = 1 * 1000 / 1 = 1000
    //   Old formula treats lower score as colder/evict-first, so it would have
    //   picked "fresh" (1000) over "hot" (13_888) as the *lower* score... but
    //   with a fresh entry inserted at recency≈0, `recency` clamps to a tiny
    //   value, driving the old score arbitrarily high (division by ~0) or, with
    //   a slightly aged fresh entry (recency=1s..few s), the old score for hot
    //   *drops relative to* fresh as hot's idle grows, and once hot has been
    //   idle long enough the "hot" score falls below "fresh" — the exact
    //   inversion this spec fixes. With hot idle 7200s and access_count as low
    //   as 1000, old_score(hot) = 1000*1000/7200 ≈ 138, well below
    //   old_score(fresh) ≈ 1000, so the buggy formula evicts the *hot* entry
    //   instead of the one-hit-wonder. The decayed-frequency fix instead keeps
    //   hot's Effective_Frequency = access_count >> halvings = 100_000 >> 2 =
    //   25_000, far above fresh's Effective_Frequency = 1, so fresh is
    //   correctly evicted.
    // Validates: Requirements 6.1
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_tinylfu_inversion_regression() {
        let payload = vec![b'H'; 20];
        let one_entry_size = std::mem::size_of::<RamCacheEntry>()
            + "inv:hot".len()
            + payload.len()
            + std::mem::size_of::<crate::cache_types::CacheMetadata>();
        // Capacity holds exactly 2 entries; a 3rd forces exactly one eviction.
        let capacity = one_entry_size * 2 + 16;
        let cache = ShardedRamCache::new(capacity, 1, CacheEvictionAlgorithm::TinyLFU);

        // Insert the hot entry, then bump its access_count high and back-date its
        // last_accessed to 2 half-lives ago (2 * 3600s = 7200s).
        cache.put(make_entry("inv:hot", &payload)).await.unwrap();
        {
            let idx = shard_index("inv:hot", 1);
            let guard = cache.shards[idx].read().await;
            let entry = guard.data.get("inv:hot").unwrap();
            entry.access_count.store(100_000, Ordering::Relaxed);
            backdate(entry, 2 * 3600);
        }

        // Insert the fresh one-hit entry (access_count == 1, last_accessed == now).
        cache.put(make_entry("inv:fresh", &payload)).await.unwrap();
        {
            let idx = shard_index("inv:fresh", 1);
            let guard = cache.shards[idx].read().await;
            let entry = guard.data.get("inv:fresh").unwrap();
            entry.access_count.store(1, Ordering::Relaxed);
        }

        // Insert a 3rd entry, forcing exactly one eviction from the 2-capacity shard.
        cache.put(make_entry("inv:new", &payload)).await.unwrap();

        let shard_guard = cache.shards[0].read().await;
        assert_eq!(
            shard_guard.data.len(),
            2,
            "shard must hold exactly 2 entries after one eviction"
        );
        assert!(
            shard_guard.data.contains_key("inv:hot"),
            "idle-hot entry must survive eviction (Effective_Frequency = 100_000 >> 2 = 25_000)"
        );
        assert!(
            !shard_guard.data.contains_key("inv:fresh"),
            "fresh one-hit-wonder must be evicted (Effective_Frequency = 1)"
        );
        assert!(
            shard_guard.data.contains_key("inv:new"),
            "newly inserted entry must be present"
        );
    }

    // -----------------------------------------------------------------
    // Test 9: test_tinylfu_large_cold_read_evicts_only_cold_tail
    //
    // Several cold entries (old, access_count == 1) plus one hot entry share a
    // shard at capacity. Inserting a new entry that requires evicting multiple
    // cold entries to fit must only evict cold entries — the hot entry must
    // survive throughout.
    // Validates: Requirements 6.1
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_tinylfu_large_cold_read_evicts_only_cold_tail() {
        let payload = vec![b'C'; 20];
        let one_entry_size = std::mem::size_of::<RamCacheEntry>()
            + "cold:tail:0".len()
            + payload.len()
            + std::mem::size_of::<crate::cache_types::CacheMetadata>();

        // Capacity for 4 small entries (3 cold + 1 hot).
        let capacity = one_entry_size * 4 + 16;
        let cache = ShardedRamCache::new(capacity, 1, CacheEvictionAlgorithm::TinyLFU);

        // Insert 3 cold entries: access_count == 1, idle for 2 half-lives.
        let cold_keys = ["cold:tail:0", "cold:tail:1", "cold:tail:2"];
        for key in cold_keys {
            cache.put(make_entry(key, &payload)).await.unwrap();
            let idx = shard_index(key, 1);
            let guard = cache.shards[idx].read().await;
            let entry = guard.data.get(key).unwrap();
            entry.access_count.store(1, Ordering::Relaxed);
            backdate(entry, 2 * 3600);
        }

        // Insert the hot entry: high access_count, idle only a fraction of a
        // half-life so its Effective_Frequency stays high.
        cache
            .put(make_entry("cold:tail:hot", &payload))
            .await
            .unwrap();
        {
            let idx = shard_index("cold:tail:hot", 1);
            let guard = cache.shards[idx].read().await;
            let entry = guard.data.get("cold:tail:hot").unwrap();
            entry.access_count.store(100_000, Ordering::Relaxed);
            backdate(entry, 60);
        }

        // A new "large" entry needs room for 2 more entries worth of bytes than
        // are currently free, forcing eviction of the 2 lowest-scoring (cold)
        // entries to fit — same eviction loop, just sized to require >1 eviction.
        let large_payload = vec![b'L'; payload.len() * 3];
        cache
            .put(make_entry("cold:tail:large", &large_payload))
            .await
            .unwrap();

        let shard_guard = cache.shards[0].read().await;
        assert!(
            shard_guard.data.contains_key("cold:tail:hot"),
            "hot entry must survive while only cold entries are evicted"
        );
        assert!(
            shard_guard.data.contains_key("cold:tail:large"),
            "newly inserted large entry must be present"
        );
        let remaining_cold = cold_keys
            .iter()
            .filter(|k| shard_guard.data.contains_key(**k))
            .count();
        assert!(
            remaining_cold < cold_keys.len(),
            "at least one cold entry must have been evicted to make room"
        );
    }

    // -----------------------------------------------------------------
    // Test 10: test_tinylfu_all_cold_lru_fallback
    //
    // All entries share access_count == 1 (Effective_Frequency == 1 for all),
    // so the `(decayed_frequency, last_accessed)` tuple falls through to the
    // Last_Accessed tiebreak. Eviction order must therefore be ascending
    // last_accessed — i.e. plain LRU order — oldest evicted first.
    // Validates: Requirements 6.4
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_tinylfu_all_cold_lru_fallback() {
        // Fixed-width keys ("allcold:0" .. "allcold:9") so every entry has an
        // identical byte size — avoids off-by-a-few-bytes capacity math from
        // differing key lengths triggering an extra eviction.
        let payload = vec![b'A'; 20];
        let one_entry_size = std::mem::size_of::<RamCacheEntry>()
            + "allcold:0".len()
            + payload.len()
            + std::mem::size_of::<crate::cache_types::CacheMetadata>();

        // Capacity for exactly 3 entries (plus margin); a 4th forces exactly
        // one eviction, which must be the oldest (by last_accessed) of the 3.
        let capacity = one_entry_size * 3 + 64;
        let cache = ShardedRamCache::new(capacity, 1, CacheEvictionAlgorithm::TinyLFU);

        // Insert 3 entries, all access_count == 1, but with distinct
        // last_accessed times: "allcold:0" is furthest in the past, "allcold:1"
        // less so, "allcold:2" barely idle at all.
        let entries = [
            ("allcold:0", 300u64),
            ("allcold:1", 150u64),
            ("allcold:2", 10u64),
        ];
        for (key, idle_secs) in entries {
            cache.put(make_entry(key, &payload)).await.unwrap();
            let idx = shard_index(key, 1);
            let guard = cache.shards[idx].read().await;
            let entry = guard.data.get(key).unwrap();
            entry.access_count.store(1, Ordering::Relaxed);
            backdate(entry, idle_secs);
        }

        // Insert a 4th entry (same key width), forcing exactly one eviction.
        // All 3 existing entries have Effective_Frequency == 1 (no decay yet —
        // idle_secs are all well under one half-life), so the tiebreak on
        // last_accessed must pick "allcold:0" (oldest last_accessed) as the
        // victim.
        cache.put(make_entry("allcold:9", &payload)).await.unwrap();

        let shard_guard = cache.shards[0].read().await;
        assert!(
            !shard_guard.data.contains_key("allcold:0"),
            "the entry with the oldest last_accessed must be evicted first under \
             an all-cold (Effective_Frequency == 1) LRU fallback"
        );
        assert!(
            shard_guard.data.contains_key("allcold:1"),
            "middle-aged entry must survive"
        );
        assert!(
            shard_guard.data.contains_key("allcold:2"),
            "newest entry must survive"
        );
        assert!(
            shard_guard.data.contains_key("allcold:9"),
            "newly inserted entry must be present"
        );
    }

    // -----------------------------------------------------------------
    // Task 7 (page-aligned-range-cache): RAM lookup slices the Page, and a
    // full 64 MiB Page-sized entry is admitted after the admission-ceiling
    // shard clamp — the page-widening-specific composition of the Task 1
    // admission-ceiling guarantee (`test_64mib_entry_admitted_at_256mib_and_512mib`
    // above) with page-keyed RAM lookup semantics (`fill_page` /
    // `get_range_from_ram_cache`).
    // Validates: Requirements 3.4 (Page as RAM unit), 7.1, 7.2, 7.7
    // -----------------------------------------------------------------
    #[tokio::test]
    async fn test_page_sized_64mib_entry_admitted_and_ram_lookup_slices_it() {
        const MIB: usize = 1024 * 1024;
        const PAGE_SIZE: usize = 64 * MIB;

        // A page-shaped RAM key, exactly as `fill_page`/`get_range_from_ram_cache`
        // construct it: "{cache_key}:range:{page_start}:{page_end}".
        let page_key = "bucket/parquet-object:range:0:67108863";
        // The entry's total tracked size includes struct/key/metadata overhead
        // on top of the data payload (see `shard_calculate_entry_size`), so the
        // payload must be shrunk by that overhead to keep the *total* entry
        // size at exactly the 64 MiB admission ceiling — the worst case that
        // must still fit in a 64 MiB shard. Mirrors
        // `test_64mib_entry_admitted_at_256mib_and_512mib` above.
        let overhead = std::mem::size_of::<RamCacheEntry>()
            + page_key.len()
            + std::mem::size_of::<crate::cache_types::CacheMetadata>();
        let payload_len = PAGE_SIZE - overhead;
        let page_data: Vec<u8> = (0u64..payload_len as u64)
            .map(|i| (i % 251) as u8)
            .collect();

        // At the default 512 MiB / 8 shards, each shard is exactly the 64 MiB
        // admission ceiling — the worst case a full Page must still fit in.
        let cache = ShardedRamCache::new(512 * MIB, 8, CacheEvictionAlgorithm::LRU);
        cache
            .put(make_entry(page_key, &page_data))
            .await
            .expect("a full 64 MiB Page must be admitted, not dropped, after the shard clamp");

        // RAM lookup for the whole Page succeeds.
        let read = cache
            .get(page_key)
            .await
            .expect("the 64 MiB Page must be a RAM hit");
        assert_eq!(read.data.len(), payload_len);

        // Slicing a small sub-range out of the Page (as `fill_page` does after
        // its page-keyed RAM lookup) must return exactly the requested bytes,
        // proving RAM lookup slices the Page rather than returning something
        // else entirely.
        let sub_start = 12_345usize;
        let sub_len = 37usize;
        let sliced = &read.data[sub_start..sub_start + sub_len];
        assert_eq!(sliced, &page_data[sub_start..sub_start + sub_len]);

        // A second sub-page slice from a different offset within the same
        // Page must also be correct, confirming the whole Page (not just a
        // sub-range) is the RAM-resident unit.
        let sub2_start = payload_len - 100;
        let sliced2 = &read.data[sub2_start..payload_len];
        assert_eq!(sliced2, &page_data[sub2_start..payload_len]);
    }
}
