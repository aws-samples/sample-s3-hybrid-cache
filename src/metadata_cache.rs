//! Metadata Cache Module
//!
//! Provides RAM-based caching for `NewCacheMetadata` objects to reduce disk I/O.
//! Uses simple LRU eviction (all metadata entries are similar size ~1-2KB).
//! Includes per-key locking to prevent concurrent disk reads and stale file handle recovery.

use crate::cache_types::NewCacheMetadata;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info};

/// Configuration for the MetadataCache
#[derive(Debug, Clone)]
pub struct MetadataCacheConfig {
    /// Whether the metadata cache is enabled
    pub enabled: bool,
    /// How often to re-read from disk (staleness threshold)
    pub refresh_interval: Duration,
    /// Maximum number of entries in the cache
    pub max_entries: usize,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            refresh_interval: Duration::from_secs(5),
            max_entries: 100000,
        }
    }
}

/// A single entry in the metadata cache
#[derive(Debug, Clone)]
struct MetadataCacheEntry {
    /// The cached metadata
    pub metadata: NewCacheMetadata,
    /// When this entry was loaded from disk
    pub loaded_at: Instant,
    /// When this entry was last accessed in RAM
    pub last_accessed: Instant,
}

impl MetadataCacheEntry {
    /// Create a new cache entry
    pub fn new(metadata: NewCacheMetadata) -> Self {
        let now = Instant::now();
        Self {
            metadata,
            loaded_at: now,
            last_accessed: now,
        }
    }

    /// Check if this entry is stale based on refresh interval
    pub(crate) fn is_stale(&self, refresh_interval: Duration) -> bool {
        self.loaded_at.elapsed() >= refresh_interval
    }

    /// Update last accessed time
    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }
}

/// Metrics for the metadata cache
#[derive(Debug, Default)]
struct MetadataCacheMetrics {
    /// Number of cache hits (found in RAM, not stale) — GET path
    pub hits: AtomicU64,
    /// Number of cache misses (key not in RAM) — GET path
    pub misses: AtomicU64,
    /// Number of stale refreshes (re-reads from disk due to staleness)
    pub stale_refreshes: AtomicU64,
    /// Number of evictions
    pub evictions: AtomicU64,
    // REMOVED: `stale_handle_errors`. Its one increment sat in a loader with no production
    // caller, so it read `0` whatever the volume
    // did. Stale file handles are counted where they actually occur, in
    // `disk_cache::read_and_parse_metadata_blocking` — see
    // `crate::disk_cache::metadata_read_stale_handles_total`.
    /// Number of disk hits (RAM miss but found on disk) — GET path
    pub disk_hits: AtomicU64,
    /// Number of RAM hits from HEAD requests specifically
    pub head_hits: AtomicU64,
    /// Number of RAM misses from HEAD requests specifically
    pub head_misses: AtomicU64,
    /// Number of disk hits from HEAD requests specifically
    pub head_disk_hits: AtomicU64,
}

impl MetadataCacheMetrics {
    /// Get a snapshot of current metrics
    pub fn snapshot(&self) -> MetadataCacheMetricsSnapshot {
        MetadataCacheMetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            stale_refreshes: self.stale_refreshes.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            disk_hits: self.disk_hits.load(Ordering::Relaxed),
            head_hits: self.head_hits.load(Ordering::Relaxed),
            head_misses: self.head_misses.load(Ordering::Relaxed),
            head_disk_hits: self.head_disk_hits.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of metrics for reporting
#[derive(Debug, Clone)]
pub struct MetadataCacheMetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub stale_refreshes: u64,
    pub evictions: u64,
    pub disk_hits: u64,
    pub head_hits: u64,
    pub head_misses: u64,
    pub head_disk_hits: u64,
}

impl MetadataCacheMetricsSnapshot {}

/// The two resolved TTLs that govern the two consumers of a metadata entry.
///
/// # Why BOTH, and why this is a type rather than two loose arguments
///
/// `get_ttl` and `head_ttl` are orthogonal by design and nothing derives one from the
/// other (`get-ttl-zero-enforcement` R1.5). One `MetadataCache` entry serves two
/// consumers with different governing TTLs: `CacheManager::get_metadata_cached`'s
/// GET-path verdicts are bounded by `get_ttl`, and `get_head_cache_entry_unified`'s
/// HEAD serve by `head_ttl` (`is_head_fresh` requires `!head_ttl.is_zero()`).
///
/// So refusing admission on `get_ttl` alone would strip the RAM tier from HEAD for a
/// key whose operator deliberately set a non-zero `head_ttl` alongside `get_ttl: "0s"`.
/// That is a performance regression rather than a correctness one — disk is
/// authoritative and `head_ttl` still bounds the serve — but it silently couples two
/// fields the config keeps orthogonal, in the direction of *more* work for a
/// configuration that explicitly asked for HEAD caching. R4.4, and it was Ed's
/// caution before it was a requirement.
///
/// The rule is therefore: refuse only when **neither** consumer may use the entry. It
/// lives on this type so it exists once rather than being restated at each admission
/// site — the same reasoning that moved the `get_ttl` check inside
/// `CacheManager::get_range_from_ram_cache_hit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataTtls {
    pub get_ttl: Duration,
    pub head_ttl: Duration,
}

impl MetadataTtls {
    /// Admission is refused only when NEITHER consumer may use the entry.
    ///
    /// Deliberately `&&`, not `||`. If you are here because HEAD got slower for a
    /// `get_ttl: "0s"` key, that is the bug this predicate exists to prevent — read
    /// the type docs above rather than changing the operator.
    pub(crate) fn forbids_admission(&self) -> bool {
        self.get_ttl.is_zero() && self.head_ttl.is_zero()
    }
}

/// The v1.9.4 admission rule: a document carrying **no ranges** must not become resident in
/// the RAM metadata tier.
///
/// # Why, in one paragraph
///
/// `CacheManager::get_metadata_cached`'s first statement returns whatever is resident with no
/// coverage test, so a resident `ranges: []` document answers every range lookup on that key
/// with "no ranges exist" for up to one `MetadataCacheConfig::refresh_interval` — *including
/// after another instance's consolidation has added ranges to the shared `.meta`*, which this
/// instance has no way to learn about inside the window. The `.bin` files are on disk and
/// nothing consults them, so the GET refetches from S3 bytes that are already cached locally.
/// The v1.9.4 fix ("Don't cache empty-ranges metadata in RAM instead of bypassing",
/// 2026-03-16) added the test for exactly this, and `ram-tier-effectiveness:R1.6` extends it to
/// the sites that skipped it.
///
/// Note what this is **not**: it is not a subset test. No admission path can publish a document
/// whose range set is a strict subset of disk's — time creates that, not a writer. It is a
/// coarse proxy for "this entry is likely to be superseded soon", and a future refactor must
/// not be justified on the grounds that it preserves a subset check.
///
/// # This deliberately lives OUTSIDE `MetadataCache::put`
///
/// Moving it inside, beside [`MetadataTtls::forbids_admission`], is the obvious refactor and
/// `ram-tier-effectiveness` task 6 weighed it. Three reasons it did not land:
///
/// - **`ram-tier-effectiveness:R1.8` asks for its guard first.** The size-accounting and
///   eviction test owed by R3.10c fills the tier with HEAD-only entries and asserts eviction;
///   inside `put` this predicate would refuse those entries outright, so that
///   fixture could not be built and the guard the spec wants *before* the refactor could never
///   be written.
/// - **An existing test depends on admitting a ranges-free document directly**, to seed a known
///   `content_length` for the page-widening path
///   (`tests/page_aligned_range_widening_test.rs`). That is a legitimate use of the tier by a
///   caller that is not a cache-coherency consumer, and a chokepoint cannot distinguish it.
/// - **Census row 4 declines by a different mechanism** —
///   `CacheManager::store_head_cache_entry_unified_witnessed`'s `create_head_only_metadata` arm yields
///   `None` so no `put` is reached at all — and a chokepoint would make that arm read as
///   redundant when it is not.
///
/// **So the default for a new caller of `put` is UNGUARDED.** That is the cost
/// of this choice, stated rather than hidden: a comment cannot enforce a contract a future
/// caller can omit. What this function buys instead is that the rule exists **once** and every
/// site that applies it is findable:
///
/// ```text
/// grep -rn ram_admission_permitted src/   # every admission site governed by the rule
/// ```
///
/// A new `put` call site that does not appear in that output has made a decision, whether or
/// not its author knew it. If you are adding one, read
/// the ram tier effectiveness feature.
pub(crate) fn ram_admission_permitted(metadata: &NewCacheMetadata) -> bool {
    !metadata.ranges.is_empty()
}

/// RAM-based cache for NewCacheMetadata objects
///
/// Features:
/// - Simple LRU eviction (all entries similar size)
/// - Per-key locking to prevent concurrent disk reads
/// - Staleness-based refresh
/// - Stale file handle recovery with retry logic
pub struct MetadataCache {
    /// Cache entries indexed by cache_key
    entries: RwLock<HashMap<String, MetadataCacheEntry>>,
    /// LRU order tracking (front = oldest/least recently used)
    lru_order: RwLock<VecDeque<String>>,
    /// Per-key locks for preventing concurrent disk reads
    pending_reads: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    /// Configuration
    config: MetadataCacheConfig,
    /// Metrics
    metrics: MetadataCacheMetrics,
}

impl MetadataCache {
    /// Create a new MetadataCache with the given configuration
    pub fn new(config: MetadataCacheConfig) -> Self {
        info!(
            "Creating MetadataCache: enabled={}, refresh_interval={:?}, max_entries={}",
            config.enabled, config.refresh_interval, config.max_entries
        );
        Self {
            entries: RwLock::new(HashMap::new()),
            lru_order: RwLock::new(VecDeque::new()),
            pending_reads: RwLock::new(HashMap::new()),
            config,
            metrics: MetadataCacheMetrics::default(),
        }
    }
    /// Check if the cache is enabled
    pub(crate) fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Record a disk hit (RAM miss but found on disk) — GET path
    pub(crate) fn record_disk_hit(&self) {
        self.metrics.disk_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a HEAD RAM hit
    pub(crate) fn record_head_hit(&self) {
        self.metrics.head_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a HEAD RAM miss (reached disk lookup)
    pub(crate) fn record_head_miss(&self) {
        self.metrics.head_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a HEAD disk hit (RAM miss but found on disk)
    pub(crate) fn record_head_disk_hit(&self) {
        self.metrics.head_disk_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the configuration
    pub fn config(&self) -> &MetadataCacheConfig {
        &self.config
    }

    /// Get metrics snapshot
    pub fn metrics(&self) -> MetadataCacheMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Get metadata from cache if present and not stale
    ///
    /// Returns `Some(metadata)` if:
    /// - Entry exists in cache
    /// - Entry is not stale (loaded_at < refresh_interval)
    ///
    /// Returns `None` if entry doesn't exist or is stale.
    pub async fn get(&self, cache_key: &str) -> Option<NewCacheMetadata> {
        if !self.config.enabled {
            return None;
        }

        let mut entries = self.entries.write().await;

        if let Some(entry) = entries.get_mut(cache_key) {
            if entry.is_stale(self.config.refresh_interval) {
                // Entry is stale, return None to trigger disk read
                self.metrics.stale_refreshes.fetch_add(1, Ordering::Relaxed);
                debug!(
                    cache_key = %cache_key,
                    age_ms = entry.loaded_at.elapsed().as_millis(),
                    "MetadataCache entry is stale"
                );
                return None;
            }

            // Entry is valid, update access tracking
            entry.touch();
            self.metrics.hits.fetch_add(1, Ordering::Relaxed);

            // Update LRU order
            drop(entries); // Release write lock before acquiring another
            self.update_lru_order(cache_key).await;

            // Re-acquire to return the metadata
            let entries = self.entries.read().await;
            return entries.get(cache_key).map(|e| e.metadata.clone());
        }

        self.metrics.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Store metadata in the cache.
    ///
    /// If the cache is at capacity, evicts the least recently used entry.
    ///
    /// `ttls` are the key's **resolved** TTLs; see [`MetadataTtls`] for why both are
    /// required and why admission is refused only when both are zero.
    ///
    /// # The prior decision this changes, and why its justification no longer holds
    ///
    /// This function used to carry the following note, and it is quoted rather than
    /// deleted because it was a **deliberate decision**, not an oversight
    /// (`get-ttl-zero-enforcement` R4.4, task 18):
    ///
    /// > Note: Metadata cache stores entries regardless of the object's get_ttl or
    /// > ram_cache_eligible setting. Even zero-TTL objects are cached here because the
    /// > metadata cache has its own refresh interval and is used for revalidation
    /// > decisions (e.g., If-Modified-Since / If-None-Match). The ram_cache_eligible
    /// > invariant only applies to the RAM range cache. (Requirement 6.6)
    ///
    /// Its stated justification is that the metadata cache is used for revalidation
    /// decisions, naming `If-Modified-Since` / `If-None-Match`. Per
    /// `docs/CACHE_FRESHNESS.md` those two — and `If-Unmodified-Since` — **always
    /// forward to S3** in both Mode A and Mode B, so the metadata cache is not used
    /// for the decisions the note names. The only locally-answered conditional is
    /// `If-Match` (Mode B), which is precisely the one that must not be local for a
    /// key the operator set to always-revalidate.
    ///
    /// (The `Requirement 6.6` reference is to an archived spec that was not
    /// identified. Do not assume it is `fs2-to-fs4-migration`'s or
    /// `cache-eviction-at-scale`'s 6.6.)
    ///
    /// # This is HYGIENE and is NOT the control for the always-revalidate contract
    ///
    /// Stated explicitly because it would be easy to read as the fix. R4.4 forbids
    /// relying on it, for three reasons already recorded in that spec:
    ///
    /// - a rule change cannot evict what already exists, only prevent a new entry, so
    ///   an operator who tightens to `get_ttl: "0s"` gets nothing for what is already
    ///   resident;
    /// - rules are per-glob and hot-reloaded, so the values resolved at admission time
    ///   are not necessarily those resolved at lookup time — the tighten-after-caching
    ///   sequence is the normal case;
    /// - it is per-instance, so proxy 1 refusing admission says nothing about proxy 2.
    ///
    /// The control is the lookup-side gate. See
    /// `CacheManager::get_range_from_ram_cache_hit`, which takes the resolved
    /// `get_ttl` as a required parameter.
    pub async fn put(&self, cache_key: &str, metadata: NewCacheMetadata, ttls: MetadataTtls) {
        if !self.config.enabled {
            return;
        }

        // Refuse only when NEITHER consumer may use the entry. See MetadataTtls.
        if ttls.forbids_admission() {
            debug!(
                cache_key = %cache_key,
                "MetadataCache declined admission: both get_ttl and head_ttl are zero"
            );
            return;
        }

        let mut entries = self.entries.write().await;
        let mut lru_order = self.lru_order.write().await;

        // Check if we need to evict
        while entries.len() >= self.config.max_entries {
            if let Some(victim_key) = lru_order.pop_front() {
                if entries.remove(&victim_key).is_some() {
                    self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
                    debug!(
                        evicted_key = %victim_key,
                        "MetadataCache evicted entry (LRU)"
                    );
                }
            } else {
                break;
            }
        }

        // Remove existing entry from LRU order if present
        if entries.contains_key(cache_key) {
            lru_order.retain(|k| k != cache_key);
        }

        // Insert new entry
        let entry = MetadataCacheEntry::new(metadata);
        entries.insert(cache_key.to_string(), entry);
        lru_order.push_back(cache_key.to_string());

        debug!(
            cache_key = %cache_key,
            entries_count = entries.len(),
            "MetadataCache stored entry"
        );
    }

    /// Invalidate a cache entry
    ///
    /// Call this when metadata is written to disk to ensure consistency.
    pub async fn invalidate(&self, cache_key: &str) {
        let mut entries = self.entries.write().await;
        let mut lru_order = self.lru_order.write().await;

        if entries.remove(cache_key).is_some() {
            lru_order.retain(|k| k != cache_key);
            debug!(
                cache_key = %cache_key,
                "MetadataCache invalidated entry"
            );
        }
    }

    /// Update LRU order for a cache key (move to back = most recently used)
    async fn update_lru_order(&self, cache_key: &str) {
        let mut lru_order = self.lru_order.write().await;
        lru_order.retain(|k| k != cache_key);
        lru_order.push_back(cache_key.to_string());
    }

    /// Get the current number of entries in the cache
    pub async fn len(&self) -> usize {
        self.entries.read().await.len()
    }

    /// Check if the cache is empty
    pub async fn is_empty(&self) -> bool {
        self.entries.read().await.is_empty()
    }

    /// Clear all entries from the cache
    pub async fn clear(&self) {
        let mut entries = self.entries.write().await;
        let mut lru_order = self.lru_order.write().await;
        let mut pending = self.pending_reads.write().await;

        let count = entries.len();
        entries.clear();
        lru_order.clear();
        pending.clear();

        info!(cleared_entries = count, "MetadataCache cleared");
    }

    /// Update range access without full reload
    ///
    /// This is an optimization for GET requests that just need to update
    /// range access tracking without re-reading the entire metadata from disk.
    pub async fn update_range_access(&self, cache_key: &str, range_start: u64, range_end: u64) {
        let mut entries = self.entries.write().await;

        if let Some(entry) = entries.get_mut(cache_key) {
            // Find and update the matching range
            for range in &mut entry.metadata.ranges {
                if range.start == range_start && range.end == range_end {
                    range.record_access();
                    break;
                }
            }
            entry.touch();

            debug!(
                cache_key = %cache_key,
                range_start = range_start,
                range_end = range_end,
                "MetadataCache updated range access"
            );
        }
    }

    /// Get a reference to the cached metadata if present (without staleness check)
    ///
    /// This is useful for checking if an entry exists without triggering
    /// staleness-based refresh logic.
    pub async fn peek(&self, cache_key: &str) -> Option<NewCacheMetadata> {
        let entries = self.entries.read().await;
        entries.get(cache_key).map(|e| e.metadata.clone())
    }

    /// Check if an entry exists in the cache (regardless of staleness)
    pub async fn contains(&self, cache_key: &str) -> bool {
        self.entries.read().await.contains_key(cache_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_types::{CompressionInfo, ObjectMetadata};

    /// Helper to create test metadata
    /// Generous TTLs, so admission is never refused in tests whose subject is LRU,
    /// staleness or concurrency rather than the admission gate. The gate itself is
    /// covered by `admission_is_refused_only_when_both_ttls_are_zero`.
    fn generous_ttls() -> MetadataTtls {
        MetadataTtls {
            get_ttl: Duration::from_secs(3600),
            head_ttl: Duration::from_secs(60),
        }
    }

    fn create_test_metadata(cache_key: &str) -> NewCacheMetadata {
        let now = std::time::SystemTime::now();
        NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata::new(
                format!("etag-{}", cache_key),
                "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                1024,
                Some("application/octet-stream".to_string()),
            ),
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            head_expires_at: Some(now + Duration::from_secs(60)),
            head_last_accessed: Some(now),
            head_access_count: 0,
            head_cached_at: None,
            last_validated_at: None,
        }
    }

    /// Test basic get/put/invalidate operations
    #[tokio::test]
    async fn test_basic_operations() {
        let config = MetadataCacheConfig {
            enabled: true,
            refresh_interval: Duration::from_secs(60), // Long interval for testing
            max_entries: 100,
        };
        let cache = MetadataCache::new(config);

        let key = "test-bucket/test-key";
        let metadata = create_test_metadata(key);

        // Initially empty
        assert!(cache.get(key).await.is_none());
        assert!(cache.is_empty().await);

        // Put and get
        cache.put(key, metadata.clone(), generous_ttls()).await;
        assert!(!cache.is_empty().await);
        assert_eq!(cache.len().await, 1);

        let retrieved = cache.get(key).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().cache_key, key);

        // Verify metrics
        let metrics = cache.metrics();
        assert_eq!(metrics.hits, 1);
        assert_eq!(metrics.misses, 1); // Initial get before put

        // Invalidate
        cache.invalidate(key).await;
        assert!(cache.get(key).await.is_none());
        assert!(cache.is_empty().await);
    }

    /// Test LRU eviction at capacity
    #[tokio::test]
    async fn test_lru_eviction() {
        let config = MetadataCacheConfig {
            enabled: true,
            refresh_interval: Duration::from_secs(60),
            max_entries: 3, // Small capacity for testing
        };
        let cache = MetadataCache::new(config);

        // Fill cache to capacity
        for i in 0..3 {
            let key = format!("key-{}", i);
            cache
                .put(&key, create_test_metadata(&key), generous_ttls())
                .await;
        }
        assert_eq!(cache.len().await, 3);

        // Access key-0 and key-2 to make key-1 the LRU
        cache.get("key-0").await;
        cache.get("key-2").await;

        // Add a new entry, should evict key-1 (LRU)
        cache
            .put("key-3", create_test_metadata("key-3"), generous_ttls())
            .await;
        assert_eq!(cache.len().await, 3);

        // key-1 should be evicted
        assert!(cache.get("key-1").await.is_none());
        // Others should still exist
        assert!(cache.peek("key-0").await.is_some());
        assert!(cache.peek("key-2").await.is_some());
        assert!(cache.peek("key-3").await.is_some());

        // Verify eviction metric
        let metrics = cache.metrics();
        assert!(metrics.evictions >= 1);
    }

    /// Test staleness/refresh behavior
    #[tokio::test]
    async fn test_staleness_refresh() {
        let config = MetadataCacheConfig {
            enabled: true,
            refresh_interval: Duration::from_millis(50), // Very short for testing
            max_entries: 100,
        };
        let cache = MetadataCache::new(config);

        let key = "test-key";
        cache
            .put(key, create_test_metadata(key), generous_ttls())
            .await;

        // Should be available immediately
        assert!(cache.get(key).await.is_some());

        // Wait for staleness
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Should return None due to staleness
        assert!(cache.get(key).await.is_none());

        // Verify stale refresh metric
        let metrics = cache.metrics();
        assert!(metrics.stale_refreshes >= 1);

        // Entry should still exist (just stale)
        assert!(cache.contains(key).await);
        assert!(cache.peek(key).await.is_some());
    }

    /// Test disabled cache behavior
    #[tokio::test]
    async fn test_disabled_cache() {
        let config = MetadataCacheConfig {
            enabled: false,
            refresh_interval: Duration::from_secs(60),
            max_entries: 100,
        };
        let cache = MetadataCache::new(config);

        let key = "test-key";

        // Put should be no-op when disabled
        cache
            .put(key, create_test_metadata(key), generous_ttls())
            .await;
        assert!(cache.is_empty().await);

        // Get should return None when disabled
        assert!(cache.get(key).await.is_none());
    }

    /// Test clear operation
    #[tokio::test]
    async fn test_clear() {
        let config = MetadataCacheConfig::default();
        let cache = MetadataCache::new(config);

        // Add some entries
        for i in 0..5 {
            let key = format!("key-{}", i);
            cache
                .put(&key, create_test_metadata(&key), generous_ttls())
                .await;
        }
        assert_eq!(cache.len().await, 5);

        // Clear
        cache.clear().await;
        assert!(cache.is_empty().await);
        assert_eq!(cache.len().await, 0);
    }

    /// Admission is refused only when BOTH TTLs are zero.
    ///
    /// `get-ttl-zero-enforcement` R4.4. All four corners of the matrix are asserted,
    /// not just the refusing one: the `get_ttl: 0` + non-zero `head_ttl` corner is the
    /// entire point of the rule, and a test that only checked the both-zero case would
    /// pass just as well against an `||` — which would strip HEAD's RAM tier for a key
    /// whose operator deliberately asked for HEAD caching.
    #[tokio::test]
    async fn admission_is_refused_only_when_both_ttls_are_zero() {
        let zero = Duration::ZERO;
        let some = Duration::from_secs(60);

        assert!(
            MetadataTtls {
                get_ttl: zero,
                head_ttl: zero
            }
            .forbids_admission(),
            "neither consumer may use the entry, so it must not be admitted"
        );
        assert!(
            !MetadataTtls {
                get_ttl: zero,
                head_ttl: some
            }
            .forbids_admission(),
            "THE load-bearing corner: get_ttl: 0 with a non-zero head_ttl must still be \
             admitted, or HEAD loses its RAM tier for a config that explicitly asked \
             for HEAD caching"
        );
        assert!(
            !MetadataTtls {
                get_ttl: some,
                head_ttl: zero
            }
            .forbids_admission(),
            "head_ttl: 0 alone must not refuse admission — the GET consumer still uses it"
        );
        assert!(
            !MetadataTtls {
                get_ttl: some,
                head_ttl: some
            }
            .forbids_admission(),
            "the ordinary case must be admitted"
        );

        // And the same four corners through the real admission path, so the predicate
        // is shown to be WIRED rather than merely correct — a unit test on the
        // predicate alone would pass with the check absent from `put`.
        let cache = MetadataCache::new(MetadataCacheConfig::default());
        for (i, (g, h, expect_present)) in [
            (zero, zero, false),
            (zero, some, true),
            (some, zero, true),
            (some, some, true),
        ]
        .iter()
        .enumerate()
        {
            let key = format!("admission-{}", i);
            cache
                .put(
                    &key,
                    create_test_metadata(&key),
                    MetadataTtls {
                        get_ttl: *g,
                        head_ttl: *h,
                    },
                )
                .await;
            assert_eq!(
                cache.peek(&key).await.is_some(),
                *expect_present,
                "put admission for get_ttl={:?} head_ttl={:?}",
                g,
                h
            );
        }
    }

    /// The v1.9.4 ranges rule, `ram-tier-effectiveness:R1.6`.
    ///
    /// Two corners, and the negative one is the rule: an empty `ranges` array must be
    /// refused. This asserts the predicate only — it deliberately does NOT assert that
    /// `put` applies it, because it does not and must not (see
    /// `ram_admission_permitted`'s own docs for why the check is per-site). The wiring at
    /// each of the eight call sites is asserted from
    /// `src/cache/moved_fill_tests/ram_metadata_admission_unification_test.rs` and
    /// `src/cache/moved_fill_tests/ram_metadata_masking_regression_test.rs`, which drive the production
    /// functions and read residency.
    #[test]
    fn ram_admission_requires_at_least_one_range() {
        let mut metadata = create_test_metadata("ranges-rule");
        assert!(
            metadata.ranges.is_empty(),
            "fixture: create_test_metadata is expected to build a ranges-free document"
        );
        assert!(
            !ram_admission_permitted(&metadata),
            "a document with no ranges must be refused: resident, it answers every range \
             lookup on the key with 'no ranges exist' for up to one refresh_interval"
        );

        metadata.ranges.push(crate::cache_types::RangeSpec::new(
            0,
            1023,
            "ranges-rule_0-1023.bin".to_string(),
            crate::compression::CompressionAlgorithm::Lz4,
            1024,
            1024,
        ));
        assert!(
            ram_admission_permitted(&metadata),
            "a ranges-bearing document must still be admitted — the rule is not 'refuse \
             everything', and an over-broad reading costs the RAM tier the entries it exists \
             for"
        );
    }
}
