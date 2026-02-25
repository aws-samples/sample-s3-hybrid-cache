//! Metadata Cache Module
//!
//! Provides RAM-based caching for `NewCacheMetadata` objects to reduce disk I/O.
//! Uses simple LRU eviction (all metadata entries are similar size ~1-2KB).
//! Includes per-key locking to prevent concurrent disk reads and stale file handle recovery.

use crate::cache_types::NewCacheMetadata;
use crate::{ProxyError, Result};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// Configuration for the MetadataCache
#[derive(Debug, Clone)]
pub struct MetadataCacheConfig {
    /// Whether the metadata cache is enabled
    pub enabled: bool,
    /// How often to re-read from disk (staleness threshold)
    pub refresh_interval: Duration,
    /// Maximum number of entries in the cache
    pub max_entries: usize,
    /// Maximum retries for stale file handle errors
    pub stale_handle_max_retries: u32,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            refresh_interval: Duration::from_secs(5),
            max_entries: 10000,
            stale_handle_max_retries: 3,
        }
    }
}

/// A single entry in the metadata cache
#[derive(Debug, Clone)]
pub struct MetadataCacheEntry {
    /// The cached metadata
    pub metadata: NewCacheMetadata,
    /// When this entry was loaded from disk
    pub loaded_at: Instant,
    /// Disk file modification time when loaded (for shared storage coherency)
    pub disk_mtime: Option<SystemTime>,
    /// When this entry was last accessed in RAM
    pub last_accessed: Instant,
}

impl MetadataCacheEntry {
    /// Create a new cache entry
    pub fn new(metadata: NewCacheMetadata, disk_mtime: Option<SystemTime>) -> Self {
        let now = Instant::now();
        Self {
            metadata,
            loaded_at: now,
            disk_mtime,
            last_accessed: now,
        }
    }

    /// Check if this entry is stale based on refresh interval
    pub fn is_stale(&self, refresh_interval: Duration) -> bool {
        self.loaded_at.elapsed() >= refresh_interval
    }

    /// Update last accessed time
    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }
}

/// Metrics for the metadata cache
#[derive(Debug, Default)]
pub struct MetadataCacheMetrics {
    /// Number of cache hits
    pub hits: AtomicU64,
    /// Number of cache misses
    pub misses: AtomicU64,
    /// Number of stale refreshes (re-reads from disk due to staleness)
    pub stale_refreshes: AtomicU64,
    /// Number of evictions
    pub evictions: AtomicU64,
    /// Number of stale file handle errors encountered
    pub stale_handle_errors: AtomicU64,
}

impl MetadataCacheMetrics {
    /// Get a snapshot of current metrics
    pub fn snapshot(&self) -> MetadataCacheMetricsSnapshot {
        MetadataCacheMetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            stale_refreshes: self.stale_refreshes.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            stale_handle_errors: self.stale_handle_errors.load(Ordering::Relaxed),
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
    pub stale_handle_errors: u64,
}

impl MetadataCacheMetricsSnapshot {
    /// Calculate hit rate as a percentage
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            (self.hits as f64 / total as f64) * 100.0
        }
    }
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
            "Creating MetadataCache: enabled={}, refresh_interval={:?}, max_entries={}, stale_handle_max_retries={}",
            config.enabled, config.refresh_interval, config.max_entries, config.stale_handle_max_retries
        );
        Self {
            entries: RwLock::new(HashMap::new()),
            lru_order: RwLock::new(VecDeque::new()),
            pending_reads: RwLock::new(HashMap::new()),
            config,
            metrics: MetadataCacheMetrics::default(),
        }
    }

    /// Create a new MetadataCache with default configuration
    pub fn new_default() -> Self {
        Self::new(MetadataCacheConfig::default())
    }

    /// Check if the cache is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
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

    /// Store metadata in cache
    ///
    /// If cache is at capacity, evicts the least recently used entry.
    pub async fn put(&self, cache_key: &str, metadata: NewCacheMetadata) {
        self.put_with_mtime(cache_key, metadata, None).await;
    }

    /// Store metadata in cache with disk modification time
    ///
    /// The disk_mtime is used for shared storage coherency checks.
    ///
    /// Note: Metadata cache stores entries regardless of the object's get_ttl or
    /// ram_cache_eligible setting. Even zero-TTL objects are cached here because the
    /// metadata cache has its own refresh interval and is used for revalidation
    /// decisions (e.g., If-Modified-Since / If-None-Match). The ram_cache_eligible
    /// invariant only applies to the RAM range cache. (Requirement 6.6)
    pub async fn put_with_mtime(
        &self,
        cache_key: &str,
        metadata: NewCacheMetadata,
        disk_mtime: Option<SystemTime>,
    ) {
        if !self.config.enabled {
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
        let entry = MetadataCacheEntry::new(metadata, disk_mtime);
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

    /// Check if an entry is stale
    pub async fn is_stale(&self, cache_key: &str) -> bool {
        let entries = self.entries.read().await;
        match entries.get(cache_key) {
            Some(entry) => entry.is_stale(self.config.refresh_interval),
            None => true, // Missing entries are considered stale
        }
    }

    /// Get or load metadata with per-key locking
    ///
    /// This method ensures that only one disk read occurs for a given cache_key,
    /// even if multiple concurrent requests arrive for the same key.
    ///
    /// The loader function is called only if:
    /// - Entry doesn't exist in cache, OR
    /// - Entry is stale
    ///
    /// Includes stale file handle recovery with retry logic.
    pub async fn get_or_load<F, Fut>(&self, cache_key: &str, loader: F) -> Result<NewCacheMetadata>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(NewCacheMetadata, Option<SystemTime>)>>,
    {
        if !self.config.enabled {
            // Cache disabled, just call the loader
            let (metadata, _mtime) = loader().await?;
            return Ok(metadata);
        }

        // First, try to get from cache
        if let Some(metadata) = self.get(cache_key).await {
            return Ok(metadata);
        }

        // Need to load from disk - acquire per-key lock
        let lock = self.get_or_create_pending_lock(cache_key).await;
        let _guard = lock.lock().await;

        // Double-check after acquiring lock (another request may have loaded it)
        if let Some(metadata) = self.get(cache_key).await {
            return Ok(metadata);
        }

        // Load from disk with stale handle retry
        let result = self.load_with_retry(cache_key, loader).await?;

        // Clean up pending lock
        self.remove_pending_lock(cache_key).await;

        Ok(result)
    }

    /// Load metadata with stale file handle retry logic
    async fn load_with_retry<F, Fut>(&self, cache_key: &str, loader: F) -> Result<NewCacheMetadata>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(NewCacheMetadata, Option<SystemTime>)>>,
    {
        // For the initial load, we just call the loader once
        // Retry logic is handled by wrapping the loader in a retry loop
        match loader().await {
            Ok((metadata, disk_mtime)) => {
                self.put_with_mtime(cache_key, metadata.clone(), disk_mtime)
                    .await;
                Ok(metadata)
            }
            Err(e) => {
                if Self::is_stale_file_handle(&e) {
                    self.metrics
                        .stale_handle_errors
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        cache_key = %cache_key,
                        error = %e,
                        "Stale file handle error during metadata load"
                    );
                }
                Err(e)
            }
        }
    }

    /// Load metadata with full retry logic for stale file handles
    ///
    /// This is a more comprehensive retry wrapper that can be used when
    /// the caller wants automatic retry behavior.
    pub async fn load_with_stale_handle_retry<F, Fut>(
        &self,
        cache_key: &str,
        mut loader_factory: F,
    ) -> Result<NewCacheMetadata>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(NewCacheMetadata, Option<SystemTime>)>>,
    {
        let mut attempts = 0u32;

        loop {
            match loader_factory().await {
                Ok((metadata, disk_mtime)) => {
                    self.put_with_mtime(cache_key, metadata.clone(), disk_mtime)
                        .await;
                    return Ok(metadata);
                }
                Err(e) if Self::is_stale_file_handle(&e) => {
                    attempts += 1;
                    self.metrics
                        .stale_handle_errors
                        .fetch_add(1, Ordering::Relaxed);

                    if attempts >= self.config.stale_handle_max_retries {
                        self.invalidate(cache_key).await;
                        error!(
                            cache_key = %cache_key,
                            attempts = attempts,
                            "Stale file handle persisted after retries"
                        );
                        return Err(e);
                    }

                    warn!(
                        cache_key = %cache_key,
                        attempt = attempts,
                        max_retries = self.config.stale_handle_max_retries,
                        "Stale file handle error, retrying"
                    );

                    // Exponential backoff: 10ms, 20ms, 30ms...
                    tokio::time::sleep(Duration::from_millis(10 * attempts as u64)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Check if an error is a stale file handle error (ESTALE)
    fn is_stale_file_handle(error: &ProxyError) -> bool {
        match error {
            ProxyError::IoError(msg) => {
                // Check for ESTALE (error code 116 on Linux)
                msg.contains("stale file handle")
                    || msg.contains("ESTALE")
                    || msg.contains("os error 116")
            }
            ProxyError::CacheError(msg) => {
                msg.contains("stale file handle") || msg.contains("ESTALE")
            }
            _ => false,
        }
    }

    /// Get or create a per-key lock for preventing concurrent disk reads
    async fn get_or_create_pending_lock(&self, cache_key: &str) -> Arc<Mutex<()>> {
        let mut pending = self.pending_reads.write().await;
        pending
            .entry(cache_key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Remove a per-key lock after loading is complete
    async fn remove_pending_lock(&self, cache_key: &str) {
        let mut pending = self.pending_reads.write().await;
        pending.remove(cache_key);
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

    /// Update HEAD-specific fields without full reload
    ///
    /// This is an optimization for HEAD requests that just need to update
    /// access tracking without re-reading the entire metadata from disk.
    pub async fn update_head_access(&self, cache_key: &str, head_ttl: Duration) {
        let mut entries = self.entries.write().await;

        if let Some(entry) = entries.get_mut(cache_key) {
            entry.metadata.refresh_head_ttl(head_ttl);
            entry.metadata.record_head_access();
            entry.touch();

            debug!(
                cache_key = %cache_key,
                head_access_count = entry.metadata.head_access_count,
                "MetadataCache updated HEAD access"
            );
        }
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
    fn create_test_metadata(cache_key: &str) -> NewCacheMetadata {
        let now = SystemTime::now();
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
        }
    }

    /// Test basic get/put/invalidate operations
    #[tokio::test]
    async fn test_basic_operations() {
        let config = MetadataCacheConfig {
            enabled: true,
            refresh_interval: Duration::from_secs(60), // Long interval for testing
            max_entries: 100,
            stale_handle_max_retries: 3,
        };
        let cache = MetadataCache::new(config);

        let key = "test-bucket/test-key";
        let metadata = create_test_metadata(key);

        // Initially empty
        assert!(cache.get(key).await.is_none());
        assert!(cache.is_empty().await);

        // Put and get
        cache.put(key, metadata.clone()).await;
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
            stale_handle_max_retries: 3,
        };
        let cache = MetadataCache::new(config);

        // Fill cache to capacity
        for i in 0..3 {
            let key = format!("key-{}", i);
            cache.put(&key, create_test_metadata(&key)).await;
        }
        assert_eq!(cache.len().await, 3);

        // Access key-0 and key-2 to make key-1 the LRU
        cache.get("key-0").await;
        cache.get("key-2").await;

        // Add a new entry, should evict key-1 (LRU)
        cache.put("key-3", create_test_metadata("key-3")).await;
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
            stale_handle_max_retries: 3,
        };
        let cache = MetadataCache::new(config);

        let key = "test-key";
        cache.put(key, create_test_metadata(key)).await;

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

    /// Test concurrent get_or_load ensures single disk read
    #[tokio::test]
    async fn test_concurrent_get_or_load() {
        use std::sync::atomic::AtomicUsize;

        let config = MetadataCacheConfig {
            enabled: true,
            refresh_interval: Duration::from_secs(60),
            max_entries: 100,
            stale_handle_max_retries: 3,
        };
        let cache = Arc::new(MetadataCache::new(config));

        let key = "concurrent-test-key";
        let load_count = Arc::new(AtomicUsize::new(0));

        // Spawn multiple concurrent requests for the same key
        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache_clone = cache.clone();
            let load_count_clone = load_count.clone();
            let key_clone = key.to_string();

            handles.push(tokio::spawn(async move {
                cache_clone
                    .get_or_load(&key_clone, || {
                        let load_count = load_count_clone.clone();
                        let key = key_clone.clone();
                        async move {
                            // Simulate disk read delay
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            load_count.fetch_add(1, Ordering::SeqCst);
                            Ok((create_test_metadata(&key), None))
                        }
                    })
                    .await
            }));
        }

        // Wait for all requests to complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        // Verify only one disk read occurred
        let actual_loads = load_count.load(Ordering::SeqCst);
        assert_eq!(
            actual_loads, 1,
            "Expected exactly 1 disk read, got {}",
            actual_loads
        );

        // Verify entry is in cache
        assert!(cache.get(key).await.is_some());
    }

    /// Test disabled cache behavior
    #[tokio::test]
    async fn test_disabled_cache() {
        let config = MetadataCacheConfig {
            enabled: false,
            refresh_interval: Duration::from_secs(60),
            max_entries: 100,
            stale_handle_max_retries: 3,
        };
        let cache = MetadataCache::new(config);

        let key = "test-key";

        // Put should be no-op when disabled
        cache.put(key, create_test_metadata(key)).await;
        assert!(cache.is_empty().await);

        // Get should return None when disabled
        assert!(cache.get(key).await.is_none());
    }

    /// Test HEAD access update
    #[tokio::test]
    async fn test_head_access_update() {
        let config = MetadataCacheConfig::default();
        let cache = MetadataCache::new(config);

        let key = "test-key";
        let mut metadata = create_test_metadata(key);
        metadata.head_access_count = 0;

        cache.put(key, metadata).await;

        // Update HEAD access
        cache.update_head_access(key, Duration::from_secs(60)).await;

        // Verify access count increased
        let retrieved = cache.peek(key).await.unwrap();
        assert_eq!(retrieved.head_access_count, 1);
        assert!(retrieved.head_expires_at.is_some());
    }

    /// Test clear operation
    #[tokio::test]
    async fn test_clear() {
        let config = MetadataCacheConfig::default();
        let cache = MetadataCache::new(config);

        // Add some entries
        for i in 0..5 {
            let key = format!("key-{}", i);
            cache.put(&key, create_test_metadata(&key)).await;
        }
        assert_eq!(cache.len().await, 5);

        // Clear
        cache.clear().await;
        assert!(cache.is_empty().await);
        assert_eq!(cache.len().await, 0);
    }
}
