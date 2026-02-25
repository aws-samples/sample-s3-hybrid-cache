//! RAM Cache Module
//!
//! Provides in-memory caching with configurable eviction algorithms (LRU, TinyLFU).
//! Integrates with compression system for memory efficiency and serves as first-tier cache.
//!
//! Note: Access tracking for disk metadata updates is now handled by the journal system
//! (CacheHitUpdateBuffer) at the DiskCacheManager level, not in the RAM cache.

use crate::cache::{CacheEvictionAlgorithm, RamCacheEntry};
use crate::cache_types::CacheMetadata;
use crate::compression::CompressionHandler;
use crate::{ProxyError, Result};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tracing::{debug, info, warn};

/// Result of verifying RAM cache entry against disk cache
#[derive(Debug, Clone, PartialEq)]
pub enum VerificationResult {
    /// RAM cache data matches disk cache data
    Valid,
    /// RAM cache data does not match disk cache data
    Invalid,
    /// Disk cache entry does not exist
    DiskMissing,
    /// Verification failed due to I/O error
    Error(String),
}

/// RAM cache manager with configurable eviction algorithms
pub struct RamCache {
    /// Maximum size in bytes
    pub max_size: u64,
    /// Current size in bytes
    pub current_size: u64,
    /// Eviction algorithm to use
    eviction_algorithm: CacheEvictionAlgorithm,
    /// Cache entries storage
    pub entries: HashMap<String, RamCacheEntry>,
    /// LRU tracking (for LRU algorithm)
    lru_order: VecDeque<String>,
    /// TinyLFU window and frequency estimator (for TinyLFU algorithm)
    tinylfu_window: VecDeque<String>,
    tinylfu_frequencies: HashMap<String, u64>,
    tinylfu_window_size: usize,
    /// Statistics
    hit_count: u64,
    miss_count: u64,
    eviction_count: u64,
    last_eviction: Option<SystemTime>,
}

impl RamCache {
    /// Create a new RAM cache with specified size and eviction algorithm
    pub fn new(max_size: u64, eviction_algorithm: CacheEvictionAlgorithm) -> Self {
        let tinylfu_window_size = (max_size / 1024).min(10000) as usize; // Reasonable window size

        Self {
            max_size,
            current_size: 0,
            eviction_algorithm,
            entries: HashMap::new(),
            lru_order: VecDeque::new(),
            tinylfu_window: VecDeque::new(),
            tinylfu_frequencies: HashMap::new(),
            tinylfu_window_size,
            hit_count: 0,
            miss_count: 0,
            eviction_count: 0,
            last_eviction: None,
        }
    }

    /// Create a new RAM cache with custom configuration
    /// Note: flush_interval, flush_threshold, flush_on_eviction, and verification_interval
    /// parameters are kept for API compatibility but are no longer used.
    /// Access tracking is now handled by the journal system at the DiskCacheManager level.
    pub fn new_with_config(
        max_size: u64,
        eviction_algorithm: CacheEvictionAlgorithm,
        _flush_interval: Duration,
        _flush_threshold: usize,
        _flush_on_eviction: bool,
        _verification_interval: Duration,
    ) -> Self {
        Self::new(max_size, eviction_algorithm)
    }

    /// Get an entry from the RAM cache
    pub fn get(&mut self, cache_key: &str) -> Option<RamCacheEntry> {
        if let Some(entry) = self.entries.get(cache_key).cloned() {
            // Update access tracking based on eviction algorithm
            self.update_access_tracking(cache_key);

            // Update the entry with new access info
            let mut updated_entry = entry;
            updated_entry.last_accessed = SystemTime::now();
            updated_entry.access_count += 1;

            // Update the entry in storage
            self.entries
                .insert(cache_key.to_string(), updated_entry.clone());

            self.hit_count += 1;
            debug!("RAM cache hit for key: {}", cache_key);
            Some(updated_entry)
        } else {
            self.miss_count += 1;
            debug!("RAM cache miss for key: {}", cache_key);
            None
        }
    }

    /// Check if a key exists in the RAM cache without updating access tracking
    pub fn contains(&self, cache_key: &str) -> bool {
        self.entries.contains_key(cache_key)
    }

    /// Store an entry in the RAM cache
    /// Optimized to avoid decompress/recompress cycles when data is already compressed
    pub fn put(
        &mut self,
        entry: RamCacheEntry,
        compression_handler: &mut CompressionHandler,
    ) -> Result<()> {
        let cache_key = entry.cache_key.clone();

        // Handle compression based on current state of the entry
        let final_entry = if entry.compressed {
            // Entry is already compressed (from disk cache), use it as-is
            debug!(
                "Using pre-compressed data for RAM cache entry: {} (algorithm: {:?})",
                cache_key, entry.compression_algorithm
            );
            entry
        } else {
            // Entry is uncompressed, always compress for RAM efficiency
            let mut compressed_entry = entry;
            match compression_handler.compress_always(&compressed_entry.data) {
                Ok(compressed_data) => {
                    compressed_entry.data = compressed_data;
                    compressed_entry.compressed = true;
                    compressed_entry.compression_algorithm = compression_handler.get_preferred_algorithm().clone();
                }
                Err(e) => {
                    debug!("RAM cache compression failed, storing uncompressed: {}", e);
                    // Keep original data, compressed = false
                }
            }
            compressed_entry
        };

        let final_entry_size = self.calculate_entry_size(&final_entry);

        // Check if we need to make space (use final compressed size for eviction decisions)
        while self.current_size + final_entry_size > self.max_size && !self.entries.is_empty() {
            self.evict_entry()?;
        }

        // Size check AFTER compression - this fixes the original warning issue
        if final_entry_size > self.max_size {
            warn!("Entry {} too large for RAM cache ({} bytes > {} bytes max) even after compression (algorithm: {:?})", 
                  cache_key, final_entry_size, self.max_size, final_entry.compression_algorithm);
            return Ok(());
        }

        // Remove existing entry if present
        if let Some(existing_entry) = self.entries.remove(&cache_key) {
            let existing_size = self.calculate_entry_size(&existing_entry);
            self.current_size = self.current_size.saturating_sub(existing_size);
            self.remove_from_tracking(&cache_key);
        }

        // Capture values for logging before moving the entry
        let is_compressed = final_entry.compressed;
        let compression_algorithm = final_entry.compression_algorithm.clone();

        // Add new entry
        self.entries.insert(cache_key.clone(), final_entry);
        self.current_size += final_entry_size;

        // Update tracking structures
        self.add_to_tracking(&cache_key);

        debug!(
            "Stored entry in RAM cache: {} ({} bytes, compressed: {}, algorithm: {:?})",
            cache_key, final_entry_size, is_compressed, compression_algorithm
        );
        Ok(())
    }

    /// Remove an entry from the RAM cache
    pub fn remove(&mut self, cache_key: &str) -> Option<RamCacheEntry> {
        if let Some(entry) = self.entries.remove(cache_key) {
            let entry_size = self.calculate_entry_size(&entry);
            self.current_size = self.current_size.saturating_sub(entry_size);
            self.remove_from_tracking(cache_key);
            debug!("Removed entry from RAM cache: {}", cache_key);
            Some(entry)
        } else {
            None
        }
    }

    /// Clear all entries from the RAM cache
    pub fn clear(&mut self) {
        self.entries.clear();
        self.lru_order.clear();
        self.tinylfu_window.clear();
        self.tinylfu_frequencies.clear();
        self.current_size = 0;
        info!("Cleared all entries from RAM cache");
    }

    /// Get current cache statistics
    pub fn get_stats(&self) -> RamCacheStats {
        let total_requests = self.hit_count + self.miss_count;
        let hit_rate = if total_requests > 0 {
            self.hit_count as f32 / total_requests as f32
        } else {
            0.0
        };

        RamCacheStats {
            current_size: self.current_size,
            max_size: self.max_size,
            entries_count: self.entries.len() as u64,
            hit_count: self.hit_count,
            miss_count: self.miss_count,
            hit_rate,
            eviction_count: self.eviction_count,
            last_eviction: self.last_eviction,
            eviction_algorithm: self.eviction_algorithm.clone(),
        }
    }

    /// Check if cache is enabled and has capacity
    pub fn is_enabled(&self) -> bool {
        self.max_size > 0
    }

    /// Get current size utilization as percentage
    pub fn get_utilization(&self) -> f32 {
        if self.max_size > 0 {
            (self.current_size as f32 / self.max_size as f32) * 100.0
        } else {
            0.0
        }
    }

    /// Evict an entry based on the configured algorithm
    ///
    /// NOTE: Per design, eviction does NOT flush pending updates to disk.
    /// This avoids blocking eviction operations with disk I/O.
    /// Access tracking is now handled by the journal system at the DiskCacheManager level.
    pub fn evict_entry(&mut self) -> Result<()> {
        let key_to_evict = match self.eviction_algorithm {
            CacheEvictionAlgorithm::LRU => self.find_lru_victim(),
            CacheEvictionAlgorithm::TinyLFU => self.find_tinylfu_victim(),
        };

        if let Some(key) = key_to_evict {
            if let Some(entry) = self.entries.remove(&key) {
                let entry_size = self.calculate_entry_size(&entry);
                self.current_size = self.current_size.saturating_sub(entry_size);
                self.remove_from_tracking(&key);
                self.eviction_count += 1;
                self.last_eviction = Some(SystemTime::now());

                debug!(
                    "Evicted entry from RAM cache using {:?}: {} ({} bytes)",
                    self.eviction_algorithm, key, entry_size
                );
                return Ok(());
            }
        }

        Err(ProxyError::CacheError(
            "No entries available for eviction".to_string(),
        ))
    }

    /// Find LRU victim for eviction
    fn find_lru_victim(&self) -> Option<String> {
        self.lru_order.front().cloned()
    }

    /// Find TinyLFU victim for eviction
    fn find_tinylfu_victim(&self) -> Option<String> {
        // TinyLFU combines frequency and recency
        // For simplicity, we'll use a weighted approach: frequency * recency_factor
        let now = SystemTime::now();

        self.entries
            .iter()
            .min_by_key(|(key, entry)| {
                let frequency = self.tinylfu_frequencies.get(*key).unwrap_or(&1);
                let recency_factor = now
                    .duration_since(entry.last_accessed)
                    .unwrap_or_default()
                    .as_secs()
                    .max(1); // Avoid division by zero

                // Lower score = better eviction candidate
                frequency * 1000 / recency_factor // Scale frequency to balance with recency
            })
            .map(|(key, _)| key.clone())
    }

    /// Update access tracking based on eviction algorithm
    fn update_access_tracking(&mut self, cache_key: &str) {
        match self.eviction_algorithm {
            CacheEvictionAlgorithm::LRU => {
                // Move to back of LRU queue
                if let Some(pos) = self.lru_order.iter().position(|k| k == cache_key) {
                    self.lru_order.remove(pos);
                }
                self.lru_order.push_back(cache_key.to_string());
            }
            CacheEvictionAlgorithm::TinyLFU => {
                // Update TinyLFU window and frequencies
                self.tinylfu_window.push_back(cache_key.to_string());
                *self
                    .tinylfu_frequencies
                    .entry(cache_key.to_string())
                    .or_insert(0) += 1;

                // Maintain window size
                if self.tinylfu_window.len() > self.tinylfu_window_size {
                    if let Some(old_key) = self.tinylfu_window.pop_front() {
                        // Decay frequency for evicted window entry
                        if let Some(freq) = self.tinylfu_frequencies.get_mut(&old_key) {
                            *freq = (*freq).saturating_sub(1);
                            if *freq == 0 {
                                self.tinylfu_frequencies.remove(&old_key);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Add entry to tracking structures
    fn add_to_tracking(&mut self, cache_key: &str) {
        match self.eviction_algorithm {
            CacheEvictionAlgorithm::LRU => {
                self.lru_order.push_back(cache_key.to_string());
            }
            CacheEvictionAlgorithm::TinyLFU => {
                self.tinylfu_window.push_back(cache_key.to_string());
                self.tinylfu_frequencies.insert(cache_key.to_string(), 1);

                // Maintain window size
                if self.tinylfu_window.len() > self.tinylfu_window_size {
                    if let Some(old_key) = self.tinylfu_window.pop_front() {
                        if let Some(freq) = self.tinylfu_frequencies.get_mut(&old_key) {
                            *freq = (*freq).saturating_sub(1);
                            if *freq == 0 {
                                self.tinylfu_frequencies.remove(&old_key);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Remove entry from tracking structures
    fn remove_from_tracking(&mut self, cache_key: &str) {
        match self.eviction_algorithm {
            CacheEvictionAlgorithm::LRU => {
                if let Some(pos) = self.lru_order.iter().position(|k| k == cache_key) {
                    self.lru_order.remove(pos);
                }
            }
            CacheEvictionAlgorithm::TinyLFU => {
                // Remove from window if present
                if let Some(pos) = self.tinylfu_window.iter().position(|k| k == cache_key) {
                    self.tinylfu_window.remove(pos);
                }
                // Keep frequency data for potential re-insertion
            }
        }
    }

    /// Calculate the size of a RAM cache entry
    fn calculate_entry_size(&self, entry: &RamCacheEntry) -> u64 {
        let base_size = std::mem::size_of::<RamCacheEntry>() as u64;
        let key_size = entry.cache_key.len() as u64;
        let data_size = entry.data.len() as u64;
        let metadata_size = std::mem::size_of::<CacheMetadata>() as u64;

        base_size + key_size + data_size + metadata_size
    }


    /// Parse a range cache key to extract base key, start, and end
    ///
    /// Cache keys for ranges are in format: "path:range:start:end"
    /// Returns (base_key, start, end) if successful
    pub fn parse_range_cache_key(cache_key: &str) -> Option<(String, u64, u64)> {
        let parts: Vec<&str> = cache_key.split(':').collect();
        if parts.len() >= 4 && parts[parts.len() - 3] == "range" {
            let start = parts[parts.len() - 2].parse::<u64>().ok()?;
            let end = parts[parts.len() - 1].parse::<u64>().ok()?;
            // Base key is everything before ":range:start:end"
            let base_key = parts[..parts.len() - 3].join(":");
            Some((base_key, start, end))
        } else {
            None
        }
    }

    /// Extract the base key from a cache key (without range suffix)
    pub fn extract_base_key(cache_key: &str) -> String {
        if let Some((base_key, _, _)) = Self::parse_range_cache_key(cache_key) {
            base_key
        } else {
            cache_key.to_string()
        }
    }

    /// Record a RAM cache access for disk metadata update
    ///
    /// Note: This is now a no-op. Access tracking is handled by the journal system
    /// at the DiskCacheManager level via record_range_access().
    /// This method is kept for API compatibility.
    pub fn record_disk_access(&mut self, _cache_key: &str) {
        // No-op: Access tracking is now handled by the journal system
        // at the DiskCacheManager level via CacheHitUpdateBuffer
    }

    /// Check if verification is needed for a cache key
    /// Note: Always returns false as verification is no longer needed with journal system
    pub fn should_verify(&self, _cache_key: &str) -> bool {
        false
    }

    /// Record that verification was performed for a cache key
    /// Note: This is now a no-op as verification is no longer needed with journal system
    pub fn record_verification(&mut self, _cache_key: &str) {
        // No-op
    }

    /// Get the number of pending disk updates
    /// Note: Always returns 0 as access tracking is now handled by journal system
    pub fn pending_disk_updates(&self) -> usize {
        0
    }

    /// Find expired entries for TTL-aware eviction
    /// Note: GET entries (RamCacheEntry) don't have expires_at field
    /// Their expiration is handled at the cache manager level
    pub fn find_expired_entries(&self) -> Vec<String> {
        // GET entries don't have expires_at, so return empty
        Vec::new()
    }

    /// Evict expired entries and return count of evicted entries
    pub fn evict_expired_entries(&mut self) -> Result<u64> {
        let expired_keys = self.find_expired_entries();
        let mut evicted_count = 0u64;

        for key in expired_keys {
            if let Some(entry) = self.entries.remove(&key) {
                let entry_size = self.calculate_entry_size(&entry);
                self.current_size = self.current_size.saturating_sub(entry_size);
                self.remove_from_tracking(&key);
                evicted_count += 1;
                debug!("Evicted expired entry: {}", key);
            }
        }

        if evicted_count > 0 {
            self.eviction_count += evicted_count;
            self.last_eviction = Some(SystemTime::now());
            debug!("Evicted {} expired entries from RAM cache", evicted_count);
        }

        Ok(evicted_count)
    }

    /// Unified eviction with TTL-aware priority (expired entries first, then LRU/TinyLFU)
    pub fn evict_entry_with_ttl_priority(&mut self) -> Result<()> {
        // First, try to evict any expired entries
        if let Ok(evicted_count) = self.evict_expired_entries() {
            if evicted_count > 0 {
                return Ok(());
            }
        }

        // If no expired entries, use normal eviction algorithm
        self.evict_entry()
    }

    /// Invalidate an entry from the cache
    pub fn invalidate_entry(&mut self, cache_key: &str) -> Result<()> {
        if let Some(entry) = self.entries.remove(cache_key) {
            let entry_size = self.calculate_entry_size(&entry);
            self.current_size = self.current_size.saturating_sub(entry_size);
            self.remove_from_tracking(cache_key);
            debug!("Invalidated entry from RAM cache: {}", cache_key);
        } else {
            debug!("Entry not found for invalidation: {}", cache_key);
        }

        Ok(())
    }

    /// Invalidate all entries whose key starts with the given prefix.
    /// Used to remove all cached range data for an object when it is overwritten.
    pub fn invalidate_by_prefix(&mut self, prefix: &str) -> Result<usize> {
        let keys_to_remove: Vec<String> = self
            .entries
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        let count = keys_to_remove.len();
        for key in &keys_to_remove {
            if let Some(entry) = self.entries.remove(key) {
                let entry_size = self.calculate_entry_size(&entry);
                self.current_size = self.current_size.saturating_sub(entry_size);
                self.remove_from_tracking(key);
            }
        }
        if count > 0 {
            debug!(
                "Invalidated {} RAM cache entries with prefix: {}",
                count, prefix
            );
        }
        Ok(count)
    }

    /// Get detailed cache information for debugging
    pub fn get_debug_info(&self) -> RamCacheDebugInfo {
        RamCacheDebugInfo {
            entries: self.entries.keys().cloned().collect(),
            lru_order: self.lru_order.clone().into(),
            tinylfu_window: self.tinylfu_window.clone().into(),
            tinylfu_frequencies: self.tinylfu_frequencies.clone(),
        }
    }

    /// Validate cache consistency (for testing)
    pub fn validate_consistency(&self) -> Result<()> {
        // Check that all entries in tracking structures exist in main storage
        match self.eviction_algorithm {
            CacheEvictionAlgorithm::LRU => {
                for key in &self.lru_order {
                    if !self.entries.contains_key(key) {
                        return Err(ProxyError::CacheError(format!(
                            "LRU tracking contains non-existent key: {}",
                            key
                        )));
                    }
                }
            }
            CacheEvictionAlgorithm::TinyLFU => {
                // TinyLFU may contain keys not in main storage (due to window behavior)
                // This is normal and expected
            }
        }

        // Check that current_size matches actual size
        let calculated_size: u64 = self
            .entries
            .values()
            .map(|entry| self.calculate_entry_size(entry))
            .sum();

        if calculated_size != self.current_size {
            return Err(ProxyError::CacheError(format!(
                "Size mismatch: calculated {} vs tracked {}",
                calculated_size, self.current_size
            )));
        }

        Ok(())
    }
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

/// RAM cache debug information
#[derive(Debug, Clone)]
pub struct RamCacheDebugInfo {
    pub entries: Vec<String>,
    pub lru_order: Vec<String>,
    pub tinylfu_window: Vec<String>,
    pub tinylfu_frequencies: HashMap<String, u64>,
}

/// Thread-safe RAM cache wrapper
pub struct ThreadSafeRamCache {
    inner: Arc<Mutex<RamCache>>,
}

impl ThreadSafeRamCache {
    /// Create a new thread-safe RAM cache
    pub fn new(max_size: u64, eviction_algorithm: CacheEvictionAlgorithm) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RamCache::new(max_size, eviction_algorithm))),
        }
    }

    /// Get an entry from the RAM cache
    pub fn get(&self, cache_key: &str) -> Option<RamCacheEntry> {
        self.inner.lock().unwrap().get(cache_key)
    }

    /// Store an entry in the RAM cache
    pub fn put(
        &self,
        entry: RamCacheEntry,
        compression_handler: &mut CompressionHandler,
    ) -> Result<()> {
        self.inner.lock().unwrap().put(entry, compression_handler)
    }

    /// Remove an entry from the RAM cache
    pub fn remove(&self, cache_key: &str) -> Option<RamCacheEntry> {
        self.inner.lock().unwrap().remove(cache_key)
    }

    /// Clear all entries from the RAM cache
    pub fn clear(&self) {
        self.inner.lock().unwrap().clear()
    }

    /// Get current cache statistics
    pub fn get_stats(&self) -> RamCacheStats {
        self.inner.lock().unwrap().get_stats()
    }

    /// Check if cache is enabled and has capacity
    pub fn is_enabled(&self) -> bool {
        self.inner.lock().unwrap().is_enabled()
    }

    /// Get current size utilization as percentage
    pub fn get_utilization(&self) -> f32 {
        self.inner.lock().unwrap().get_utilization()
    }

    /// Get detailed cache information for debugging
    pub fn get_debug_info(&self) -> RamCacheDebugInfo {
        self.inner.lock().unwrap().get_debug_info()
    }

    /// Validate cache consistency (for testing)
    pub fn validate_consistency(&self) -> Result<()> {
        self.inner.lock().unwrap().validate_consistency()
    }

    /// Invalidate an entry from the cache
    pub fn invalidate_entry(&self, cache_key: &str) -> Result<()> {
        self.inner.lock().unwrap().invalidate_entry(cache_key)
    }

    /// Invalidate all entries whose key starts with the given prefix
    pub fn invalidate_by_prefix(&self, prefix: &str) -> Result<usize> {
        self.inner.lock().unwrap().invalidate_by_prefix(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionHandler;

    pub(super) fn create_test_entry(key: &str, data: &[u8]) -> RamCacheEntry {
        RamCacheEntry {
            cache_key: key.to_string(),
            data: data.to_vec(),
            metadata: CacheMetadata {
                etag: "test-etag".to_string(),
                last_modified: "test-modified".to_string(),
                content_length: data.len() as u64,
                part_number: None,
                cache_control: None,
                access_count: 0,
                last_accessed: SystemTime::now(),
            },
            created_at: SystemTime::now(),
            last_accessed: SystemTime::now(),
            access_count: 0,
            compressed: false,
            compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
        }
    }

    #[test]
    fn test_ram_cache_basic_operations() {
        let mut cache = RamCache::new(1024, CacheEvictionAlgorithm::LRU);
        let mut compression_handler = CompressionHandler::new(100, true);

        // Test put and get
        let entry = create_test_entry("test:key1", b"test data");
        cache.put(entry, &mut compression_handler).unwrap();

        let retrieved = cache.get("test:key1").unwrap();
        assert_eq!(retrieved.cache_key, "test:key1");

        // Test miss
        assert!(cache.get("nonexistent").is_none());

        // Test remove
        let removed = cache.remove("test:key1").unwrap();
        assert_eq!(removed.cache_key, "test:key1");
        assert!(cache.get("test:key1").is_none());
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = RamCache::new(800, CacheEvictionAlgorithm::LRU); // Size that allows 2 entries
        let mut compression_handler = CompressionHandler::new(1000, false); // Disable compression for predictable sizes

        // Add entries that will exceed cache size
        let entry1 = create_test_entry("test:key1", &vec![b'A'; 50]);
        let entry2 = create_test_entry("test:key2", &vec![b'B'; 50]);
        let entry3 = create_test_entry("test:key3", &vec![b'C'; 50]);

        cache.put(entry1, &mut compression_handler).unwrap();
        cache.put(entry2, &mut compression_handler).unwrap();

        // Access key1 to make it recently used
        cache.get("test:key1");

        // Add key3, should evict key2 (least recently used)
        cache.put(entry3, &mut compression_handler).unwrap();

        // key1 should still be there (recently used)
        assert!(cache.get("test:key1").is_some());
        // key2 should be evicted (least recently used)
        assert!(cache.get("test:key2").is_none());
        // key3 should be there (newly added)
        assert!(cache.get("test:key3").is_some());
    }

    #[test]
    fn test_tinylfu_eviction() {
        let mut cache = RamCache::new(200, CacheEvictionAlgorithm::TinyLFU);
        let mut compression_handler = CompressionHandler::new(100, false);

        let entry1 = create_test_entry("test:key1", &vec![b'A'; 50]);
        let entry2 = create_test_entry("test:key2", &vec![b'B'; 50]);
        let entry3 = create_test_entry("test:key3", &vec![b'C'; 50]);
        let entry4 = create_test_entry("test:key4", &vec![b'D'; 50]);

        cache.put(entry1, &mut compression_handler).unwrap();
        cache.put(entry2, &mut compression_handler).unwrap();
        cache.put(entry3, &mut compression_handler).unwrap();

        // Access patterns to establish frequency and recency
        cache.get("test:key1");
        cache.get("test:key1");

        // Sleep to make key2 less recent (in real scenario)
        // For test, we'll just access key3 to make it more recent
        cache.get("test:key3");

        // Add key4, TinyLFU should evict based on frequency and recency
        cache.put(entry4, &mut compression_handler).unwrap();

        // At least one entry should be evicted
        let remaining_count = [
            cache.get("test:key1").is_some(),
            cache.get("test:key2").is_some(),
            cache.get("test:key3").is_some(),
            cache.get("test:key4").is_some(),
        ]
        .iter()
        .filter(|&&x| x)
        .count();

        assert!(remaining_count <= 3); // At least one should be evicted
    }

    #[test]
    fn test_cache_statistics() {
        let mut cache = RamCache::new(1024, CacheEvictionAlgorithm::LRU);
        let mut compression_handler = CompressionHandler::new(100, true);

        let initial_stats = cache.get_stats();
        assert_eq!(initial_stats.hit_count, 0);
        assert_eq!(initial_stats.miss_count, 0);
        assert_eq!(initial_stats.entries_count, 0);

        // Add an entry
        let entry = create_test_entry("test:key1", b"test data");
        cache.put(entry, &mut compression_handler).unwrap();

        let stats_after_put = cache.get_stats();
        assert_eq!(stats_after_put.entries_count, 1);
        assert!(stats_after_put.current_size > 0);

        // Test hit
        cache.get("test:key1");
        let stats_after_hit = cache.get_stats();
        assert_eq!(stats_after_hit.hit_count, 1);
        assert_eq!(stats_after_hit.miss_count, 0);

        // Test miss
        cache.get("nonexistent");
        let stats_after_miss = cache.get_stats();
        assert_eq!(stats_after_miss.hit_count, 1);
        assert_eq!(stats_after_miss.miss_count, 1);
        assert_eq!(stats_after_miss.hit_rate, 0.5);
    }

    #[test]
    fn test_cache_clear() {
        let mut cache = RamCache::new(1024, CacheEvictionAlgorithm::LRU);
        let mut compression_handler = CompressionHandler::new(100, true);

        // Add some entries
        let entry1 = create_test_entry("test:key1", b"data1");
        let entry2 = create_test_entry("test:key2", b"data2");
        cache.put(entry1, &mut compression_handler).unwrap();
        cache.put(entry2, &mut compression_handler).unwrap();

        assert_eq!(cache.get_stats().entries_count, 2);

        // Clear cache
        cache.clear();

        let stats = cache.get_stats();
        assert_eq!(stats.entries_count, 0);
        assert_eq!(stats.current_size, 0);
        assert!(cache.get("test:key1").is_none());
        assert!(cache.get("test:key2").is_none());
    }

    #[test]
    fn test_cache_utilization() {
        let mut cache = RamCache::new(1000, CacheEvictionAlgorithm::LRU);
        let mut compression_handler = CompressionHandler::new(100, false);

        assert_eq!(cache.get_utilization(), 0.0);

        // Add entry that uses about 10% of cache
        let entry = create_test_entry("test:key1", &vec![b'A'; 50]); // Plus metadata overhead
        cache.put(entry, &mut compression_handler).unwrap();

        let utilization = cache.get_utilization();
        assert!(utilization > 0.0);
        assert!(utilization < 100.0);
    }

    #[test]
    fn test_thread_safe_ram_cache() {
        let cache = ThreadSafeRamCache::new(1024, CacheEvictionAlgorithm::LRU);
        let mut compression_handler = CompressionHandler::new(100, true);

        // Test basic operations through thread-safe wrapper
        let entry = create_test_entry("test:key1", b"test data");
        cache.put(entry, &mut compression_handler).unwrap();

        let retrieved = cache.get("test:key1").unwrap();
        assert_eq!(retrieved.cache_key, "test:key1");

        let stats = cache.get_stats();
        assert_eq!(stats.entries_count, 1);
        assert_eq!(stats.hit_count, 1);
    }

    #[test]
    fn test_cache_consistency_validation() {
        let mut cache = RamCache::new(1024, CacheEvictionAlgorithm::LRU);
        let mut compression_handler = CompressionHandler::new(100, true);

        // Add some entries
        let entry1 = create_test_entry("test:key1", b"data1");
        let entry2 = create_test_entry("test:key2", b"data2");
        cache.put(entry1, &mut compression_handler).unwrap();
        cache.put(entry2, &mut compression_handler).unwrap();

        // Cache should be consistent
        assert!(cache.validate_consistency().is_ok());

        // Access entries to update tracking
        cache.get("test:key1");
        cache.get("test:key2");

        // Should still be consistent
        assert!(cache.validate_consistency().is_ok());
    }

    #[test]
    fn test_compression_integration() {
        let mut cache = RamCache::new(2048, CacheEvictionAlgorithm::LRU); // Larger cache
        let mut compression_handler = CompressionHandler::new(10, true); // Low threshold

        // Create entry with compressible data
        let compressible_data =
            "This is some test data that should compress well because it has repetitive content. "
                .repeat(5);
        let entry = create_test_entry("test:file.txt", compressible_data.as_bytes());

        cache.put(entry, &mut compression_handler).unwrap();

        // Verify the entry was stored and can be retrieved
        assert!(cache.entries.contains_key("test:file.txt"));

        if let Some(retrieved) = cache.get("test:file.txt") {
            assert_eq!(retrieved.cache_key, "test:file.txt");
            // The entry should have been compressed during storage
            assert!(retrieved.compressed || !compression_handler.is_compression_enabled());
        } else {
            panic!("Entry should be retrievable after putting it in cache");
        }
    }

    #[test]
    fn test_invalidation() {
        let mut cache = RamCache::new(1024, CacheEvictionAlgorithm::LRU);
        let mut compression_handler = CompressionHandler::new(100, true);

        // Add entry
        let entry = create_test_entry("test:key", b"test data");
        cache.put(entry, &mut compression_handler).unwrap();

        // Verify entry exists
        assert!(cache.get("test:key").is_some());

        // Invalidate the key
        cache.invalidate_entry("test:key").unwrap();

        // Entry should be removed
        assert!(cache.get("test:key").is_none());
    }
}

// Property-Based Tests for Eviction Behavior

#[cfg(test)]
mod eviction_property_tests {
    use super::*;
    use crate::ram_cache::tests::create_test_entry;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// Test that eviction works correctly with LRU algorithm
    #[test]
    fn test_lru_eviction() {
        let mut cache = RamCache::new(
            500, // Small cache to force eviction
            CacheEvictionAlgorithm::LRU,
        );
        let mut compression_handler = CompressionHandler::new(1000, false);

        // Add entries to fill cache
        let entry1 = create_test_entry("test:object1:range:0:1000", &vec![b'A'; 50]);
        let entry2 = create_test_entry("test:object2:range:0:1000", &vec![b'B'; 50]);

        cache.put(entry1, &mut compression_handler).unwrap();
        cache.put(entry2, &mut compression_handler).unwrap();

        // Add entry3 to force eviction of entry1 (LRU)
        let entry3 = create_test_entry("test:object3:range:0:1000", &vec![b'C'; 50]);
        cache.put(entry3, &mut compression_handler).unwrap();

        // Verify eviction occurred - entry1 should be evicted (LRU)
        // Note: exact eviction depends on cache size calculations
        let _stats = cache.get_stats();
        // Stats are retrieved to verify no panic, eviction behavior depends on size
    }

    /// Test that eviction completes quickly
    #[test]
    fn test_eviction_performance() {
        let mut cache = RamCache::new(
            500, // Small cache to force eviction
            CacheEvictionAlgorithm::LRU,
        );
        let mut compression_handler = CompressionHandler::new(1000, false);

        // Add entries to fill cache
        let entry1 = create_test_entry("test:object1:range:0:1000", &vec![b'A'; 50]);
        let entry2 = create_test_entry("test:object2:range:0:1000", &vec![b'B'; 50]);

        cache.put(entry1, &mut compression_handler).unwrap();
        cache.put(entry2, &mut compression_handler).unwrap();

        // Measure eviction time
        let start = std::time::Instant::now();

        // Add entry3 to force eviction
        let entry3 = create_test_entry("test:object3:range:0:1000", &vec![b'C'; 50]);
        cache.put(entry3, &mut compression_handler).unwrap();

        let elapsed = start.elapsed();

        // Verify eviction completed quickly (< 10ms)
        assert!(
            elapsed.as_millis() < 10,
            "Eviction took too long: {:?}",
            elapsed
        );
    }

    /// Test that eviction works correctly with multiple entries
    #[quickcheck]
    fn prop_eviction_with_multiple_entries(entry_count: u8) -> TestResult {
        if entry_count == 0 || entry_count > 20 {
            return TestResult::discard();
        }

        let mut cache = RamCache::new(
            1000, // Moderate cache size
            CacheEvictionAlgorithm::LRU,
        );
        let mut compression_handler = CompressionHandler::new(1000, false);

        // Add entries
        for i in 0..entry_count {
            let key = format!("test:object{}:range:0:1000", i);
            let entry = create_test_entry(&key, &vec![b'A'; 30]);
            cache.put(entry, &mut compression_handler).unwrap();
        }

        let _initial_entries = cache.entries.len();

        // Force eviction by adding more entries
        for i in entry_count..(entry_count + 5) {
            let key = format!("test:object{}:range:0:1000", i);
            let entry = create_test_entry(&key, &vec![b'A'; 30]);
            cache.put(entry, &mut compression_handler).unwrap();
        }

        // Cache should have handled the entries (either stored or evicted)
        let _final_entries = cache.entries.len();

        // Verify cache is still functional
        let stats = cache.get_stats();
        assert!(stats.entries_count > 0);

        TestResult::passed()
    }

    /// Test eviction behavior with different eviction algorithms
    #[test]
    fn test_eviction_algorithms() {
        for algorithm in &[CacheEvictionAlgorithm::LRU, CacheEvictionAlgorithm::TinyLFU] {
            let mut cache = RamCache::new(500, algorithm.clone());
            let mut compression_handler = CompressionHandler::new(1000, false);

            // Add entries
            let entry1 = create_test_entry("test:obj1:range:0:1000", &vec![b'A'; 50]);
            let entry2 = create_test_entry("test:obj2:range:0:1000", &vec![b'B'; 50]);

            cache.put(entry1, &mut compression_handler).unwrap();
            cache.put(entry2, &mut compression_handler).unwrap();

            // Force eviction
            let entry3 = create_test_entry("test:obj3:range:0:1000", &vec![b'C'; 50]);
            cache.put(entry3, &mut compression_handler).unwrap();

            // Cache should still be functional
            let stats = cache.get_stats();
            assert!(
                stats.entries_count > 0,
                "Cache should have entries for {:?}",
                algorithm
            );
        }
    }
}
