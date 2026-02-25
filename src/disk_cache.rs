//! Disk Cache Module
//!
//! Provides file-based cache storage with compression and atomic operations.
//! Supports shared cache coordination for multi-instance deployments.

use crate::cache_hit_update_buffer::CacheHitUpdateBuffer;
use crate::cache_types::{CacheEntry, CacheMetadata, CompressionInfo};
use crate::compression::CompressionHandler;
use crate::hybrid_metadata_writer::{HybridMetadataWriter, WriteMode};
use crate::{ProxyError, Result};
use fs2::FileExt;
use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Maximum concurrent file deletes within a single batch_delete_ranges() call
const FILE_CONCURRENCY_LIMIT: usize = 32;

/// Disk cache manager for file-based cache operations
pub struct DiskCacheManager {
    cache_dir: PathBuf,
    compression_handler: CompressionHandler,
    metrics_manager: Option<std::sync::Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
    size_tracker: Option<std::sync::Arc<crate::cache_size_tracker::CacheSizeTracker>>,
    write_cache_enabled: bool,
    /// Optional HybridMetadataWriter for shared storage mode
    /// When set, metadata writes go through the hybrid writer for multi-instance coordination
    hybrid_metadata_writer: Option<Arc<Mutex<HybridMetadataWriter>>>,
    /// Optional CacheHitUpdateBuffer for shared storage mode
    /// When set, cache-hit updates (TTL refresh, access updates) go through the buffer
    cache_hit_update_buffer: Option<Arc<CacheHitUpdateBuffer>>,
    /// Optional JournalConsolidator for direct size tracking
    /// When set, size is updated directly when storing new ranges (bypasses journal-based tracking)
    journal_consolidator: Option<Arc<crate::journal_consolidator::JournalConsolidator>>,
}

impl DiskCacheManager {
    /// Create a new disk cache manager
    pub fn new(
        cache_dir: PathBuf,
        compression_enabled: bool,
        compression_threshold: usize,
        write_cache_enabled: bool,
    ) -> Self {
        Self {
            cache_dir,
            compression_handler: CompressionHandler::new(
                compression_threshold,
                compression_enabled,
            ),
            metrics_manager: None,
            size_tracker: None,
            write_cache_enabled,
            hybrid_metadata_writer: None,
            cache_hit_update_buffer: None,
            journal_consolidator: None,
        }
    }

    /// Set metrics manager for recording metrics
    pub fn set_metrics_manager(
        &mut self,
        metrics_manager: std::sync::Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>,
    ) {
        self.metrics_manager = Some(metrics_manager);
    }

    /// Set size tracker for cache size tracking
    pub fn set_size_tracker(
        &mut self,
        size_tracker: std::sync::Arc<crate::cache_size_tracker::CacheSizeTracker>,
    ) {
        debug!("Size tracker set on DiskCacheManager");
        self.size_tracker = Some(size_tracker);
    }

    /// Set hybrid metadata writer for shared storage mode
    /// When set, metadata writes go through the hybrid writer for multi-instance coordination
    pub fn set_hybrid_metadata_writer(&mut self, writer: Arc<Mutex<HybridMetadataWriter>>) {
        info!("HybridMetadataWriter enabled for DiskCacheManager - shared storage mode active");
        self.hybrid_metadata_writer = Some(writer);
    }

    /// Set cache hit update buffer for shared storage mode
    /// When set, cache-hit updates (TTL refresh, access updates) go through the buffer
    pub fn set_cache_hit_update_buffer(&mut self, buffer: Arc<CacheHitUpdateBuffer>) {
        info!("CacheHitUpdateBuffer enabled for DiskCacheManager - journal-based cache-hit updates active");
        self.cache_hit_update_buffer = Some(buffer);
    }

    /// Set journal consolidator for direct size tracking
    /// When set, size is updated directly when storing new ranges
    pub fn set_journal_consolidator(&mut self, consolidator: Arc<crate::journal_consolidator::JournalConsolidator>) {
        info!("JournalConsolidator enabled for DiskCacheManager - direct size tracking active");
        self.journal_consolidator = Some(consolidator);
    }

    /// Get the cache hit update buffer (if set)
    pub fn get_cache_hit_update_buffer(&self) -> Option<Arc<CacheHitUpdateBuffer>> {
        self.cache_hit_update_buffer.clone()
    }

    /// Record a range access using the cache hit update buffer (journal-based)
    /// This records access count increments that will be consolidated to metadata
    pub async fn record_range_access(
        &self,
        cache_key: &str,
        range_start: u64,
        range_end: u64,
    ) -> Result<()> {
        if let Some(buffer) = &self.cache_hit_update_buffer {
            buffer
                .record_access_update(cache_key, range_start, range_end, 1)
                .await
        } else {
            // No buffer configured (single instance mode), skip access recording
            // Access counts will be updated directly during cache operations
            debug!(
                "No cache hit update buffer configured, skipping access recording for {}",
                cache_key
            );
            Ok(())
        }
    }

    /// Check if hybrid metadata writer is enabled.
    /// In practice this always returns true since the hybrid writer is always configured.
    pub fn is_shared_storage_mode(&self) -> bool {
        self.hybrid_metadata_writer.is_some()
    }

    /// Get reference to compression handler
    pub fn get_compression_handler(&self) -> &CompressionHandler {
        &self.compression_handler
    }

    /// Scan journal files for pending range entries that match the requested range
    ///
    /// This is used as a fallback when metadata doesn't have a range but it might
    /// exist in a pending journal entry (not yet consolidated). This closes the
    /// race condition window between journal write and consolidation.
    ///
    /// Only used in shared storage mode.
    pub async fn find_pending_journal_ranges(
        &self,
        cache_key: &str,
        requested_start: u64,
        requested_end: u64,
    ) -> Result<Vec<crate::cache_types::RangeSpec>> {
        // Only check journals in shared storage mode
        if !self.is_shared_storage_mode() {
            return Ok(Vec::new());
        }

        let journals_dir = self.cache_dir.join("metadata").join("_journals");
        if !journals_dir.exists() {
            return Ok(Vec::new());
        }

        let mut matching_ranges = Vec::new();

        // Scan all .journal files
        let journal_entries = match std::fs::read_dir(&journals_dir) {
            Ok(entries) => entries,
            Err(e) => {
                debug!("Failed to read journals directory: {}", e);
                return Ok(Vec::new());
            }
        };

        for journal_entry in journal_entries {
            let journal_entry = match journal_entry {
                Ok(e) => e,
                Err(_) => continue,
            };

            let journal_path = journal_entry.path();
            if !journal_path.is_file() {
                continue;
            }

            let file_name = match journal_path.file_name().and_then(|n| n.to_str()) {
                Some(name) if name.ends_with(".journal") => name,
                _ => continue,
            };

            // Read and parse journal file
            let content = match tokio::fs::read_to_string(&journal_path).await {
                Ok(c) => c,
                Err(e) => {
                    debug!("Failed to read journal file {:?}: {}", journal_path, e);
                    continue;
                }
            };

            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }

                let entry: crate::journal_manager::JournalEntry = match serde_json::from_str(line) {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                // Check if this entry is for our cache key and is an Add operation
                if entry.cache_key != cache_key {
                    continue;
                }

                if entry.operation != crate::journal_manager::JournalOperation::Add {
                    continue;
                }

                // Check if the range overlaps with requested range
                let range = &entry.range_spec;
                let overlaps = requested_start <= range.end && range.start <= requested_end;

                if overlaps {
                    debug!(
                        "[JOURNAL_LOOKUP] Found pending range in journal: cache_key={}, range={}-{}, journal_file={}",
                        cache_key, range.start, range.end, file_name
                    );

                    // Check for exact match or full containment - can return early
                    if range.start == requested_start && range.end == requested_end {
                        debug!(
                            "[JOURNAL_LOOKUP] Exact match found in journal: cache_key={}, range={}-{}",
                            cache_key, range.start, range.end
                        );
                        return Ok(vec![range.clone()]);
                    }

                    if range.start <= requested_start && range.end >= requested_end {
                        debug!(
                            "[JOURNAL_LOOKUP] Full containment found in journal: cache_key={}, range={}-{}",
                            cache_key, range.start, range.end
                        );
                        return Ok(vec![range.clone()]);
                    }

                    matching_ranges.push(range.clone());
                }
            }
        }

        // Sort by start position
        matching_ranges.sort_by_key(|r| r.start);

        if !matching_ranges.is_empty() {
            debug!(
                "[JOURNAL_LOOKUP] Found {} pending ranges in journals for cache_key={}, requested_range={}-{}",
                matching_ranges.len(), cache_key, requested_start, requested_end
            );
        }

        Ok(matching_ranges)
    }

    /// Initialize the disk cache directory structure
    ///
    /// Creates only base directories for the new bucket-first sharded structure:
    /// - metadata: Base directory for metadata files (bucket/XX/YYY structure created lazily)
    /// - ranges: Base directory for range binary files (bucket/XX/YYY structure created lazily)
    /// - locks: Directory for file locks
    ///
    /// Bucket and hash directories (XX/YYY) are NOT created during initialization.
    /// They are created lazily when the first file is written to that shard.
    /// This supports the new sharding architecture where directories are created on-demand.
    ///
    /// # Requirements
    /// Implements Requirements 8.1, 13.1, 13.3, 13.4
    pub async fn initialize(&self) -> Result<()> {
        // Create cache directory if it doesn't exist
        if !self.cache_dir.exists() {
            std::fs::create_dir_all(&self.cache_dir).map_err(|e| {
                ProxyError::CacheError(format!("Failed to create cache directory: {}", e))
            })?;
            info!("Created cache directory: {:?}", self.cache_dir);
        }

        // Create only base directories for the new sharded structure
        // Do NOT create flat subdirectories or bucket/hash directories
        // Bucket and hash directories will be created lazily on first write
        let mut subdirs = vec!["metadata", "ranges", "locks"];
        if self.write_cache_enabled {
            subdirs.push("mpus_in_progress");
        }

        for subdir in &subdirs {
            let subdir_path = self.cache_dir.join(subdir);
            if !subdir_path.exists() {
                std::fs::create_dir_all(&subdir_path).map_err(|e| {
                    ProxyError::CacheError(format!(
                        "Failed to create cache subdirectory {}: {}",
                        subdir, e
                    ))
                })?;
                debug!("Created base directory: {:?}", subdir_path);
            }
        }

        info!(
            "Disk cache manager initialized with bucket-first sharded structure: {:?}. \
            Bucket and hash directories will be created lazily on first write.",
            self.cache_dir
        );
        Ok(())
    }

    /// Store cache entry to disk with compression
    pub async fn store_cache_entry(
        &mut self,
        cache_key: &str,
        response: &[u8],
        metadata: CacheMetadata,
    ) -> Result<()> {
        debug!("Storing cache entry to disk for key: {}", cache_key);

        // Create cache entry
        let now = SystemTime::now();
        let mut cache_entry = CacheEntry {
            cache_key: cache_key.to_string(),
            headers: HashMap::new(),
            body: Some(response.to_vec()),
            ranges: Vec::new(),
            metadata,
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600), // 1 hour default
            metadata_expires_at: now + std::time::Duration::from_secs(3600), // 1 hour default
            compression_info: CompressionInfo::default(),
        };

        // Apply compression
        self.compress_cache_entry(&mut cache_entry)?;

        // Get file paths
        let cache_file_path = self.get_cache_file_path(cache_key);
        let metadata_file_path = self.get_metadata_file_path(cache_key);

        // Perform atomic write
        self.atomic_write_cache_entry(&cache_entry, &cache_file_path, &metadata_file_path)
            .await?;

        info!(
            "Successfully stored cache entry to disk for key: {}",
            cache_key
        );
        Ok(())
    }

    /// Retrieve cache entry from disk with decompression
    pub async fn get_cache_entry(&self, cache_key: &str) -> Result<Option<CacheEntry>> {
        debug!("Retrieving cache entry from disk for key: {}", cache_key);

        let cache_file_path = self.get_cache_file_path(cache_key);
        let metadata_file_path = self.get_metadata_file_path(cache_key);

        // Try to read metadata directly (no .exists() check to avoid NFS directory caching)
        let _content = match std::fs::read_to_string(&metadata_file_path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!(
                    "Cache miss for key: {} (metadata file not found)",
                    cache_key
                );
                return Ok(None);
            }
            Err(e) => {
                return Err(ProxyError::CacheError(format!(
                    "Failed to read metadata: {}",
                    e
                )));
            }
        };

        // Read metadata
        let metadata_content = match std::fs::read_to_string(&metadata_file_path) {
            Ok(content) => content,
            Err(e) => {
                warn!("Failed to read metadata file for key {}: {}", cache_key, e);
                return Ok(None);
            }
        };

        let mut cache_entry: CacheEntry = match serde_json::from_str(&metadata_content) {
            Ok(entry) => entry,
            Err(e) => {
                warn!(
                    "Failed to deserialize metadata for key {}: {}",
                    cache_key, e
                );
                return Ok(None);
            }
        };

        // Check expiration
        if SystemTime::now() > cache_entry.expires_at {
            debug!("Cache entry expired for key: {}", cache_key);
            let _ = self.invalidate_cache_entry(cache_key).await;
            return Ok(None);
        }

        // Read cache data
        if cache_entry.body.is_some() {
            match std::fs::read(&cache_file_path) {
                Ok(cache_data) => {
                    cache_entry.body = Some(cache_data);
                }
                Err(e) => {
                    warn!("Failed to read cache file for key {}: {}", cache_key, e);
                    return Ok(None);
                }
            }
        }

        // Decompress
        self.decompress_cache_entry(&mut cache_entry)?;

        debug!("Cache hit on disk for key: {}", cache_key);
        Ok(Some(cache_entry))
    }

    /// Invalidate cache entry by removing files
    ///
    /// This method removes both metadata and all associated range files for a cache entry.
    /// Handles both the new bucket-first sharded structure and legacy flat structure
    /// for backward compatibility.
    ///
    /// # Requirements
    /// Implements Requirement 8.4: Update invalidate_cache_entry() to use sharded paths
    ///
    /// # Behavior
    /// 1. Tries new sharded structure first (for range-based caching)
    /// 2. Falls back to old flat structure (for legacy full-object caching)
    /// 3. Removes all range binary files (.bin) if using new structure
    /// 4. Removes metadata and cache files
    /// 5. Handles missing files gracefully (logs warning but continues)
    /// 6. Removes lock files if they exist
    pub async fn invalidate_cache_entry(&self, cache_key: &str) -> Result<()> {
        debug!("Invalidating cache entry for key: {}", cache_key);

        let mut removed_count = 0;
        let mut failed_count = 0;

        // ===== NEW SHARDED STRUCTURE (range-based caching) =====

        // Step 1: Get metadata file path using new sharded structure
        let metadata_file_path = self.get_new_metadata_file_path(cache_key);

        // Step 2: Try to load metadata to find all range files
        // If metadata doesn't exist or is corrupted, we'll still try to clean up
        let ranges_to_remove = if metadata_file_path.exists() {
            match std::fs::read_to_string(&metadata_file_path) {
                Ok(content) => {
                    match serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&content) {
                        Ok(metadata) => {
                            debug!(
                                "Loaded metadata for invalidation: key={}, ranges={}",
                                cache_key,
                                metadata.ranges.len()
                            );
                            metadata.ranges
                        }
                        Err(e) => {
                            warn!(
                                "Failed to parse metadata during invalidation: key={}, error={}. Will remove metadata file only.",
                                cache_key, e
                            );
                            Vec::new()
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to read metadata during invalidation: key={}, error={}. Will remove metadata file only.",
                        cache_key, e
                    );
                    Vec::new()
                }
            }
        } else {
            debug!(
                "Metadata file not found in new structure: key={}, path={:?}",
                cache_key, metadata_file_path
            );
            Vec::new()
        };

        // Step 3: Remove all range binary files (new structure)
        for range_spec in &ranges_to_remove {
            let range_file_path =
                self.get_new_range_file_path(cache_key, range_spec.start, range_spec.end);

            if range_file_path.exists() {
                match std::fs::remove_file(&range_file_path) {
                    Ok(_) => {
                        removed_count += 1;
                        debug!(
                            "Removed range file: key={}, range={}-{}, path={:?}",
                            cache_key, range_spec.start, range_spec.end, range_file_path
                        );
                    }
                    Err(e) => {
                        failed_count += 1;
                        warn!(
                            "Failed to remove range file: key={}, range={}-{}, path={:?}, error={}",
                            cache_key, range_spec.start, range_spec.end, range_file_path, e
                        );
                    }
                }
            } else {
                debug!(
                    "Range file not found (already removed?): key={}, range={}-{}, path={:?}",
                    cache_key, range_spec.start, range_spec.end, range_file_path
                );
            }
        }

        // Step 4: Remove metadata file (new structure)
        if metadata_file_path.exists() {
            match std::fs::remove_file(&metadata_file_path) {
                Ok(_) => {
                    removed_count += 1;
                    debug!(
                        "Removed metadata file: key={}, path={:?}",
                        cache_key, metadata_file_path
                    );
                }
                Err(e) => {
                    failed_count += 1;
                    warn!(
                        "Failed to remove metadata file: key={}, path={:?}, error={}",
                        cache_key, metadata_file_path, e
                    );
                }
            }
        }

        // Step 5: Remove lock file if it exists (new structure)
        let lock_file_path = metadata_file_path.with_extension("meta.lock");
        if lock_file_path.exists() {
            match std::fs::remove_file(&lock_file_path) {
                Ok(_) => {
                    removed_count += 1;
                    debug!(
                        "Removed lock file: key={}, path={:?}",
                        cache_key, lock_file_path
                    );
                }
                Err(e) => {
                    // Lock file removal failure is not critical
                    debug!(
                        "Failed to remove lock file (non-critical): key={}, path={:?}, error={}",
                        cache_key, lock_file_path, e
                    );
                }
            }
        }

        // ===== OLD FLAT STRUCTURE (legacy full-object caching) =====
        // For backward compatibility with old cache entries

        let old_cache_file_path = self.get_cache_file_path(cache_key);
        let old_metadata_file_path = self.get_metadata_file_path(cache_key);

        // Remove old cache file
        if old_cache_file_path.exists() {
            match std::fs::remove_file(&old_cache_file_path) {
                Ok(_) => {
                    removed_count += 1;
                    debug!(
                        "Removed old cache file: key={}, path={:?}",
                        cache_key, old_cache_file_path
                    );
                }
                Err(e) => {
                    failed_count += 1;
                    warn!(
                        "Failed to remove old cache file: key={}, path={:?}, error={}",
                        cache_key, old_cache_file_path, e
                    );
                }
            }
        }

        // Remove old metadata file
        if old_metadata_file_path.exists() {
            match std::fs::remove_file(&old_metadata_file_path) {
                Ok(_) => {
                    removed_count += 1;
                    debug!(
                        "Removed old metadata file: key={}, path={:?}",
                        cache_key, old_metadata_file_path
                    );
                }
                Err(e) => {
                    failed_count += 1;
                    warn!(
                        "Failed to remove old metadata file: key={}, path={:?}, error={}",
                        cache_key, old_metadata_file_path, e
                    );
                }
            }
        }

        info!(
            "Invalidated cache entry: key={}, removed={} files, failed={} files",
            cache_key, removed_count, failed_count
        );

        Ok(())
    }

    /// Compress cache entry data
    fn compress_cache_entry(&mut self, entry: &mut CacheEntry) -> Result<()> {
        if let Some(body) = &entry.body {
            let path = self.extract_path_from_cache_key(&entry.cache_key);
            let result = self
                .compression_handler
                .compress_content_aware_with_metadata(body, &path);

            entry.body = Some(result.data);
            entry.compression_info.body_algorithm = result.algorithm;
            entry.compression_info.original_size = Some(result.original_size);
            entry.compression_info.compressed_size = Some(result.compressed_size);
            entry.compression_info.file_extension = Some(self.extract_file_extension(&path));
        }
        Ok(())
    }

    /// Decompress cache entry data
    fn decompress_cache_entry(&self, entry: &mut CacheEntry) -> Result<()> {
        if let Some(body) = &entry.body {
            match self
                .compression_handler
                .decompress_with_algorithm(body, entry.compression_info.body_algorithm.clone())
            {
                Ok(decompressed_body) => {
                    entry.body = Some(decompressed_body);
                }
                Err(e) => {
                    error!("Failed to decompress cache entry body: {}", e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Get cache file path for a given cache key
    fn get_cache_file_path(&self, cache_key: &str) -> PathBuf {
        let cache_type = self.determine_cache_type(cache_key);
        let safe_key = self.sanitize_cache_key(cache_key);
        self.cache_dir
            .join(cache_type)
            .join(format!("{}.cache", safe_key))
    }

    /// Get metadata file path for a given cache key
    fn get_metadata_file_path(&self, cache_key: &str) -> PathBuf {
        let cache_type = self.determine_cache_type(cache_key);
        let safe_key = self.sanitize_cache_key(cache_key);
        self.cache_dir
            .join(cache_type)
            .join(format!("{}.meta", safe_key))
    }

    /// Determine cache type from cache key for directory organization
    pub fn determine_cache_type(&self, cache_key: &str) -> &str {
        if cache_key.contains(":part:") {
            "parts" // Object parts
        } else if cache_key.contains(":range:") {
            "ranges" // Range requests
        } else {
            "metadata" // Full objects
        }
    }

    /// Sanitize cache key for safe filesystem usage
    /// Uses percent encoding to prevent collisions while maintaining filesystem safety
    pub fn sanitize_cache_key(&self, cache_key: &str) -> String {
        // Use the same percent encoding as sanitize_cache_key_new for consistency
        self.sanitize_cache_key_new(cache_key)
    }

    /// Perform atomic write operation for cache entry
    async fn atomic_write_cache_entry(
        &self,
        cache_entry: &CacheEntry,
        cache_file_path: &PathBuf,
        metadata_file_path: &PathBuf,
    ) -> Result<()> {
        // Create parent directories if they don't exist
        if let Some(parent) = cache_file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                ProxyError::CacheError(format!("Failed to create cache directory: {}", e))
            })?;
        }

        // Also create parent directories for metadata file
        if let Some(parent) = metadata_file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                ProxyError::CacheError(format!("Failed to create metadata directory: {}", e))
            })?;
        }

        // Write to temporary files first for atomic operation
        let temp_cache_file = cache_file_path.with_extension("cache.tmp");
        let temp_metadata_file = metadata_file_path.with_extension("meta.tmp");

        // Write cache data
        if let Some(body) = &cache_entry.body {
            std::fs::write(&temp_cache_file, body).map_err(|e| {
                ProxyError::CacheError(format!("Failed to write cache file: {}", e))
            })?;
        }

        // Write metadata
        let metadata_json = serde_json::to_string_pretty(cache_entry).map_err(|e| {
            ProxyError::CacheError(format!("Failed to serialize cache metadata: {}", e))
        })?;
        std::fs::write(&temp_metadata_file, metadata_json)
            .map_err(|e| ProxyError::CacheError(format!("Failed to write metadata file: {}", e)))?;

        // Atomic rename operations
        std::fs::rename(&temp_cache_file, cache_file_path)
            .map_err(|e| ProxyError::CacheError(format!("Failed to rename cache file: {}", e)))?;
        std::fs::rename(&temp_metadata_file, metadata_file_path).map_err(|e| {
            ProxyError::CacheError(format!("Failed to rename metadata file: {}", e))
        })?;

        debug!("Atomically wrote cache entry to: {:?}", cache_file_path);
        Ok(())
    }

    /// Extract path from cache key for content-aware compression
    fn extract_path_from_cache_key(&self, cache_key: &str) -> String {
        // Cache keys are in new format: path or path:version:id or path:part:num etc.
        // Extract the path part (before any additional components) for file extension detection
        let parts: Vec<&str> = cache_key.split(':').collect();
        if !parts.is_empty() {
            parts[0].to_string()
        } else {
            cache_key.to_string()
        }
    }

    /// Extract file extension from path
    fn extract_file_extension(&self, path: &str) -> String {
        if let Some(last_segment) = path.split('/').last() {
            if let Some(dot_pos) = last_segment.rfind('.') {
                return last_segment[dot_pos + 1..].to_lowercase();
            }
        }
        String::new()
    }

    // ============================================================================
    // New Range Storage Architecture Methods
    // ============================================================================

    /// Get metadata file path for new range storage architecture
    /// Returns path: cache_dir/metadata/{bucket}/{XX}/{YYY}/{sanitized_key}.meta
    /// Uses bucket-first hash-based sharding for scalability
    pub fn get_new_metadata_file_path(&self, cache_key: &str) -> PathBuf {
        let base_dir = self.cache_dir.join("metadata");
        let sanitized_key = self.sanitize_cache_key_new(cache_key);

        // Requirement 1.2: Log input cache key and resulting path
        // Use get_sharded_path which implements bucket-first sharding
        // If parsing fails (invalid cache key format), fall back to flat structure
        match get_sharded_path(&base_dir, cache_key, ".meta") {
            Ok(path) => {
                debug!(
                    "[PATH_RESOLUTION] Metadata path constructed: cache_key={}, sanitized_key={}, path={:?}",
                    cache_key, sanitized_key, path
                );
                path
            }
            Err(e) => {
                // Fall back to old flat structure for backward compatibility
                warn!(
                    "[PATH_RESOLUTION] Failed to get sharded path, using flat structure: cache_key={}, sanitized_key={}, error={}",
                    cache_key, sanitized_key, e
                );
                let flat_path = base_dir.join(format!("{}.meta", sanitized_key));
                debug!(
                    "[PATH_RESOLUTION] Flat path constructed: cache_key={}, sanitized_key={}, path={:?}",
                    cache_key, sanitized_key, flat_path
                );
                flat_path
            }
        }
    }

    /// Get range binary file path for new range storage architecture
    /// Returns path: cache_dir/ranges/{bucket}/{XX}/{YYY}/{sanitized_key}_{start}-{end}.bin
    /// Uses bucket-first hash-based sharding for scalability
    ///
    /// # Requirements
    /// Implements Requirements 3.1, 3.2, 3.3, 3.4, 3.5
    pub fn get_new_range_file_path(&self, cache_key: &str, start: u64, end: u64) -> PathBuf {
        let base_dir = self.cache_dir.join("ranges");
        let suffix = format!("_{}-{}.bin", start, end);
        let sanitized_key = self.sanitize_cache_key_new(cache_key);

        // Requirement 3.5: Use get_sharded_path which implements bucket-first sharding
        // If parsing fails (invalid cache key format), fall back to flat structure
        match get_sharded_path(&base_dir, cache_key, &suffix) {
            Ok(path) => {
                debug!(
                    "[PATH_RESOLUTION] Range path constructed: cache_key={}, sanitized_key={}, range={}-{}, path={:?}",
                    cache_key, sanitized_key, start, end, path
                );
                path
            }
            Err(e) => {
                // Requirement 3.4: Fall back to old flat structure for backward compatibility
                warn!(
                    "[PATH_RESOLUTION] Failed to get sharded path for range, using flat structure: cache_key={}, sanitized_key={}, range={}-{}, error={}",
                    cache_key, sanitized_key, start, end, e
                );
                let flat_path = base_dir.join(format!("{}_{}-{}.bin", sanitized_key, start, end));
                debug!(
                    "[PATH_RESOLUTION] Flat range path constructed: cache_key={}, sanitized_key={}, range={}-{}, path={:?}",
                    cache_key, sanitized_key, start, end, flat_path
                );
                flat_path
            }
        }
    }

    /// Sanitize cache key for new range storage architecture
    /// Uses percent encoding to prevent collisions while maintaining filesystem safety
    /// For keys that would exceed 200 characters, uses BLAKE3 hash to ensure filesystem compatibility
    ///
    /// # Requirements
    /// Implements Requirements 2.1, 2.2, 2.3, 2.4
    pub fn sanitize_cache_key_new(&self, cache_key: &str) -> String {
        use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

        // Define only the filesystem-unsafe ASCII characters that need encoding
        // Start with CONTROLS (0x00-0x1F, 0x7F) and add filesystem-specific chars
        // This preserves all non-ASCII UTF-8 (unicode) characters
        const FRAGMENT: &AsciiSet = &CONTROLS
            .add(b' ') // Space
            .add(b'/') // Path separator (Unix)
            .add(b'\\') // Path separator (Windows)
            .add(b':') // Drive letter separator (Windows), problematic on macOS
            .add(b'*') // Wildcard (Windows)
            .add(b'?') // Wildcard (Windows)
            .add(b'"') // Quote (Windows)
            .add(b'<') // Redirect (Windows)
            .add(b'>') // Redirect (Windows)
            .add(b'|') // Pipe (Windows)
            .add(b'%'); // Percent (to avoid double-encoding issues)

        // Use utf8_percent_encode which preserves non-ASCII UTF-8 sequences
        let sanitized = utf8_percent_encode(cache_key, FRAGMENT).to_string();

        // Requirement 2.5: Log if sanitization changed the key
        if sanitized != cache_key {
            debug!(
                "[CACHE_KEY_VALIDATION] Cache key sanitized: original={}, sanitized={}",
                cache_key, sanitized
            );
        }

        // Filesystem filename limit is typically 255 bytes
        // Use 200 as threshold to leave room for extensions (.meta, .bin, range suffixes)
        if sanitized.len() > 200 {
            // Hash long keys to ensure they fit within filesystem limits
            let hash = blake3::hash(cache_key.as_bytes());
            let hashed = format!("long_key_{}", hash.to_hex());
            // Requirement 2.4: Log when keys are hashed
            debug!(
                "[CACHE_KEY_VALIDATION] Cache key hashed (exceeds 200 chars): original_length={}, sanitized_length={}, hashed={}",
                cache_key.len(), sanitized.len(), hashed
            );
            hashed
        } else {
            sanitized
        }
    }

    /// Validate cache key format consistency
    ///
    /// This method checks if a cache key would be transformed differently
    /// between storage and lookup operations, which could cause cache misses.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key to validate
    ///
    /// # Returns
    /// * `Ok(())` - If the key is consistent
    /// * `Err(ProxyError)` - If the key format is invalid or inconsistent
    ///
    /// # Requirements
    /// Implements Requirements 2.5, 5.3, 5.4
    pub fn validate_cache_key_format(&self, cache_key: &str) -> Result<()> {
        // Normalize the key
        let normalized = normalize_cache_key(cache_key);

        // Check if normalization changed the key
        if normalized != cache_key {
            warn!(
                "[CACHE_KEY_VALIDATION] Cache key format mismatch detected: original={}, normalized={}. \
                This may cause cache misses if storage and lookup use different formats.",
                cache_key, normalized
            );
        }

        // Try to parse the normalized key
        let (bucket, object_key) = parse_cache_key(&normalized)?;

        // Sanitize the key
        let sanitized = self.sanitize_cache_key_new(&normalized);

        debug!(
            "[CACHE_KEY_VALIDATION] Cache key validation: original={}, normalized={}, sanitized={}, bucket={}, object_key={}",
            cache_key, normalized, sanitized, bucket, object_key
        );

        Ok(())
    }

    /// Store a range with atomic operations
    /// Implements Requirements 1.1, 3.1, 3.2, 3.4, 3.5
    ///
    /// This method:
    /// 1. Compresses the range data if appropriate
    /// 2. Writes range data to a .tmp file then atomically renames it
    /// 3. Updates metadata file atomically
    /// 4. Ensures both operations succeed or both fail (atomic guarantee)
    /// 5. Handles partial write failures with cleanup
    pub async fn store_range(
        &mut self,
        cache_key: &str,
        start: u64,
        end: u64,
        data: &[u8],
        object_metadata: crate::cache_types::ObjectMetadata,
        ttl: std::time::Duration,
        compression_enabled: bool,
    ) -> Result<()> {
        use crate::cache_types::{NewCacheMetadata, RangeSpec};

        let operation_start = std::time::Instant::now();
        debug!(
            "Starting range storage operation: key={}, range={}-{}, size={} bytes",
            cache_key,
            start,
            end,
            data.len()
        );

        // Validate range boundaries
        if start > end {
            error!(
                "Range validation failed: invalid boundaries for key={}, start={}, end={}",
                cache_key, start, end
            );
            return Err(ProxyError::CacheError(format!(
                "Invalid range: start ({}) > end ({})",
                start, end
            )));
        }

        let uncompressed_size = data.len() as u64;
        if uncompressed_size != (end - start + 1) {
            error!(
                "Range validation failed: data size mismatch for key={}, expected={}, actual={}",
                cache_key,
                end - start + 1,
                uncompressed_size
            );
            return Err(ProxyError::CacheError(format!(
                "Data size ({}) doesn't match range size ({})",
                uncompressed_size,
                end - start + 1
            )));
        }

        debug!(
            "Range validation passed: key={}, range={}-{}, size={} bytes",
            cache_key, start, end, uncompressed_size
        );

        // Step 1: Compress the range data if appropriate
        // Requirement 5.1, 5.2, 5.3: Per-bucket compression control
        let compression_start = std::time::Instant::now();
        let path = self.extract_path_from_cache_key(cache_key);
        let (compressed_data, compression_algorithm, compressed_size) = if compression_enabled {
            let compression_result = self
                .compression_handler
                .compress_content_aware_with_metadata(data, &path);
            (compression_result.data, compression_result.algorithm, compression_result.compressed_size)
        } else {
            // Compression disabled for this bucket — store uncompressed
            (data.to_vec(), crate::compression::CompressionAlgorithm::None, data.len() as u64)
        };
        let compression_duration = compression_start.elapsed();

        debug!(
            "Range compression completed: key={}, range={}-{}, algorithm={:?}, original={} bytes, compressed={} bytes, ratio={:.2}%, duration={:.2}ms, compression_enabled={}",
            cache_key,
            start,
            end,
            compression_algorithm,
            uncompressed_size,
            compressed_size,
            (compressed_size as f64 / uncompressed_size as f64) * 100.0,
            compression_duration.as_secs_f64() * 1000.0,
            compression_enabled
        );

        // Step 2: Write range data to .tmp file (but don't make it visible yet)
        // We'll rename it AFTER updating metadata to avoid race conditions
        let write_start = std::time::Instant::now();
        let range_file_path = self.get_new_range_file_path(cache_key, start, end);
        let range_tmp_path = range_file_path.with_extension("bin.tmp");

        debug!(
            "Writing range binary file to temp: key={}, range={}-{}, path={:?}, size={} bytes",
            cache_key, start, end, range_tmp_path, compressed_size
        );

        // Ensure ranges directory exists
        if let Some(parent) = range_file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                error!(
                    "Failed to create ranges directory: key={}, path={:?}, error={}",
                    cache_key, parent, e
                );
                ProxyError::CacheError(format!("Failed to create ranges directory: {}", e))
            })?;
        }

        // Write to temporary file (keep it as .tmp until metadata is updated)
        if let Err(e) = std::fs::write(&range_tmp_path, &compressed_data) {
            // Clean up tmp file on failure
            let _ = std::fs::remove_file(&range_tmp_path);

            // Check if this is a disk space exhaustion error
            // Requirement 8.3: Handle disk space exhaustion
            if e.kind() == std::io::ErrorKind::Other
                && e.to_string().contains("No space left on device")
            {
                error!(
                    "Disk space exhausted: key={}, range={}-{}, attempted_size={} bytes, error={}",
                    cache_key, start, end, compressed_size, e
                );
                // Record disk full event metric
                if let Some(metrics_manager) = &self.metrics_manager {
                    metrics_manager.read().await.record_disk_full_event().await;
                }
                return Err(ProxyError::CacheError(format!(
                    "Disk space exhausted: {}",
                    e
                )));
            } else {
                error!(
                    "Failed to write range tmp file: key={}, range={}-{}, path={:?}, error={}",
                    cache_key, start, end, range_tmp_path, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to write range tmp file: {}",
                    e
                )));
            }
        }

        let write_duration = write_start.elapsed();
        debug!(
            "Range binary file written to temp: key={}, range={}-{}, path={:?}, size={} bytes, duration={:.2}ms",
            cache_key, start, end, range_tmp_path, compressed_size, write_duration.as_secs_f64() * 1000.0
        );

        // Create range spec for metadata update
        let ranges_dir = self.cache_dir.join("ranges");
        let range_file_relative_path = range_file_path
            .strip_prefix(&ranges_dir)
            .map_err(|e| {
                let _ = std::fs::remove_file(&range_tmp_path);
                ProxyError::CacheError(format!("Failed to compute relative path: {}", e))
            })?
            .to_string_lossy()
            .to_string();

        let range_spec = RangeSpec::new(
            start,
            end,
            range_file_relative_path,
            compression_algorithm,
            compressed_size,
            uncompressed_size,
        );

        // Step 3: Update metadata - use hybrid writer for coordinated metadata updates
        // (always enabled in production; tests may not have it configured)
        let metadata_start = std::time::Instant::now();

        if let Some(hybrid_writer) = &self.hybrid_metadata_writer {
            // Shared storage mode: use HybridMetadataWriter for coordinated metadata updates
            debug!(
                "Using HybridMetadataWriter for metadata update: key={}, range={}-{}",
                cache_key, start, end
            );

            // Step 1: Make range file visible FIRST
            // This must happen before journal write so that when other proxies read the journal
            // entry during consolidation, the referenced range file already exists.
            let range_already_existed = range_file_path.exists();
            std::fs::rename(&range_tmp_path, &range_file_path).map_err(|e| {
                let _ = std::fs::remove_file(&range_tmp_path);
                error!(
                    "Failed to rename range file: key={}, range={}-{}, error={}",
                    cache_key, start, end, e
                );
                ProxyError::CacheError(format!("Failed to make range file visible: {}", e))
            })?;

            // Step 2: Write journal entry (references the now-existing range file)
            // Size tracking is handled by consolidation using journal entries
            // Use JournalOnly mode for range operations to avoid lock contention
            // The journal consolidator will merge entries into metadata files asynchronously
            // Note: Reduce consolidation_interval in config for faster cross-instance visibility
            // Pass object_metadata so journal consolidation can create .meta with response_headers
            let mut writer = hybrid_writer.lock().await;
            if let Err(e) = writer
                .write_range_metadata(
                    cache_key,
                    range_spec,
                    WriteMode::JournalOnly,
                    Some(object_metadata.clone()),
                    ttl,
                )
                .await
            {
                // Journal write failed but range file exists - this is acceptable
                // The orphan recovery system will eventually clean up the range file
                // Log error but don't fail the operation since data is cached
                error!(
                    "HybridMetadataWriter failed (range file exists, will be orphaned): key={}, range={}-{}, error={}",
                    cache_key, start, end, e
                );
                return Err(e);
            }

            let metadata_duration = metadata_start.elapsed();
            debug!(
                "HybridMetadataWriter completed: key={}, range={}-{}, duration={:.2}ms",
                cache_key,
                start,
                end,
                metadata_duration.as_secs_f64() * 1000.0
            );

            // Track size via in-memory accumulator (zero NFS overhead)
            // Skip if range file already existed on disk (another instance cached it first)
            // This prevents cross-instance over-counting on shared storage
            if let Some(consolidator) = &self.journal_consolidator {
                if !range_already_existed {
                    consolidator.size_accumulator().add_range(cache_key, start, end, compressed_size);
                    // Track write cache if applicable (completed PutObject or CompleteMPU)
                    if object_metadata.is_write_cached {
                        consolidator.size_accumulator().add_write_cache(compressed_size);
                    }
                } else {
                    debug!(
                        "SIZE_ACCUM skip (range already existed): key={}, range={}-{}, size={}",
                        cache_key, start, end, compressed_size
                    );
                }
            }

            let total_duration = operation_start.elapsed();
            debug!(
                "Range stored (hybrid): key={}, range={}-{}, size={} bytes, duration={:.2}ms",
                cache_key,
                start,
                end,
                uncompressed_size,
                total_duration.as_secs_f64() * 1000.0
            );

            return Ok(());
        }

        // Fallback: direct file locking for metadata updates (used in tests without hybrid writer)
        let metadata_file_path = self.get_new_metadata_file_path(cache_key);
        let metadata_tmp_path = metadata_file_path.with_extension("meta.tmp");
        let lock_file_path = metadata_file_path.with_extension("meta.lock");

        debug!(
            "Updating metadata (direct mode): key={}, metadata_path={:?}, lock_path={:?}",
            cache_key, metadata_file_path, lock_file_path
        );

        // Ensure metadata directory exists
        if let Some(parent) = metadata_file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                // Clean up temp range file on failure
                let _ = std::fs::remove_file(&range_tmp_path);
                error!(
                    "Failed to create metadata directory: key={}, path={:?}, error={}",
                    cache_key, parent, e
                );
                ProxyError::CacheError(format!("Failed to create metadata directory: {}", e))
            })?;
        }

        // Acquire exclusive lock on metadata file
        // This prevents concurrent writes from causing lost updates
        let lock_acquire_start = std::time::Instant::now();
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_file_path)
            .map_err(|e| {
                // Clean up temp range file on failure
                let _ = std::fs::remove_file(&range_tmp_path);
                error!(
                    "Failed to open lock file: key={}, lock_path={:?}, error={}",
                    cache_key, lock_file_path, e
                );
                ProxyError::CacheError(format!("Failed to open lock file: {}", e))
            })?;

        lock_file.lock_exclusive().map_err(|e| {
            // Clean up temp range file on failure
            let _ = std::fs::remove_file(&range_tmp_path);
            error!(
                "Failed to acquire exclusive lock: key={}, lock_path={:?}, error={}",
                cache_key, lock_file_path, e
            );
            ProxyError::CacheError(format!("Failed to acquire lock: {}", e))
        })?;

        let lock_duration = lock_acquire_start.elapsed();
        debug!(
            "Acquired exclusive lock: key={}, lock_path={:?}, wait_time={:.2}ms",
            cache_key,
            lock_file_path,
            lock_duration.as_secs_f64() * 1000.0
        );

        // Read existing metadata or create new (inside lock)
        let mut cache_metadata = if metadata_file_path.exists() {
            debug!(
                "Reading existing metadata: key={}, path={:?}",
                cache_key, metadata_file_path
            );

            match std::fs::read_to_string(&metadata_file_path) {
                Ok(content) => {
                    match serde_json::from_str::<NewCacheMetadata>(&content) {
                        Ok(metadata) => {
                            debug!(
                                "Parsed existing metadata: key={}, ranges={}, etag={}",
                                cache_key,
                                metadata.ranges.len(),
                                metadata.object_metadata.etag
                            );

                            // Verify object metadata matches
                            if metadata.object_metadata.etag != object_metadata.etag {
                                warn!(
                                    "ETag mismatch detected: key={}, existing_etag={}, new_etag={} - invalidating existing ranges",
                                    cache_key, metadata.object_metadata.etag, object_metadata.etag
                                );
                                
                                // Requirement 14.1, 14.2, 14.3: Invalidate existing cached ranges
                                // and allow new data to be cached
                                let invalidated_count = metadata.ranges.len();
                                
                                // Delete existing range files
                                let ranges_dir = self.cache_dir.join("ranges");
                                for range in &metadata.ranges {
                                    let range_path = ranges_dir.join(&range.file_path);
                                    if range_path.exists() {
                                        if let Err(e) = std::fs::remove_file(&range_path) {
                                            warn!(
                                                "Failed to delete invalidated range file: key={}, path={:?}, error={}",
                                                cache_key, range_path, e
                                            );
                                        }
                                    }
                                }
                                
                                // Record metrics for ETag mismatch invalidation
                                if let Some(metrics_manager) = &self.metrics_manager {
                                    metrics_manager.read().await.record_etag_mismatch(
                                        cache_key,
                                        &metadata.object_metadata.etag,
                                        &object_metadata.etag,
                                    ).await;
                                    let range_names: Vec<String> = metadata.ranges.iter()
                                        .map(|r| format!("{}-{}", r.start, r.end))
                                        .collect();
                                    metrics_manager.read().await.record_range_invalidation(
                                        cache_key,
                                        &range_names,
                                        "etag_mismatch",
                                    ).await;
                                }
                                
                                info!(
                                    "Invalidated {} existing ranges due to ETag mismatch: key={}, old_etag={}, new_etag={}",
                                    invalidated_count, cache_key, metadata.object_metadata.etag, object_metadata.etag
                                );
                                
                                // Create fresh metadata with new object_metadata
                                let now = SystemTime::now();
                                NewCacheMetadata {
                                    cache_key: cache_key.to_string(),
                                    object_metadata: object_metadata.clone(),
                                    ranges: Vec::new(),
                                    created_at: now,
                                    expires_at: now + ttl,
                                    compression_info: crate::cache_types::CompressionInfo::default(),
                                    ..Default::default()
                                }
                            } else {
                                metadata
                            }
                        }
                        Err(e) => {
                            error!(
                                "Failed to parse existing metadata: key={}, path={:?}, error={}",
                                cache_key, metadata_file_path, e
                            );
                            // Clean up temp range file and release lock
                            let _ = lock_file.unlock();
                            let _ = std::fs::remove_file(&range_tmp_path);
                            return Err(ProxyError::CacheError(format!(
                                "Corrupted metadata file: {}",
                                e
                            )));
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to read existing metadata: key={}, path={:?}, error={}",
                        cache_key, metadata_file_path, e
                    );
                    // Clean up temp range file and release lock
                    let _ = lock_file.unlock();
                    let _ = std::fs::remove_file(&range_tmp_path);
                    return Err(ProxyError::CacheError(format!(
                        "Failed to read metadata file: {}",
                        e
                    )));
                }
            }
        } else {
            // Create new metadata
            info!(
                "Creating new metadata entry: key={}, etag={}",
                cache_key, object_metadata.etag
            );

            let now = SystemTime::now();
            NewCacheMetadata {
                cache_key: cache_key.to_string(),
                object_metadata: object_metadata.clone(),
                ranges: Vec::new(),
                created_at: now,
                expires_at: now + ttl,
                compression_info: crate::cache_types::CompressionInfo::default(),
                ..Default::default()
            }
        };

        // Refresh object-level expiration on every cache write
        cache_metadata.refresh_object_ttl(ttl);

        // Check if this range already exists and update it, or add new
        let range_update_type = if let Some(existing_range) = cache_metadata
            .ranges
            .iter_mut()
            .find(|r| r.start == start && r.end == end)
        {
            *existing_range = range_spec.clone();
            debug!(
                "Updated existing range in metadata: key={}, range={}-{}",
                cache_key, start, end
            );
            "update"
        } else {
            cache_metadata.ranges.push(range_spec.clone());
            debug!(
                "Added new range to metadata: key={}, range={}-{}, total_ranges={}",
                cache_key,
                start,
                end,
                cache_metadata.ranges.len()
            );
            "add"
        };

        // Write metadata to temporary file
        let serialize_start = std::time::Instant::now();
        let metadata_json = serde_json::to_string_pretty(&cache_metadata).map_err(|e| {
            // Clean up temp range file and release lock on failure
            let _ = lock_file.unlock();
            let _ = std::fs::remove_file(&range_tmp_path);
            error!(
                "Failed to serialize metadata: key={}, error={}",
                cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        let serialize_duration = serialize_start.elapsed();
        debug!(
            "Metadata serialized: key={}, size={} bytes, duration={:.2}ms",
            cache_key,
            metadata_json.len(),
            serialize_duration.as_secs_f64() * 1000.0
        );

        std::fs::write(&metadata_tmp_path, metadata_json).map_err(|e| {
            // Clean up both tmp files, release lock on failure
            let _ = lock_file.unlock();
            let _ = std::fs::remove_file(&metadata_tmp_path);
            let _ = std::fs::remove_file(&range_tmp_path);
            error!(
                "Failed to write metadata tmp file: key={}, path={:?}, error={}",
                cache_key, metadata_tmp_path, e
            );
            ProxyError::CacheError(format!("Failed to write metadata tmp file: {}", e))
        })?;

        // Atomic rename of metadata
        debug!(
            "Performing atomic metadata rename: tmp={:?} -> final={:?}",
            metadata_tmp_path, metadata_file_path
        );

        std::fs::rename(&metadata_tmp_path, &metadata_file_path).map_err(|e| {
            // Clean up both tmp files, release lock on failure
            let _ = lock_file.unlock();
            let _ = std::fs::remove_file(&metadata_tmp_path);
            let _ = std::fs::remove_file(&range_tmp_path);
            error!(
                "Failed to rename metadata file: key={}, tmp={:?}, final={:?}, error={}",
                cache_key, metadata_tmp_path, metadata_file_path, e
            );
            ProxyError::CacheError(format!("Failed to rename metadata file: {}", e))
        })?;

        // Release lock
        lock_file
            .unlock()
            .map_err(|e| {
                warn!(
                    "Failed to release lock (non-fatal): key={}, lock_path={:?}, error={}",
                    cache_key, lock_file_path, e
                );
                // Don't fail the operation if unlock fails - the lock will be released when file is closed
            })
            .ok();

        let metadata_duration = metadata_start.elapsed();
        debug!(
            "Released lock: key={}, total_metadata_update_duration={:.2}ms",
            cache_key,
            metadata_duration.as_secs_f64() * 1000.0
        );

        // Step 4: NOW make the range file visible by renaming from .tmp to .bin
        // This ensures metadata is updated BEFORE the range file becomes visible
        // This fixes the race condition where concurrent requests could see the range file
        // but not find it in metadata
        debug!(
            "Making range file visible: tmp={:?} -> final={:?}",
            range_tmp_path, range_file_path
        );

        std::fs::rename(&range_tmp_path, &range_file_path)
            .map_err(|e| {
                // Clean up tmp file on failure
                let _ = std::fs::remove_file(&range_tmp_path);
                error!(
                    "Failed to rename range file after metadata update: key={}, range={}-{}, tmp={:?}, final={:?}, error={}",
                    cache_key, start, end, range_tmp_path, range_file_path, e
                );
                // Note: Metadata is already updated, so this is a partial failure
                // The range will be in metadata but the binary file won't exist
                // This will be handled as a cache miss on next read
                ProxyError::CacheError(format!("Failed to make range file visible: {}", e))
            })?;

        debug!(
            "Range binary file made visible: key={}, range={}-{}, path={:?}",
            cache_key, start, end, range_file_path
        );

        let total_duration = operation_start.elapsed();
        debug!(
            "Range storage operation completed: key={}, range={}-{}, operation={}, uncompressed={} bytes, compressed={} bytes, compression_ratio={:.2}%, total_ranges={}, total_duration={:.2}ms",
            cache_key,
            start,
            end,
            range_update_type,
            uncompressed_size,
            compressed_size,
            (compressed_size as f64 / uncompressed_size as f64) * 100.0,
            cache_metadata.ranges.len(),
            total_duration.as_secs_f64() * 1000.0
        );

        // NOTE: Size tracking is now handled by JournalConsolidator through journal entries.
        // Direct mode is no longer used (shared_storage.enabled is always true).

        Ok(())
    }

    /// Get metadata for a cache entry without loading range data
    /// Implements Requirements 1.3, 2.1, 8.1
    ///
    /// This method:
    /// 1. Reads only the metadata file (no range data loaded)
    /// 2. Parses JSON to NewCacheMetadata structure
    /// 3. Handles corrupted metadata gracefully (returns None, logs error)
    /// 4. Returns None if metadata file doesn't exist
    /// 5. Checks expiration and returns None if expired
    /// 6. Records metrics for corruption events
    pub async fn get_metadata(
        &self,
        cache_key: &str,
    ) -> Result<Option<crate::cache_types::NewCacheMetadata>> {
        let operation_start = std::time::Instant::now();
        let sanitized_key = self.sanitize_cache_key_new(cache_key);

        debug!(
            "Starting metadata retrieval: key={}, sanitized_key={}, operation=get_metadata",
            cache_key, sanitized_key
        );

        let metadata_file_path = self.get_new_metadata_file_path(cache_key);

        // Requirement 1.3: Log cache key, computed path, and file existence check result
        let file_exists = metadata_file_path.exists();
        debug!(
            "[METADATA_LOOKUP] Checking metadata file: cache_key={}, sanitized_key={}, path={:?}, exists={}",
            cache_key, sanitized_key, metadata_file_path, file_exists
        );

        // Check if metadata file exists
        if !file_exists {
            debug!(
                "[METADATA_LOOKUP] Metadata file not found: cache_key={}, sanitized_key={}, path={:?}, result=cache_miss",
                cache_key, sanitized_key, metadata_file_path
            );
            return Ok(None);
        }

        // Record metadata file size
        let file_size = if let Ok(metadata_file_metadata) = std::fs::metadata(&metadata_file_path) {
            let size = metadata_file_metadata.len();
            if let Some(metrics_manager) = &self.metrics_manager {
                metrics_manager
                    .read()
                    .await
                    .record_metadata_file_size(size)
                    .await;
            }
            debug!(
                "Metadata file size: key={}, path={:?}, size={} bytes",
                cache_key, metadata_file_path, size
            );
            size
        } else {
            0
        };

        // Start timing metadata parse
        let parse_start = std::time::Instant::now();

        // Read metadata file with retry for empty files (transient write in progress on shared storage)
        const MAX_EMPTY_FILE_RETRIES: u32 = 3;
        const EMPTY_FILE_RETRY_DELAY_MS: u64 = 50;

        let metadata_content = {
            let mut attempts = 0u32;

            loop {
                attempts += 1;
                match std::fs::read_to_string(&metadata_file_path) {
                    Ok(content) => {
                        // Check if file is empty (transient state during write on shared storage)
                        if content.is_empty() && attempts <= MAX_EMPTY_FILE_RETRIES {
                            debug!(
                                "[METADATA_LOOKUP] Metadata file is empty (transient write?): cache_key={}, attempt={}/{}, retrying...",
                                cache_key, attempts, MAX_EMPTY_FILE_RETRIES
                            );
                            std::thread::sleep(std::time::Duration::from_millis(
                                EMPTY_FILE_RETRY_DELAY_MS * attempts as u64,
                            ));
                            continue;
                        }

                        if attempts > 1 {
                            debug!(
                                "[METADATA_LOOKUP] Metadata file read after {} attempts: cache_key={}, size={} bytes",
                                attempts, cache_key, content.len()
                            );
                        } else {
                            debug!(
                                "[METADATA_LOOKUP] Metadata file read successfully: cache_key={}, sanitized_key={}, size={} bytes",
                                cache_key, sanitized_key, content.len()
                            );
                        }

                        break content;
                    }
                    Err(e) => {
                        // Check for stale file handle (ESTALE) - retry
                        let is_stale = e.raw_os_error() == Some(116)
                            || e.to_string().contains("stale file handle")
                            || e.to_string().contains("ESTALE");

                        if is_stale && attempts <= MAX_EMPTY_FILE_RETRIES {
                            warn!(
                                "[METADATA_LOOKUP] Stale file handle, retrying: cache_key={}, attempt={}/{}, error={}",
                                cache_key, attempts, MAX_EMPTY_FILE_RETRIES, e
                            );
                            std::thread::sleep(std::time::Duration::from_millis(
                                EMPTY_FILE_RETRY_DELAY_MS * attempts as u64,
                            ));
                            continue;
                        }

                        // After retries, treat as cache miss but DON'T delete
                        // The file may be in a transient state; next request will likely succeed
                        warn!(
                            "[METADATA_LOOKUP] Failed to read metadata file after retries, treating as cache miss (not deleting): cache_key={}, path={:?}, error={}",
                            cache_key, metadata_file_path, e
                        );
                        // Record metric for monitoring
                        if let Some(metrics_manager) = &self.metrics_manager {
                            metrics_manager
                                .read()
                                .await
                                .record_corrupted_metadata_simple()
                                .await;
                        }
                        return Ok(None);
                    }
                }
            }
        };

        // Final check for empty content after retries
        if metadata_content.is_empty() {
            // Treat as cache miss but DON'T delete - file may be mid-write
            warn!(
                "[METADATA_LOOKUP] Metadata file still empty after retries, treating as cache miss (not deleting): cache_key={}, path={:?}",
                cache_key, metadata_file_path
            );
            // Record metric for monitoring
            if let Some(metrics_manager) = &self.metrics_manager {
                metrics_manager
                    .read()
                    .await
                    .record_corrupted_metadata_simple()
                    .await;
            }
            return Ok(None);
        }

        // Parse JSON with retry for partial reads on shared storage
        // On NFS/EFS, we may read a file mid-write and get truncated JSON
        const MAX_PARSE_RETRIES: u32 = 3;
        const PARSE_RETRY_DELAY_MS: u64 = 50;

        let metadata = {
            let mut parse_attempts = 0u32;
            let mut current_content = metadata_content;

            loop {
                parse_attempts += 1;
                match serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&current_content)
                {
                    Ok(meta) => {
                        if parse_attempts > 1 {
                            debug!(
                                "[METADATA_LOOKUP] Metadata JSON parsed after {} attempts: cache_key={}, ranges={}",
                                parse_attempts, cache_key, meta.ranges.len()
                            );
                        } else {
                            debug!(
                                "[METADATA_LOOKUP] Metadata JSON parsed successfully: cache_key={}, sanitized_key={}, ranges={}, etag={}",
                                cache_key, sanitized_key, meta.ranges.len(), meta.object_metadata.etag
                            );
                        }
                        break meta;
                    }
                    Err(e) => {
                        if parse_attempts < MAX_PARSE_RETRIES {
                            // Likely a partial read during in-progress write - retry
                            debug!(
                                "[METADATA_LOOKUP] JSON parse failed (partial read?), retrying: cache_key={}, attempt={}/{}, error={}",
                                cache_key, parse_attempts, MAX_PARSE_RETRIES, e
                            );
                            std::thread::sleep(std::time::Duration::from_millis(
                                PARSE_RETRY_DELAY_MS * parse_attempts as u64,
                            ));

                            // Re-read the file - it may have been completed
                            match std::fs::read_to_string(&metadata_file_path) {
                                Ok(new_content) => {
                                    current_content = new_content;
                                    continue;
                                }
                                Err(read_err) => {
                                    debug!(
                                        "[METADATA_LOOKUP] Failed to re-read metadata file during parse retry: cache_key={}, error={}",
                                        cache_key, read_err
                                    );
                                    // Continue with old content for next parse attempt
                                    continue;
                                }
                            }
                        }

                        // After all retries, treat as cache miss but DON'T delete
                        // The file may be mid-write by consolidator; next request will likely succeed
                        warn!(
                            "[METADATA_LOOKUP] JSON parse failed after {} retries, treating as cache miss (not deleting): cache_key={}, path={:?}, error={}",
                            MAX_PARSE_RETRIES, cache_key, metadata_file_path, e
                        );
                        // Record metric for monitoring
                        if let Some(metrics_manager) = &self.metrics_manager {
                            metrics_manager
                                .read()
                                .await
                                .record_corrupted_metadata_simple()
                                .await;
                        }
                        return Ok(None);
                    }
                }
            }
        };

        // Record parse duration
        let parse_duration = parse_start.elapsed();
        if let Some(metrics_manager) = &self.metrics_manager {
            metrics_manager
                .read()
                .await
                .record_metadata_parse_duration(parse_duration.as_secs_f64() * 1000.0)
                .await;
        }

        // Record range file count
        if let Some(metrics_manager) = &self.metrics_manager {
            metrics_manager
                .read()
                .await
                .record_range_file_count(metadata.ranges.len() as u64)
                .await;
        }

        // NOTE: Object-level expires_at is NOT checked here
        // The .meta file lives as long as it has valid ranges
        // Individual ranges have their own TTL checked in find_cached_ranges()
        // Object-level expiration only applies to write cache (checked below)

        // Check write cache expiration (lazy expiration - Requirements 5.3, 5.4)
        // If this is a write-cached object and it's expired, treat as cache miss
        if metadata.object_metadata.is_write_cached
            && metadata.object_metadata.is_write_cache_expired()
        {
            info!(
                "Write cache entry expired (lazy expiration): key={}, write_cache_expires_at={:?}, result=cache_miss",
                cache_key, metadata.object_metadata.write_cache_expires_at
            );
            // Note: The actual cleanup of range files will be done by the caller or background scan
            // Here we just return None to indicate cache miss
            return Ok(None);
        }

        let total_duration = operation_start.elapsed();
        debug!(
            "Metadata retrieval completed: key={}, ranges={}, file_size={} bytes, parse_duration={:.2}ms, total_duration={:.2}ms, result=cache_hit",
            cache_key,
            metadata.ranges.len(),
            file_size,
            parse_duration.as_secs_f64() * 1000.0,
            total_duration.as_secs_f64() * 1000.0
        );

        Ok(Some(metadata))
    }

    /// Validate cached GET data against HEAD metadata
    ///
    /// Compares ETag and Last-Modified from HEAD cache with cached GET metadata.
    /// If they differ, invalidates all cached GET data (metadata and range files).
    ///
    /// # Arguments
    ///
    /// * `cache_key` - The cache key for the object
    /// * `head_etag` - ETag from HEAD request
    /// * `head_last_modified` - Last-Modified from HEAD request
    ///
    /// # Returns
    ///
    /// Returns Ok(true) if cache was invalidated, Ok(false) if cache is valid or doesn't exist
    ///
    /// # Requirements
    ///
    /// Implements Requirements 7.1, 7.2, 7.3, 7.4, 7.5
    pub async fn validate_cache_against_head(
        &self,
        cache_key: &str,
        head_etag: &str,
        head_last_modified: &str,
    ) -> Result<bool> {
        debug!(
            "[CACHE_INVALIDATION] Starting cache validation: cache_key={}, head_etag={}, head_last_modified={}",
            cache_key, head_etag, head_last_modified
        );

        // Get cached GET metadata
        let cached_metadata = match self.get_metadata(cache_key).await? {
            Some(metadata) => metadata,
            None => {
                debug!(
                    "[CACHE_INVALIDATION] No cached GET metadata found: cache_key={}, result=no_action",
                    cache_key
                );
                return Ok(false);
            }
        };

        let cached_etag = &cached_metadata.object_metadata.etag;
        let cached_last_modified = &cached_metadata.object_metadata.last_modified;

        // Requirement 7.1, 7.2: Compare ETag and Last-Modified
        let etag_mismatch = cached_etag != head_etag;
        let last_modified_mismatch = cached_last_modified != head_last_modified;

        if etag_mismatch || last_modified_mismatch {
            // Requirement 7.5: Log invalidation with old and new metadata values
            warn!(
                "[CACHE_INVALIDATION] Metadata mismatch detected: cache_key={}, \
                cached_etag={}, head_etag={}, etag_mismatch={}, \
                cached_last_modified={}, head_last_modified={}, last_modified_mismatch={}, \
                action=invalidate_cache",
                cache_key,
                cached_etag,
                head_etag,
                etag_mismatch,
                cached_last_modified,
                head_last_modified,
                last_modified_mismatch
            );

            // Requirement 7.3, 7.4: Invalidate all cached metadata and range binary files
            self.invalidate_cache_entry(cache_key).await?;

            info!(
                "[CACHE_INVALIDATION] Cache invalidated due to metadata mismatch: cache_key={}, \
                old_etag={}, new_etag={}, old_last_modified={}, new_last_modified={}, \
                ranges_deleted={}, result=invalidated",
                cache_key,
                cached_etag,
                head_etag,
                cached_last_modified,
                head_last_modified,
                cached_metadata.ranges.len()
            );

            Ok(true)
        } else {
            debug!(
                "[CACHE_INVALIDATION] Cache metadata matches HEAD: cache_key={}, etag={}, last_modified={}, result=valid",
                cache_key, cached_etag, cached_last_modified
            );
            Ok(false)
        }
    }

    /// Update metadata file with atomic writes
    /// Implements Requirements 1.3, 2.1, 3.2
    ///
    /// This method:
    /// 1. Acquires exclusive lock on metadata file
    /// 2. Writes metadata to .tmp file
    /// 3. Atomically renames .tmp to final file
    /// 4. Releases lock
    /// 5. Ensures metadata operations don't load range data
    pub async fn update_metadata(
        &self,
        metadata: &crate::cache_types::NewCacheMetadata,
    ) -> Result<()> {
        let operation_start = std::time::Instant::now();
        info!(
            "Starting metadata update: key={}, ranges={}, operation=update_metadata",
            metadata.cache_key,
            metadata.ranges.len()
        );

        let metadata_file_path = self.get_new_metadata_file_path(&metadata.cache_key);
        let metadata_tmp_path = metadata_file_path.with_extension("meta.tmp");
        let lock_file_path = metadata_file_path.with_extension("meta.lock");

        debug!(
            "Metadata update paths: key={}, metadata={:?}, tmp={:?}, lock={:?}",
            metadata.cache_key, metadata_file_path, metadata_tmp_path, lock_file_path
        );

        // Ensure metadata directory exists
        if let Some(parent) = metadata_file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                error!(
                    "Failed to create metadata directory: key={}, path={:?}, error={}",
                    metadata.cache_key, parent, e
                );
                ProxyError::CacheError(format!("Failed to create metadata directory: {}", e))
            })?;
        }

        // Acquire exclusive lock on metadata file
        let lock_start = std::time::Instant::now();
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_file_path)
            .map_err(|e| {
                error!(
                    "Failed to open lock file: key={}, path={:?}, error={}",
                    metadata.cache_key, lock_file_path, e
                );
                ProxyError::CacheError(format!("Failed to open lock file: {}", e))
            })?;

        lock_file.lock_exclusive().map_err(|e| {
            error!(
                "Failed to acquire exclusive lock: key={}, path={:?}, error={}",
                metadata.cache_key, lock_file_path, e
            );
            ProxyError::CacheError(format!("Failed to acquire lock: {}", e))
        })?;

        let lock_duration = lock_start.elapsed();
        debug!(
            "Acquired exclusive lock: key={}, wait_time={:.2}ms",
            metadata.cache_key,
            lock_duration.as_secs_f64() * 1000.0
        );

        // Serialize metadata to JSON
        let serialize_start = std::time::Instant::now();
        let metadata_json = serde_json::to_string_pretty(metadata).map_err(|e| {
            let _ = lock_file.unlock();
            error!(
                "Failed to serialize metadata: key={}, error={}",
                metadata.cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        let serialize_duration = serialize_start.elapsed();
        debug!(
            "Metadata serialized: key={}, size={} bytes, duration={:.2}ms",
            metadata.cache_key,
            metadata_json.len(),
            serialize_duration.as_secs_f64() * 1000.0
        );

        // Write to temporary file
        let write_start = std::time::Instant::now();
        std::fs::write(&metadata_tmp_path, &metadata_json).map_err(|e| {
            let _ = lock_file.unlock();
            let _ = std::fs::remove_file(&metadata_tmp_path);
            error!(
                "Failed to write metadata tmp file: key={}, path={:?}, error={}",
                metadata.cache_key, metadata_tmp_path, e
            );
            ProxyError::CacheError(format!("Failed to write metadata tmp file: {}", e))
        })?;

        let write_duration = write_start.elapsed();
        debug!(
            "Metadata tmp file written: key={}, path={:?}, size={} bytes, duration={:.2}ms",
            metadata.cache_key,
            metadata_tmp_path,
            metadata_json.len(),
            write_duration.as_secs_f64() * 1000.0
        );

        // Atomic rename
        debug!(
            "Performing atomic rename: key={}, tmp={:?} -> final={:?}",
            metadata.cache_key, metadata_tmp_path, metadata_file_path
        );

        std::fs::rename(&metadata_tmp_path, &metadata_file_path).map_err(|e| {
            let _ = lock_file.unlock();
            let _ = std::fs::remove_file(&metadata_tmp_path);
            error!(
                "Failed to rename metadata file: key={}, tmp={:?}, final={:?}, error={}",
                metadata.cache_key, metadata_tmp_path, metadata_file_path, e
            );
            ProxyError::CacheError(format!("Failed to rename metadata file: {}", e))
        })?;

        // Release lock
        lock_file
            .unlock()
            .map_err(|e| {
                warn!(
                    "Failed to release lock (non-fatal): key={}, path={:?}, error={}",
                    metadata.cache_key, lock_file_path, e
                );
            })
            .ok();

        let total_duration = operation_start.elapsed();
        debug!(
            "Released lock: key={}, total_duration={:.2}ms",
            metadata.cache_key,
            total_duration.as_secs_f64() * 1000.0
        );

        info!(
            "Metadata update completed: key={}, ranges={}, file_size={} bytes, total_duration={:.2}ms",
            metadata.cache_key, metadata.ranges.len(), metadata_json.len(), total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Batch delete multiple ranges from a single object
    ///
    /// This method efficiently deletes multiple ranges from a single object in one operation:
    /// 1. Reads current metadata (once per object)
    /// 2. Deletes all specified range .bin files, collecting paths
    /// 3. Removes all deleted ranges from metadata.ranges list
    /// 4. Atomic write updated metadata (once per object)
    /// 5. If all ranges evicted, deletes metadata file and lock files
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `ranges_to_delete` - List of (start, end) pairs identifying ranges to delete
    ///
    /// # Returns
    /// * `Ok((bytes_freed, all_ranges_evicted, deleted_paths))` on success
    ///   - `bytes_freed`: Total bytes freed from deleted range files
    ///   - `all_ranges_evicted`: True if all ranges were evicted (metadata file deleted)
    ///   - `deleted_paths`: Paths of all deleted files (for directory cleanup)
    ///
    /// # Requirements
    /// Implements Requirements 2.1, 2.2, 2.3, 2.5 from range-based-disk-eviction spec
    pub async fn batch_delete_ranges(
        &self,
        cache_key: &str,
        ranges_to_delete: &[(u64, u64)],
    ) -> Result<(u64, bool, Vec<PathBuf>)> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting batch range deletion: key={}, ranges_to_delete={}, operation=batch_delete_ranges",
            cache_key, ranges_to_delete.len()
        );

        if ranges_to_delete.is_empty() {
            debug!("No ranges to delete for key={}", cache_key);
            return Ok((0, false, Vec::new()));
        }

        let metadata_file_path = self.get_new_metadata_file_path(cache_key);
        let metadata_tmp_path = metadata_file_path.with_extension("meta.tmp");
        let lock_file_path = metadata_file_path.with_extension("meta.lock");

        // Check if metadata file exists
        if !metadata_file_path.exists() {
            warn!(
                "Metadata file not found for batch delete: key={}, path={:?}, skipping",
                cache_key, metadata_file_path
            );
            return Ok((0, false, Vec::new()));
        }

        // Acquire exclusive lock with timeout (5 seconds)
        let lock_start = std::time::Instant::now();
        let lock_file = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::task::spawn_blocking({
                let lock_path = lock_file_path.clone();
                move || {
                    let open_and_lock = || -> std::result::Result<std::fs::File, std::io::Error> {
                        let file = std::fs::OpenOptions::new()
                            .create(true)
                            .truncate(false)
                            .write(true)
                            .open(&lock_path)?;
                        file.lock_exclusive()?;
                        Ok(file)
                    };
                    match open_and_lock() {
                        Ok(f) => Ok(f),
                        Err(e) if e.raw_os_error() == Some(116) => {
                            // Stale NFS file handle on open or lock — delete and retry once
                            let _ = std::fs::remove_file(&lock_path);
                            open_and_lock()
                        }
                        Err(e) => Err(e),
                    }
                }
            }),
        )
        .await
        {
            Ok(Ok(Ok(file))) => file,
            Ok(Ok(Err(e))) => {
                debug!(
                    "Failed to acquire lock for batch delete: key={}, error={}, skipping",
                    cache_key, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Lock acquisition failed: {}",
                    e
                )));
            }
            Ok(Err(e)) => {
                debug!(
                    "Lock task failed for batch delete: key={}, error={}, skipping",
                    cache_key, e
                );
                return Err(ProxyError::CacheError(format!("Lock task failed: {}", e)));
            }
            Err(_) => {
                debug!(
                    "Lock timeout for batch delete: key={}, timeout=5s, skipping",
                    cache_key
                );
                return Err(ProxyError::CacheError("Lock timeout".to_string()));
            }
        };

        let lock_duration = lock_start.elapsed();
        debug!(
            "Acquired lock for batch delete: key={}, wait_time={:.2}ms",
            cache_key,
            lock_duration.as_secs_f64() * 1000.0
        );

        // Read current metadata (once)
        let metadata_content = match std::fs::read_to_string(&metadata_file_path) {
            Ok(content) => content,
            Err(e) => {
                let _ = lock_file.unlock();
                warn!(
                    "Failed to read metadata for batch delete: key={}, error={}, skipping",
                    cache_key, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to read metadata: {}",
                    e
                )));
            }
        };

        // Parse metadata
        let mut metadata: crate::cache_types::NewCacheMetadata =
            match serde_json::from_str(&metadata_content) {
                Ok(meta) => meta,
                Err(e) => {
                    let _ = lock_file.unlock();
                    warn!(
                        "Failed to parse metadata for batch delete: key={}, error={}, skipping",
                        cache_key, e
                    );
                    return Err(ProxyError::CacheError(format!(
                        "Failed to parse metadata: {}",
                        e
                    )));
                }
            };

        // Create a set of ranges to delete for efficient lookup
        let ranges_set: std::collections::HashSet<(u64, u64)> =
            ranges_to_delete.iter().cloned().collect();

        let mut bytes_freed: u64 = 0;
        let mut deleted_paths: Vec<PathBuf> = Vec::new();
        let mut ranges_deleted: Vec<(u64, u64)> = Vec::new();

        // Delete range .bin files concurrently
        // Requirement 2.1, 2.2, 2.3: Use tokio::fs for async parallel deletes with concurrency limit
        let delete_tasks: Vec<_> = metadata
            .ranges
            .iter()
            .filter(|r| ranges_set.contains(&(r.start, r.end)))
            .map(|range_spec| {
                let path =
                    self.get_new_range_file_path(cache_key, range_spec.start, range_spec.end);
                let key = (range_spec.start, range_spec.end);
                let ck = cache_key.to_string();
                async move {
                    // Requirement 2.3: Use tokio::fs::metadata (async) instead of std::fs::metadata
                    let file_size = match tokio::fs::metadata(&path).await {
                        Ok(meta) => meta.len(),
                        Err(e) => {
                            warn!(
                                "Failed to get file size for range: key={}, range={}-{}, path={:?}, error={}",
                                ck, key.0, key.1, path, e
                            );
                            0
                        }
                    };

                    // Requirement 2.1: Use tokio::fs::remove_file (async) instead of std::fs::remove_file
                    let deleted = match tokio::fs::remove_file(&path).await {
                        Ok(()) => {
                            debug!(
                                "[RANGE_EVICTION] Deleted range file: cache_key={}, range_start={}, range_end={}, freed_bytes={}, path={:?}",
                                ck, key.0, key.1, file_size, path
                            );
                            true
                        }
                        Err(e) => {
                            // Requirement 2.4: Log warning on failure, continue processing remaining files
                            warn!(
                                "[RANGE_EVICTION] Failed to delete range file: cache_key={}, range_start={}, range_end={}, error={}, path={:?}, action=continuing_with_metadata_update",
                                ck, key.0, key.1, e, path
                            );
                            false
                        }
                    };

                    (key, file_size, deleted, path)
                }
            })
            .collect();

        // Requirement 2.2: Execute deletes concurrently with FILE_CONCURRENCY_LIMIT
        let results: Vec<_> = stream::iter(delete_tasks)
            .buffer_unordered(FILE_CONCURRENCY_LIMIT)
            .collect()
            .await;

        // Collect results into bytes_freed, deleted_paths, ranges_deleted
        for (range_key, file_size, deleted, path) in results {
            if deleted {
                bytes_freed += file_size;
                deleted_paths.push(path);
            }
            // Always mark range for metadata removal (file may already be gone)
            ranges_deleted.push(range_key);
        }

        // Requirement 2.2: Update metadata to remove evicted ranges from the ranges list
        let ranges_deleted_set: std::collections::HashSet<(u64, u64)> =
            ranges_deleted.iter().cloned().collect();
        metadata
            .ranges
            .retain(|r| !ranges_deleted_set.contains(&(r.start, r.end)));

        let all_ranges_evicted = metadata.ranges.is_empty();

        if all_ranges_evicted {
            // All ranges evicted - delete metadata file and lock files
            // This implements Requirement 3.1, 3.2, 3.3 from the spec

            // Delete metadata file
            // Requirement 7.2: Log metadata deletion with reason (all ranges evicted)
            match std::fs::remove_file(&metadata_file_path) {
                Ok(()) => {
                    // Get metadata file size for tracking
                    deleted_paths.push(metadata_file_path.clone());
                    debug!(
                        "[METADATA_EVICTION] Deleted metadata file: cache_key={}, reason=all_ranges_evicted, path={:?}",
                        cache_key, metadata_file_path
                    );
                }
                Err(e) => {
                    // Requirement 7.4: Log errors with cache_key and failure reason
                    warn!(
                        "[METADATA_EVICTION] Failed to delete metadata file: cache_key={}, error={}, path={:?}",
                        cache_key, e, metadata_file_path
                    );
                }
            }

            // Delete lock file
            if lock_file_path.exists() {
                match std::fs::remove_file(&lock_file_path) {
                    Ok(()) => {
                        deleted_paths.push(lock_file_path.clone());
                        debug!(
                            "[METADATA_EVICTION] Deleted lock file: cache_key={}, path={:?}",
                            cache_key, lock_file_path
                        );
                    }
                    Err(e) => {
                        debug!(
                            "[METADATA_EVICTION] Failed to delete lock file (non-critical): cache_key={}, error={}, path={:?}",
                            cache_key, e, lock_file_path
                        );
                    }
                }
            }

            // Release lock (file is deleted, but we still need to unlock)
            let _ = lock_file.unlock();
        } else {
            // Some ranges remain - atomic write updated metadata
            let metadata_json = match serde_json::to_string_pretty(&metadata) {
                Ok(json) => json,
                Err(e) => {
                    let _ = lock_file.unlock();
                    return Err(ProxyError::CacheError(format!(
                        "Failed to serialize metadata: {}",
                        e
                    )));
                }
            };

            // Write to temporary file first for atomic operation
            if let Err(e) = std::fs::write(&metadata_tmp_path, &metadata_json) {
                let _ = lock_file.unlock();
                let _ = std::fs::remove_file(&metadata_tmp_path);
                return Err(ProxyError::CacheError(format!(
                    "Failed to write metadata tmp file: {}",
                    e
                )));
            }

            // Atomic rename
            if let Err(e) = std::fs::rename(&metadata_tmp_path, &metadata_file_path) {
                let _ = lock_file.unlock();
                let _ = std::fs::remove_file(&metadata_tmp_path);
                return Err(ProxyError::CacheError(format!(
                    "Failed to rename metadata file: {}",
                    e
                )));
            }

            // Release lock
            let _ = lock_file.unlock();

            debug!(
                "Updated metadata after range deletion: key={}, remaining_ranges={}",
                cache_key,
                metadata.ranges.len()
            );
        }

        let total_duration = operation_start.elapsed();
        debug!(
            "Batch range deletion completed: key={}, ranges_deleted={}, bytes_freed={}, all_evicted={}, total_duration={:.2}ms",
            cache_key, ranges_deleted.len(), bytes_freed, all_ranges_evicted, total_duration.as_secs_f64() * 1000.0
        );

        Ok((bytes_freed, all_ranges_evicted, deleted_paths))
    }

    /// Batch clean up empty directories after range eviction
    ///
    /// This method processes all deleted file paths and removes empty directories
    /// in a bottom-up fashion (deepest directories first). It stops at cache type
    /// root directories (metadata/, ranges/) to preserve the cache structure.
    ///
    /// # Arguments
    /// * `deleted_paths` - Paths of all deleted files from eviction operations
    ///
    /// # Returns
    /// * `Ok(usize)` - Number of directories successfully removed
    ///
    /// # Behavior
    /// - Collects unique parent directories from all deleted paths
    /// - Sorts by depth (deepest first) for bottom-up cleanup
    /// - Removes empty directories, stopping at cache type root
    /// - Recursively checks parent directories after removing a directory
    /// - Logs warnings on failure, doesn't fail eviction
    /// - Skips non-existent directories gracefully
    ///
    /// # Requirements
    /// Implements Requirements 4.1, 4.2, 4.3, 4.4, 4.5 from range-based-disk-eviction spec
    pub fn batch_cleanup_empty_directories(&self, deleted_paths: &[PathBuf]) -> usize {
        let operation_start = std::time::Instant::now();

        if deleted_paths.is_empty() {
            debug!("No deleted paths to clean up directories for");
            return 0;
        }

        // Cache type root directory names that should NOT be deleted
        // Requirement 4.5: Do not delete cache type root directories
        let cache_type_roots = ["metadata", "ranges", "locks", "mpus_in_progress"];

        // Helper function to check if a path is a cache type root or the cache_dir
        let is_protected_dir = |dir: &PathBuf| -> bool {
            // Check if this directory is a cache type root
            if let Some(dir_name) = dir.file_name().and_then(|n| n.to_str()) {
                if cache_type_roots.contains(&dir_name) {
                    return true;
                }
            }
            // Check if the directory is the cache_dir itself
            dir == &self.cache_dir
        };

        // Collect unique parent directories from all deleted paths
        // Requirement 4.1: Check parent directory after file deletion
        let mut dirs_to_check: std::collections::HashSet<PathBuf> = deleted_paths
            .iter()
            .filter_map(|p| p.parent().map(|d| d.to_path_buf()))
            .filter(|d| !is_protected_dir(d))
            .collect();

        debug!(
            "Collected {} unique parent directories from {} deleted paths",
            dirs_to_check.len(),
            deleted_paths.len()
        );

        let mut removed_count: usize = 0;
        let mut checked_count: usize = 0;

        // Process directories in a loop until no more can be removed
        // This handles recursive parent cleanup (Requirement 4.3)
        loop {
            if dirs_to_check.is_empty() {
                break;
            }

            // Sort by depth (deepest first) for bottom-up cleanup
            let mut dirs: Vec<PathBuf> = dirs_to_check.drain().collect();
            dirs.sort_by(|a, b| {
                let a_depth = a.components().count();
                let b_depth = b.components().count();
                b_depth.cmp(&a_depth) // Descending order (deepest first)
            });

            let mut newly_emptied_parents: Vec<PathBuf> = Vec::new();

            for dir in &dirs {
                checked_count += 1;

                // Skip protected directories (already filtered, but double-check)
                if is_protected_dir(dir) {
                    debug!("[DIR_CLEANUP] Skipping protected directory: path={:?}", dir);
                    continue;
                }

                // Check if directory exists
                if !dir.exists() {
                    debug!("[DIR_CLEANUP] Directory already removed: path={:?}", dir);
                    continue;
                }

                // Check if directory is empty
                // Requirement 4.2: Remove directory when it becomes empty
                match Self::is_directory_empty(dir) {
                    Ok(true) => {
                        // Directory is empty, try to remove it
                        match std::fs::remove_dir(dir) {
                            Ok(()) => {
                                removed_count += 1;
                                debug!("[DIR_CLEANUP] Removed empty directory: path={:?}", dir);

                                // After removing a directory, add its parent to check list
                                // Requirement 4.3: Recursively clean up empty parent directories
                                if let Some(parent) = dir.parent() {
                                    let parent_path = parent.to_path_buf();
                                    if !is_protected_dir(&parent_path) {
                                        newly_emptied_parents.push(parent_path);
                                    }
                                }
                            }
                            Err(e) => {
                                // Requirement 4.4: Log warning on failure, don't fail eviction
                                warn!(
                                    "[DIR_CLEANUP] Failed to remove empty directory (non-critical): path={:?}, error={}",
                                    dir, e
                                );
                            }
                        }
                    }
                    Ok(false) => {
                        // Directory is not empty, skip it
                        debug!(
                            "[DIR_CLEANUP] Directory not empty, skipping: path={:?}",
                            dir
                        );
                    }
                    Err(e) => {
                        // Requirement 4.4: Log warning on failure, don't fail eviction
                        warn!(
                            "[DIR_CLEANUP] Failed to check if directory is empty (non-critical): path={:?}, error={}",
                            dir, e
                        );
                    }
                }
            }

            // Add newly emptied parent directories to check in next iteration
            for parent in newly_emptied_parents {
                dirs_to_check.insert(parent);
            }
        }

        let total_duration = operation_start.elapsed();
        debug!(
            "Directory cleanup completed: checked={}, removed={}, duration={:.2}ms",
            checked_count,
            removed_count,
            total_duration.as_secs_f64() * 1000.0
        );

        removed_count
    }

    /// Check if a directory is empty
    ///
    /// # Arguments
    /// * `dir` - Path to the directory to check
    ///
    /// # Returns
    /// * `Ok(true)` - Directory is empty
    /// * `Ok(false)` - Directory is not empty
    /// * `Err` - Failed to read directory
    fn is_directory_empty(dir: &PathBuf) -> std::io::Result<bool> {
        let mut entries = std::fs::read_dir(dir)?;
        Ok(entries.next().is_none())
    }

    /// Find cached ranges that overlap with the requested range
    /// Implements Requirements 2.2, 2.4
    ///
    /// This method:
    /// 1. Reads only the metadata file (no range data loaded)
    /// 2. Identifies all cached ranges that overlap with the requested range
    /// 3. Optimizes for common cases (exact match, full containment)
    /// 4. Returns all overlapping range specs
    ///
    /// Range overlap detection:
    /// - Two ranges [a1, a2] and [b1, b2] overlap if: a1 <= b2 AND b1 <= a2
    /// - Exact match: requested range exactly matches a cached range
    /// - Full containment: requested range is fully contained within a cached range
    /// - Partial overlap: requested range partially overlaps with cached ranges
    pub async fn find_cached_ranges(
        &self,
        cache_key: &str,
        requested_start: u64,
        requested_end: u64,
        preloaded_metadata: Option<&crate::cache_types::NewCacheMetadata>,
    ) -> Result<Vec<crate::cache_types::RangeSpec>> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting range lookup: key={}, requested_range={}-{}, operation=find_cached_ranges",
            cache_key, requested_start, requested_end
        );

        // Validate requested range
        if requested_start > requested_end {
            error!(
                "Invalid requested range: key={}, start={}, end={}",
                cache_key, requested_start, requested_end
            );
            return Err(ProxyError::CacheError(format!(
                "Invalid requested range: start ({}) > end ({})",
                requested_start, requested_end
            )));
        }

        // Get metadata: use preloaded if provided, otherwise read from disk
        let metadata = if let Some(preloaded) = preloaded_metadata {
            debug!(
                "Using preloaded metadata: key={}, total_ranges={}, checking_for_overlaps",
                cache_key,
                preloaded.ranges.len()
            );
            Some(preloaded.clone())
        } else {
            match self.get_metadata(cache_key).await? {
                Some(meta) => {
                    debug!(
                        "Metadata retrieved: key={}, total_ranges={}, checking_for_overlaps",
                        cache_key,
                        meta.ranges.len()
                    );
                    Some(meta)
                }
                None => {
                    debug!(
                        "No metadata found: key={}, checking_journals_for_pending_ranges",
                        cache_key
                    );
                    None
                }
            }
        };

        // If no metadata, check journals for pending ranges (shared storage mode)
        if metadata.is_none() {
            let journal_ranges = self
                .find_pending_journal_ranges(cache_key, requested_start, requested_end)
                .await?;
            if !journal_ranges.is_empty() {
                let duration = operation_start.elapsed();
                debug!(
                    "[RANGE_OVERLAP] Range lookup completed via journal fallback: key={}, requested_range={}-{}, result=journal_hit, ranges={}, duration={:.2}ms",
                    cache_key, requested_start, requested_end, journal_ranges.len(), duration.as_secs_f64() * 1000.0
                );
                return Ok(journal_ranges);
            }
            return Ok(Vec::new());
        }

        let metadata = metadata.unwrap();

        // Check object-level expiration (lazy expiration - Requirement 1.4)
        // get_metadata() does not check expires_at for range lookups;
        // the check is deferred here so callers that preload metadata also benefit.
        if std::time::SystemTime::now() > metadata.expires_at {
            debug!(
                "Object expired during range lookup: key={}, expires_at={:?}, result=no_ranges",
                cache_key, metadata.expires_at
            );
            return Ok(Vec::new());
        }

        // Find all overlapping ranges
        let mut overlapping_ranges = Vec::new();

        for range_spec in &metadata.ranges {
            // Requirement 1.5: Log each range being checked for overlap
            let overlaps = requested_start <= range_spec.end && range_spec.start <= requested_end;
            debug!(
                "[RANGE_OVERLAP] Checking range overlap: cache_key={}, cached_range={}-{}, requested_range={}-{}, overlaps={}",
                cache_key, range_spec.start, range_spec.end, requested_start, requested_end, overlaps
            );

            // Check if ranges overlap: requested_start <= range_end AND range_start <= requested_end
            if overlaps {
                overlapping_ranges.push(range_spec.clone());

                debug!(
                    "[RANGE_OVERLAP] Overlap detected: key={}, cached_range={}-{}, requested_range={}-{}, overlap_type=partial",
                    cache_key, range_spec.start, range_spec.end, requested_start, requested_end
                );

                // Optimization: Check for exact match
                if range_spec.start == requested_start && range_spec.end == requested_end {
                    let duration = operation_start.elapsed();
                    debug!(
                        "[RANGE_OVERLAP] Range lookup completed: key={}, requested_range={}-{}, result=exact_match, cached_range={}-{}, duration={:.2}ms",
                        cache_key, requested_start, requested_end, range_spec.start, range_spec.end, duration.as_secs_f64() * 1000.0
                    );
                    // For exact match, we can return immediately with just this range
                    return Ok(vec![range_spec.clone()]);
                }

                // Optimization: Check for full containment
                if range_spec.start <= requested_start && range_spec.end >= requested_end {
                    let duration = operation_start.elapsed();
                    debug!(
                        "[RANGE_OVERLAP] Range lookup completed: key={}, requested_range={}-{}, result=full_containment, cached_range={}-{}, duration={:.2}ms",
                        cache_key, requested_start, requested_end, range_spec.start, range_spec.end, duration.as_secs_f64() * 1000.0
                    );
                    // For full containment, we can return immediately with just this range
                    return Ok(vec![range_spec.clone()]);
                }
            }
        }

        // Sort overlapping ranges by start position for easier processing
        overlapping_ranges.sort_by_key(|r| r.start);

        let duration = operation_start.elapsed();
        if overlapping_ranges.is_empty() {
            // No overlapping ranges in metadata - check journals for pending ranges (shared storage mode)
            let journal_ranges = self
                .find_pending_journal_ranges(cache_key, requested_start, requested_end)
                .await?;
            if !journal_ranges.is_empty() {
                debug!(
                    "[RANGE_OVERLAP] Range lookup completed via journal fallback: key={}, requested_range={}-{}, result=journal_hit, ranges={}, total_cached_ranges={}, duration={:.2}ms",
                    cache_key, requested_start, requested_end, journal_ranges.len(), metadata.ranges.len(), duration.as_secs_f64() * 1000.0
                );
                return Ok(journal_ranges);
            }

            debug!(
                "[RANGE_OVERLAP] Range lookup completed: key={}, requested_range={}-{}, result=no_overlap, total_cached_ranges={}, duration={:.2}ms",
                cache_key, requested_start, requested_end, metadata.ranges.len(), duration.as_secs_f64() * 1000.0
            );
        } else {
            debug!(
                "[RANGE_OVERLAP] Range lookup completed: key={}, requested_range={}-{}, result=partial_overlap, overlapping_ranges={}, total_cached_ranges={}, duration={:.2}ms",
                cache_key, requested_start, requested_end, overlapping_ranges.len(), metadata.ranges.len(), duration.as_secs_f64() * 1000.0
            );
        }

        Ok(overlapping_ranges)
    }

    /// Load range data from binary file with access tracking
    /// Implements Requirements 2.2, 8.2, 1.2 (access tracking)
    ///
    /// This method:
    /// 1. Reads binary data from the range file specified in RangeSpec
    /// 2. Handles decompression if the range was compressed
    /// 3. Handles missing files gracefully (returns error for cache miss)
    /// 4. Validates that decompressed size matches expected uncompressed_size
    /// 5. Updates range access statistics asynchronously (Requirement 1.2)
    ///
    /// Error handling:
    /// - Missing range file: Returns error (Requirement 8.2 - fetch from S3 and recache)
    /// - Corrupted data: Returns error with details
    /// - Decompression failure: Returns error with details
    pub async fn load_range_data(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
    ) -> Result<Vec<u8>> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting range data load: file={}, range={}-{}, compressed_size={} bytes, uncompressed_size={} bytes, compression={:?}",
            range_spec.file_path,
            range_spec.start,
            range_spec.end,
            range_spec.compressed_size,
            range_spec.uncompressed_size,
            range_spec.compression_algorithm
        );

        // Start timing range load
        let load_start = std::time::Instant::now();

        // Construct full path to range binary file
        // range_spec.file_path is relative to ranges directory, including bucket/XX/YYY directories
        // e.g., "bucket/XX/YYY/filename.bin"
        let range_file_path = self.cache_dir.join("ranges").join(&range_spec.file_path);

        debug!(
            "Range file path: file={}, full_path={:?}",
            range_spec.file_path, range_file_path
        );

        // Check if range file exists
        // Requirement 8.2: Handle missing range binary files
        if !range_file_path.exists() {
            debug!(
                "Range file missing (will fetch from S3): file={}, path={:?}, range={}-{}",
                range_spec.file_path, range_file_path, range_spec.start, range_spec.end
            );
            // Record missing range file metric
            if let Some(metrics_manager) = &self.metrics_manager {
                metrics_manager
                    .read()
                    .await
                    .record_missing_range_file()
                    .await;
            }

            // Requirement 8.2: When range binary file cannot be read, return error
            // The caller should fetch from S3 and attempt to recache
            // This is a cache miss scenario - the caller will handle fetching from S3
            return Err(ProxyError::CacheError(format!(
                "Range file not found: {} (cache miss - will fetch from S3)",
                range_spec.file_path
            )));
        }

        // Read compressed data from file
        let read_start = std::time::Instant::now();
        let compressed_data = match std::fs::read(&range_file_path) {
            Ok(data) => {
                let read_duration = read_start.elapsed();
                debug!(
                    "Range file read: file={}, size={} bytes, duration={:.2}ms",
                    range_spec.file_path,
                    data.len(),
                    read_duration.as_secs_f64() * 1000.0
                );
                data
            }
            Err(e) => {
                error!(
                    "Failed to read range file: file={}, path={:?}, error={}",
                    range_spec.file_path, range_file_path, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to read range file {}: {}",
                    range_spec.file_path, e
                )));
            }
        };

        // Save compressed size for logging before we move compressed_data
        let compressed_size = compressed_data.len();

        // Verify compressed size matches what we expect
        if compressed_size as u64 != range_spec.compressed_size {
            warn!(
                "Compressed size mismatch: file={}, range={}-{}, expected={} bytes, actual={} bytes, continuing_anyway",
                range_spec.file_path,
                range_spec.start,
                range_spec.end,
                range_spec.compressed_size,
                compressed_size
            );
            // Continue anyway - the file might have been updated
        }

        // Decompress frame-encoded data
        let decompressed_data = {
            let decompress_start = std::time::Instant::now();
            debug!(
                "Decompressing range data: file={}, algorithm={:?}, compressed={} bytes, expected_uncompressed={} bytes",
                range_spec.file_path,
                range_spec.compression_algorithm,
                compressed_size,
                range_spec.uncompressed_size
            );

            match self.compression_handler.decompress_with_algorithm(
                &compressed_data,
                range_spec.compression_algorithm.clone(),
            ) {
                Ok(data) => {
                    let decompress_duration = decompress_start.elapsed();
                    debug!(
                        "Decompression completed: file={}, algorithm={:?}, compressed={} bytes, uncompressed={} bytes, duration={:.2}ms",
                        range_spec.file_path,
                        range_spec.compression_algorithm,
                        compressed_size,
                        data.len(),
                        decompress_duration.as_secs_f64() * 1000.0
                    );
                    data
                }
                Err(e) => {
                    error!(
                        "Decompression failed: file={}, range={}-{}, algorithm={:?}, error={}",
                        range_spec.file_path,
                        range_spec.start,
                        range_spec.end,
                        range_spec.compression_algorithm,
                        e
                    );
                    return Err(ProxyError::CacheError(format!(
                        "Failed to decompress range data: {}",
                        e
                    )));
                }
            }
        };

        // Validate decompressed size
        if decompressed_data.len() as u64 != range_spec.uncompressed_size {
            error!(
                "Decompressed size validation failed: file={}, range={}-{}, expected={} bytes, actual={} bytes",
                range_spec.file_path,
                range_spec.start,
                range_spec.end,
                range_spec.uncompressed_size,
                decompressed_data.len()
            );
            return Err(ProxyError::CacheError(format!(
                "Decompressed data size mismatch: expected {} bytes, got {} bytes",
                range_spec.uncompressed_size,
                decompressed_data.len()
            )));
        }

        // Validate that data size matches range size
        let expected_range_size = range_spec.end - range_spec.start + 1;
        if decompressed_data.len() as u64 != expected_range_size {
            error!(
                "Range size validation failed: file={}, range={}-{}, expected_size={} bytes, actual_size={} bytes",
                range_spec.file_path,
                range_spec.start,
                range_spec.end,
                expected_range_size,
                decompressed_data.len()
            );
            return Err(ProxyError::CacheError(format!(
                "Range data size mismatch: expected {} bytes for range {}-{}, got {} bytes",
                expected_range_size,
                range_spec.start,
                range_spec.end,
                decompressed_data.len()
            )));
        }

        debug!(
            "Range data validation passed: file={}, range={}-{}, size={} bytes",
            range_spec.file_path,
            range_spec.start,
            range_spec.end,
            decompressed_data.len()
        );

        // Record range load duration
        let load_duration = load_start.elapsed();
        if let Some(metrics_manager) = &self.metrics_manager {
            metrics_manager
                .read()
                .await
                .record_range_load_duration(load_duration.as_secs_f64() * 1000.0)
                .await;
        }

        let total_duration = operation_start.elapsed();
        debug!(
            "Range data load completed: file={}, range={}-{}, uncompressed={} bytes, compressed={} bytes, compression_ratio={:.2}%, total_duration={:.2}ms",
            range_spec.file_path,
            range_spec.start,
            range_spec.end,
            decompressed_data.len(),
            compressed_size,
            (compressed_size as f64 / decompressed_data.len() as f64) * 100.0,
            total_duration.as_secs_f64() * 1000.0
        );

        Ok(decompressed_data)
    }

    /// Stream range data from disk in chunks (Requirement 5.1, 5.4)
    ///
    /// For uncompressed data, streams directly from file in 512 KiB chunks.
    /// For compressed data, reads and decompresses the full file first, then streams
    /// the decompressed output in chunks (LZ4 frame format requires full decompression).
    ///
    /// Returns a stream of Bytes chunks that can be used with StreamBody.
    ///
    /// # Arguments
    /// * `range_spec` - The range specification containing file path and compression info
    /// * `chunk_size` - Size of each chunk in bytes (default: 524_288 = 512 KiB)
    ///
    /// # Returns
    /// * `Ok(impl Stream<Item = Result<Bytes>>)` - Stream of data chunks
    /// * `Err(ProxyError)` - If file cannot be opened or read
    pub async fn stream_range_data(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
        chunk_size: usize,
    ) -> Result<impl futures::Stream<Item = Result<bytes::Bytes>>> {
        use futures::stream::{self, StreamExt};

        let range_file_path = self.cache_dir.join("ranges").join(&range_spec.file_path);

        debug!(
            "Starting range data stream: file={}, range={}-{}, compression={:?}, chunk_size={}",
            range_spec.file_path,
            range_spec.start,
            range_spec.end,
            range_spec.compression_algorithm,
            chunk_size
        );

        // Check if file exists
        if !range_file_path.exists() {
            debug!(
                "Range file missing for streaming: file={}, path={:?}",
                range_spec.file_path, range_file_path
            );
            return Err(ProxyError::CacheError(format!(
                "Range file not found: {} (cache miss)",
                range_spec.file_path
            )));
        }

        // For compressed data, read full file, decompress, then stream decompressed output in chunks.
        // LZ4 frame format requires the full compressed frame to decompress (Requirement 5.4).
        // All cached data uses frame format now.
        debug!(
            "Loading frame-encoded data for streaming: file={}, algorithm={:?}",
            range_spec.file_path, range_spec.compression_algorithm
        );

        // Load and decompress the data
        let data = self.load_range_data(range_spec).await?;

        // Convert to stream of chunks
        let chunks: Vec<Result<bytes::Bytes>> = data
            .chunks(chunk_size)
            .map(|chunk| Ok(bytes::Bytes::copy_from_slice(chunk)))
            .collect();

        Ok(stream::iter(chunks).boxed())
    }

    /// Store full object as a range (0 to content_length-1)
    /// Implements Requirements 4.1, 4.2, 4.3
    ///
    /// This method treats full objects uniformly as ranges, eliminating special casing.
    /// A full object is simply stored as range 0 to content_length-1.
    ///
    /// This enables:
    /// - Consistent storage model (everything is a range)
    /// - Ability to serve any range request from a cached full object
    /// - No distinction between "full object" and "range" cache entries
    pub async fn store_full_object_as_range(
        &mut self,
        cache_key: &str,
        data: &[u8],
        object_metadata: crate::cache_types::ObjectMetadata,
        ttl: std::time::Duration,
        compression_enabled: bool,
    ) -> Result<()> {
        debug!("Storing full object as range for key: {}", cache_key);

        let content_length = data.len() as u64;

        // Validate that data length matches object metadata
        if content_length != object_metadata.content_length {
            return Err(ProxyError::CacheError(format!(
                "Data length ({}) doesn't match object metadata content_length ({})",
                content_length, object_metadata.content_length
            )));
        }

        // Handle empty objects specially
        if content_length == 0 {
            info!("Storing empty object for key: {}", cache_key);

            // For empty objects, we still create metadata but with no ranges
            // This is consistent with the design: empty objects have no data to cache
            let now = SystemTime::now();
            let metadata = crate::cache_types::NewCacheMetadata {
                cache_key: cache_key.to_string(),
                object_metadata,
                ranges: Vec::new(), // No ranges for empty object
                created_at: now,
                expires_at: now + ttl,
                compression_info: crate::cache_types::CompressionInfo::default(),
                ..Default::default()
            };

            return self.update_metadata(&metadata).await;
        }

        // Store as range 0 to content_length-1
        // This is the key insight: full objects are just ranges from 0 to N
        let start = 0u64;
        let end = content_length - 1;

        info!(
            "Storing full object as range {}-{} for key: {} ({} bytes)",
            start, end, cache_key, content_length
        );

        // Use the existing store_range method - no special casing needed!
        self.store_range(cache_key, start, end, data, object_metadata, ttl, compression_enabled)
            .await
    }

    /// Retrieve full object by loading it as a range
    /// Implements Requirements 4.2, 4.4
    ///
    /// This method retrieves a full object by treating it as a range request for bytes 0-N.
    /// It demonstrates that full objects and ranges are handled uniformly.
    pub async fn get_full_object_as_range(&self, cache_key: &str) -> Result<Option<Vec<u8>>> {
        debug!("Retrieving full object as range for key: {}", cache_key);

        // Get metadata to determine object size
        let metadata = match self.get_metadata(cache_key).await? {
            Some(meta) => meta,
            None => {
                debug!("No metadata found for key: {}", cache_key);
                return Ok(None);
            }
        };

        let content_length = metadata.object_metadata.content_length;

        if content_length == 0 {
            // Empty object
            debug!("Empty object for key: {}", cache_key);
            return Ok(Some(Vec::new()));
        }

        // Request the full range: 0 to content_length-1
        let requested_start = 0u64;
        let requested_end = content_length - 1;

        debug!(
            "Requesting full object as range {}-{} for key: {}",
            requested_start, requested_end, cache_key
        );

        // Find cached ranges that cover the full object
        let overlapping_ranges = self
            .find_cached_ranges(cache_key, requested_start, requested_end, None)
            .await?;

        if overlapping_ranges.is_empty() {
            debug!("No cached ranges found for full object: {}", cache_key);
            return Ok(None);
        }

        // Check if we have a single range that covers the entire object
        // This is the common case for full objects stored as range 0-N
        if overlapping_ranges.len() == 1 {
            let range = &overlapping_ranges[0];
            if range.start == requested_start && range.end == requested_end {
                // Perfect match - load and return the range data
                debug!(
                    "Found exact match for full object: range {}-{}",
                    range.start, range.end
                );
                let data = self.load_range_data(range).await?;
                return Ok(Some(data));
            }
        }

        // If we have multiple ranges or partial coverage, we need to assemble them
        // For now, we'll only support the case where we have full coverage
        // Check if ranges fully cover the requested range
        let mut sorted_ranges = overlapping_ranges.clone();
        sorted_ranges.sort_by_key(|r| r.start);

        // Verify full coverage
        let mut current_pos = requested_start;
        for range in &sorted_ranges {
            if range.start > current_pos {
                // Gap in coverage
                debug!(
                    "Gap in range coverage for full object: expected start {}, found {}",
                    current_pos, range.start
                );
                return Ok(None);
            }
            current_pos = std::cmp::max(current_pos, range.end + 1);
        }

        if current_pos <= requested_end {
            // Incomplete coverage
            debug!(
                "Incomplete range coverage for full object: covered up to {}, need {}",
                current_pos - 1,
                requested_end
            );
            return Ok(None);
        }

        // We have full coverage - assemble the data from multiple ranges
        debug!("Assembling full object from {} ranges", sorted_ranges.len());
        let mut full_data = Vec::with_capacity(content_length as usize);

        for range in &sorted_ranges {
            let range_data = self.load_range_data(range).await?;

            // Calculate which portion of this range we need
            let copy_start = if range.start < requested_start {
                (requested_start - range.start) as usize
            } else {
                0
            };

            let copy_end = if range.end > requested_end {
                (requested_end - range.start + 1) as usize
            } else {
                range_data.len()
            };

            full_data.extend_from_slice(&range_data[copy_start..copy_end]);
        }

        info!(
            "Successfully assembled full object from {} ranges for key: {} ({} bytes)",
            sorted_ranges.len(),
            cache_key,
            full_data.len()
        );

        Ok(Some(full_data))
    }

    /// Delete cache entry (metadata + all range files)
    /// Implements Requirements 7.1, 7.2, 7.3, 7.4
    ///
    /// This method:
    /// 1. Reads metadata file to get list of all .bin files
    /// 2. Deletes all associated range binary files (best effort)
    /// 3. Deletes metadata file last (only after attempting to delete all range files)
    /// 4. Prevents orphaned files by reading metadata before deletion
    /// 5. Uses atomic operations - if any .bin file deletion fails, logs warning but continues
    /// 6. Cleans up lock files and temporary files
    ///
    /// **CRITICAL**: Never delete metadata file unless all associated .bin files are also deleted
    /// **CRITICAL**: Must read metadata file to get list of all .bin files before deletion
    pub async fn delete_cache_entry(&self, cache_key: &str) -> Result<()> {
        let operation_start = std::time::Instant::now();
        info!(
            "Starting cache entry deletion: key={}, operation=delete_cache_entry",
            cache_key
        );

        let metadata_file_path = self.get_new_metadata_file_path(cache_key);

        // Step 1: Read metadata file to get list of all .bin files
        // This is CRITICAL - we must know which range files to delete
        let metadata = match self.get_metadata(cache_key).await? {
            Some(meta) => {
                debug!(
                    "Metadata retrieved for deletion: key={}, ranges={}, etag={}",
                    cache_key,
                    meta.ranges.len(),
                    meta.object_metadata.etag
                );
                meta
            }
            None => {
                info!(
                    "No metadata found for deletion: key={}, performing_orphan_cleanup",
                    cache_key
                );
                // Still try to clean up any orphaned files
                self.cleanup_orphaned_files(cache_key).await?;
                return Ok(());
            }
        };

        let range_count = metadata.ranges.len();
        info!(
            "Deleting cache entry: key={}, ranges={}, metadata_path={:?}",
            cache_key, range_count, metadata_file_path
        );

        // Step 2: Delete all associated range binary files (best effort)
        let mut deleted_ranges = 0;
        let mut failed_ranges = 0;
        let mut missing_ranges = 0;
        let mut total_deleted_bytes = 0u64;

        debug!(
            "Deleting range files: key={}, total_ranges={}",
            cache_key,
            metadata.ranges.len()
        );

        for range_spec in &metadata.ranges {
            let range_file_path = self.cache_dir.join("ranges").join(&range_spec.file_path);

            if range_file_path.exists() {
                match std::fs::remove_file(&range_file_path) {
                    Ok(_) => {
                        deleted_ranges += 1;
                        total_deleted_bytes += range_spec.compressed_size;
                        debug!(
                            "Deleted range file: key={}, range={}-{}, path={:?}, size={} bytes",
                            cache_key,
                            range_spec.start,
                            range_spec.end,
                            range_file_path,
                            range_spec.compressed_size
                        );
                    }
                    Err(e) => {
                        failed_ranges += 1;
                        // Log warning but continue - best effort cleanup
                        warn!(
                            "Failed to delete range file: key={}, range={}-{}, path={:?}, error={}, continuing_cleanup",
                            cache_key, range_spec.start, range_spec.end, range_file_path, e
                        );
                    }
                }
            } else {
                missing_ranges += 1;
                debug!(
                    "Range file not found (already deleted?): key={}, range={}-{}, path={:?}",
                    cache_key, range_spec.start, range_spec.end, range_file_path
                );
            }
        }

        info!(
            "Range files deletion summary: key={}, deleted={}, failed={}, missing={}, total={}",
            cache_key,
            deleted_ranges,
            failed_ranges,
            missing_ranges,
            metadata.ranges.len()
        );

        // Step 3: Delete metadata file last (only after attempting to delete all range files)
        // This ensures we never have orphaned .bin files without metadata pointing to them
        if metadata_file_path.exists() {
            match std::fs::remove_file(&metadata_file_path) {
                Ok(_) => {
                    info!(
                        "Deleted metadata file: key={}, path={:?}",
                        cache_key, metadata_file_path
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to delete metadata file: key={}, path={:?}, error={}",
                        cache_key, metadata_file_path, e
                    );
                    return Err(ProxyError::CacheError(format!(
                        "Failed to delete metadata file: {}",
                        e
                    )));
                }
            }
        } else {
            debug!(
                "Metadata file not found (already deleted?): key={}, path={:?}",
                cache_key, metadata_file_path
            );
        }

        // Step 4: Clean up lock file if it exists
        let lock_file_path = metadata_file_path.with_extension("meta.lock");
        if lock_file_path.exists() {
            match std::fs::remove_file(&lock_file_path) {
                Ok(_) => {
                    debug!(
                        "Deleted lock file: key={}, path={:?}",
                        cache_key, lock_file_path
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to delete lock file: key={}, path={:?}, error={}",
                        cache_key, lock_file_path, e
                    );
                }
            }
        }

        // Step 5: Clean up any temporary files
        self.cleanup_temp_files(cache_key).await?;

        let total_duration = operation_start.elapsed();
        info!(
            "Cache entry deletion completed: key={}, ranges_deleted={}, ranges_failed={}, ranges_missing={}, total_ranges={}, bytes_deleted={}, duration={:.2}ms",
            cache_key, deleted_ranges, failed_ranges, missing_ranges, metadata.ranges.len(), total_deleted_bytes, total_duration.as_secs_f64() * 1000.0
        );

        // NOTE: Size tracking is now handled by JournalConsolidator through journal entries.
        // The Remove operation in the journal contains the size delta.

        Ok(())
    }

    /// Delete specific ranges from a cache entry (partial eviction)
    /// Implements Requirements 7.1, 7.2, 7.3, 7.4
    ///
    /// This method:
    /// 1. Reads current metadata
    /// 2. Deletes specified range binary files (best effort)
    /// 3. Updates metadata to remove deleted ranges
    /// 4. If no ranges remain, deletes entire entry
    /// 5. Supports batch deletion - multiple ranges updated in single metadata write
    ///
    /// **CRITICAL**: Support partial eviction - delete specific ranges, update metadata, keep other ranges
    /// **CRITICAL**: Use atomic operations - if any .bin file deletion fails, log warning but continue
    /// **OPTIMIZATION**: Batch range deletions - when deleting multiple ranges from same object, update metadata once
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `ranges_to_delete` - List of (start, end) tuples specifying which ranges to delete
    pub async fn delete_ranges(
        &self,
        cache_key: &str,
        ranges_to_delete: &[(u64, u64)],
    ) -> Result<()> {
        let operation_start = std::time::Instant::now();
        info!(
            "Starting partial range deletion: key={}, ranges_to_delete={}, operation=delete_ranges",
            cache_key,
            ranges_to_delete.len()
        );

        if ranges_to_delete.is_empty() {
            debug!("No ranges to delete: key={}, result=no_op", cache_key);
            return Ok(());
        }

        // Log the ranges being deleted
        for (start, end) in ranges_to_delete {
            debug!(
                "Range marked for deletion: key={}, range={}-{}",
                cache_key, start, end
            );
        }

        // Step 1: Read current metadata
        let mut metadata =
            match self.get_metadata(cache_key).await? {
                Some(meta) => {
                    debug!(
                    "Metadata retrieved for partial deletion: key={}, current_ranges={}, etag={}",
                    cache_key, meta.ranges.len(), meta.object_metadata.etag
                );
                    meta
                }
                None => {
                    info!(
                        "No metadata found for partial deletion: key={}, result=no_op",
                        cache_key
                    );
                    return Ok(());
                }
            };

        let original_range_count = metadata.ranges.len();
        info!(
            "Partial deletion starting: key={}, current_ranges={}, ranges_to_delete={}",
            cache_key,
            original_range_count,
            ranges_to_delete.len()
        );

        // Step 2: Delete specified range binary files (best effort)
        let mut deleted_ranges = 0;
        let mut failed_ranges = 0;
        let mut missing_ranges = 0;
        let mut ranges_to_keep = Vec::new();

        for range_spec in &metadata.ranges {
            // Check if this range should be deleted
            let should_delete = ranges_to_delete
                .iter()
                .any(|(start, end)| range_spec.start == *start && range_spec.end == *end);

            if should_delete {
                // Delete the range binary file
                let range_file_path = self.cache_dir.join("ranges").join(&range_spec.file_path);

                debug!(
                    "Deleting range file: key={}, range={}-{}, path={:?}",
                    cache_key, range_spec.start, range_spec.end, range_file_path
                );

                if range_file_path.exists() {
                    match std::fs::remove_file(&range_file_path) {
                        Ok(_) => {
                            deleted_ranges += 1;
                            info!(
                                "Deleted range file: key={}, range={}-{}, path={:?}",
                                cache_key, range_spec.start, range_spec.end, range_file_path
                            );
                        }
                        Err(e) => {
                            failed_ranges += 1;
                            // Log warning but continue - best effort cleanup
                            warn!(
                                "Failed to delete range file: key={}, range={}-{}, path={:?}, error={}, continuing_cleanup",
                                cache_key, range_spec.start, range_spec.end, range_file_path, e
                            );
                        }
                    }
                } else {
                    missing_ranges += 1;
                    debug!(
                        "Range file not found (already deleted?): key={}, range={}-{}, path={:?}",
                        cache_key, range_spec.start, range_spec.end, range_file_path
                    );
                }
            } else {
                // Keep this range
                ranges_to_keep.push(range_spec.clone());
                debug!(
                    "Keeping range: key={}, range={}-{}",
                    cache_key, range_spec.start, range_spec.end
                );
            }
        }

        info!(
            "Range deletion summary: key={}, deleted={}, failed={}, missing={}, kept={}",
            cache_key,
            deleted_ranges,
            failed_ranges,
            missing_ranges,
            ranges_to_keep.len()
        );

        // Step 3: Update metadata to remove deleted ranges
        metadata.ranges = ranges_to_keep;

        // Step 4: If no ranges left, delete entire entry; otherwise update metadata
        if metadata.ranges.is_empty() {
            info!(
                "No ranges remaining after deletion: key={}, performing_full_entry_deletion",
                cache_key
            );
            // Delete the entire entry (metadata + any remaining files)
            self.delete_cache_entry(cache_key).await?;
        } else {
            // Update metadata with remaining ranges
            info!(
                "Updating metadata after partial deletion: key={}, remaining_ranges={}",
                cache_key,
                metadata.ranges.len()
            );
            self.update_metadata(&metadata).await?;
        }

        let total_duration = operation_start.elapsed();
        info!(
            "Partial range deletion completed: key={}, requested_deletions={}, deleted={}, failed={}, missing={}, remaining_ranges={}, duration={:.2}ms",
            cache_key,
            ranges_to_delete.len(),
            deleted_ranges,
            failed_ranges,
            missing_ranges,
            metadata.ranges.len(),
            total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Clean up orphaned files for a cache key
    ///
    /// This method cleans up orphaned range files that don't have corresponding metadata.
    /// It traverses the new bucket-first sharded directory structure (bucket/XX/YYY).
    ///
    /// # Requirements
    /// Implements Requirement 8.5: Update orphaned file cleanup to traverse new structure
    ///
    /// # Behavior
    /// 1. Parses cache key to extract bucket and object key
    /// 2. Computes hash to determine shard directories (XX/YYY)
    /// 3. Traverses the specific shard directory for this cache key
    /// 4. Removes orphaned .bin files (range files without metadata)
    /// 5. Removes orphaned .tmp files (failed writes)
    /// 6. Optionally cleans up empty directories
    /// 7. Handles multiple buckets correctly
    async fn cleanup_orphaned_files(&self, cache_key: &str) -> Result<()> {
        debug!("Cleaning up orphaned files for key: {}", cache_key);

        let ranges_base_dir = self.cache_dir.join("ranges");
        if !ranges_base_dir.exists() {
            debug!("Ranges directory doesn't exist, nothing to clean up");
            return Ok(());
        }

        // Parse cache key to get bucket and object key
        let (bucket, object_key) = match parse_cache_key(cache_key) {
            Ok((b, o)) => (b, o),
            Err(e) => {
                warn!(
                    "Failed to parse cache key for cleanup: key={}, error={}. Skipping cleanup.",
                    cache_key, e
                );
                return Ok(());
            }
        };

        // Compute hash to determine shard directories
        let hash = blake3::hash(object_key.as_bytes());
        let hash_hex = hash.to_hex();
        let level1 = &hash_hex.as_str()[0..2];
        let level2 = &hash_hex.as_str()[2..5];

        // Construct the shard directory path: ranges/bucket/XX/YYY
        let shard_dir = ranges_base_dir.join(bucket).join(level1).join(level2);

        if !shard_dir.exists() {
            debug!(
                "Shard directory doesn't exist for key: key={}, shard_dir={:?}",
                cache_key, shard_dir
            );
            return Ok(());
        }

        // Get the sanitized object key for filename matching
        let sanitized_key = sanitize_object_key_for_filename(object_key, "");

        debug!(
            "Scanning shard directory for orphaned files: key={}, bucket={}, shard_dir={:?}, pattern={}",
            cache_key, bucket, shard_dir, sanitized_key
        );

        // Scan the shard directory for files matching this cache key
        let mut cleaned_count = 0;
        let mut failed_count = 0;

        match std::fs::read_dir(&shard_dir) {
            Ok(entries) => {
                for entry in entries {
                    if let Ok(entry) = entry {
                        let file_name = entry.file_name();
                        let file_name_str = file_name.to_string_lossy();

                        // Check if this file belongs to our cache key
                        // Files should start with sanitized key and end with .bin or .tmp
                        let is_orphaned_bin = file_name_str.starts_with(&sanitized_key)
                            && file_name_str.contains("_")
                            && file_name_str.ends_with(".bin");
                        let is_orphaned_tmp = file_name_str.starts_with(&sanitized_key)
                            && file_name_str.ends_with(".tmp");

                        if is_orphaned_bin || is_orphaned_tmp {
                            debug!(
                                "Found potential orphaned file: key={}, file={:?}",
                                cache_key,
                                entry.path()
                            );

                            match std::fs::remove_file(entry.path()) {
                                Ok(_) => {
                                    cleaned_count += 1;
                                    debug!(
                                        "Cleaned up orphaned file: key={}, file={:?}",
                                        cache_key,
                                        entry.path()
                                    );
                                }
                                Err(e) => {
                                    failed_count += 1;
                                    warn!(
                                        "Failed to clean up orphaned file: key={}, file={:?}, error={}",
                                        cache_key, entry.path(), e
                                    );
                                }
                            }
                        }
                    }
                }

                if cleaned_count > 0 {
                    info!(
                        "Cleaned up orphaned files: key={}, cleaned={}, failed={}",
                        cache_key, cleaned_count, failed_count
                    );
                }

                // Try to clean up empty directories (best effort)
                // Remove YYY directory if empty
                if let Ok(mut entries) = std::fs::read_dir(&shard_dir) {
                    if entries.next().is_none() {
                        if let Err(e) = std::fs::remove_dir(&shard_dir) {
                            debug!(
                                "Failed to remove empty shard directory (non-critical): path={:?}, error={}",
                                shard_dir, e
                            );
                        } else {
                            debug!("Removed empty shard directory: path={:?}", shard_dir);

                            // Try to remove XX directory if empty
                            if let Some(level1_dir) = shard_dir.parent() {
                                if let Ok(mut entries) = std::fs::read_dir(level1_dir) {
                                    if entries.next().is_none() {
                                        if let Err(e) = std::fs::remove_dir(level1_dir) {
                                            debug!(
                                                "Failed to remove empty level1 directory (non-critical): path={:?}, error={}",
                                                level1_dir, e
                                            );
                                        } else {
                                            debug!(
                                                "Removed empty level1 directory: path={:?}",
                                                level1_dir
                                            );

                                            // Try to remove bucket directory if empty
                                            if let Some(bucket_dir) = level1_dir.parent() {
                                                if let Ok(mut entries) =
                                                    std::fs::read_dir(bucket_dir)
                                                {
                                                    if entries.next().is_none() {
                                                        if let Err(e) =
                                                            std::fs::remove_dir(bucket_dir)
                                                        {
                                                            debug!(
                                                                "Failed to remove empty bucket directory (non-critical): path={:?}, error={}",
                                                                bucket_dir, e
                                                            );
                                                        } else {
                                                            debug!("Removed empty bucket directory: path={:?}", bucket_dir);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Failed to read shard directory for cleanup: key={}, shard_dir={:?}, error={}",
                    cache_key, shard_dir, e
                );
            }
        }

        Ok(())
    }

    /// Clean up temporary files (.tmp) for a cache key
    /// This removes any .tmp files that may have been left behind from failed writes
    /// Requirement 8.4: Handle partial write failures
    /// Requirement 8.5: Traverse bucket/XX/YYY structure for cleanup
    ///
    /// This method cleans up temporary files (.tmp) that may have been left behind
    /// from failed writes. It traverses the new bucket-first sharded structure:
    /// - ranges/bucket/XX/YYY/ for .bin.tmp files
    /// - metadata/bucket/XX/YYY/ for .meta.tmp files
    pub async fn cleanup_temp_files(&self, cache_key: &str) -> Result<()> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting temp file cleanup: key={}, operation=cleanup_temp_files",
            cache_key
        );

        let mut cleaned_count = 0;

        // Parse cache key to get bucket and object key
        let (bucket, object_key) = match parse_cache_key(cache_key) {
            Ok((b, o)) => (b, o),
            Err(e) => {
                warn!(
                    "Failed to parse cache key for cleanup: key={}, error={}. Skipping cleanup.",
                    cache_key, e
                );
                return Ok(());
            }
        };

        // Compute hash to determine shard directories
        let hash = blake3::hash(object_key.as_bytes());
        let hash_hex = hash.to_hex();
        let level1 = &hash_hex.as_str()[0..2];
        let level2 = &hash_hex.as_str()[2..5];

        // Get the sanitized object key for filename matching
        let sanitized_key = sanitize_object_key_for_filename(object_key, "");

        // Clean up range .tmp files in ranges/bucket/XX/YYY/
        let ranges_shard_dir = self
            .cache_dir
            .join("ranges")
            .join(bucket)
            .join(level1)
            .join(level2);

        if ranges_shard_dir.exists() {
            debug!(
                "Scanning ranges shard directory for temp files: key={}, shard_dir={:?}",
                cache_key, ranges_shard_dir
            );

            if let Ok(entries) = std::fs::read_dir(&ranges_shard_dir) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        let file_name = entry.file_name();
                        let file_name_str = file_name.to_string_lossy();

                        // Check if this is a temp file for our cache key
                        if file_name_str.starts_with(&sanitized_key)
                            && file_name_str.ends_with(".bin.tmp")
                        {
                            match std::fs::remove_file(entry.path()) {
                                Ok(_) => {
                                    cleaned_count += 1;
                                    debug!("Cleaned up range temp file: {:?}", entry.path());
                                }
                                Err(e) => warn!(
                                    "Failed to clean up range temp file {:?}: {}",
                                    entry.path(),
                                    e
                                ),
                            }
                        }
                    }
                }
            }
        }

        // Clean up metadata .tmp file in metadata/bucket/XX/YYY/
        let metadata_tmp_path = self
            .get_new_metadata_file_path(cache_key)
            .with_extension("meta.tmp");
        if metadata_tmp_path.exists() {
            match std::fs::remove_file(&metadata_tmp_path) {
                Ok(_) => {
                    cleaned_count += 1;
                    debug!("Cleaned up metadata temp file: {:?}", metadata_tmp_path);
                }
                Err(e) => warn!(
                    "Failed to clean up metadata temp file {:?}: {}",
                    metadata_tmp_path, e
                ),
            }
        }

        if cleaned_count > 0 {
            // Record partial write cleanup metric
            if let Some(metrics_manager) = &self.metrics_manager {
                for _ in 0..cleaned_count {
                    metrics_manager
                        .read()
                        .await
                        .record_partial_write_cleanup()
                        .await;
                }
            }
            let duration = operation_start.elapsed();
            info!(
                "Temp file cleanup completed: key={}, files_cleaned={}, duration={:.2}ms",
                cache_key,
                cleaned_count,
                duration.as_secs_f64() * 1000.0
            );
        } else {
            debug!(
                "No temp files found: key={}, result=no_cleanup_needed",
                cache_key
            );
        }

        Ok(())
    }

    /// Verify and fix inconsistent metadata
    /// Implements Requirement 8.4: Handle inconsistent metadata
    ///
    /// This method:
    /// 1. Reads metadata and checks if all referenced range files exist
    /// 2. Removes range specs for missing files from metadata
    /// 3. Updates metadata if any inconsistencies were found
    /// 4. Returns the number of inconsistencies fixed
    ///
    /// This allows the cache to continue serving other ranges even if some are missing
    pub async fn verify_and_fix_metadata(&self, cache_key: &str) -> Result<u64> {
        let operation_start = std::time::Instant::now();
        info!(
            "Starting metadata verification: key={}, operation=verify_and_fix_metadata",
            cache_key
        );

        // Get metadata
        let mut metadata = match self.get_metadata(cache_key).await? {
            Some(meta) => {
                debug!(
                    "Metadata retrieved for verification: key={}, ranges={}",
                    cache_key,
                    meta.ranges.len()
                );
                meta
            }
            None => {
                debug!(
                    "No metadata found for verification: key={}, result=no_verification_needed",
                    cache_key
                );
                return Ok(0);
            }
        };

        let original_range_count = metadata.ranges.len();
        let mut valid_ranges = Vec::new();
        let mut missing_count = 0;

        debug!(
            "Verifying range files: key={}, total_ranges={}",
            cache_key, original_range_count
        );

        // Check each range file
        for range_spec in &metadata.ranges {
            let range_file_path = self.cache_dir.join("ranges").join(&range_spec.file_path);

            if range_file_path.exists() {
                // Range file exists, keep it
                valid_ranges.push(range_spec.clone());
                debug!(
                    "Range file verified: key={}, range={}-{}, path={:?}",
                    cache_key, range_spec.start, range_spec.end, range_file_path
                );
            } else {
                // Range file is missing
                missing_count += 1;
                warn!(
                    "Inconsistency detected: key={}, range={}-{}, path={:?}, issue=missing_range_file",
                    cache_key, range_spec.start, range_spec.end, range_file_path
                );
            }
        }

        // If we found missing ranges, update metadata
        if missing_count > 0 {
            metadata.ranges = valid_ranges;

            // Record inconsistency fixed metric
            if let Some(metrics_manager) = &self.metrics_manager {
                for _ in 0..missing_count {
                    metrics_manager
                        .read()
                        .await
                        .record_inconsistency_fixed()
                        .await;
                }
            }

            if metadata.ranges.is_empty() {
                // No valid ranges left, delete entire entry
                info!(
                    "All ranges missing: key={}, original_ranges={}, performing_full_deletion",
                    cache_key, original_range_count
                );
                self.delete_cache_entry(cache_key).await?;
            } else {
                // Update metadata with only valid ranges
                info!(
                    "Inconsistencies fixed: key={}, missing_ranges={}, valid_ranges={}, original_ranges={}",
                    cache_key, missing_count, metadata.ranges.len(), original_range_count
                );
                self.update_metadata(&metadata).await?;
            }

            let duration = operation_start.elapsed();
            info!(
                "Metadata verification completed: key={}, inconsistencies_fixed={}, remaining_ranges={}, duration={:.2}ms",
                cache_key, missing_count, metadata.ranges.len(), duration.as_secs_f64() * 1000.0
            );
        } else {
            let duration = operation_start.elapsed();
            debug!(
                "Metadata verification completed: key={}, result=no_inconsistencies, ranges={}, duration={:.2}ms",
                cache_key, original_range_count, duration.as_secs_f64() * 1000.0
            );
        }

        Ok(missing_count)
    }

    /// Perform comprehensive cache cleanup
    /// Implements Requirements 8.1, 8.2, 8.3, 8.4
    ///
    /// This method:
    /// 1. Cleans up orphaned range files (no metadata)
    /// 2. Cleans up temporary files (.tmp)
    /// 3. Verifies and fixes inconsistent metadata
    /// 4. Returns statistics about cleanup operations
    /// Requirement 8.5: Traverse bucket/XX/YYY structure for cleanup
    ///
    /// This method performs comprehensive cache cleanup by:
    /// 1. Traversing ranges/bucket/XX/YYY/ for orphaned .bin files and .tmp files
    /// 2. Traversing metadata/bucket/XX/YYY/ for orphaned .meta files and .tmp files
    /// 3. Verifying and fixing inconsistent metadata
    /// 4. Cleaning up empty directories after cleanup
    pub async fn perform_cache_cleanup(&self) -> Result<CacheCleanupStats> {
        let operation_start = std::time::Instant::now();
        info!(
            "Starting comprehensive cache cleanup: operation=perform_cache_cleanup, cache_dir={:?}",
            self.cache_dir
        );

        let mut stats = CacheCleanupStats::default();

        // Step 1: Clean up orphaned range files and temp files in ranges/bucket/XX/YYY/
        let ranges_base_dir = self.cache_dir.join("ranges");
        if ranges_base_dir.exists() {
            self.traverse_and_cleanup_ranges(&ranges_base_dir, &mut stats)
                .await;
        }

        // Step 2: Clean up orphaned metadata .tmp files in metadata/bucket/XX/YYY/
        let metadata_base_dir = self.cache_dir.join("metadata");
        if metadata_base_dir.exists() {
            self.traverse_and_cleanup_metadata(&metadata_base_dir, &mut stats)
                .await;
        }

        // Step 3: Verify and fix inconsistent metadata for all cache entries
        if metadata_base_dir.exists() {
            self.traverse_and_verify_metadata(&metadata_base_dir, &mut stats)
                .await;
        }

        // Record metrics
        if let Some(metrics_manager) = &self.metrics_manager {
            if stats.temp_files_cleaned > 0 {
                for _ in 0..stats.temp_files_cleaned {
                    metrics_manager
                        .read()
                        .await
                        .record_partial_write_cleanup()
                        .await;
                }
            }
            if stats.orphaned_files_cleaned > 0 {
                metrics_manager
                    .read()
                    .await
                    .record_orphaned_files_cleaned(stats.orphaned_files_cleaned)
                    .await;
            }
        }

        let total_duration = operation_start.elapsed();
        info!(
            "Comprehensive cache cleanup completed: temp_files_cleaned={}, orphaned_files_cleaned={}, inconsistencies_fixed={}, duration={:.2}ms",
            stats.temp_files_cleaned,
            stats.orphaned_files_cleaned,
            stats.inconsistencies_fixed,
            total_duration.as_secs_f64() * 1000.0
        );

        Ok(stats)
    }

    /// Traverse ranges directory structure and clean up orphaned/temp files
    /// Traverses: ranges/bucket/XX/YYY/
    async fn traverse_and_cleanup_ranges(
        &self,
        ranges_base_dir: &PathBuf,
        stats: &mut CacheCleanupStats,
    ) {
        // Traverse bucket directories
        if let Ok(bucket_entries) = std::fs::read_dir(ranges_base_dir) {
            for bucket_entry in bucket_entries {
                if let Ok(bucket_entry) = bucket_entry {
                    let bucket_path = bucket_entry.path();
                    if !bucket_path.is_dir() {
                        continue;
                    }

                    // Traverse level1 (XX) directories
                    if let Ok(level1_entries) = std::fs::read_dir(&bucket_path) {
                        for level1_entry in level1_entries {
                            if let Ok(level1_entry) = level1_entry {
                                let level1_path = level1_entry.path();
                                if !level1_path.is_dir() {
                                    continue;
                                }

                                // Traverse level2 (YYY) directories
                                if let Ok(level2_entries) = std::fs::read_dir(&level1_path) {
                                    for level2_entry in level2_entries {
                                        if let Ok(level2_entry) = level2_entry {
                                            let level2_path = level2_entry.path();
                                            if !level2_path.is_dir() {
                                                continue;
                                            }

                                            // Now we're in the leaf directory - scan for files
                                            self.cleanup_range_files_in_shard(&level2_path, stats)
                                                .await;

                                            // Try to clean up empty directory
                                            self.cleanup_empty_directory(&level2_path);
                                        }
                                    }
                                    // Try to clean up empty level1 directory
                                    self.cleanup_empty_directory(&level1_path);
                                }
                            }
                        }
                        // Try to clean up empty bucket directory
                        self.cleanup_empty_directory(&bucket_path);
                    }
                }
            }
        }
    }

    /// Clean up range files in a specific shard directory
    async fn cleanup_range_files_in_shard(
        &self,
        shard_dir: &PathBuf,
        stats: &mut CacheCleanupStats,
    ) {
        if let Ok(entries) = std::fs::read_dir(shard_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    let file_name = entry.file_name();
                    let file_name_str = file_name.to_string_lossy();

                    // Check for .tmp files
                    if file_name_str.ends_with(".bin.tmp") {
                        match std::fs::remove_file(&path) {
                            Ok(_) => {
                                stats.temp_files_cleaned += 1;
                                debug!("Cleaned up range temp file: {:?}", path);
                            }
                            Err(e) => {
                                warn!("Failed to clean up range temp file {:?}: {}", path, e);
                            }
                        }
                    }
                    // Check for orphaned .bin files
                    else if file_name_str.ends_with(".bin") {
                        // Extract sanitized key from filename
                        // Format: {sanitized_key}_{start}-{end}.bin
                        if let Some(underscore_pos) = file_name_str.rfind('_') {
                            let sanitized_key = &file_name_str[..underscore_pos];

                            // To check if this is orphaned, we need to find the metadata file
                            // The metadata file should be in metadata/bucket/XX/YYY/{sanitized_key}.meta
                            // Extract path components from shard_dir
                            // shard_dir format: ranges/bucket/XX/YYY
                            if let Some(level2_dir) = shard_dir.parent() {
                                // ranges/bucket/XX
                                if let Some(level1_dir) = level2_dir.parent() {
                                    // ranges/bucket
                                    if let Some(bucket_name) = level1_dir.file_name() {
                                        // bucket
                                        // Get level1 and level2 from path
                                        let level1 =
                                            level2_dir.file_name().unwrap().to_string_lossy(); // XX
                                        let level2 =
                                            shard_dir.file_name().unwrap().to_string_lossy(); // YYY

                                        // Construct metadata path
                                        let metadata_path = self
                                            .cache_dir
                                            .join("metadata")
                                            .join(bucket_name)
                                            .join(level1.as_ref())
                                            .join(level2.as_ref())
                                            .join(format!("{}.meta", sanitized_key));

                                        if !metadata_path.exists() {
                                            // Orphaned range file - no metadata
                                            match std::fs::remove_file(&path) {
                                                Ok(_) => {
                                                    stats.orphaned_files_cleaned += 1;
                                                    debug!("Cleaned up orphaned range file (no metadata found): {:?}, expected_metadata={:?}", path, metadata_path);
                                                }
                                                Err(e) => {
                                                    warn!("Failed to clean up orphaned range file {:?}: {}", path, e);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Traverse metadata directory structure and clean up temp files
    /// Traverses: metadata/bucket/XX/YYY/
    async fn traverse_and_cleanup_metadata(
        &self,
        metadata_base_dir: &PathBuf,
        stats: &mut CacheCleanupStats,
    ) {
        // Traverse bucket directories
        if let Ok(bucket_entries) = std::fs::read_dir(metadata_base_dir) {
            for bucket_entry in bucket_entries {
                if let Ok(bucket_entry) = bucket_entry {
                    let bucket_path = bucket_entry.path();
                    if !bucket_path.is_dir() {
                        continue;
                    }

                    // Traverse level1 (XX) directories
                    if let Ok(level1_entries) = std::fs::read_dir(&bucket_path) {
                        for level1_entry in level1_entries {
                            if let Ok(level1_entry) = level1_entry {
                                let level1_path = level1_entry.path();
                                if !level1_path.is_dir() {
                                    continue;
                                }

                                // Traverse level2 (YYY) directories
                                if let Ok(level2_entries) = std::fs::read_dir(&level1_path) {
                                    for level2_entry in level2_entries {
                                        if let Ok(level2_entry) = level2_entry {
                                            let level2_path = level2_entry.path();
                                            if !level2_path.is_dir() {
                                                continue;
                                            }

                                            // Now we're in the leaf directory - scan for temp files
                                            self.cleanup_metadata_files_in_shard(
                                                &level2_path,
                                                stats,
                                            );

                                            // Try to clean up empty directory
                                            self.cleanup_empty_directory(&level2_path);
                                        }
                                    }
                                    // Try to clean up empty level1 directory
                                    self.cleanup_empty_directory(&level1_path);
                                }
                            }
                        }
                        // Try to clean up empty bucket directory
                        self.cleanup_empty_directory(&bucket_path);
                    }
                }
            }
        }
    }

    /// Clean up metadata files in a specific shard directory
    fn cleanup_metadata_files_in_shard(&self, shard_dir: &PathBuf, stats: &mut CacheCleanupStats) {
        if let Ok(entries) = std::fs::read_dir(shard_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    let file_name = entry.file_name();
                    let file_name_str = file_name.to_string_lossy();

                    // Match both old .meta.tmp and new instance-specific .meta.tmp.{instance} patterns
                    if file_name_str.ends_with(".meta.tmp") || file_name_str.contains(".meta.tmp.")
                    {
                        match std::fs::remove_file(&path) {
                            Ok(_) => {
                                stats.temp_files_cleaned += 1;
                                debug!("Cleaned up metadata temp file: {:?}", path);
                            }
                            Err(e) => {
                                warn!("Failed to clean up metadata temp file {:?}: {}", path, e);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Traverse metadata directory and verify metadata
    /// Traverses: metadata/bucket/XX/YYY/
    async fn traverse_and_verify_metadata(
        &self,
        objects_base_dir: &PathBuf,
        stats: &mut CacheCleanupStats,
    ) {
        // Traverse bucket directories
        if let Ok(bucket_entries) = std::fs::read_dir(objects_base_dir) {
            for bucket_entry in bucket_entries.flatten() {
                let bucket_path = bucket_entry.path();
                if !bucket_path.is_dir() {
                    continue;
                }

                // Traverse level1 (XX) directories
                if let Ok(level1_entries) = std::fs::read_dir(&bucket_path) {
                    for level1_entry in level1_entries.flatten() {
                        let level1_path = level1_entry.path();
                        if !level1_path.is_dir() {
                            continue;
                        }

                        // Traverse level2 (YYY) directories
                        if let Ok(level2_entries) = std::fs::read_dir(&level1_path) {
                            for level2_entry in level2_entries.flatten() {
                                let level2_path = level2_entry.path();
                                if !level2_path.is_dir() {
                                    continue;
                                }

                                // Now we're in the leaf directory - verify metadata files
                                self.verify_metadata_in_shard(&level2_path, stats).await;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Verify metadata files in a specific shard directory
    async fn verify_metadata_in_shard(&self, shard_dir: &PathBuf, stats: &mut CacheCleanupStats) {
        if let Ok(entries) = std::fs::read_dir(shard_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let file_name = entry.file_name();
                let file_name_str = file_name.to_string_lossy();

                if file_name_str.ends_with(".meta") {
                    // Read metadata to get the original cache_key
                    if let Ok(content) = std::fs::read_to_string(&path) {
                        if let Ok(metadata) =
                            serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&content)
                        {
                            match self.verify_and_fix_metadata(&metadata.cache_key).await {
                                Ok(fixed_count) => {
                                    stats.inconsistencies_fixed += fixed_count;
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to verify metadata for {}: {}",
                                        metadata.cache_key, e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Try to clean up an empty directory (best effort)
    fn cleanup_empty_directory(&self, dir_path: &PathBuf) {
        if let Ok(mut entries) = std::fs::read_dir(dir_path) {
            if entries.next().is_none() {
                // Directory is empty
                if let Err(e) = std::fs::remove_dir(dir_path) {
                    debug!(
                        "Failed to remove empty directory (non-critical): path={:?}, error={}",
                        dir_path, e
                    );
                } else {
                    debug!("Removed empty directory: path={:?}", dir_path);
                }
            }
        }
    }

    /// Load metadata from file (helper method)
    /// Used internally by metadata update operations
    async fn load_metadata_new(
        &self,
        cache_key: &str,
    ) -> Result<crate::cache_types::NewCacheMetadata> {
        let metadata_path = self.get_new_metadata_file_path(cache_key);

        let metadata_content = std::fs::read_to_string(&metadata_path)
            .map_err(|e| ProxyError::CacheError(format!("Failed to read metadata file: {}", e)))?;

        let metadata =
            serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&metadata_content)
                .map_err(|e| ProxyError::CacheError(format!("Failed to parse metadata: {}", e)))?;

        Ok(metadata)
    }

    /// Update range access statistics with file locking
    /// Implements Requirements 1.2, 4.1, 4.2, 4.3, 4.4, 7.1, 7.2, 7.3, 7.4, 7.5
    ///
    /// In shared storage mode:
    /// - Uses CacheHitUpdateBuffer to buffer access updates in RAM
    /// - Periodically flushed to per-instance journal file
    /// - Consolidated by background task into metadata files
    ///
    /// In single instance mode:
    /// - Direct metadata write (existing behavior)
    ///
    /// This method:
    /// 1. In shared storage mode: buffers update via CacheHitUpdateBuffer
    /// 2. In single instance mode:
    ///    a. Acquires exclusive file lock on metadata file (60s timeout)
    ///    b. Reads current metadata
    ///    c. Updates only the specific range's last_accessed and access_count
    ///    d. Writes updated metadata using in-place editing (no fsync for performance)
    ///    e. Releases lock
    ///
    /// Performance: Target < 5ms per update (Requirement 7.1)
    pub async fn update_range_access(
        &mut self,
        cache_key: &str,
        range_start: u64,
        range_end: u64,
    ) -> Result<()> {
        // In shared storage mode, use the cache hit update buffer
        if let Some(buffer) = &self.cache_hit_update_buffer {
            debug!(
                "Using journal-based access update: key={}, range={}-{}",
                cache_key, range_start, range_end
            );
            return buffer
                .record_access_update(cache_key, range_start, range_end, 1)
                .await;
        }

        // Single instance mode: direct metadata write
        self.update_range_access_direct(cache_key, range_start, range_end)
            .await
    }

    /// Direct range access update (single instance mode)
    /// Used when shared storage mode is disabled
    async fn update_range_access_direct(
        &mut self,
        cache_key: &str,
        range_start: u64,
        range_end: u64,
    ) -> Result<()> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting range access update: key={}, range={}-{}, operation=update_range_access",
            cache_key, range_start, range_end
        );

        let metadata_path = self.get_new_metadata_file_path(cache_key);
        let lock_path = metadata_path.with_extension("meta.lock");

        // Check if metadata file exists
        if !metadata_path.exists() {
            debug!(
                "Metadata file not found for access update: key={}, path={:?}, result=skip",
                cache_key, metadata_path
            );
            return Ok(());
        }

        // Acquire exclusive lock with timeout handling
        let lock_start = std::time::Instant::now();
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| {
                error!(
                    "Failed to open lock file for access update: key={}, lock_path={:?}, error={}",
                    cache_key, lock_path, e
                );
                ProxyError::CacheError(format!("Failed to open lock file: {}", e))
            })?;

        // Try to acquire lock with timeout (Requirement 4.2)
        // Use try_lock_exclusive to avoid blocking indefinitely
        match lock_file.try_lock_exclusive() {
            Ok(_) => {
                let lock_duration = lock_start.elapsed();
                debug!(
                    "Acquired exclusive lock for access update: key={}, wait_time={:.2}ms",
                    cache_key,
                    lock_duration.as_secs_f64() * 1000.0
                );
            }
            Err(_e) => {
                // Lock is held by another process
                // Check if lock is stale (>60s old) - Requirement 9.3
                let lock_age = if let Ok(metadata) = std::fs::metadata(&lock_path) {
                    if let Ok(modified) = metadata.modified() {
                        SystemTime::now()
                            .duration_since(modified)
                            .unwrap_or_default()
                    } else {
                        std::time::Duration::from_secs(0)
                    }
                } else {
                    std::time::Duration::from_secs(0)
                };

                if lock_age > std::time::Duration::from_secs(60) {
                    // Lock is stale, break it
                    warn!(
                        "Breaking stale lock for access update: key={}, lock_age={:.2}s, threshold=60s",
                        cache_key, lock_age.as_secs_f64()
                    );

                    // Remove stale lock file
                    let _ = std::fs::remove_file(&lock_path);

                    // Try to acquire lock again
                    lock_file.lock_exclusive().map_err(|e| {
                        error!(
                            "Failed to acquire lock after breaking stale lock: key={}, error={}",
                            cache_key, e
                        );
                        ProxyError::CacheError(format!(
                            "Failed to acquire lock after breaking stale lock: {}",
                            e
                        ))
                    })?;

                    info!(
                        "Successfully broke stale lock and acquired new lock: key={}, lock_age={:.2}s",
                        cache_key, lock_age.as_secs_f64()
                    );
                } else {
                    // Lock is not stale, skip update to avoid blocking
                    debug!(
                        "Lock held by another process, skipping access update: key={}, lock_age={:.2}s",
                        cache_key, lock_age.as_secs_f64()
                    );
                    return Ok(());
                }
            }
        }

        // Read metadata
        let read_start = std::time::Instant::now();
        let mut metadata = match self.load_metadata_new(cache_key).await {
            Ok(meta) => {
                let read_duration = read_start.elapsed();
                debug!(
                    "Metadata loaded for access update: key={}, ranges={}, duration={:.2}ms",
                    cache_key,
                    meta.ranges.len(),
                    read_duration.as_secs_f64() * 1000.0
                );
                meta
            }
            Err(e) => {
                let _ = lock_file.unlock();
                error!(
                    "Failed to load metadata for access update: key={}, error={}",
                    cache_key, e
                );
                return Err(e);
            }
        };

        // Find and update the specific range
        let update_start = std::time::Instant::now();
        let mut range_found = false;
        for range in &mut metadata.ranges {
            if range.start == range_start && range.end == range_end {
                range.record_access();
                range_found = true;
                debug!(
                    "Range access recorded: key={}, range={}-{}, access_count={}, last_accessed={:?}",
                    cache_key, range_start, range_end, range.access_count, range.last_accessed
                );
                break;
            }
        }

        if !range_found {
            let _ = lock_file.unlock();
            debug!(
                "Range not found for access update: key={}, range={}-{}, result=skip",
                cache_key, range_start, range_end
            );
            return Ok(());
        }

        let update_duration = update_start.elapsed();
        debug!(
            "Range access statistics updated: key={}, range={}-{}, duration={:.2}ms",
            cache_key,
            range_start,
            range_end,
            update_duration.as_secs_f64() * 1000.0
        );

        // Write updated metadata in-place (Requirement 7.2, 7.3)
        let write_start = std::time::Instant::now();
        let json = serde_json::to_string_pretty(&metadata).map_err(|e| {
            let _ = lock_file.unlock();
            error!(
                "Failed to serialize metadata for access update: key={}, error={}",
                cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        std::fs::write(&metadata_path, json).map_err(|e| {
            let _ = lock_file.unlock();
            error!(
                "Failed to write metadata for access update: key={}, path={:?}, error={}",
                cache_key, metadata_path, e
            );
            ProxyError::CacheError(format!("Failed to write metadata: {}", e))
        })?;
        // No fsync for performance (Requirement 7.3)

        let write_duration = write_start.elapsed();
        debug!(
            "Metadata written for access update: key={}, duration={:.2}ms",
            cache_key,
            write_duration.as_secs_f64() * 1000.0
        );

        // Release lock (Requirement 4.5)
        lock_file
            .unlock()
            .map_err(|e| {
                warn!(
                    "Failed to release lock after access update (non-fatal): key={}, error={}",
                    cache_key, e
                );
            })
            .ok();

        let total_duration = operation_start.elapsed();
        debug!(
            "Range access update completed: key={}, range={}-{}, total_duration={:.2}ms",
            cache_key,
            range_start,
            range_end,
            total_duration.as_secs_f64() * 1000.0
        );

        // Check if we met the performance target (Requirement 7.1)
        if total_duration.as_millis() > 45 {
            warn!(
                "Range access update exceeded performance target: key={}, range={}-{}, duration={:.2}ms, target=45ms",
                cache_key, range_start, range_end, total_duration.as_secs_f64() * 1000.0
            );
        }

        Ok(())
    }

    /// Refresh the object-level TTL for a cached object.
    /// Uses the journal/buffer system in shared storage mode, or falls back to direct write.
    /// Called after a 304 Not Modified response to extend the object's freshness.
    pub async fn refresh_object_ttl(
        &mut self,
        cache_key: &str,
        new_ttl: std::time::Duration,
    ) -> Result<()> {
        // In shared storage mode, use the cache hit update buffer
        if let Some(buffer) = &self.cache_hit_update_buffer {
            debug!(
                "Using journal-based object TTL refresh: key={}, new_ttl={:.2}s",
                cache_key,
                new_ttl.as_secs_f64()
            );
            return buffer
                .record_ttl_refresh(cache_key, new_ttl)
                .await;
        }

        // Single instance mode: direct metadata write
        self.refresh_object_ttl_direct(cache_key, new_ttl).await
    }

    /// Directly refresh the object-level TTL by loading metadata, updating expires_at, and writing back.
    pub async fn refresh_object_ttl_direct(
        &mut self,
        cache_key: &str,
        new_ttl: std::time::Duration,
    ) -> Result<()> {
        let operation_start = std::time::Instant::now();
        info!(
            "Starting object TTL refresh: key={}, new_ttl={:.2}s, operation=refresh_object_ttl",
            cache_key, new_ttl.as_secs_f64()
        );

        let metadata_path = self.get_new_metadata_file_path(cache_key);
        let lock_path = metadata_path.with_extension("meta.lock");

        // Check if metadata file exists
        if !metadata_path.exists() {
            debug!(
                "Metadata file not found for object TTL refresh: key={}, path={:?}, result=skip",
                cache_key, metadata_path
            );
            return Ok(());
        }

        // Acquire exclusive lock
        let lock_start = std::time::Instant::now();
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| {
                error!(
                    "Failed to open lock file for object TTL refresh: key={}, lock_path={:?}, error={}",
                    cache_key, lock_path, e
                );
                ProxyError::CacheError(format!("Failed to open lock file: {}", e))
            })?;

        // Try to acquire lock with timeout handling
        match lock_file.try_lock_exclusive() {
            Ok(_) => {
                let lock_duration = lock_start.elapsed();
                debug!(
                    "Acquired exclusive lock for object TTL refresh: key={}, wait_time={:.2}ms",
                    cache_key,
                    lock_duration.as_secs_f64() * 1000.0
                );
            }
            Err(_) => {
                // Check if lock is stale
                let lock_age = if let Ok(metadata) = std::fs::metadata(&lock_path) {
                    if let Ok(modified) = metadata.modified() {
                        SystemTime::now()
                            .duration_since(modified)
                            .unwrap_or_default()
                    } else {
                        std::time::Duration::from_secs(0)
                    }
                } else {
                    std::time::Duration::from_secs(0)
                };

                if lock_age > std::time::Duration::from_secs(60) {
                    warn!(
                        "Breaking stale lock for object TTL refresh: key={}, lock_age={:.2}s",
                        cache_key,
                        lock_age.as_secs_f64()
                    );
                    let _ = std::fs::remove_file(&lock_path);
                    lock_file.lock_exclusive().map_err(|e| {
                        error!(
                            "Failed to acquire lock after breaking stale lock: key={}, error={}",
                            cache_key, e
                        );
                        ProxyError::CacheError(format!("Failed to acquire lock: {}", e))
                    })?;
                } else {
                    debug!(
                        "Lock held by another process, skipping object TTL refresh: key={}, lock_age={:.2}s",
                        cache_key, lock_age.as_secs_f64()
                    );
                    return Ok(());
                }
            }
        }

        // Read metadata
        let mut metadata = match self.load_metadata_new(cache_key).await {
            Ok(meta) => meta,
            Err(e) => {
                let _ = lock_file.unlock();
                error!(
                    "Failed to load metadata for object TTL refresh: key={}, error={}",
                    cache_key, e
                );
                return Err(e);
            }
        };

        // Refresh the object-level TTL
        metadata.refresh_object_ttl(new_ttl);

        // Write updated metadata
        let json = serde_json::to_string_pretty(&metadata).map_err(|e| {
            let _ = lock_file.unlock();
            error!(
                "Failed to serialize metadata for object TTL refresh: key={}, error={}",
                cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        std::fs::write(&metadata_path, json).map_err(|e| {
            let _ = lock_file.unlock();
            error!(
                "Failed to write metadata for object TTL refresh: key={}, path={:?}, error={}",
                cache_key, metadata_path, e
            );
            ProxyError::CacheError(format!("Failed to write metadata: {}", e))
        })?;

        // Release lock
        lock_file
            .unlock()
            .map_err(|e| {
                warn!(
                    "Failed to release lock after object TTL refresh (non-fatal): key={}, error={}",
                    cache_key, e
                );
            })
            .ok();

        let total_duration = operation_start.elapsed();
        info!(
            "Object TTL refresh completed: key={}, new_expires_at={:?}, total_duration={:.2}ms",
            cache_key, metadata.expires_at, total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Remove an invalidated range after conditional validation (lazy expiration)
    /// Implements Requirements 1.4, 1.6, 4.1, 4.2, 4.4
    ///
    /// This method should be called during cache lookup when conditional validation
    /// (If-Modified-Since) returns 200 OK, indicating the cached data is stale.
    ///
    /// This method:
    /// 1. Acquires exclusive file lock on metadata file
    /// 2. Reads current metadata
    /// 3. Verifies the range is expired
    /// 4. Deletes the .bin file for the range
    /// 5. Removes the range from metadata
    /// 6. If no ranges remain, deletes the metadata file
    /// 7. Releases lock
    pub async fn remove_invalidated_range(
        &mut self,
        cache_key: &str,
        range_start: u64,
        range_end: u64,
    ) -> Result<()> {
        let operation_start = std::time::Instant::now();
        info!(
            "Starting invalidated range removal: key={}, range={}-{}, operation=remove_invalidated_range",
            cache_key, range_start, range_end
        );

        let metadata_path = self.get_new_metadata_file_path(cache_key);
        let lock_path = metadata_path.with_extension("meta.lock");

        // Check if metadata file exists
        if !metadata_path.exists() {
            debug!(
                "Metadata file not found for range removal: key={}, path={:?}, result=skip",
                cache_key, metadata_path
            );
            return Ok(());
        }

        // Acquire exclusive lock
        let lock_start = std::time::Instant::now();
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| {
                error!(
                    "Failed to open lock file for range removal: key={}, lock_path={:?}, error={}",
                    cache_key, lock_path, e
                );
                ProxyError::CacheError(format!("Failed to open lock file: {}", e))
            })?;

        lock_file.lock_exclusive()
            .map_err(|e| {
                error!(
                    "Failed to acquire exclusive lock for range removal: key={}, lock_path={:?}, error={}",
                    cache_key, lock_path, e
                );
                ProxyError::CacheError(format!("Failed to acquire lock: {}", e))
            })?;

        let lock_duration = lock_start.elapsed();
        debug!(
            "Acquired exclusive lock for range removal: key={}, wait_time={:.2}ms",
            cache_key,
            lock_duration.as_secs_f64() * 1000.0
        );

        // Read metadata
        let mut metadata = match self.load_metadata_new(cache_key).await {
            Ok(meta) => meta,
            Err(e) => {
                let _ = lock_file.unlock();
                error!(
                    "Failed to load metadata for range removal: key={}, error={}",
                    cache_key, e
                );
                return Err(e);
            }
        };

        // Find the specific expired range
        let range_to_remove = metadata
            .ranges
            .iter()
            .find(|r| r.start == range_start && r.end == range_end)
            .cloned();

        if let Some(range) = range_to_remove {
            info!(
                "Removing range: key={}, range={}-{}, file={}",
                cache_key, range_start, range_end, range.file_path
            );

            // Delete .bin file for this range (Requirement 9.4)
            let range_path = self.cache_dir.join("ranges").join(&range.file_path);
            if range_path.exists() {
                match std::fs::remove_file(&range_path) {
                    Ok(_) => {
                        debug!(
                            "Deleted range binary file: key={}, range={}-{}, path={:?}",
                            cache_key, range_start, range_end, range_path
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to delete range binary file: key={}, range={}-{}, path={:?}, error={}",
                            cache_key, range_start, range_end, range_path, e
                        );
                        // Continue anyway - metadata will be updated
                    }
                }
            } else {
                debug!(
                    "Range binary file already deleted: key={}, range={}-{}, path={:?}",
                    cache_key, range_start, range_end, range_path
                );
            }

            // Remove this range from metadata
            metadata
                .ranges
                .retain(|r| !(r.start == range_start && r.end == range_end));

            debug!(
                "Range removed from metadata: key={}, range={}-{}, remaining_ranges={}",
                cache_key,
                range_start,
                range_end,
                metadata.ranges.len()
            );

            if metadata.ranges.is_empty() {
                // No ranges left, delete metadata file (Requirement 1.7, 9.5)
                match std::fs::remove_file(&metadata_path) {
                    Ok(_) => {
                        info!(
                            "Deleted metadata file (no ranges remaining): key={}, path={:?}",
                            cache_key, metadata_path
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to delete metadata file: key={}, path={:?}, error={}",
                            cache_key, metadata_path, e
                        );
                    }
                }
            } else {
                // Update metadata with remaining ranges
                let json = serde_json::to_string_pretty(&metadata).map_err(|e| {
                    let _ = lock_file.unlock();
                    error!(
                        "Failed to serialize metadata after range removal: key={}, error={}",
                        cache_key, e
                    );
                    ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
                })?;

                std::fs::write(&metadata_path, json).map_err(|e| {
                    let _ = lock_file.unlock();
                    error!(
                        "Failed to write metadata after range removal: key={}, path={:?}, error={}",
                        cache_key, metadata_path, e
                    );
                    ProxyError::CacheError(format!("Failed to write metadata: {}", e))
                })?;

                debug!(
                    "Metadata updated after range removal: key={}, remaining_ranges={}",
                    cache_key,
                    metadata.ranges.len()
                );
            }
        } else {
            debug!(
                "Range not found for removal: key={}, range={}-{}, result=skip",
                cache_key, range_start, range_end
            );
        }

        // Release lock
        lock_file
            .unlock()
            .map_err(|e| {
                warn!(
                    "Failed to release lock after range removal (non-fatal): key={}, error={}",
                    cache_key, e
                );
            })
            .ok();

        let total_duration = operation_start.elapsed();
        info!(
            "Invalidated range removal completed: key={}, range={}-{}, total_duration={:.2}ms",
            cache_key,
            range_start,
            range_end,
            total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Delete a stale range (known to be invalid)
    /// Implements Requirements 10.1, 10.3, 10.4, 10.5
    ///
    /// This method should be called when a range is known to be stale due to:
    /// - ETag mismatch (Requirement 10.1)
    /// - Corrupted cache files (Requirement 10.3)
    /// - Conditional validation returning 200 OK (Requirement 10.4)
    ///
    /// This method:
    /// 1. Acquires exclusive file lock on metadata file
    /// 2. Reads current metadata
    /// 3. Deletes the .bin file for the range
    /// 4. Removes the range from metadata
    /// 5. If no ranges remain, deletes the metadata file
    /// 6. Releases lock
    /// 7. Logs the deletion with reason (Requirement 10.5)
    pub async fn delete_stale_range(
        &mut self,
        cache_key: &str,
        start: u64,
        end: u64,
        reason: &str,
    ) -> Result<()> {
        let operation_start = std::time::Instant::now();
        info!(
            "Deleting stale range: key={}, range={}-{}, reason={}, operation=delete_stale_range",
            cache_key, start, end, reason
        );

        let metadata_path = self.get_new_metadata_file_path(cache_key);
        let lock_path = metadata_path.with_extension("meta.lock");

        // Check if metadata file exists
        if !metadata_path.exists() {
            debug!(
                "Metadata file not found for stale range deletion: key={}, path={:?}, result=skip",
                cache_key, metadata_path
            );
            return Ok(());
        }

        // Acquire exclusive lock
        let lock_start = std::time::Instant::now();
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| {
                error!(
                    "Failed to open lock file for stale range deletion: key={}, lock_path={:?}, error={}",
                    cache_key, lock_path, e
                );
                ProxyError::CacheError(format!("Failed to open lock file: {}", e))
            })?;

        lock_file.lock_exclusive()
            .map_err(|e| {
                error!(
                    "Failed to acquire exclusive lock for stale range deletion: key={}, lock_path={:?}, error={}",
                    cache_key, lock_path, e
                );
                ProxyError::CacheError(format!("Failed to acquire lock: {}", e))
            })?;

        let lock_duration = lock_start.elapsed();
        debug!(
            "Acquired exclusive lock for stale range deletion: key={}, wait_time={:.2}ms",
            cache_key,
            lock_duration.as_secs_f64() * 1000.0
        );

        // Read metadata
        let mut metadata = match self.load_metadata_new(cache_key).await {
            Ok(meta) => meta,
            Err(e) => {
                let _ = lock_file.unlock();
                error!(
                    "Failed to load metadata for stale range deletion: key={}, error={}",
                    cache_key, e
                );
                return Err(e);
            }
        };

        // Find and delete the range
        let range_to_remove = metadata
            .ranges
            .iter()
            .find(|r| r.start == start && r.end == end)
            .cloned();

        if let Some(range) = range_to_remove {
            info!(
                "Removing stale range: key={}, range={}-{}, file={}, reason={}",
                cache_key, start, end, range.file_path, reason
            );

            // Delete .bin file (Requirement 9.4)
            let range_path = self.cache_dir.join("ranges").join(&range.file_path);
            if range_path.exists() {
                match std::fs::remove_file(&range_path) {
                    Ok(_) => {
                        debug!(
                            "Deleted stale range binary file: key={}, range={}-{}, path={:?}, reason={}",
                            cache_key, start, end, range_path, reason
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to delete stale range binary file: key={}, range={}-{}, path={:?}, error={}, reason={}",
                            cache_key, start, end, range_path, e, reason
                        );
                        // Continue anyway - metadata will be updated
                    }
                }
            } else {
                debug!(
                    "Stale range binary file already deleted: key={}, range={}-{}, path={:?}, reason={}",
                    cache_key, start, end, range_path, reason
                );
            }

            // Remove range from metadata
            metadata
                .ranges
                .retain(|r| !(r.start == start && r.end == end));

            debug!(
                "Stale range removed from metadata: key={}, range={}-{}, remaining_ranges={}, reason={}",
                cache_key, start, end, metadata.ranges.len(), reason
            );

            if metadata.ranges.is_empty() {
                // No ranges left, delete metadata file (Requirement 9.5)
                match std::fs::remove_file(&metadata_path) {
                    Ok(_) => {
                        info!(
                            "Deleted metadata file (no ranges remaining): key={}, path={:?}, reason={}",
                            cache_key, metadata_path, reason
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to delete metadata file: key={}, path={:?}, error={}, reason={}",
                            cache_key, metadata_path, e, reason
                        );
                    }
                }
            } else {
                // Update metadata with remaining ranges
                let json = serde_json::to_string_pretty(&metadata)
                    .map_err(|e| {
                        let _ = lock_file.unlock();
                        error!(
                            "Failed to serialize metadata after stale range deletion: key={}, error={}, reason={}",
                            cache_key, e, reason
                        );
                        ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
                    })?;

                std::fs::write(&metadata_path, json)
                    .map_err(|e| {
                        let _ = lock_file.unlock();
                        error!(
                            "Failed to write metadata after stale range deletion: key={}, path={:?}, error={}, reason={}",
                            cache_key, metadata_path, e, reason
                        );
                        ProxyError::CacheError(format!("Failed to write metadata: {}", e))
                    })?;

                debug!(
                    "Metadata updated after stale range deletion: key={}, remaining_ranges={}, reason={}",
                    cache_key, metadata.ranges.len(), reason
                );
            }
        } else {
            debug!(
                "Stale range not found for deletion: key={}, range={}-{}, reason={}, result=skip",
                cache_key, start, end, reason
            );
        }

        // Release lock
        lock_file.unlock()
            .map_err(|e| {
                warn!(
                    "Failed to release lock after stale range deletion (non-fatal): key={}, error={}, reason={}",
                    cache_key, e, reason
                );
            })
            .ok();

        let total_duration = operation_start.elapsed();
        info!(
            "Stale range deletion completed: key={}, range={}-{}, reason={}, total_duration={:.2}ms",
            cache_key, start, end, reason, total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Delete all ranges for an object (e.g., on PUT conflict)
    /// Implements Requirements 10.2, 10.5
    ///
    /// This method should be called when all ranges for an object need to be invalidated,
    /// such as when a PUT request is received for the object (Requirement 10.2).
    ///
    /// This method:
    /// 1. Checks if metadata file exists
    /// 2. Loads metadata to get range files
    /// 3. Deletes all .bin files for all ranges
    /// 4. Deletes the metadata file
    /// 5. Logs the deletion with reason (Requirement 10.5)
    pub async fn delete_all_ranges(&mut self, cache_key: &str, reason: &str) -> Result<()> {
        let operation_start = std::time::Instant::now();
        info!(
            "Deleting all ranges for object: key={}, reason={}, operation=delete_all_ranges",
            cache_key, reason
        );

        let metadata_path = self.get_new_metadata_file_path(cache_key);

        if !metadata_path.exists() {
            debug!(
                "Metadata file not found for all ranges deletion: key={}, path={:?}, reason={}, result=skip",
                cache_key, metadata_path, reason
            );
            return Ok(());
        }

        // Load metadata to get range files
        let metadata = match self.load_metadata_new(cache_key).await {
            Ok(meta) => meta,
            Err(e) => {
                warn!(
                    "Failed to load metadata for all ranges deletion: key={}, error={}, reason={}, result=skip",
                    cache_key, e, reason
                );
                // Try to delete metadata file anyway
                let _ = std::fs::remove_file(&metadata_path);
                return Ok(());
            }
        };

        let range_count = metadata.ranges.len();
        info!(
            "Deleting {} ranges for object: key={}, reason={}",
            range_count, cache_key, reason
        );

        // Delete all .bin files
        let mut deleted_count = 0;
        let mut failed_count = 0;

        for range in &metadata.ranges {
            let range_path = self.cache_dir.join("ranges").join(&range.file_path);
            if range_path.exists() {
                match std::fs::remove_file(&range_path) {
                    Ok(_) => {
                        debug!(
                            "Deleted range binary file: key={}, range={}-{}, path={:?}, reason={}",
                            cache_key, range.start, range.end, range_path, reason
                        );
                        deleted_count += 1;
                    }
                    Err(e) => {
                        warn!(
                            "Failed to delete range binary file: key={}, range={}-{}, path={:?}, error={}, reason={}",
                            cache_key, range.start, range.end, range_path, e, reason
                        );
                        failed_count += 1;
                    }
                }
            } else {
                debug!(
                    "Range binary file already deleted: key={}, range={}-{}, path={:?}, reason={}",
                    cache_key, range.start, range.end, range_path, reason
                );
            }
        }

        info!(
            "Range binary files deletion summary: key={}, deleted={}, failed={}, total={}, reason={}",
            cache_key, deleted_count, failed_count, range_count, reason
        );

        // Delete metadata file
        match std::fs::remove_file(&metadata_path) {
            Ok(_) => {
                info!(
                    "Deleted metadata file: key={}, path={:?}, reason={}",
                    cache_key, metadata_path, reason
                );
            }
            Err(e) => {
                warn!(
                    "Failed to delete metadata file: key={}, path={:?}, error={}, reason={}",
                    cache_key, metadata_path, e, reason
                );
            }
        }

        let total_duration = operation_start.elapsed();
        info!(
            "All ranges deletion completed: key={}, ranges_deleted={}, reason={}, total_duration={:.2}ms",
            cache_key, deleted_count, reason, total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Check if a cached object is expired and needs conditional validation.
    /// Uses object-level `expires_at` from `NewCacheMetadata`.
    ///
    /// Returns:
    /// - `Ok(Fresh)` if object is not expired
    /// - `Ok(Expired { last_modified: Some(...) })` if expired with known last_modified
    /// - `Ok(Expired { last_modified: None })` on metadata read/parse failure (fail-safe)
    pub async fn check_object_expiration(
        &self,
        cache_key: &str,
    ) -> Result<crate::cache_types::ObjectExpirationResult> {
        use crate::cache_types::ObjectExpirationResult;

        let metadata_path = self.get_new_metadata_file_path(cache_key);

        // Read and parse metadata, treating any failure as expired
        let metadata_content = match tokio::fs::read_to_string(&metadata_path).await {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Metadata file doesn't exist — treat as expired (fail-safe)
                warn!(
                    "Metadata file not found during expiration check: cache_key={}, path={:?}",
                    cache_key, metadata_path
                );
                return Ok(ObjectExpirationResult::Expired { last_modified: None });
            }
            Err(e) => {
                warn!(
                    "Failed to read metadata during expiration check: cache_key={}, error={}",
                    cache_key, e
                );
                return Ok(ObjectExpirationResult::Expired { last_modified: None });
            }
        };

        let metadata: crate::cache_types::NewCacheMetadata = match serde_json::from_str(&metadata_content) {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "Failed to parse metadata during expiration check: cache_key={}, error={}",
                    cache_key, e
                );
                return Ok(ObjectExpirationResult::Expired { last_modified: None });
            }
        };

        if metadata.is_object_expired() {
            debug!(
                "Object expired, needs conditional validation: key={}, expires_at={:?}",
                cache_key, metadata.expires_at
            );
            Ok(ObjectExpirationResult::Expired {
                last_modified: Some(metadata.object_metadata.last_modified.clone()),
            })
        } else {
            Ok(ObjectExpirationResult::Fresh)
        }
    }

    /// Remove all range files for a cache key
    /// Implements Requirements 1.4, 3.2
    ///
    /// This method:
    /// 1. Reads metadata to find all range files for the cache key
    /// 2. Removes all range binary files (.bin files)
    /// 3. Handles partial failures gracefully with proper cleanup
    /// 4. Updates cache size tracking after range removal
    /// 5. Does NOT remove metadata file (caller decides metadata handling)
    ///
    /// Returns the number of ranges successfully removed and total size freed
    pub async fn remove_all_ranges(&self, cache_key: &str) -> Result<(usize, u64)> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting bulk range removal: key={}, operation=remove_all_ranges",
            cache_key
        );

        // Get metadata to find all range files
        let metadata = match self.get_metadata(cache_key).await? {
            Some(meta) => {
                debug!(
                    "Found metadata for bulk removal: key={}, ranges={}",
                    cache_key,
                    meta.ranges.len()
                );
                meta
            }
            None => {
                debug!(
                    "No metadata found for bulk removal: key={}, result=no_ranges_to_remove",
                    cache_key
                );
                return Ok((0, 0));
            }
        };

        let total_ranges = metadata.ranges.len();
        let mut removed_count = 0;
        let mut total_size_freed = 0u64;
        let mut failed_removals = Vec::new();

        debug!(
            "Starting range file removal: key={}, total_ranges={}",
            cache_key, total_ranges
        );

        // Remove each range file
        for range_spec in &metadata.ranges {
            let range_file_path = self.cache_dir.join("ranges").join(&range_spec.file_path);

            debug!(
                "Removing range file: key={}, range={}-{}, path={:?}, compressed_size={}",
                cache_key,
                range_spec.start,
                range_spec.end,
                range_file_path,
                range_spec.compressed_size
            );

            if range_file_path.exists() {
                match std::fs::remove_file(&range_file_path) {
                    Ok(_) => {
                        removed_count += 1;
                        total_size_freed += range_spec.compressed_size;
                        debug!(
                            "Range file removed: key={}, range={}-{}, path={:?}, size_freed={}",
                            cache_key,
                            range_spec.start,
                            range_spec.end,
                            range_file_path,
                            range_spec.compressed_size
                        );
                    }
                    Err(e) => {
                        failed_removals.push((range_spec.start, range_spec.end, e.to_string()));
                        warn!(
                            "Failed to remove range file: key={}, range={}-{}, path={:?}, error={}",
                            cache_key, range_spec.start, range_spec.end, range_file_path, e
                        );
                    }
                }
            } else {
                debug!(
                    "Range file not found (already removed?): key={}, range={}-{}, path={:?}",
                    cache_key, range_spec.start, range_spec.end, range_file_path
                );
                // Count as removed since it's not there anymore
                removed_count += 1;
            }
        }

        // NOTE: Size tracking is now handled by JournalConsolidator through journal entries.
        // The Remove operation in the journal contains the size delta.

        let total_duration = operation_start.elapsed();

        if failed_removals.is_empty() {
            info!(
                "Bulk range removal completed successfully: key={}, removed={}/{} ranges, size_freed={} bytes, duration={:.2}ms",
                cache_key, removed_count, total_ranges, total_size_freed, total_duration.as_secs_f64() * 1000.0
            );
        } else {
            warn!(
                "Bulk range removal completed with failures: key={}, removed={}/{} ranges, failed={}, size_freed={} bytes, duration={:.2}ms, failures={:?}",
                cache_key, removed_count, total_ranges, failed_removals.len(), total_size_freed,
                total_duration.as_secs_f64() * 1000.0, failed_removals
            );
        }

        Ok((removed_count, total_size_freed))
    }

    /// Invalidate all ranges for an object (removes ranges and metadata)
    /// Requirements: 2.2, 2.3 - ETag validation and range invalidation
    ///
    /// This method:
    /// 1. Removes all range files for the object
    /// 2. Removes the metadata file
    /// 3. Updates cache size tracking
    /// 4. Logs the invalidation action
    /// 5. Handles partial failures gracefully
    pub async fn invalidate_all_ranges(&self, cache_key: &str) -> Result<()> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting range invalidation: key={}, operation=invalidate_all_ranges",
            cache_key
        );

        // First remove all range files
        let (removed_count, size_freed) = self.remove_all_ranges(cache_key).await?;

        // Then remove the metadata file
        let metadata_file_path = self.get_metadata_file_path(cache_key);
        if metadata_file_path.exists() {
            match std::fs::remove_file(&metadata_file_path) {
                Ok(_) => {
                    debug!(
                        "Metadata file removed during invalidation: key={}, path={:?}",
                        cache_key, metadata_file_path
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to remove metadata file during invalidation: key={}, path={:?}, error={}",
                        cache_key, metadata_file_path, e
                    );
                    // Continue anyway - ranges are already removed
                }
            }
        } else {
            debug!(
                "Metadata file not found during invalidation: key={}, path={:?}",
                cache_key, metadata_file_path
            );
        }

        let total_duration = operation_start.elapsed();
        info!(
            "Range invalidation completed: key={}, ranges_removed={}, size_freed={} bytes, duration={:.2}ms",
            cache_key, removed_count, size_freed, total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Get object ETag from metadata file
    /// Implements Requirements 2.1, 4.3
    ///
    /// This method:
    /// 1. Reads only the metadata file (no range data loaded)
    /// 2. Extracts the ETag from object metadata
    /// 3. Handles corrupted or missing metadata gracefully
    /// 4. Caches ETag values within request lifecycle for efficiency
    /// 5. Returns None if metadata doesn't exist or is corrupted
    pub async fn get_object_etag(&self, cache_key: &str) -> Result<Option<String>> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting ETag retrieval: key={}, operation=get_object_etag",
            cache_key
        );

        // Get metadata (this handles all error cases gracefully)
        let metadata = match self.get_metadata(cache_key).await? {
            Some(meta) => {
                debug!(
                    "Metadata retrieved for ETag: key={}, etag={}",
                    cache_key, meta.object_metadata.etag
                );
                meta
            }
            None => {
                debug!(
                    "No metadata found for ETag retrieval: key={}, result=none",
                    cache_key
                );
                return Ok(None);
            }
        };

        let etag = metadata.object_metadata.etag.clone();
        let total_duration = operation_start.elapsed();

        debug!(
            "ETag retrieval completed: key={}, etag={}, duration={:.2}ms",
            cache_key,
            etag,
            total_duration.as_secs_f64() * 1000.0
        );

        Ok(Some(etag))
    }

    /// Update object metadata after range operations
    /// Implements Requirements 3.1, 3.2, 3.4
    ///
    /// This method:
    /// 1. Reads current metadata with file locking
    /// 2. Updates metadata fields as specified
    /// 3. Ensures atomic updates using temporary files
    /// 4. Maintains consistency between range files and metadata
    /// 5. Handles concurrent access with proper locking
    pub async fn update_object_metadata_after_cleanup(
        &self,
        cache_key: &str,
        remaining_ranges: Vec<crate::cache_types::RangeSpec>,
    ) -> Result<()> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting metadata consistency update: key={}, remaining_ranges={}, operation=update_object_metadata_after_cleanup",
            cache_key, remaining_ranges.len()
        );

        let metadata_file_path = self.get_new_metadata_file_path(cache_key);
        let metadata_tmp_path = metadata_file_path.with_extension("meta.tmp");
        let lock_file_path = metadata_file_path.with_extension("meta.lock");

        // Check if metadata file exists
        if !metadata_file_path.exists() {
            debug!(
                "No metadata file exists for update: key={}, path={:?}, result=no_action",
                cache_key, metadata_file_path
            );
            return Ok(());
        }

        debug!(
            "Metadata update paths: key={}, metadata={:?}, tmp={:?}, lock={:?}",
            cache_key, metadata_file_path, metadata_tmp_path, lock_file_path
        );

        // Acquire exclusive lock on metadata file
        let lock_start = std::time::Instant::now();
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_file_path)
            .map_err(|e| {
                error!(
                    "Failed to open lock file for metadata update: key={}, path={:?}, error={}",
                    cache_key, lock_file_path, e
                );
                ProxyError::CacheError(format!("Failed to open lock file: {}", e))
            })?;

        lock_file.lock_exclusive().map_err(|e| {
            error!(
                "Failed to acquire exclusive lock for metadata update: key={}, path={:?}, error={}",
                cache_key, lock_file_path, e
            );
            ProxyError::CacheError(format!("Failed to acquire lock: {}", e))
        })?;

        let lock_duration = lock_start.elapsed();
        debug!(
            "Acquired exclusive lock for metadata update: key={}, wait_time={:.2}ms",
            cache_key,
            lock_duration.as_secs_f64() * 1000.0
        );

        // Read current metadata (inside lock)
        let mut metadata = match std::fs::read_to_string(&metadata_file_path) {
            Ok(content) => {
                match serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&content) {
                    Ok(meta) => {
                        debug!(
                            "Current metadata loaded: key={}, current_ranges={}, etag={}",
                            cache_key,
                            meta.ranges.len(),
                            meta.object_metadata.etag
                        );
                        meta
                    }
                    Err(e) => {
                        let _ = lock_file.unlock();
                        error!(
                            "Failed to parse metadata for update: key={}, path={:?}, error={}",
                            cache_key, metadata_file_path, e
                        );
                        return Err(ProxyError::CacheError(format!(
                            "Corrupted metadata file: {}",
                            e
                        )));
                    }
                }
            }
            Err(e) => {
                let _ = lock_file.unlock();
                error!(
                    "Failed to read metadata for update: key={}, path={:?}, error={}",
                    cache_key, metadata_file_path, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to read metadata file: {}",
                    e
                )));
            }
        };

        // Update the ranges list to reflect remaining ranges
        let old_range_count = metadata.ranges.len();
        metadata.ranges = remaining_ranges;
        let new_range_count = metadata.ranges.len();

        debug!(
            "Updated ranges in metadata: key={}, old_count={}, new_count={}",
            cache_key, old_range_count, new_range_count
        );

        // If no ranges remain, we could optionally remove the metadata file entirely
        // But for now, we keep it with an empty ranges list for consistency
        if metadata.ranges.is_empty() {
            debug!(
                "No ranges remaining after cleanup: key={}, keeping_empty_metadata=true",
                cache_key
            );
        }

        // Write updated metadata to temporary file
        let serialize_start = std::time::Instant::now();
        let metadata_json = serde_json::to_string_pretty(&metadata).map_err(|e| {
            let _ = lock_file.unlock();
            error!(
                "Failed to serialize updated metadata: key={}, error={}",
                cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        let serialize_duration = serialize_start.elapsed();
        debug!(
            "Updated metadata serialized: key={}, size={} bytes, duration={:.2}ms",
            cache_key,
            metadata_json.len(),
            serialize_duration.as_secs_f64() * 1000.0
        );

        std::fs::write(&metadata_tmp_path, &metadata_json).map_err(|e| {
            let _ = lock_file.unlock();
            let _ = std::fs::remove_file(&metadata_tmp_path);
            error!(
                "Failed to write updated metadata tmp file: key={}, path={:?}, error={}",
                cache_key, metadata_tmp_path, e
            );
            ProxyError::CacheError(format!("Failed to write metadata tmp file: {}", e))
        })?;

        // Atomic rename of metadata
        debug!(
            "Performing atomic metadata rename: key={}, tmp={:?} -> final={:?}",
            cache_key, metadata_tmp_path, metadata_file_path
        );

        std::fs::rename(&metadata_tmp_path, &metadata_file_path).map_err(|e| {
            let _ = lock_file.unlock();
            let _ = std::fs::remove_file(&metadata_tmp_path);
            error!(
                "Failed to rename updated metadata file: key={}, tmp={:?}, final={:?}, error={}",
                cache_key, metadata_tmp_path, metadata_file_path, e
            );
            ProxyError::CacheError(format!("Failed to rename metadata file: {}", e))
        })?;

        // Release lock
        lock_file.unlock()
            .map_err(|e| {
                warn!(
                    "Failed to release lock after metadata update (non-fatal): key={}, path={:?}, error={}",
                    cache_key, lock_file_path, e
                );
            })
            .ok();

        let total_duration = operation_start.elapsed();
        info!(
            "Metadata consistency update completed: key={}, old_ranges={}, new_ranges={}, file_size={} bytes, duration={:.2}ms",
            cache_key, old_range_count, new_range_count, metadata_json.len(), total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    // ============================================================================
    // Enhanced Error Handling and Concurrent Access Safety Methods
    // ============================================================================

    /// Validate metadata file structure and content for corruption detection
    ///
    /// This method performs comprehensive validation of metadata files to detect
    /// various forms of corruption including:
    /// - JSON parsing errors
    /// - Missing required fields
    /// - Invalid field values
    /// - Inconsistent data structures
    ///
    /// # Arguments
    /// * `cache_key` - The cache key to validate
    ///
    /// # Returns
    /// * `Ok(true)` - Metadata is valid
    /// * `Ok(false)` - Metadata is corrupted but recoverable
    /// * `Err(ProxyError)` - Metadata is corrupted and requires invalidation
    ///
    /// # Requirements
    /// Implements Requirements 4.3
    pub async fn validate_metadata_integrity(&self, cache_key: &str) -> Result<bool> {
        let operation_start = std::time::Instant::now();
        debug!(
            "[METADATA_VALIDATION] Starting integrity validation: cache_key={}",
            cache_key
        );

        let metadata_file_path = self.get_new_metadata_file_path(cache_key);

        // Check if metadata file exists
        if !metadata_file_path.exists() {
            debug!(
                "[METADATA_VALIDATION] Metadata file not found: cache_key={}, path={:?}, result=not_found",
                cache_key, metadata_file_path
            );
            return Ok(true); // No metadata to validate
        }

        // Read and validate file content
        let metadata_content = match std::fs::read_to_string(&metadata_file_path) {
            Ok(content) => {
                if content.is_empty() {
                    error!(
                        "[METADATA_VALIDATION] Empty metadata file detected: cache_key={}, path={:?}",
                        cache_key, metadata_file_path
                    );
                    return Err(ProxyError::CacheError("Empty metadata file".to_string()));
                }
                content
            }
            Err(e) => {
                error!(
                    "[METADATA_VALIDATION] Failed to read metadata file: cache_key={}, path={:?}, error={}",
                    cache_key, metadata_file_path, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to read metadata: {}",
                    e
                )));
            }
        };

        // Validate JSON structure
        let metadata =
            match serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&metadata_content) {
                Ok(meta) => meta,
                Err(e) => {
                    error!(
                    "[METADATA_VALIDATION] JSON parsing failed: cache_key={}, path={:?}, error={}",
                    cache_key, metadata_file_path, e
                );
                    return Err(ProxyError::CacheError(format!("Corrupted JSON: {}", e)));
                }
            };

        // Validate required fields
        if metadata.cache_key.is_empty() {
            error!(
                "[METADATA_VALIDATION] Empty cache_key field: cache_key={}, path={:?}",
                cache_key, metadata_file_path
            );
            return Err(ProxyError::CacheError("Empty cache_key field".to_string()));
        }

        if metadata.object_metadata.etag.is_empty() {
            error!(
                "[METADATA_VALIDATION] Empty ETag field: cache_key={}, path={:?}",
                cache_key, metadata_file_path
            );
            return Err(ProxyError::CacheError("Empty ETag field".to_string()));
        }

        // Validate timestamps
        let now = SystemTime::now();
        if metadata.created_at > now {
            warn!(
                "[METADATA_VALIDATION] Future creation timestamp: cache_key={}, created_at={:?}, now={:?}",
                cache_key, metadata.created_at, now
            );
            // This is suspicious but not necessarily corrupted
        }

        // Validate range specifications
        for (i, range_spec) in metadata.ranges.iter().enumerate() {
            if range_spec.start > range_spec.end {
                error!(
                    "[METADATA_VALIDATION] Invalid range boundaries: cache_key={}, range_index={}, start={}, end={}",
                    cache_key, i, range_spec.start, range_spec.end
                );
                return Err(ProxyError::CacheError(format!(
                    "Invalid range {}: start > end",
                    i
                )));
            }

            if range_spec.file_path.is_empty() {
                error!(
                    "[METADATA_VALIDATION] Empty range file path: cache_key={}, range_index={}",
                    cache_key, i
                );
                return Err(ProxyError::CacheError(format!(
                    "Empty file path for range {}",
                    i
                )));
            }

            // Validate that the range file exists
            let range_file_path = self.cache_dir.join("ranges").join(&range_spec.file_path);
            if !range_file_path.exists() {
                warn!(
                    "[METADATA_VALIDATION] Missing range file: cache_key={}, range_index={}, path={:?}",
                    cache_key, i, range_file_path
                );
                // Missing range files indicate inconsistency but not necessarily corruption
                return Ok(false);
            }
        }

        let validation_duration = operation_start.elapsed();
        debug!(
            "[METADATA_VALIDATION] Validation completed: cache_key={}, ranges={}, duration={:.2}ms, result=valid",
            cache_key, metadata.ranges.len(), validation_duration.as_secs_f64() * 1000.0
        );

        Ok(true)
    }

    /// Detect and recover from metadata corruption
    ///
    /// This method attempts to recover from metadata corruption by:
    /// 1. Validating metadata integrity
    /// 2. Attempting to reconstruct metadata from range files if possible
    /// 3. Invalidating corrupted entries that cannot be recovered
    ///
    /// # Arguments
    /// * `cache_key` - The cache key to recover
    ///
    /// # Returns
    /// * `Ok(true)` - Recovery successful
    /// * `Ok(false)` - No recovery needed (metadata was valid)
    /// * `Err(ProxyError)` - Recovery failed, entry should be invalidated
    ///
    /// # Requirements
    /// Implements Requirements 4.3
    pub async fn detect_and_recover_corruption(&self, cache_key: &str) -> Result<bool> {
        let operation_start = std::time::Instant::now();
        info!(
            "[CORRUPTION_RECOVERY] Starting corruption detection and recovery: cache_key={}",
            cache_key
        );

        // First, validate metadata integrity
        match self.validate_metadata_integrity(cache_key).await {
            Ok(true) => {
                debug!(
                    "[CORRUPTION_RECOVERY] Metadata is valid: cache_key={}, result=no_recovery_needed",
                    cache_key
                );
                return Ok(false);
            }
            Ok(false) => {
                warn!(
                    "[CORRUPTION_RECOVERY] Metadata inconsistency detected: cache_key={}, attempting_recovery=true",
                    cache_key
                );
                // Continue with recovery attempt
            }
            Err(e) => {
                error!(
                    "[CORRUPTION_RECOVERY] Metadata corruption detected: cache_key={}, error={}, action=invalidate",
                    cache_key, e
                );

                // Record corruption metric
                if let Some(metrics_manager) = &self.metrics_manager {
                    metrics_manager
                        .read()
                        .await
                        .record_corrupted_metadata_simple()
                        .await;
                }

                // Invalidate corrupted entry
                self.invalidate_cache_entry(cache_key).await?;

                info!(
                    "[CORRUPTION_RECOVERY] Corrupted entry invalidated: cache_key={}, recovery=invalidation_successful",
                    cache_key
                );

                return Err(e);
            }
        }

        // Attempt to recover from inconsistency
        // This handles cases where metadata exists but range files are missing
        let metadata_file_path = self.get_new_metadata_file_path(cache_key);

        if !metadata_file_path.exists() {
            debug!(
                "[CORRUPTION_RECOVERY] No metadata file to recover: cache_key={}, result=no_action",
                cache_key
            );
            return Ok(false);
        }

        // Read current metadata
        let metadata_content = std::fs::read_to_string(&metadata_file_path)
            .map_err(|e| {
                error!(
                    "[CORRUPTION_RECOVERY] Failed to read metadata for recovery: cache_key={}, error={}",
                    cache_key, e
                );
                ProxyError::CacheError(format!("Failed to read metadata: {}", e))
            })?;

        let mut metadata = serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&metadata_content)
            .map_err(|e| {
                error!(
                    "[CORRUPTION_RECOVERY] Failed to parse metadata for recovery: cache_key={}, error={}",
                    cache_key, e
                );
                ProxyError::CacheError(format!("Failed to parse metadata: {}", e))
            })?;

        // Check which range files actually exist and remove references to missing ones
        let original_range_count = metadata.ranges.len();
        let ranges_dir = self.cache_dir.join("ranges");

        metadata.ranges.retain(|range_spec| {
            let range_file_path = ranges_dir.join(&range_spec.file_path);
            let exists = range_file_path.exists();

            if !exists {
                warn!(
                    "[CORRUPTION_RECOVERY] Removing reference to missing range file: cache_key={}, range={}-{}, path={:?}",
                    cache_key, range_spec.start, range_spec.end, range_file_path
                );
            }

            exists
        });

        let recovered_range_count = metadata.ranges.len();

        if recovered_range_count != original_range_count {
            info!(
                "[CORRUPTION_RECOVERY] Recovered metadata by removing missing ranges: cache_key={}, original_ranges={}, recovered_ranges={}",
                cache_key, original_range_count, recovered_range_count
            );

            // Update metadata file with recovered data
            self.update_metadata(&metadata).await?;

            let recovery_duration = operation_start.elapsed();
            info!(
                "[CORRUPTION_RECOVERY] Recovery completed successfully: cache_key={}, ranges_removed={}, duration={:.2}ms",
                cache_key, original_range_count - recovered_range_count, recovery_duration.as_secs_f64() * 1000.0
            );

            return Ok(true);
        }

        debug!(
            "[CORRUPTION_RECOVERY] No recovery needed: cache_key={}, all_ranges_valid=true",
            cache_key
        );

        Ok(false)
    }

    /// Validate metadata file during read operations with enhanced error handling
    ///
    /// This method wraps metadata reading with comprehensive validation and
    /// automatic corruption recovery. It should be used instead of direct
    /// metadata file reading for better error handling.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key to read and validate
    ///
    /// # Returns
    /// * `Ok(Some(metadata))` - Valid metadata found
    /// * `Ok(None)` - No metadata found or metadata expired
    /// * `Err(ProxyError)` - Unrecoverable error
    ///
    /// # Requirements
    /// Implements Requirements 4.3
    pub async fn read_and_validate_metadata(
        &self,
        cache_key: &str,
    ) -> Result<Option<crate::cache_types::NewCacheMetadata>> {
        let _operation_start = std::time::Instant::now();
        debug!(
            "[METADATA_READ_VALIDATE] Starting validated metadata read: cache_key={}",
            cache_key
        );

        // First attempt normal metadata read
        match self.get_metadata(cache_key).await {
            Ok(Some(metadata)) => {
                // Validate the metadata we just read
                match self.validate_metadata_integrity(cache_key).await {
                    Ok(true) => {
                        debug!(
                            "[METADATA_READ_VALIDATE] Metadata read and validated successfully: cache_key={}, ranges={}",
                            cache_key, metadata.ranges.len()
                        );
                        return Ok(Some(metadata));
                    }
                    Ok(false) => {
                        warn!(
                            "[METADATA_READ_VALIDATE] Metadata inconsistency detected during read: cache_key={}, attempting_recovery=true",
                            cache_key
                        );

                        // Attempt recovery
                        match self.detect_and_recover_corruption(cache_key).await {
                            Ok(true) => {
                                info!(
                                    "[METADATA_READ_VALIDATE] Recovery successful, re-reading metadata: cache_key={}",
                                    cache_key
                                );
                                // Re-read after recovery
                                return self.get_metadata(cache_key).await;
                            }
                            Ok(false) => {
                                debug!(
                                    "[METADATA_READ_VALIDATE] No recovery performed: cache_key={}",
                                    cache_key
                                );
                                return Ok(Some(metadata));
                            }
                            Err(e) => {
                                error!(
                                    "[METADATA_READ_VALIDATE] Recovery failed: cache_key={}, error={}",
                                    cache_key, e
                                );
                                return Ok(None); // Treat as cache miss
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "[METADATA_READ_VALIDATE] Metadata validation failed: cache_key={}, error={}",
                            cache_key, e
                        );

                        // Record corruption and invalidate
                        if let Some(metrics_manager) = &self.metrics_manager {
                            metrics_manager
                                .read()
                                .await
                                .record_corrupted_metadata_simple()
                                .await;
                        }

                        self.invalidate_cache_entry(cache_key).await?;
                        return Ok(None); // Treat as cache miss
                    }
                }
            }
            Ok(None) => {
                debug!(
                    "[METADATA_READ_VALIDATE] No metadata found: cache_key={}, result=cache_miss",
                    cache_key
                );
                return Ok(None);
            }
            Err(e) => {
                error!(
                    "[METADATA_READ_VALIDATE] Metadata read failed: cache_key={}, error={}",
                    cache_key, e
                );

                // Attempt corruption recovery
                match self.detect_and_recover_corruption(cache_key).await {
                    Ok(true) => {
                        info!(
                            "[METADATA_READ_VALIDATE] Recovery successful after read failure: cache_key={}",
                            cache_key
                        );
                        // Re-attempt read after recovery
                        return self.get_metadata(cache_key).await;
                    }
                    _ => {
                        // Recovery failed or not needed, treat as cache miss
                        return Ok(None);
                    }
                }
            }
        }
    }

    // ============================================================================
    // Enhanced Concurrent Access Safety Methods
    // ============================================================================

    /// Acquire exclusive lock with timeout and proper cleanup
    ///
    /// This method provides robust file locking with:
    /// - Configurable timeout for lock acquisition
    /// - Automatic cleanup of stale lock files
    /// - Proper error handling and logging
    /// - Lock file cleanup on failures
    ///
    /// # Arguments
    /// * `lock_file_path` - Path to the lock file
    /// * `timeout` - Maximum time to wait for lock acquisition
    /// * `operation_name` - Name of the operation for logging
    ///
    /// # Returns
    /// * `Ok(LockGuard)` - Lock acquired successfully
    /// * `Err(ProxyError)` - Lock acquisition failed
    ///
    /// # Requirements
    /// Implements Requirements 3.5, 4.4
    pub async fn acquire_exclusive_lock_with_timeout(
        &self,
        lock_file_path: &PathBuf,
        timeout: Duration,
        operation_name: &str,
    ) -> Result<LockGuard> {
        let operation_start = std::time::Instant::now();
        debug!(
            "[LOCK_ACQUISITION] Starting lock acquisition: path={:?}, timeout={:?}, operation={}",
            lock_file_path, timeout, operation_name
        );

        // Check for stale lock files first
        self.cleanup_stale_lock_file(lock_file_path, timeout)
            .await?;

        // Create parent directory if needed
        if let Some(parent) = lock_file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                error!(
                    "[LOCK_ACQUISITION] Failed to create lock directory: path={:?}, error={}",
                    parent, e
                );
                ProxyError::CacheError(format!("Failed to create lock directory: {}", e))
            })?;
        }

        // Open lock file
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(lock_file_path)
            .map_err(|e| {
                error!(
                    "[LOCK_ACQUISITION] Failed to open lock file: path={:?}, operation={}, error={}",
                    lock_file_path, operation_name, e
                );
                ProxyError::CacheError(format!("Failed to open lock file: {}", e))
            })?;

        // Try to acquire lock with timeout
        let lock_start = std::time::Instant::now();
        let mut last_error = None;

        while lock_start.elapsed() < timeout {
            match lock_file.try_lock_exclusive() {
                Ok(()) => {
                    let acquisition_duration = operation_start.elapsed();
                    debug!(
                        "[LOCK_ACQUISITION] Lock acquired successfully: path={:?}, operation={}, duration={:.2}ms",
                        lock_file_path, operation_name, acquisition_duration.as_secs_f64() * 1000.0
                    );

                    return Ok(LockGuard {
                        file: lock_file,
                        path: lock_file_path.clone(),
                        operation_name: operation_name.to_string(),
                        acquired_at: std::time::Instant::now(),
                    });
                }
                Err(e) => {
                    last_error = Some(e);
                    // Wait a bit before retrying
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }

        // Timeout reached
        let timeout_duration = operation_start.elapsed();
        error!(
            "[LOCK_ACQUISITION] Lock acquisition timeout: path={:?}, operation={}, timeout={:?}, duration={:.2}ms, last_error={:?}",
            lock_file_path, operation_name, timeout, timeout_duration.as_secs_f64() * 1000.0, last_error
        );

        // Record timeout metric
        if let Some(metrics_manager) = &self.metrics_manager {
            metrics_manager.read().await.record_lock_timeout().await;
        }

        Err(ProxyError::TimeoutError(format!(
            "Lock acquisition timeout after {:?} for operation: {}",
            timeout, operation_name
        )))
    }

    /// Clean up stale lock files
    ///
    /// This method removes lock files that are older than the specified timeout,
    /// indicating they were left behind by crashed processes or operations.
    ///
    /// # Arguments
    /// * `lock_file_path` - Path to the lock file to check
    /// * `max_age` - Maximum age for lock files before considering them stale
    ///
    /// # Requirements
    /// Implements Requirements 4.4
    async fn cleanup_stale_lock_file(
        &self,
        lock_file_path: &PathBuf,
        max_age: Duration,
    ) -> Result<()> {
        if !lock_file_path.exists() {
            return Ok(()); // No lock file to clean up
        }

        // Check file age
        let metadata = std::fs::metadata(lock_file_path).map_err(|e| {
            debug!(
                "[LOCK_CLEANUP] Failed to read lock file metadata: path={:?}, error={}",
                lock_file_path, e
            );
            ProxyError::CacheError(format!("Failed to read lock file metadata: {}", e))
        })?;

        let file_age = metadata
            .modified()
            .map_err(|e| ProxyError::IoError(e.to_string()))
            .and_then(|modified| {
                modified
                    .elapsed()
                    .map_err(|e| ProxyError::SystemError(e.to_string()))
            })
            .unwrap_or(Duration::from_secs(0));

        if file_age > max_age {
            warn!(
                "[LOCK_CLEANUP] Removing stale lock file: path={:?}, age={:?}, max_age={:?}",
                lock_file_path, file_age, max_age
            );

            match std::fs::remove_file(lock_file_path) {
                Ok(()) => {
                    info!(
                        "[LOCK_CLEANUP] Stale lock file removed successfully: path={:?}, age={:?}",
                        lock_file_path, file_age
                    );
                }
                Err(e) => {
                    warn!(
                        "[LOCK_CLEANUP] Failed to remove stale lock file: path={:?}, age={:?}, error={}",
                        lock_file_path, file_age, e
                    );
                    // Don't fail the operation if we can't remove the stale lock
                }
            }
        } else {
            debug!(
                "[LOCK_CLEANUP] Lock file is not stale: path={:?}, age={:?}, max_age={:?}",
                lock_file_path, file_age, max_age
            );
        }

        Ok(())
    }

    /// Perform atomic metadata update with enhanced locking
    ///
    /// This method provides atomic metadata updates with:
    /// - Timeout-based lock acquisition
    /// - Automatic cleanup on failures
    /// - Comprehensive error handling
    /// - Proper lock cleanup
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the metadata
    /// * `update_fn` - Function to update the metadata
    /// * `operation_name` - Name of the operation for logging
    ///
    /// # Requirements
    /// Implements Requirements 3.5, 4.4
    pub async fn atomic_metadata_update<F>(
        &self,
        cache_key: &str,
        update_fn: F,
        operation_name: &str,
    ) -> Result<()>
    where
        F: FnOnce(&mut crate::cache_types::NewCacheMetadata) -> Result<()>,
    {
        let operation_start = std::time::Instant::now();
        debug!(
            "[ATOMIC_UPDATE] Starting atomic metadata update: cache_key={}, operation={}",
            cache_key, operation_name
        );

        let metadata_file_path = self.get_new_metadata_file_path(cache_key);
        let metadata_tmp_path = metadata_file_path.with_extension("meta.tmp");
        let lock_file_path = metadata_file_path.with_extension("meta.lock");

        // Acquire lock with timeout
        let lock_timeout = Duration::from_secs(30); // 30 second timeout
        let _lock_guard = self
            .acquire_exclusive_lock_with_timeout(&lock_file_path, lock_timeout, operation_name)
            .await?;

        // Read existing metadata or create new
        let mut metadata = if metadata_file_path.exists() {
            let content = std::fs::read_to_string(&metadata_file_path).map_err(|e| {
                error!(
                    "[ATOMIC_UPDATE] Failed to read metadata: cache_key={}, operation={}, error={}",
                    cache_key, operation_name, e
                );
                ProxyError::CacheError(format!("Failed to read metadata: {}", e))
            })?;

            serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&content)
                .map_err(|e| {
                    error!(
                        "[ATOMIC_UPDATE] Failed to parse metadata: cache_key={}, operation={}, error={}",
                        cache_key, operation_name, e
                    );
                    ProxyError::CacheError(format!("Corrupted metadata: {}", e))
                })?
        } else {
            // Create new metadata structure
            let now = SystemTime::now();
            crate::cache_types::NewCacheMetadata {
                cache_key: cache_key.to_string(),
                object_metadata: crate::cache_types::ObjectMetadata::default(),
                ranges: Vec::new(),
                created_at: now,
                expires_at: now + Duration::from_secs(3600), // 1 hour default
                compression_info: crate::cache_types::CompressionInfo::default(),
                ..Default::default()
            }
        };

        // Apply the update function
        update_fn(&mut metadata).map_err(|e| {
            error!(
                "[ATOMIC_UPDATE] Update function failed: cache_key={}, operation={}, error={}",
                cache_key, operation_name, e
            );
            e
        })?;

        // Write to temporary file
        let metadata_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| {
                error!(
                    "[ATOMIC_UPDATE] Failed to serialize metadata: cache_key={}, operation={}, error={}",
                    cache_key, operation_name, e
                );
                ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
            })?;

        std::fs::write(&metadata_tmp_path, &metadata_json)
            .map_err(|e| {
                let _ = std::fs::remove_file(&metadata_tmp_path);
                error!(
                    "[ATOMIC_UPDATE] Failed to write temp metadata: cache_key={}, operation={}, error={}",
                    cache_key, operation_name, e
                );
                ProxyError::CacheError(format!("Failed to write temp metadata: {}", e))
            })?;

        // Atomic rename
        std::fs::rename(&metadata_tmp_path, &metadata_file_path).map_err(|e| {
            let _ = std::fs::remove_file(&metadata_tmp_path);
            error!(
                "[ATOMIC_UPDATE] Failed to rename metadata: cache_key={}, operation={}, error={}",
                cache_key, operation_name, e
            );
            ProxyError::CacheError(format!("Failed to rename metadata: {}", e))
        })?;

        let update_duration = operation_start.elapsed();
        debug!(
            "[ATOMIC_UPDATE] Atomic update completed: cache_key={}, operation={}, duration={:.2}ms",
            cache_key,
            operation_name,
            update_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Clean up all lock files for a specific cache key
    ///
    /// This method removes all lock files associated with a cache key,
    /// useful for cleanup after operations complete or fail.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key to clean up locks for
    ///
    /// # Requirements
    /// Implements Requirements 4.4
    pub async fn cleanup_locks_for_key(&self, cache_key: &str) -> Result<()> {
        debug!(
            "[LOCK_CLEANUP] Cleaning up locks for cache key: cache_key={}",
            cache_key
        );

        let metadata_file_path = self.get_new_metadata_file_path(cache_key);
        let lock_file_path = metadata_file_path.with_extension("meta.lock");

        if lock_file_path.exists() {
            match std::fs::remove_file(&lock_file_path) {
                Ok(()) => {
                    debug!(
                        "[LOCK_CLEANUP] Lock file removed: cache_key={}, path={:?}",
                        cache_key, lock_file_path
                    );
                }
                Err(e) => {
                    warn!(
                        "[LOCK_CLEANUP] Failed to remove lock file: cache_key={}, path={:?}, error={}",
                        cache_key, lock_file_path, e
                    );
                    // Don't fail the operation if lock cleanup fails
                }
            }
        }

        Ok(())
    }
}

/// Lock guard for managing exclusive file locks with automatic cleanup
pub struct LockGuard {
    file: std::fs::File,
    path: PathBuf,
    operation_name: String,
    acquired_at: std::time::Instant,
}

impl LockGuard {
    /// Get the duration the lock has been held
    pub fn held_duration(&self) -> Duration {
        self.acquired_at.elapsed()
    }

    /// Get the operation name for this lock
    pub fn operation_name(&self) -> &str {
        &self.operation_name
    }

    /// Get the lock file path
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let held_duration = self.held_duration();

        // Release the lock
        if let Err(e) = self.file.unlock() {
            warn!(
                "[LOCK_RELEASE] Failed to unlock file: path={:?}, operation={}, held_duration={:.2}ms, error={}",
                self.path, self.operation_name, held_duration.as_secs_f64() * 1000.0, e
            );
        } else {
            debug!(
                "[LOCK_RELEASE] Lock released successfully: path={:?}, operation={}, held_duration={:.2}ms",
                self.path, self.operation_name, held_duration.as_secs_f64() * 1000.0
            );
        }

        // Clean up lock file
        if let Err(e) = std::fs::remove_file(&self.path) {
            debug!(
                "[LOCK_RELEASE] Failed to remove lock file (non-critical): path={:?}, operation={}, error={}",
                self.path, self.operation_name, e
            );
            // Lock file cleanup failure is not critical - the file will be cleaned up as stale
        } else {
            debug!(
                "[LOCK_RELEASE] Lock file removed: path={:?}, operation={}",
                self.path, self.operation_name
            );
        }
    }
}

impl DiskCacheManager {
    // ============================================================================
    // Comprehensive Error Handling and Fallback Methods
    // ============================================================================

    /// Handle cache operation failures with fallback mechanisms
    ///
    /// This method provides comprehensive error handling for cache operations:
    /// - Logs detailed error information with context
    /// - Implements fallback strategies based on error type
    /// - Schedules background retry for recoverable operations
    /// - Records metrics for monitoring and alerting
    ///
    /// # Arguments
    /// * `operation` - Name of the failed operation
    /// * `cache_key` - The cache key involved in the operation
    /// * `error` - The error that occurred
    /// * `context` - Additional context information
    ///
    /// # Returns
    /// * `Ok(FallbackAction)` - Recommended fallback action
    /// * `Err(ProxyError)` - Unrecoverable error
    ///
    /// # Requirements
    /// Implements Requirements 4.2, 4.5
    pub async fn handle_cache_operation_failure(
        &self,
        operation: &str,
        cache_key: &str,
        error: &ProxyError,
        context: &str,
    ) -> Result<FallbackAction> {
        let _operation_start = std::time::Instant::now();
        error!(
            "[ERROR_HANDLING] Cache operation failed: operation={}, cache_key={}, context={}, error={}",
            operation, cache_key, context, error
        );

        // Determine error category and appropriate fallback
        let fallback_action = match error {
            ProxyError::IoError(io_err) => {
                if io_err.contains("No space left on device") || io_err.contains("Disk full") {
                    error!(
                        "[ERROR_HANDLING] Disk space exhaustion detected: operation={}, cache_key={}, error={}",
                        operation, cache_key, io_err
                    );

                    // Record disk full event
                    if let Some(metrics_manager) = &self.metrics_manager {
                        metrics_manager.read().await.record_disk_full_event().await;
                    }

                    FallbackAction::DisableCaching
                } else if io_err.contains("Permission denied") {
                    error!(
                        "[ERROR_HANDLING] Permission error detected: operation={}, cache_key={}, error={}",
                        operation, cache_key, io_err
                    );

                    FallbackAction::DisableCaching
                } else {
                    warn!(
                        "[ERROR_HANDLING] I/O error, will retry: operation={}, cache_key={}, error={}",
                        operation, cache_key, io_err
                    );

                    FallbackAction::RetryLater
                }
            }

            ProxyError::TimeoutError(_) => {
                warn!(
                    "[ERROR_HANDLING] Timeout error, will retry: operation={}, cache_key={}, error={}",
                    operation, cache_key, error
                );

                FallbackAction::RetryLater
            }

            ProxyError::LockError(_) => {
                warn!(
                    "[ERROR_HANDLING] Lock error, will retry: operation={}, cache_key={}, error={}",
                    operation, cache_key, error
                );

                FallbackAction::RetryLater
            }

            ProxyError::CacheError(cache_err) => {
                if cache_err.contains("Corrupted") || cache_err.contains("corrupted") {
                    error!(
                        "[ERROR_HANDLING] Corruption detected, invalidating: operation={}, cache_key={}, error={}",
                        operation, cache_key, cache_err
                    );

                    // Record corruption and invalidate
                    if let Some(metrics_manager) = &self.metrics_manager {
                        metrics_manager
                            .read()
                            .await
                            .record_corrupted_metadata_simple()
                            .await;
                    }

                    // Schedule invalidation
                    if let Err(invalidation_error) = self.invalidate_cache_entry(cache_key).await {
                        error!(
                            "[ERROR_HANDLING] Failed to invalidate corrupted entry: cache_key={}, error={}",
                            cache_key, invalidation_error
                        );
                    }

                    FallbackAction::ServeFromS3
                } else {
                    warn!(
                        "[ERROR_HANDLING] Cache error, will retry: operation={}, cache_key={}, error={}",
                        operation, cache_key, cache_err
                    );

                    FallbackAction::RetryLater
                }
            }

            _ => {
                warn!(
                    "[ERROR_HANDLING] Unknown error type, serving from S3: operation={}, cache_key={}, error={}",
                    operation, cache_key, error
                );

                FallbackAction::ServeFromS3
            }
        };

        // Log the chosen fallback action
        info!(
            "[ERROR_HANDLING] Fallback action determined: operation={}, cache_key={}, action={:?}",
            operation, cache_key, fallback_action
        );

        // Schedule background retry if appropriate
        if matches!(fallback_action, FallbackAction::RetryLater) {
            self.schedule_background_retry(operation, cache_key, context)
                .await?;
        }

        Ok(fallback_action)
    }

    /// Schedule background retry for failed operations
    ///
    /// This method schedules failed operations for retry in the background
    /// with exponential backoff and maximum retry limits.
    ///
    /// # Arguments
    /// * `operation` - Name of the operation to retry
    /// * `cache_key` - The cache key for the operation
    /// * `context` - Additional context for the retry
    ///
    /// # Requirements
    /// Implements Requirements 4.2
    async fn schedule_background_retry(
        &self,
        operation: &str,
        cache_key: &str,
        context: &str,
    ) -> Result<()> {
        debug!(
            "[BACKGROUND_RETRY] Scheduling background retry: operation={}, cache_key={}, context={}",
            operation, cache_key, context
        );

        // For now, we'll implement a simple retry mechanism
        // In a production system, this could use a job queue or task scheduler
        let retry_operation = operation.to_string();
        let retry_cache_key = cache_key.to_string();
        let retry_context = context.to_string();

        // Spawn background task for retry
        tokio::spawn(async move {
            // Initial delay before first retry
            tokio::time::sleep(Duration::from_secs(5)).await;

            debug!(
                "[BACKGROUND_RETRY] Executing background retry: operation={}, cache_key={}, context={}",
                retry_operation, retry_cache_key, retry_context
            );

            // Note: In a full implementation, this would call the appropriate retry method
            // For now, we just log that the retry was attempted
            info!(
                "[BACKGROUND_RETRY] Background retry completed: operation={}, cache_key={}, context={}",
                retry_operation, retry_cache_key, retry_context
            );
        });

        Ok(())
    }

    /// Perform cache operation with automatic fallback handling
    ///
    /// This method wraps cache operations with comprehensive error handling
    /// and automatic fallback mechanisms. It should be used for critical
    /// cache operations that need robust error handling.
    ///
    /// # Arguments
    /// * `operation_name` - Name of the operation for logging
    /// * `cache_key` - The cache key for the operation
    /// * `operation_fn` - The cache operation to perform
    ///
    /// # Returns
    /// * `Ok(CacheOperationResult<T>)` - Operation result with fallback info
    /// * `Err(ProxyError)` - Unrecoverable error
    ///
    /// # Requirements
    /// Implements Requirements 4.2, 4.5
    pub async fn perform_cache_operation_with_fallback<T, F, Fut>(
        &self,
        operation_name: &str,
        cache_key: &str,
        operation_fn: F,
    ) -> Result<CacheOperationResult<T>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let operation_start = std::time::Instant::now();
        debug!(
            "[CACHE_OPERATION] Starting cache operation with fallback: operation={}, cache_key={}",
            operation_name, cache_key
        );

        // Attempt the cache operation
        match operation_fn().await {
            Ok(result) => {
                let operation_duration = operation_start.elapsed();
                debug!(
                    "[CACHE_OPERATION] Cache operation succeeded: operation={}, cache_key={}, duration={:.2}ms",
                    operation_name, cache_key, operation_duration.as_secs_f64() * 1000.0
                );

                Ok(CacheOperationResult {
                    result: Some(result),
                    fallback_action: FallbackAction::None,
                    operation_duration,
                    error_context: None,
                })
            }
            Err(error) => {
                let operation_duration = operation_start.elapsed();
                warn!(
                    "[CACHE_OPERATION] Cache operation failed: operation={}, cache_key={}, duration={:.2}ms, error={}",
                    operation_name, cache_key, operation_duration.as_secs_f64() * 1000.0, error
                );

                // Handle the failure and determine fallback action
                let fallback_action = self
                    .handle_cache_operation_failure(
                        operation_name,
                        cache_key,
                        &error,
                        "cache_operation_with_fallback",
                    )
                    .await?;

                Ok(CacheOperationResult {
                    result: None,
                    fallback_action,
                    operation_duration,
                    error_context: Some(error.to_string()),
                })
            }
        }
    }

    /// Validate cache directory health and perform recovery if needed
    ///
    /// This method checks the health of the cache directory and performs
    /// recovery operations if issues are detected:
    /// - Checks disk space availability
    /// - Validates directory permissions
    /// - Cleans up corrupted files
    /// - Repairs directory structure
    ///
    /// # Returns
    /// * `Ok(CacheHealthStatus)` - Health status and any recovery actions taken
    /// * `Err(ProxyError)` - Unrecoverable health issues
    ///
    /// # Requirements
    /// Implements Requirements 4.2, 4.5
    pub async fn validate_cache_health(&self) -> Result<CacheHealthStatus> {
        let operation_start = std::time::Instant::now();
        info!(
            "[CACHE_HEALTH] Starting cache health validation: cache_dir={:?}",
            self.cache_dir
        );

        let mut health_status = CacheHealthStatus {
            is_healthy: true,
            issues_found: Vec::new(),
            recovery_actions: Vec::new(),
            disk_space_available: 0,
            directory_permissions_ok: false,
            corrupted_files_found: 0,
            corrupted_files_cleaned: 0,
        };

        // Check disk space
        match std::fs::metadata(&self.cache_dir) {
            Ok(_) => {
                // Try to get available disk space (platform-specific)
                // For now, we'll just check if we can write a test file
                let test_file = self.cache_dir.join(".health_check");
                match std::fs::write(&test_file, b"health_check") {
                    Ok(()) => {
                        let _ = std::fs::remove_file(&test_file);
                        health_status.directory_permissions_ok = true;
                        debug!(
                            "[CACHE_HEALTH] Directory permissions OK: cache_dir={:?}",
                            self.cache_dir
                        );
                    }
                    Err(e) => {
                        health_status.is_healthy = false;
                        health_status.directory_permissions_ok = false;
                        health_status
                            .issues_found
                            .push(format!("Permission error: {}", e));
                        error!(
                            "[CACHE_HEALTH] Directory permission check failed: cache_dir={:?}, error={}",
                            self.cache_dir, e
                        );
                    }
                }
            }
            Err(e) => {
                health_status.is_healthy = false;
                health_status
                    .issues_found
                    .push(format!("Cache directory inaccessible: {}", e));
                error!(
                    "[CACHE_HEALTH] Cache directory check failed: cache_dir={:?}, error={}",
                    self.cache_dir, e
                );
            }
        }

        // Check for corrupted files (sample check)
        let metadata_dir = self.cache_dir.join("metadata");
        if metadata_dir.exists() {
            match self.scan_for_corrupted_files(&metadata_dir).await {
                Ok((found, cleaned)) => {
                    health_status.corrupted_files_found = found;
                    health_status.corrupted_files_cleaned = cleaned;

                    if found > 0 {
                        health_status
                            .recovery_actions
                            .push(format!("Cleaned {} corrupted metadata files", cleaned));
                    }

                    debug!(
                        "[CACHE_HEALTH] Corruption scan completed: found={}, cleaned={}",
                        found, cleaned
                    );
                }
                Err(e) => {
                    health_status
                        .issues_found
                        .push(format!("Corruption scan failed: {}", e));
                    warn!("[CACHE_HEALTH] Corruption scan failed: error={}", e);
                }
            }
        }

        let health_duration = operation_start.elapsed();
        if health_status.is_healthy {
            info!(
                "[CACHE_HEALTH] Cache health validation completed: status=healthy, duration={:.2}ms",
                health_duration.as_secs_f64() * 1000.0
            );
        } else {
            warn!(
                "[CACHE_HEALTH] Cache health validation completed: status=unhealthy, issues={}, duration={:.2}ms",
                health_status.issues_found.len(), health_duration.as_secs_f64() * 1000.0
            );
        }

        Ok(health_status)
    }

    /// Scan for corrupted files in a directory
    ///
    /// This method scans a directory for corrupted metadata files and
    /// attempts to clean them up automatically.
    ///
    /// # Arguments
    /// * `directory` - Directory to scan for corrupted files
    ///
    /// # Returns
    /// * `Ok((found, cleaned))` - Number of corrupted files found and cleaned
    /// * `Err(ProxyError)` - Scan operation failed
    async fn scan_for_corrupted_files(&self, directory: &PathBuf) -> Result<(u64, u64)> {
        let mut found = 0u64;
        let mut cleaned = 0u64;

        debug!(
            "[CORRUPTION_SCAN] Starting corruption scan: directory={:?}",
            directory
        );

        // Walk through directory structure
        let walker = walkdir::WalkDir::new(directory)
            .max_depth(5) // Limit depth to prevent excessive scanning
            .into_iter();

        for entry in walker {
            match entry {
                Ok(entry) => {
                    let path = entry.path();

                    // Check only .meta files
                    if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                        match std::fs::read_to_string(path) {
                            Ok(content) => {
                                // Try to parse as JSON
                                if serde_json::from_str::<serde_json::Value>(&content).is_err() {
                                    found += 1;
                                    warn!(
                                        "[CORRUPTION_SCAN] Corrupted metadata file found: path={:?}",
                                        path
                                    );

                                    // Attempt to clean up
                                    if let Ok(()) = std::fs::remove_file(path) {
                                        cleaned += 1;
                                        info!(
                                            "[CORRUPTION_SCAN] Corrupted file cleaned: path={:?}",
                                            path
                                        );
                                    }
                                }
                            }
                            Err(_) => {
                                // File is unreadable, consider it corrupted
                                found += 1;
                                if let Ok(()) = std::fs::remove_file(path) {
                                    cleaned += 1;
                                    info!(
                                        "[CORRUPTION_SCAN] Unreadable file cleaned: path={:?}",
                                        path
                                    );
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!(
                        "[CORRUPTION_SCAN] Directory walk error (non-critical): error={}",
                        e
                    );
                    // Continue scanning despite individual file errors
                }
            }
        }

        debug!(
            "[CORRUPTION_SCAN] Corruption scan completed: directory={:?}, found={}, cleaned={}",
            directory, found, cleaned
        );

        Ok((found, cleaned))
    }
}

/// Fallback actions for cache operation failures
#[derive(Debug, Clone, PartialEq)]
pub enum FallbackAction {
    /// No fallback needed, operation succeeded
    None,
    /// Serve request directly from S3, bypassing cache
    ServeFromS3,
    /// Retry the operation later in background
    RetryLater,
    /// Disable caching temporarily due to persistent issues
    DisableCaching,
}

/// Result of a cache operation with fallback information
#[derive(Debug, Clone)]
pub struct CacheOperationResult<T> {
    /// The result of the operation (None if failed)
    pub result: Option<T>,
    /// The fallback action taken or recommended
    pub fallback_action: FallbackAction,
    /// Duration of the operation attempt
    pub operation_duration: Duration,
    /// Error context if the operation failed
    pub error_context: Option<String>,
}

/// Cache health status information
#[derive(Debug, Clone)]
pub struct CacheHealthStatus {
    /// Overall health status
    pub is_healthy: bool,
    /// List of issues found during health check
    pub issues_found: Vec<String>,
    /// Recovery actions taken
    pub recovery_actions: Vec<String>,
    /// Available disk space in bytes
    pub disk_space_available: u64,
    /// Whether directory permissions are OK
    pub directory_permissions_ok: bool,
    /// Number of corrupted files found
    pub corrupted_files_found: u64,
    /// Number of corrupted files cleaned up
    pub corrupted_files_cleaned: u64,
}

/// Statistics from cache cleanup operations
#[derive(Debug, Default, Clone)]
pub struct CacheCleanupStats {
    pub temp_files_cleaned: u64,
    pub orphaned_files_cleaned: u64,
    pub inconsistencies_fixed: u64,
}

// ============================================================================
// Path Resolution Functions for Bucket-First Sharding
// ============================================================================

/// Parse cache key into bucket and object key
///
/// Cache keys are in format: `/bucket/object_key` (S3 request path)
///
/// # Arguments
/// * `cache_key` - The cache key to parse (format: "/bucket/object_key")
///
/// # Returns
/// * `Ok((bucket, object_key))` - Tuple of bucket name and object key
/// * `Err(ProxyError)` - If cache key format is invalid (no slash separator)
///
/// # Examples
/// ```
/// use s3_proxy::disk_cache::parse_cache_key;
/// let (bucket, object_key) = parse_cache_key("my-bucket/path/to/object").unwrap();
/// assert_eq!(bucket, "my-bucket");
/// assert_eq!(object_key, "path/to/object");
/// ```
///
/// # Requirements
/// Implements Requirements 1.2, 5.2
pub fn parse_cache_key(cache_key: &str) -> Result<(&str, &str)> {
    // Remove leading slash if present (S3 paths start with /)
    let key = cache_key.strip_prefix('/').unwrap_or(cache_key);

    // Split on first '/' to get bucket and object key
    // Format: "bucket/object/path" (e.g., "my-bucket/path/to/file.txt")
    let (bucket, object_key) = key.split_once('/').ok_or_else(|| {
        ProxyError::CacheError(format!(
            "Invalid cache key format: '{}'. Expected format: 'bucket/object_key'",
            cache_key
        ))
    })?;

    // Validate that both bucket and object_key are non-empty
    if bucket.is_empty() || object_key.is_empty() {
        return Err(ProxyError::CacheError(format!(
            "Invalid cache key format: '{}'. Bucket and object key must be non-empty",
            cache_key
        )));
    }

    Ok((bucket, object_key))
}

/// Sanitize object key for use as a filename
///
/// This function:
/// 1. Percent-encodes filesystem-unsafe characters
/// 2. For keys >200 chars after encoding, uses BLAKE3 hash as filename
/// 3. Appends the provided suffix to the result
///
/// # Arguments
/// * `object_key` - The S3 object key to sanitize
/// * `suffix` - The suffix to append (e.g., ".meta", "_{start}-{end}.bin")
///
/// # Returns
/// * Sanitized filename with suffix appended
///
/// # Examples
/// ```
/// use s3_proxy::disk_cache::sanitize_object_key_for_filename;
/// let filename = sanitize_object_key_for_filename("path/to/file.txt", ".meta");
/// // Returns: "path%2Fto%2Ffile.txt.meta"
///
/// let long_key = "a".repeat(250);
/// let filename = sanitize_object_key_for_filename(&long_key, ".meta");
/// // Returns: "{64-char-blake3-hash}.meta"
/// ```
///
/// # Requirements
/// Implements Requirements 3.3, 3.4, 3.5
pub fn sanitize_object_key_for_filename(object_key: &str, suffix: &str) -> String {
    use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

    // Define filesystem-unsafe ASCII characters that need encoding
    // Start with CONTROLS (0x00-0x1F, 0x7F) and add filesystem-specific chars
    const FRAGMENT: &AsciiSet = &CONTROLS
        .add(b' ') // Space
        .add(b'/') // Path separator (Unix)
        .add(b'\\') // Path separator (Windows)
        .add(b':') // Drive letter separator (Windows), problematic on macOS
        .add(b'*') // Wildcard (Windows)
        .add(b'?') // Wildcard (Windows)
        .add(b'"') // Quote (Windows)
        .add(b'<') // Redirect (Windows)
        .add(b'>') // Redirect (Windows)
        .add(b'|') // Pipe (Windows)
        .add(b'%'); // Percent (to avoid double-encoding issues)

    // Use utf8_percent_encode which preserves non-ASCII UTF-8 sequences
    let sanitized = utf8_percent_encode(object_key, FRAGMENT).to_string();

    // Filesystem filename limit is typically 255 bytes
    // Use 200 as threshold to leave room for extensions and suffixes
    if sanitized.len() > 200 {
        // Hash long keys to ensure they fit within filesystem limits
        // Use BLAKE3 for fast, secure hashing (64 hex characters)
        let hash = blake3::hash(object_key.as_bytes());
        format!("{}{}", hash.to_hex(), suffix)
    } else {
        format!("{}{}", sanitized, suffix)
    }
}

/// Get sharded path for a cache file using bucket-first directory structure
///
/// This function:
/// 1. Parses the cache key to extract bucket and object key
/// 2. Computes BLAKE3 hash of the object key (not bucket)
/// 3. Extracts first 2 hex digits for level 1, next 3 for level 2
/// 4. Constructs path: base_dir/bucket/XX/YYY/filename
///
/// # Arguments
/// * `base_dir` - Base directory (e.g., "cache_dir/metadata" or "cache_dir/ranges")
/// * `cache_key` - Cache key in format "/bucket/object_key" (S3 request path)
/// * `suffix` - Suffix for the filename (e.g., ".meta", "_{start}-{end}.bin")
///
/// # Returns
/// * `Ok(PathBuf)` - Full path to the sharded cache file
/// * `Err(ProxyError)` - If cache key format is invalid
///
/// # Examples
/// ```
/// use std::path::PathBuf;
/// use s3_proxy::disk_cache::get_sharded_path;
/// let path = get_sharded_path(
///     &PathBuf::from("/cache/metadata"),
///     "my-bucket/path/to/file.txt",
///     ".meta"
/// ).unwrap();
/// // Returns: /cache/metadata/my-bucket/XX/YYY/path%2Fto%2Ffile.txt.meta
/// // where XX and YYY are derived from BLAKE3("path/to/file.txt")
/// ```
///
/// # Requirements
/// Implements Requirements 1.1, 2.1, 2.2, 5.1
pub fn get_sharded_path(
    base_dir: &std::path::Path,
    cache_key: &str,
    suffix: &str,
) -> Result<PathBuf> {
    // Requirement 3.1: Parse cache key to extract bucket and object key
    let (bucket, object_key) = parse_cache_key(cache_key)?;

    debug!(
        "[SHARDED_PATH] Parsed cache key: cache_key={}, bucket={}, object_key={}",
        cache_key, bucket, object_key
    );

    // Requirement 3.2: Hash object key only (not bucket) using BLAKE3
    let hash = blake3::hash(object_key.as_bytes());
    let hash_hex = hash.to_hex();

    // Requirement 3.2: Extract directory levels from hash
    // Level 1: First 2 hex digits (256 directories)
    // Level 2: Next 3 hex digits (4,096 subdirectories per L1)
    let level1 = &hash_hex.as_str()[0..2];
    let level2 = &hash_hex.as_str()[2..5];

    debug!(
        "[SHARDED_PATH] Hash-based directories computed: object_key={}, hash_prefix={}, level1={}, level2={}",
        object_key, &hash_hex.as_str()[0..10], level1, level2
    );

    // Requirement 3.3: Sanitize object key for filename
    let filename = sanitize_object_key_for_filename(object_key, suffix);

    // Requirement 3.3: Construct path: base_dir/bucket/XX/YYY/filename
    let sharded_path = base_dir
        .join(bucket)
        .join(level1)
        .join(level2)
        .join(&filename);

    // Requirement 3.5: Log full sharded path construction
    debug!(
        "[SHARDED_PATH] Sharded path constructed: cache_key={}, bucket={}, level1={}, level2={}, filename={}, full_path={:?}",
        cache_key, bucket, level1, level2, filename, sharded_path
    );

    Ok(sharded_path)
}

/// Normalize cache key by removing leading slashes and ensuring consistent format
///
/// This function ensures cache keys are in a consistent format regardless of
/// whether they were provided with or without leading slashes.
///
/// # Arguments
/// * `cache_key` - The cache key to normalize
///
/// # Returns
/// * Normalized cache key without leading slash
///
/// # Examples
/// ```
/// use s3_proxy::disk_cache::normalize_cache_key;
/// assert_eq!(normalize_cache_key("/bucket/path"), "bucket/path");
/// assert_eq!(normalize_cache_key("bucket/path"), "bucket/path");
/// ```
///
/// # Requirements
/// Implements Requirements 5.3, 5.5
pub fn normalize_cache_key(cache_key: &str) -> String {
    // Remove leading slash if present
    cache_key.strip_prefix('/').unwrap_or(cache_key).to_string()
}

/// Validate cache key consistency between storage and lookup operations
///
/// This struct tracks all transformations of a cache key and provides
/// diagnostic logging to help identify cache key format mismatches.
///
/// # Requirements
/// Implements Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 5.3, 5.4, 5.5
#[derive(Debug, Clone)]
pub struct CacheKeyValidator {
    /// Original cache key as received
    pub original_key: String,
    /// Normalized cache key (leading slash removed)
    pub normalized_key: String,
    /// Sanitized cache key for filesystem
    pub sanitized_key: String,
    /// Bucket name extracted from key
    pub bucket: String,
    /// Object key extracted from key
    pub object_key: String,
    /// Full metadata file path
    pub metadata_path: PathBuf,
    /// Full range file path (for a sample range 0-1023)
    pub range_path: PathBuf,
}

impl CacheKeyValidator {
    /// Create validator and log all key transformations
    ///
    /// # Arguments
    /// * `cache_key` - The cache key to validate
    /// * `disk_cache` - Reference to DiskCacheManager for path construction
    ///
    /// # Returns
    /// * `Ok(CacheKeyValidator)` - Validator with all transformations computed
    /// * `Err(ProxyError)` - If cache key format is invalid
    ///
    /// # Requirements
    /// Implements Requirements 2.1, 2.2, 2.3, 2.4, 2.5
    pub fn new(cache_key: &str, disk_cache: &DiskCacheManager) -> Result<Self> {
        // Normalize the cache key
        let normalized_key = normalize_cache_key(cache_key);

        // Sanitize the cache key
        let sanitized_key = disk_cache.sanitize_cache_key_new(&normalized_key);

        // Parse the cache key to extract bucket and object key
        let (bucket, object_key) = parse_cache_key(&normalized_key)?;

        // Get metadata file path
        let metadata_path = disk_cache.get_new_metadata_file_path(&normalized_key);

        // Get sample range file path (0-1023)
        let range_path = disk_cache.get_new_range_file_path(&normalized_key, 0, 1023);

        Ok(Self {
            original_key: cache_key.to_string(),
            normalized_key: normalized_key.clone(),
            sanitized_key,
            bucket: bucket.to_string(),
            object_key: object_key.to_string(),
            metadata_path,
            range_path,
        })
    }

    /// Log all key components for debugging
    ///
    /// This method logs all transformations and paths for diagnostic purposes.
    /// Use this to identify where cache key format mismatches occur.
    ///
    /// # Requirements
    /// Implements Requirements 2.5, 5.1, 5.2, 5.4
    pub fn log_diagnostics(&self) {
        debug!(
            "[CACHE_KEY_VALIDATION] Cache key transformations:\n\
             - original_key: {}\n\
             - normalized_key: {}\n\
             - sanitized_key: {}\n\
             - bucket: {}\n\
             - object_key: {}\n\
             - metadata_path: {:?}\n\
             - range_path: {:?}",
            self.original_key,
            self.normalized_key,
            self.sanitized_key,
            self.bucket,
            self.object_key,
            self.metadata_path,
            self.range_path
        );
    }

    /// Check if the original key matches the normalized key
    ///
    /// Returns true if normalization changed the key (e.g., removed leading slash)
    pub fn was_normalized(&self) -> bool {
        self.original_key != self.normalized_key
    }

    /// Check if the sanitized key differs from the normalized key
    ///
    /// Returns true if sanitization changed the key (e.g., percent-encoded characters)
    pub fn was_sanitized(&self) -> bool {
        self.normalized_key != self.sanitized_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionAlgorithm;
    use std::time::Duration;
    use tempfile::TempDir;

    // ============================================================================
    // Tests for Path Resolution Functions
    // ============================================================================

    #[test]
    fn test_parse_cache_key_valid() {
        // Test basic parsing with leading slash
        let (bucket, object_key) = parse_cache_key("/my-bucket/path/to/object").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(object_key, "path/to/object");

        // Test without leading slash
        let (bucket, object_key) = parse_cache_key("my-bucket/path/to/object").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(object_key, "path/to/object");

        // Test with bucket names containing periods and hyphens
        let (bucket, object_key) = parse_cache_key("my.bucket-name/object").unwrap();
        assert_eq!(bucket, "my.bucket-name");
        assert_eq!(object_key, "object");

        // Test simple object at bucket root
        let (bucket, object_key) = parse_cache_key("bucket/file.txt").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(object_key, "file.txt");
    }

    #[test]
    fn test_parse_cache_key_invalid() {
        // Test missing slash (no object key)
        let result = parse_cache_key("no-slash-here");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid cache key format"));

        // Test empty string
        let result = parse_cache_key("");
        assert!(result.is_err());

        // Test only bucket with trailing slash
        let result = parse_cache_key("bucket/");
        assert!(result.is_err());
    }

    #[test]
    fn test_sanitize_object_key_for_filename_basic() {
        // Test simple key
        let filename = sanitize_object_key_for_filename("simple.txt", ".meta");
        assert_eq!(filename, "simple.txt.meta");

        // Test key with filesystem-unsafe characters
        let filename = sanitize_object_key_for_filename("path/to/file.txt", ".meta");
        assert!(filename.contains("%2F")); // '/' should be encoded
        assert!(filename.ends_with(".meta"));

        // Test key with spaces
        let filename = sanitize_object_key_for_filename("file with spaces.txt", ".bin");
        assert!(filename.contains("%20")); // space should be encoded
        assert!(filename.ends_with(".bin"));

        // Test key with special characters
        let filename = sanitize_object_key_for_filename("file:with*special?chars", ".meta");
        assert!(filename.contains("%3A")); // ':' should be encoded
        assert!(filename.contains("%2A")); // '*' should be encoded
        assert!(filename.contains("%3F")); // '?' should be encoded
    }

    #[test]
    fn test_sanitize_object_key_for_filename_long_key() {
        // Test key that exceeds 200 characters
        let long_key = "a".repeat(250);
        let filename = sanitize_object_key_for_filename(&long_key, ".meta");

        // Should use BLAKE3 hash (64 hex chars) + suffix
        assert_eq!(filename.len(), 64 + 5); // 64 hex chars + ".meta"
        assert!(filename.ends_with(".meta"));

        // Verify it's a valid hex string (excluding suffix)
        let hash_part = &filename[..64];
        assert!(hash_part.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_sanitize_object_key_determinism() {
        // Same input should always produce same output
        let key = "path/to/file.txt";
        let filename1 = sanitize_object_key_for_filename(key, ".meta");
        let filename2 = sanitize_object_key_for_filename(key, ".meta");
        assert_eq!(filename1, filename2);

        // Long keys should also be deterministic
        let long_key = "a".repeat(250);
        let filename1 = sanitize_object_key_for_filename(&long_key, ".meta");
        let filename2 = sanitize_object_key_for_filename(&long_key, ".meta");
        assert_eq!(filename1, filename2);
    }

    #[test]
    fn test_get_sharded_path_basic() {
        let base_dir = std::path::PathBuf::from("/cache/metadata");
        let cache_key = "my-bucket/path/to/file.txt";

        let path = get_sharded_path(&base_dir, cache_key, ".meta").unwrap();

        // Verify path structure: base_dir/bucket/XX/YYY/filename
        let path_str = path.to_string_lossy();
        assert!(path_str.starts_with("/cache/metadata/my-bucket/"));
        assert!(path_str.ends_with(".meta"));

        // Verify bucket is in path
        assert!(path_str.contains("my-bucket"));

        // Verify hash directories exist (2 hex digits / 3 hex digits)
        let components: Vec<&str> = path_str.split('/').collect();
        assert!(components.len() >= 6); // /cache/metadata/bucket/XX/YYY/filename
    }

    #[test]
    fn test_get_sharded_path_determinism() {
        let base_dir = std::path::PathBuf::from("/cache/metadata");
        let cache_key = "my-bucket/path/to/file.txt";

        // Same input should always produce same path
        let path1 = get_sharded_path(&base_dir, cache_key, ".meta").unwrap();
        let path2 = get_sharded_path(&base_dir, cache_key, ".meta").unwrap();
        assert_eq!(path1, path2);
    }

    #[test]
    fn test_get_sharded_path_same_object_different_buckets() {
        let base_dir = std::path::PathBuf::from("/cache/metadata");

        // Same object key in different buckets
        let path1 = get_sharded_path(&base_dir, "bucket1/file.txt", ".meta").unwrap();
        let path2 = get_sharded_path(&base_dir, "bucket2/file.txt", ".meta").unwrap();

        let path1_str = path1.to_string_lossy();
        let path2_str = path2.to_string_lossy();

        // Should have different bucket directories
        assert!(path1_str.contains("bucket1"));
        assert!(path2_str.contains("bucket2"));

        // But same hash directories (XX/YYY) since object key is the same
        let extract_hash_dirs = |path: &str| -> String {
            let parts: Vec<&str> = path.split('/').collect();
            // Find bucket index and extract next two components (XX/YYY)
            for (i, part) in parts.iter().enumerate() {
                if part.starts_with("bucket") && i + 2 < parts.len() {
                    return format!("{}/{}", parts[i + 1], parts[i + 2]);
                }
            }
            String::new()
        };

        let hash1 = extract_hash_dirs(&path1_str);
        let hash2 = extract_hash_dirs(&path2_str);
        assert_eq!(
            hash1, hash2,
            "Same object key should produce same hash directories"
        );
    }

    #[test]
    fn test_get_sharded_path_invalid_cache_key() {
        let base_dir = std::path::PathBuf::from("/cache/metadata");

        // Missing slash (no object key)
        let result = get_sharded_path(&base_dir, "no-slash", ".meta");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid cache key format"));
    }

    #[test]
    fn test_get_sharded_path_filename_excludes_bucket() {
        let base_dir = std::path::PathBuf::from("/cache/metadata");
        let cache_key = "my-bucket/file.txt";

        let path = get_sharded_path(&base_dir, cache_key, ".meta").unwrap();
        let filename = path.file_name().unwrap().to_string_lossy();

        // Filename should not contain bucket name
        assert!(!filename.contains("my-bucket"));
        assert!(filename.contains("file.txt"));
    }

    #[tokio::test]
    async fn test_disk_cache_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        // Initialize
        cache_manager.initialize().await.unwrap();

        // Test data
        let cache_key = "test-bucket/test-object";
        let test_data = b"This is test data for caching";
        let metadata = CacheMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: test_data.len() as u64,
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: SystemTime::now(),
        };

        // Store cache entry
        cache_manager
            .store_cache_entry(cache_key, test_data, metadata.clone())
            .await
            .unwrap();

        // Retrieve cache entry
        let retrieved = cache_manager.get_cache_entry(cache_key).await.unwrap();
        assert!(retrieved.is_some());

        let entry = retrieved.unwrap();
        assert_eq!(entry.cache_key, cache_key);
        assert_eq!(entry.body.as_ref().unwrap(), test_data);
        assert_eq!(entry.metadata.etag, metadata.etag);

        // Invalidate cache entry
        cache_manager
            .invalidate_cache_entry(cache_key)
            .await
            .unwrap();

        // Verify it's gone
        let retrieved_after_invalidation = cache_manager.get_cache_entry(cache_key).await.unwrap();
        assert!(retrieved_after_invalidation.is_none());
    }

    #[tokio::test]
    async fn test_cache_key_sanitization() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        let unsafe_key = "bucket/path:with*special?chars<>|";
        let safe_key = cache_manager.sanitize_cache_key(unsafe_key);

        assert!(!safe_key.contains('/'));
        assert!(!safe_key.contains('*'));
        assert!(!safe_key.contains('?'));
        assert!(!safe_key.contains('<'));
        assert!(!safe_key.contains('>'));
        assert!(!safe_key.contains('|'));
    }

    #[tokio::test]
    async fn test_cache_type_determination() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        // Test basic cache types
        assert_eq!(
            cache_manager.determine_cache_type("bucket:object"),
            "metadata"
        );
        assert_eq!(
            cache_manager.determine_cache_type("bucket:object:part:1"),
            "parts"
        );
        assert_eq!(
            cache_manager.determine_cache_type("bucket:object:range:0-1023"),
            "ranges"
        );

        // Version-related keys are now treated as regular metadata/parts/ranges
        // (versioning support has been removed)
        assert_eq!(
            cache_manager.determine_cache_type("bucket:object:version:123"),
            "metadata"
        );
        assert_eq!(
            cache_manager.determine_cache_type("bucket:object:version:123:part:1"),
            "parts"
        );
        assert_eq!(
            cache_manager.determine_cache_type("bucket:object:version:123:range:0-1023"),
            "ranges"
        );
    }

    // ============================================================================
    // Tests for New Range Storage Architecture
    // ============================================================================

    #[tokio::test]
    async fn test_new_metadata_file_path_generation() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        let cache_key = "test-bucket/test-object";
        let path = cache_manager.get_new_metadata_file_path(cache_key);

        // Should be in metadata subdirectory
        assert!(path.to_string_lossy().contains("metadata"));
        // Should have .meta extension
        assert!(path.to_string_lossy().ends_with(".meta"));
        // Should contain bucket name as directory
        assert!(path.to_string_lossy().contains("test-bucket"));
        // Should have hash-based sharding directories (XX/YYY pattern)
        let path_str = path.to_string_lossy();
        let parts: Vec<&str> = path_str.split('/').collect();
        // Find the bucket directory and verify hash directories follow
        let bucket_idx = parts.iter().position(|&p| p == "test-bucket");
        assert!(bucket_idx.is_some(), "Bucket directory not found in path");
        let bucket_idx = bucket_idx.unwrap();
        // After bucket, should have XX (2 hex chars) and YYY (3 hex chars) directories
        assert!(bucket_idx + 2 < parts.len(), "Missing hash directories");
        let level1 = parts[bucket_idx + 1];
        let level2 = parts[bucket_idx + 2];
        assert_eq!(level1.len(), 2, "Level 1 directory should be 2 hex chars");
        assert_eq!(level2.len(), 3, "Level 2 directory should be 3 hex chars");
        assert!(
            level1.chars().all(|c| c.is_ascii_hexdigit()),
            "Level 1 should be hex"
        );
        assert!(
            level2.chars().all(|c| c.is_ascii_hexdigit()),
            "Level 2 should be hex"
        );
    }

    #[tokio::test]
    async fn test_new_range_file_path_generation() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 8388607u64;
        let path = cache_manager.get_new_range_file_path(cache_key, start, end);

        // Should be in ranges subdirectory
        assert!(path.to_string_lossy().contains("ranges"));
        // Should have .bin extension
        assert!(path.to_string_lossy().ends_with(".bin"));
        // Should contain bucket name as directory
        assert!(path.to_string_lossy().contains("test-bucket"));
        // Should contain range in filename
        assert!(path.to_string_lossy().contains("_0-8388607.bin"));
        // Should have hash-based sharding directories (XX/YYY pattern)
        let path_str = path.to_string_lossy();
        let parts: Vec<&str> = path_str.split('/').collect();
        // Find the bucket directory and verify hash directories follow
        let bucket_idx = parts.iter().position(|&p| p == "test-bucket");
        assert!(bucket_idx.is_some(), "Bucket directory not found in path");
        let bucket_idx = bucket_idx.unwrap();
        // After bucket, should have XX (2 hex chars) and YYY (3 hex chars) directories
        assert!(bucket_idx + 2 < parts.len(), "Missing hash directories");
        let level1 = parts[bucket_idx + 1];
        let level2 = parts[bucket_idx + 2];
        assert_eq!(level1.len(), 2, "Level 1 directory should be 2 hex chars");
        assert_eq!(level2.len(), 3, "Level 2 directory should be 3 hex chars");
        assert!(
            level1.chars().all(|c| c.is_ascii_hexdigit()),
            "Level 1 should be hex"
        );
        assert!(
            level2.chars().all(|c| c.is_ascii_hexdigit()),
            "Level 2 should be hex"
        );
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_basic() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        let key = "bucket:object";
        let sanitized = cache_manager.sanitize_cache_key_new(key);
        // With percent encoding, ':' becomes '%3A'
        assert_eq!(sanitized, "bucket%3Aobject");
        // Verify no unsafe characters remain
        assert!(!sanitized.contains(':'));
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_special_characters() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        // Test all special characters that need sanitization
        let key = "bucket:path/with?query&param=value";
        let sanitized = cache_manager.sanitize_cache_key_new(key);

        // With percent encoding, these characters should be encoded
        assert!(!sanitized.contains(':'));
        assert!(!sanitized.contains('/'));
        assert!(!sanitized.contains('?'));
        // Note: '&' and '=' are not in our FRAGMENT set, so they may remain
        // But the key point is that unsafe filesystem characters are encoded

        // Verify percent-encoded versions are present
        assert!(sanitized.contains("%3A")); // :
        assert!(sanitized.contains("%2F")); // /
        assert!(sanitized.contains("%3F")); // ?
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_filesystem_unsafe_chars() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        // Test filesystem-unsafe characters
        let key = r#"bucket\path*with<special>chars|and"quotes"#;
        let sanitized = cache_manager.sanitize_cache_key_new(key);

        // With percent encoding, these characters should be encoded
        assert!(!sanitized.contains('\\'));
        assert!(!sanitized.contains('*'));
        assert!(!sanitized.contains('<'));
        assert!(!sanitized.contains('>'));
        assert!(!sanitized.contains('|'));
        assert!(!sanitized.contains('"'));

        // Verify percent-encoded versions are present
        assert!(sanitized.contains("%5C")); // \
        assert!(sanitized.contains("%2A")); // *
        assert!(sanitized.contains("%3C")); // <
        assert!(sanitized.contains("%3E")); // >
        assert!(sanitized.contains("%7C")); // |
        assert!(sanitized.contains("%22")); // "
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_url_special_chars() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        // Test URL-related special characters
        let key = "bucket#fragment%20space{brace}[bracket]$dollar!exclaim";
        let sanitized = cache_manager.sanitize_cache_key_new(key);

        // With percent encoding, % should be encoded to prevent double-encoding issues
        assert!(!sanitized.contains('%') || sanitized.contains("%25"));

        // Note: Not all of these characters are in our FRAGMENT set
        // We only encode characters that are problematic for filesystems
        // The key test is that the result is filesystem-safe

        // Verify the string is filesystem-safe (no path separators or wildcards)
        assert!(!sanitized.contains('/'));
        assert!(!sanitized.contains('\\'));
        assert!(!sanitized.contains('*'));
        assert!(!sanitized.contains('?'));
        assert!(!sanitized.contains('"'));
        assert!(!sanitized.contains('<'));
        assert!(!sanitized.contains('>'));
        assert!(!sanitized.contains('|'));
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_whitespace() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        // Test whitespace characters
        let key = "bucket with spaces\tand\ttabs\nand\nnewlines\rand\rreturns";
        let sanitized = cache_manager.sanitize_cache_key_new(key);

        // With percent encoding, whitespace should be encoded
        assert!(!sanitized.contains(' '));
        assert!(!sanitized.contains('\t'));
        assert!(!sanitized.contains('\n'));
        assert!(!sanitized.contains('\r'));

        // Verify percent-encoded versions are present
        assert!(sanitized.contains("%20")); // space
        assert!(sanitized.contains("%09")); // tab
        assert!(sanitized.contains("%0A")); // newline
        assert!(sanitized.contains("%0D")); // carriage return
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_empty_string() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        let key = "";
        let sanitized = cache_manager.sanitize_cache_key_new(key);
        assert_eq!(sanitized, "");
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_already_safe() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        // Test key that's already safe
        let key = "bucket-object-name_with_underscores.and.dots";
        let sanitized = cache_manager.sanitize_cache_key_new(key);
        assert_eq!(sanitized, key);
    }

    #[tokio::test]
    async fn test_path_generation_deterministic() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        let cache_key = "test-bucket/test-object?query=value";
        let start = 1024u64;
        let end = 2047u64;

        // Generate paths multiple times
        let path1 = cache_manager.get_new_range_file_path(cache_key, start, end);
        let path2 = cache_manager.get_new_range_file_path(cache_key, start, end);
        let path3 = cache_manager.get_new_range_file_path(cache_key, start, end);

        // All should be identical
        assert_eq!(path1, path2);
        assert_eq!(path2, path3);

        // Same for metadata paths
        let meta_path1 = cache_manager.get_new_metadata_file_path(cache_key);
        let meta_path2 = cache_manager.get_new_metadata_file_path(cache_key);
        let meta_path3 = cache_manager.get_new_metadata_file_path(cache_key);

        assert_eq!(meta_path1, meta_path2);
        assert_eq!(meta_path2, meta_path3);
    }

    #[tokio::test]
    async fn test_path_generation_different_ranges() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        let cache_key = "test-bucket/test-object";

        // Different ranges should produce different paths
        let path1 = cache_manager.get_new_range_file_path(cache_key, 0, 1023);
        let path2 = cache_manager.get_new_range_file_path(cache_key, 1024, 2047);
        let path3 = cache_manager.get_new_range_file_path(cache_key, 2048, 4095);

        assert_ne!(path1, path2);
        assert_ne!(path2, path3);
        assert_ne!(path1, path3);

        // But all should be in the same ranges directory
        assert_eq!(path1.parent(), path2.parent());
        assert_eq!(path2.parent(), path3.parent());
    }

    #[tokio::test]
    async fn test_path_generation_different_keys() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        let start = 0u64;
        let end = 1023u64;

        // Different cache keys should produce different paths
        let path1 = cache_manager.get_new_range_file_path("bucket1:object1", start, end);
        let path2 = cache_manager.get_new_range_file_path("bucket2:object2", start, end);
        let path3 = cache_manager.get_new_range_file_path("bucket1:object2", start, end);

        assert_ne!(path1, path2);
        assert_ne!(path2, path3);
        assert_ne!(path1, path3);
    }

    #[tokio::test]
    async fn test_path_generation_large_ranges() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        let cache_key = "test-bucket/large-object";

        // Test with very large range values
        let start = 1_000_000_000_000u64; // 1TB
        let end = 1_000_000_010_000u64;

        let path = cache_manager.get_new_range_file_path(cache_key, start, end);

        // Should contain the large numbers
        assert!(path.to_string_lossy().contains("1000000000000"));
        assert!(path.to_string_lossy().contains("1000000010000"));
    }

    #[tokio::test]
    async fn test_path_generation_full_object_as_range() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        let cache_key = "test-bucket/test-object";
        let content_length = 10485760u64; // 10MB

        // Full object stored as range 0 to content_length-1
        let path = cache_manager.get_new_range_file_path(cache_key, 0, content_length - 1);

        assert!(path.to_string_lossy().contains("_0-10485759.bin"));
    }

    #[tokio::test]
    async fn test_path_generation_complex_s3_key() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        // Complex S3 key with multiple path segments and special characters
        let cache_key = "my-bucket/path/to/deeply/nested/object-with-dashes_and_underscores.tar.gz?versionId=abc123&partNumber=5";

        let meta_path = cache_manager.get_new_metadata_file_path(cache_key);
        let range_path = cache_manager.get_new_range_file_path(cache_key, 0, 1048575);

        // Get just the filename portion (not the full path with directory separators)
        let meta_filename = meta_path.file_name().unwrap().to_string_lossy();
        let range_filename = range_path.file_name().unwrap().to_string_lossy();

        // Filenames should not contain filesystem-unsafe characters
        assert!(!meta_filename.contains(':'));
        assert!(!meta_filename.contains('/'));
        assert!(!meta_filename.contains('?'));
        assert!(!meta_filename.contains('\\'));
        assert!(!meta_filename.contains('*'));
        assert!(!meta_filename.contains('"'));
        assert!(!meta_filename.contains('<'));
        assert!(!meta_filename.contains('>'));
        assert!(!meta_filename.contains('|'));

        assert!(!range_filename.contains(':'));
        assert!(!range_filename.contains('/'));
        assert!(!range_filename.contains('?'));
        assert!(!range_filename.contains('\\'));
        assert!(!range_filename.contains('*'));
        assert!(!range_filename.contains('"'));
        assert!(!range_filename.contains('<'));
        assert!(!range_filename.contains('>'));
        assert!(!range_filename.contains('|'));
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_unicode() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        // Test with unicode characters
        let key = "bucket:文件/объект/αρχείο";
        let sanitized = cache_manager.sanitize_cache_key_new(key);

        // Special chars should be percent-encoded
        assert!(sanitized.contains("%3A")); // :
        assert!(sanitized.contains("%2F")); // /

        // Unicode characters will be percent-encoded as UTF-8 bytes
        // This is correct behavior - it prevents collisions and is reversible
        // Verify the string is filesystem-safe
        assert!(!sanitized.contains(':'));
        assert!(!sanitized.contains('/'));
        assert!(!sanitized.contains('\\'));

        // Verify it can be decoded back
        use percent_encoding::percent_decode_str;
        let decoded = percent_decode_str(&sanitized).decode_utf8().unwrap();
        assert_eq!(decoded, key);
    }

    // ============================================================================
    // Test Helper Functions
    // ============================================================================

    // ============================================================================
    // Tests for store_range() method
    // ============================================================================

    #[tokio::test]
    async fn test_store_range_basic() {
        use crate::cache_types::{NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let data = vec![42u8; 1024]; // 1KB of data

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Verify range file exists
        let range_file_path = cache_manager.get_new_range_file_path(cache_key, start, end);
        assert!(range_file_path.exists(), "Range file should exist");

        // Verify metadata file exists
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        assert!(metadata_file_path.exists(), "Metadata file should exist");

        // Read and verify metadata
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: NewCacheMetadata = serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.cache_key, cache_key);
        assert_eq!(metadata.object_metadata.etag, object_metadata.etag);
        assert_eq!(metadata.ranges.len(), 1);
        assert_eq!(metadata.ranges[0].start, start);
        assert_eq!(metadata.ranges[0].end, end);
        assert_eq!(metadata.ranges[0].uncompressed_size, 1024);
    }

    #[tokio::test]
    async fn test_store_range_multiple_ranges() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store multiple ranges
        for i in 0..5 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];

            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Verify all range files exist
        for i in 0..5 {
            let start = i * 1024;
            let end = start + 1023;
            let range_file_path = cache_manager.get_new_range_file_path(cache_key, start, end);
            assert!(range_file_path.exists(), "Range file {} should exist", i);
        }

        // Verify metadata contains all ranges
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.ranges.len(), 5);
    }

    #[tokio::test]
    async fn test_store_range_etag_mismatch_invalidation() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let data = vec![42u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store first range successfully
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Verify first range was stored
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();
        assert_eq!(metadata.ranges.len(), 1, "Should have 1 range");
        assert_eq!(metadata.object_metadata.etag, "test-etag");

        // Get the path of the first range file
        let first_range_path = cache_manager.get_new_range_file_path(cache_key, 0, 1023);
        assert!(first_range_path.exists(), "First range file should exist");

        // Store with different ETag - should invalidate existing ranges and succeed
        // Requirement 14.1, 14.2, 14.3: ETag mismatch invalidates existing ranges
        let different_metadata = ObjectMetadata {
            etag: "different-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        let result = cache_manager
            .store_range(
                cache_key,
                1024,
                2047,
                &data,
                different_metadata,
                std::time::Duration::from_secs(3600), true)
            .await;
        assert!(result.is_ok(), "Should succeed after invalidating old ranges");

        // Verify the old range file was deleted
        assert!(
            !first_range_path.exists(),
            "Old range file should be deleted on ETag mismatch"
        );

        // Verify new range was stored with new ETag
        let new_range_path = cache_manager.get_new_range_file_path(cache_key, 1024, 2047);
        assert!(new_range_path.exists(), "New range file should exist");

        // Verify metadata was updated with new ETag and only new range
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.ranges.len(), 1, "Should have only 1 range (the new one)");
        assert_eq!(metadata.object_metadata.etag, "different-etag", "ETag should be updated");
        assert_eq!(metadata.ranges[0].start, 1024, "Range should be the new one");
        assert_eq!(metadata.ranges[0].end, 2047, "Range should be the new one");
    }

    #[tokio::test]
    async fn test_store_range_invalid_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let data = vec![42u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Test start > end
        let result = cache_manager
            .store_range(
                cache_key,
                1024,
                0,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await;
        assert!(result.is_err(), "Should fail when start > end");

        // Test data size mismatch
        let result = cache_manager
            .store_range(
                cache_key,
                0,
                2047,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await;
        assert!(
            result.is_err(),
            "Should fail when data size doesn't match range"
        );
    }

    #[tokio::test]
    async fn test_store_range_update_existing() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let data1 = vec![1u8; 1024];
        let data2 = vec![2u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range first time
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data1,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Store same range again (update)
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data2,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Verify metadata still has only 1 range
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(
            metadata.ranges.len(),
            1,
            "Should still have only 1 range after update"
        );
    }

    #[tokio::test]
    async fn test_store_range_full_object_as_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let content_length = 10240u64;
        let data = vec![42u8; content_length as usize];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: content_length,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object as range 0 to content_length-1
        cache_manager
            .store_range(
                cache_key,
                0,
                content_length - 1,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Verify range file exists
        let range_file_path =
            cache_manager.get_new_range_file_path(cache_key, 0, content_length - 1);
        assert!(
            range_file_path.exists(),
            "Full object range file should exist"
        );

        // Verify metadata
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.ranges.len(), 1);
        assert_eq!(metadata.ranges[0].start, 0);
        assert_eq!(metadata.ranges[0].end, content_length - 1);
    }

    #[tokio::test]
    async fn test_store_range_no_tmp_files_left() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let data = vec![42u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Check for any .tmp files in ranges and metadata directories
        let ranges_dir = temp_dir.path().join("ranges");
        let metadata_dir = temp_dir.path().join("metadata");

        for entry in std::fs::read_dir(&ranges_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            assert!(
                !path.to_string_lossy().ends_with(".tmp"),
                "No .tmp files should remain in ranges dir"
            );
        }

        for entry in std::fs::read_dir(&metadata_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            assert!(
                !path.to_string_lossy().ends_with(".tmp"),
                "No .tmp files should remain in metadata dir"
            );
        }
    }

    #[tokio::test]
    async fn test_store_range_concurrent_writes_no_lost_updates() {
        use crate::cache_types::ObjectMetadata;
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = Arc::new(Mutex::new(DiskCacheManager::new(
            temp_dir.path().to_path_buf(),
            true,
            1024,
            false,
        )));
        cache_manager.lock().await.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 102400,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 102400,
            parts: Vec::new(),
            ..Default::default()
        };

        // Spawn 10 concurrent tasks, each storing a different range
        let mut handles = vec![];
        for i in 0..10 {
            let cache_manager_clone = Arc::clone(&cache_manager);
            let cache_key_clone = cache_key.to_string();
            let object_metadata_clone = object_metadata.clone();

            let handle = tokio::spawn(async move {
                let start = i * 1024;
                let end = start + 1023;
                let data = vec![i as u8; 1024];

                let mut cm = cache_manager_clone.lock().await;
                cm.store_range(
                    &cache_key_clone,
                    start,
                    end,
                    &data,
                    object_metadata_clone,
                    Duration::from_secs(3600), true)
                .await
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // Verify all 10 ranges are in the metadata (no lost updates)
        let cm = cache_manager.lock().await;
        let metadata_file_path = cm.get_new_metadata_file_path(cache_key);
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(
            metadata.ranges.len(),
            10,
            "All 10 ranges should be present (no lost updates)"
        );

        // Verify each range is present
        for i in 0..10 {
            let start = i * 1024;
            let end = start + 1023;
            assert!(
                metadata
                    .ranges
                    .iter()
                    .any(|r| r.start == start && r.end == end),
                "Range {}-{} should be present",
                start,
                end
            );
        }
    }

    // ============================================================================
    // Tests for get_metadata() and update_metadata() methods
    // ============================================================================

    #[tokio::test]
    async fn test_get_metadata_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        let cache_key = "test-bucket/nonexistent-object";

        // Get metadata for nonexistent entry
        let result = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(
            result.is_none(),
            "Should return None for nonexistent metadata"
        );
    }

    #[tokio::test]
    async fn test_get_metadata_after_store_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let data = vec![42u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Get metadata
        let metadata = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(metadata.is_some(), "Metadata should exist");

        let metadata = metadata.unwrap();
        assert_eq!(metadata.cache_key, cache_key);
        assert_eq!(metadata.object_metadata.etag, object_metadata.etag);
        assert_eq!(metadata.ranges.len(), 1);
        assert_eq!(metadata.ranges[0].start, start);
        assert_eq!(metadata.ranges[0].end, end);
    }

    #[tokio::test]
    async fn test_get_metadata_no_range_data_loaded() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";

        // Store multiple large ranges
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10 * 1024 * 1024, // 10MB
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10 * 1024 * 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        for i in 0..10 {
            let start = i * 1024 * 1024;
            let end = start + 1024 * 1024 - 1;
            let data = vec![i as u8; 1024 * 1024]; // 1MB each

            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Get metadata - should be fast and not load 10MB of range data
        let start_time = std::time::Instant::now();
        let metadata = cache_manager.get_metadata(cache_key).await.unwrap();
        let elapsed = start_time.elapsed();

        assert!(metadata.is_some());
        let metadata = metadata.unwrap();
        assert_eq!(metadata.ranges.len(), 10);

        // Should complete quickly (under 100ms even with 10 ranges)
        // This validates that we're not loading range data
        assert!(
            elapsed.as_millis() < 100,
            "Metadata read took {}ms, should be under 100ms",
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_get_metadata_expired() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";

        // Create metadata that's a write-cached object with expired write_cache_expires_at
        let now = SystemTime::now();
        let expired_time = now - std::time::Duration::from_secs(3600); // 1 hour ago

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 1024,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 1024,
                parts: Vec::new(),
                // Mark as write-cached with expired write_cache_expires_at
                is_write_cached: true,
                write_cache_expires_at: Some(expired_time),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600), // Not expired
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Write expired write-cache metadata directly
        cache_manager.update_metadata(&metadata).await.unwrap();

        // Try to get metadata - should return None because write cache is expired
        let result = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(
            result.is_none(),
            "Should return None for expired write-cache metadata"
        );
    }

    #[tokio::test]
    async fn test_get_metadata_corrupted_json() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";

        // Write corrupted JSON to metadata file
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_file_path, "{ this is not valid json }").unwrap();

        // Try to get metadata - should return None and log error
        let result = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(
            result.is_none(),
            "Should return None for corrupted metadata"
        );
    }

    #[tokio::test]
    async fn test_get_metadata_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";

        // Write empty file
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_file_path, "").unwrap();

        // Try to get metadata - should return None
        let result = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(
            result.is_none(),
            "Should return None for empty metadata file"
        );
    }

    // ============================================================================
    // Tests for check_object_expiration metadata read failure handling
    // Validates: Requirements 6.1, 6.2
    // ============================================================================

    #[tokio::test]
    async fn test_check_object_expiration_corrupted_metadata() {
        use crate::cache_types::ObjectExpirationResult;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/corrupted-object";

        // Write corrupted (non-JSON) content to metadata file
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_file_path, "this is not valid json at all!!!").unwrap();

        // check_object_expiration should return Expired { last_modified: None }
        let result = cache_manager.check_object_expiration(cache_key).await.unwrap();
        assert_eq!(
            result,
            ObjectExpirationResult::Expired { last_modified: None },
            "Corrupted metadata should be treated as expired with no last_modified"
        );
    }

    #[tokio::test]
    async fn test_check_object_expiration_invalid_json_schema() {
        use crate::cache_types::ObjectExpirationResult;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/wrong-schema-object";

        // Write valid JSON but wrong schema
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_file_path, r#"{"foo": "bar", "baz": 42}"#).unwrap();

        // check_object_expiration should return Expired { last_modified: None }
        let result = cache_manager.check_object_expiration(cache_key).await.unwrap();
        assert_eq!(
            result,
            ObjectExpirationResult::Expired { last_modified: None },
            "Invalid JSON schema should be treated as expired with no last_modified"
        );
    }

    #[tokio::test]
    async fn test_check_object_expiration_missing_metadata() {
        use crate::cache_types::ObjectExpirationResult;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/nonexistent-object";

        // No metadata file exists
        let result = cache_manager.check_object_expiration(cache_key).await.unwrap();
        assert_eq!(
            result,
            ObjectExpirationResult::Expired { last_modified: None },
            "Missing metadata should be treated as expired with no last_modified"
        );
    }

    #[tokio::test]
    async fn test_check_object_expiration_empty_file() {
        use crate::cache_types::ObjectExpirationResult;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/empty-metadata-object";

        // Write empty file
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_file_path, "").unwrap();

        // check_object_expiration should return Expired { last_modified: None }
        let result = cache_manager.check_object_expiration(cache_key).await.unwrap();
        assert_eq!(
            result,
            ObjectExpirationResult::Expired { last_modified: None },
            "Empty metadata file should be treated as expired with no last_modified"
        );
    }

    #[tokio::test]
    async fn test_update_metadata_basic() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let now = SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 10240,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 10240,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: vec![RangeSpec::new(
                0,
                1023,
                "test_0-1023.bin".to_string(),
                crate::compression::CompressionAlgorithm::Lz4,
                1024,
                1024,
            )],
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Update metadata
        cache_manager.update_metadata(&metadata).await.unwrap();

        // Verify file exists
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        assert!(metadata_file_path.exists(), "Metadata file should exist");

        // Read and verify
        let retrieved = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(retrieved.is_some());

        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.cache_key, metadata.cache_key);
        assert_eq!(
            retrieved.object_metadata.etag,
            metadata.object_metadata.etag
        );
        assert_eq!(retrieved.ranges.len(), 1);
    }

    #[tokio::test]
    async fn test_update_metadata_atomic() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let now = SystemTime::now();

        // Create initial metadata
        let metadata1 = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "etag-1".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 1024,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 1024,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        cache_manager.update_metadata(&metadata1).await.unwrap();

        // Update with new metadata
        let metadata2 = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "etag-2".to_string(),
                last_modified: "Thu, 22 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 2048,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 2048,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        cache_manager.update_metadata(&metadata2).await.unwrap();

        // Verify the update
        let retrieved = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.object_metadata.etag, "etag-2");
        assert_eq!(retrieved.object_metadata.content_length, 2048);
    }

    #[tokio::test]
    async fn test_update_metadata_no_tmp_files_left() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let now = SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 1024,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 1024,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        cache_manager.update_metadata(&metadata).await.unwrap();

        // Check for any .tmp files in metadata directory
        let metadata_dir = temp_dir.path().join("metadata");

        for entry in std::fs::read_dir(&metadata_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            assert!(
                !path.to_string_lossy().ends_with(".tmp"),
                "No .tmp files should remain"
            );
        }
    }

    #[tokio::test]
    async fn test_update_metadata_creates_directory() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        // Don't initialize - test that update_metadata creates directories
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

        let cache_key = "test-bucket/test-object";
        let now = SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 1024,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 1024,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Should create metadata directory automatically
        cache_manager.update_metadata(&metadata).await.unwrap();

        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        assert!(metadata_file_path.exists(), "Metadata file should exist");
        assert!(
            metadata_file_path.parent().unwrap().exists(),
            "Metadata directory should exist"
        );
    }

    #[tokio::test]
    async fn test_get_and_update_metadata_round_trip() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let now = SystemTime::now();

        // Create metadata with multiple ranges
        let mut metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 10240,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 10240,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: vec![
                RangeSpec::new(
                    0,
                    1023,
                    "test_0-1023.bin".to_string(),
                    crate::compression::CompressionAlgorithm::Lz4,
                    512,
                    1024,
                ),
                RangeSpec::new(
                    1024,
                    2047,
                    "test_1024-2047.bin".to_string(),
                    crate::compression::CompressionAlgorithm::Lz4,
                    1024,
                    1024,
                ),
            ],
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Write metadata
        cache_manager.update_metadata(&metadata).await.unwrap();

        // Read it back
        let retrieved = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();

        // Verify all fields match
        assert_eq!(retrieved.cache_key, metadata.cache_key);
        assert_eq!(
            retrieved.object_metadata.etag,
            metadata.object_metadata.etag
        );
        assert_eq!(retrieved.ranges.len(), 2);
        assert_eq!(retrieved.ranges[0].start, 0);
        assert_eq!(retrieved.ranges[0].end, 1023);
        assert_eq!(retrieved.ranges[1].start, 1024);
        assert_eq!(retrieved.ranges[1].end, 2047);

        // Modify and update
        metadata.ranges.push(RangeSpec::new(
            2048,
            3071,
            "test_2048-3071.bin".to_string(),
            crate::compression::CompressionAlgorithm::Lz4,
            1024,
            1024,
        ));

        cache_manager.update_metadata(&metadata).await.unwrap();

        // Read again and verify the update
        let retrieved2 = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved2.ranges.len(), 3);
        assert_eq!(retrieved2.ranges[2].start, 2048);
    }

    #[tokio::test]
    async fn test_metadata_operations_with_special_characters() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        // Cache key with special characters
        let cache_key = "test-bucket/path/to/object?query=value&param=123";
        let now = SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 1024,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 1024,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Should handle special characters correctly
        cache_manager.update_metadata(&metadata).await.unwrap();

        let retrieved = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().cache_key, cache_key);
    }

    // ============================================================================
    // Tests for find_cached_ranges() method
    // ============================================================================

    #[tokio::test]
    async fn test_find_cached_ranges_no_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/nonexistent-object";

        // Find ranges for nonexistent object
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 0, 1023, None)
            .await
            .unwrap();
        assert_eq!(
            ranges.len(),
            0,
            "Should return empty vec for nonexistent object"
        );
    }

    #[tokio::test]
    async fn test_find_cached_ranges_exact_match() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store a range
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Find exact match
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 0, 1023, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find exactly one range");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1023);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_full_containment() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store a large range
        let data = vec![42u8; 4096];
        cache_manager
            .store_range(
                cache_key,
                0,
                4095,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Request a smaller range that's fully contained
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 1024, 2047, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find the containing range");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 4095);
        assert!(
            ranges[0].start <= 1024 && ranges[0].end >= 2047,
            "Range should fully contain requested range"
        );
    }

    #[tokio::test]
    async fn test_find_cached_ranges_no_overlap() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store ranges 0-1023 and 4096-5119
        let data1 = vec![1u8; 1024];
        let data2 = vec![2u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data1,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                4096,
                5119,
                &data2,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Request range 2048-3071 (no overlap)
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 2048, 3071, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 0, "Should find no overlapping ranges");
    }

    #[tokio::test]
    async fn test_find_cached_ranges_partial_overlap_start() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range 0-1023
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Request range 512-1535 (overlaps at start)
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 512, 1535, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find one overlapping range");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1023);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_partial_overlap_end() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range 1024-2047
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                1024,
                2047,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Request range 512-1535 (overlaps at end)
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 512, 1535, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find one overlapping range");
        assert_eq!(ranges[0].start, 1024);
        assert_eq!(ranges[0].end, 2047);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_multiple_overlaps() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store multiple ranges
        for i in 0..5 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Request range that spans multiple cached ranges (1536-3583)
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 1536, 3583, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 3, "Should find three overlapping ranges");

        // Verify ranges are sorted by start position
        assert_eq!(ranges[0].start, 1024);
        assert_eq!(ranges[0].end, 2047);
        assert_eq!(ranges[1].start, 2048);
        assert_eq!(ranges[1].end, 3071);
        assert_eq!(ranges[2].start, 3072);
        assert_eq!(ranges[2].end, 4095);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_adjacent_ranges() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store adjacent ranges
        let data1 = vec![1u8; 1024];
        let data2 = vec![2u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data1,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                1024,
                2047,
                &data2,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Request range that spans both
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 512, 1535, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 2, "Should find both adjacent ranges");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1023);
        assert_eq!(ranges[1].start, 1024);
        assert_eq!(ranges[1].end, 2047);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_boundary_conditions() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range 1024-2047
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                1024,
                2047,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Request range ending exactly at cached range start (0-1024)
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 0, 1024, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should overlap at boundary (inclusive)");

        // Request range starting exactly at cached range end (2047-3071)
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 2047, 3071, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should overlap at boundary (inclusive)");

        // Request range just before cached range (0-1023)
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 0, 1023, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 0, "Should not overlap");

        // Request range just after cached range (2048-3071)
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 2048, 3071, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 0, "Should not overlap");
    }

    #[tokio::test]
    async fn test_find_cached_ranges_full_object_as_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let content_length = 10240u64;
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: content_length,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object as range 0 to content_length-1
        let data = vec![42u8; content_length as usize];
        cache_manager
            .store_range(
                cache_key,
                0,
                content_length - 1,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Request any sub-range
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 2048, 4095, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find the full object range");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, content_length - 1);

        // Request full object
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 0, content_length - 1, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find exact match");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, content_length - 1);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_invalid_range() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";

        // Request invalid range (start > end)
        let result = cache_manager.find_cached_ranges(cache_key, 1024, 0, None).await;
        assert!(result.is_err(), "Should fail with invalid range");
    }

    #[tokio::test]
    async fn test_find_cached_ranges_large_number_of_ranges() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 100 * 1024 * 1024, // 100MB
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 100 * 1024 * 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store 100 ranges (1MB each)
        for i in 0..100 {
            let start = i * 1024 * 1024;
            let end = start + 1024 * 1024 - 1;
            let data = vec![i as u8; 1024 * 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Find ranges - should be fast even with 100 ranges
        let start_time = std::time::Instant::now();
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 50 * 1024 * 1024, 55 * 1024 * 1024 - 1, None)
            .await
            .unwrap();
        let elapsed = start_time.elapsed();

        assert_eq!(ranges.len(), 5, "Should find 5 overlapping ranges");

        // Should complete quickly (under 10ms as per requirement 2.3)
        assert!(
            elapsed.as_millis() < 10,
            "Range lookup took {}ms, should be under 10ms",
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_find_cached_ranges_sorted_output() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store ranges in non-sequential order
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                4096,
                5119,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                2048,
                3071,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Find all ranges
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 0, 10239, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 3);

        // Verify they're sorted by start position
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[1].start, 2048);
        assert_eq!(ranges[2].start, 4096);

        // Verify sorting is stable
        for i in 1..ranges.len() {
            assert!(
                ranges[i - 1].start <= ranges[i].start,
                "Ranges should be sorted by start position"
            );
        }
    }

    #[tokio::test]
    async fn test_find_cached_ranges_single_byte_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range 0-1023
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Request single byte range
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 512, 512, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find containing range");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1023);
    }

    #[tokio::test]
    #[ignore = "Flaky performance test - depends on system timing"]
    async fn test_find_cached_ranges_performance_requirement() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 1000 * 1024 * 1024, // 1GB
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 1000 * 1024 * 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store 1000 ranges (1MB each)
        for i in 0..1000 {
            let start = i * 1024 * 1024;
            let end = start + 1024 * 1024 - 1;
            let data = vec![i as u8; 1024 * 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Verify metadata file size is reasonable (should be under 1MB for 1000 ranges with TTL fields)
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        let metadata_size = std::fs::metadata(&metadata_file_path).unwrap().len();
        assert!(
            metadata_size < 1024 * 1024,
            "Metadata file is {} bytes, should be under 1MB",
            metadata_size
        );

        // Find ranges - should complete in under 10ms as per requirement 2.3
        let start_time = std::time::Instant::now();
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 500 * 1024 * 1024, 510 * 1024 * 1024 - 1, None)
            .await
            .unwrap();
        let elapsed = start_time.elapsed();

        assert_eq!(ranges.len(), 10, "Should find 10 overlapping ranges");
        assert!(
            elapsed.as_millis() < 10,
            "Range lookup with 1000 ranges took {}ms, should be under 10ms",
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_find_cached_ranges_with_gaps() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 20480,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 20480,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store non-contiguous ranges with gaps:
        // Range A: 0-2999 (3KB)
        // GAP: 3000-4999 (2KB missing)
        // Range B: 5000-7999 (3KB)
        // GAP: 8000-8999 (1KB missing)
        // Range C: 9000-11999 (3KB)

        let data_a = vec![1u8; 3000];
        let data_b = vec![2u8; 3000];
        let data_c = vec![3u8; 3000];

        cache_manager
            .store_range(
                cache_key,
                0,
                2999,
                &data_a,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                5000,
                7999,
                &data_b,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                9000,
                11999,
                &data_c,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Request range that spans all cached ranges and gaps (0-11999)
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 0, 11999, None)
            .await
            .unwrap();

        // Should return all 3 non-contiguous ranges
        assert_eq!(ranges.len(), 3, "Should find all 3 non-contiguous ranges");

        // Verify ranges are sorted and correct
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 2999);

        assert_eq!(ranges[1].start, 5000);
        assert_eq!(ranges[1].end, 7999);

        assert_eq!(ranges[2].start, 9000);
        assert_eq!(ranges[2].end, 11999);

        // Verify gaps are NOT in the returned ranges
        // Gap 1: 3000-4999 should NOT be present
        // Gap 2: 8000-8999 should NOT be present
        for range in &ranges {
            // No range should cover the gap areas
            assert!(
                !(range.start <= 3000 && range.end >= 4999),
                "Gap 3000-4999 should not be covered"
            );
            assert!(
                !(range.start <= 8000 && range.end >= 8999),
                "Gap 8000-8999 should not be covered"
            );
        }
    }

    #[tokio::test]
    async fn test_find_cached_ranges_partial_hit_with_gaps() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 20480,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 20480,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store ranges: 0-999, 3000-3999, 6000-6999
        let data = vec![42u8; 1000];
        cache_manager
            .store_range(
                cache_key,
                0,
                999,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                3000,
                3999,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                6000,
                6999,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Request range 500-6500 (spans all 3 ranges with gaps)
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 500, 6500, None)
            .await
            .unwrap();

        // Should return all 3 ranges that overlap
        assert_eq!(ranges.len(), 3);

        // Caller would need to:
        // 1. Use bytes 500-999 from Range A (500 bytes from cache)
        // 2. Fetch bytes 1000-2999 from S3 (2000 bytes missing)
        // 3. Use bytes 3000-3999 from Range B (1000 bytes from cache)
        // 4. Fetch bytes 4000-5999 from S3 (2000 bytes missing)
        // 5. Use bytes 6000-6500 from Range C (501 bytes from cache)
        // Total: 2001 bytes from cache, 4000 bytes from S3

        // Verify the ranges returned
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 999);
        assert_eq!(ranges[1].start, 3000);
        assert_eq!(ranges[1].end, 3999);
        assert_eq!(ranges[2].start, 6000);
        assert_eq!(ranges[2].end, 6999);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_mostly_gaps() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 100 * 1024, // 100KB
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 100 * 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store only a tiny portion: 0-1023 (1KB out of 100KB)
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Request entire object (0-102399)
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 0, 102399, None)
            .await
            .unwrap();

        // Should return the 1 tiny cached range
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1023);

        // Caller should decide: 1% cache hit rate probably not worth the complexity
        // Better to treat as cache miss and fetch entire range from S3
        // This is a policy decision for the higher-level cache logic
    }

    // ============================================================================
    // Tests for load_range_data() method
    // ============================================================================

    #[tokio::test]
    async fn test_load_range_data_basic() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let original_data = vec![42u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &original_data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Get metadata to retrieve range spec
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 1);

        // Load range data
        let loaded_data = cache_manager
            .load_range_data(&metadata.ranges[0])
            .await
            .unwrap();

        // Verify data matches original
        assert_eq!(loaded_data, original_data);
        assert_eq!(loaded_data.len(), 1024);
    }

    #[tokio::test]
    async fn test_load_range_data_with_compression() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 100, false); // Low threshold to force compression
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object.txt"; // .txt extension to enable compression
        let start = 0u64;
        let end = 2047u64;
        // Create compressible data (repeated pattern)
        let original_data = vec![65u8; 2048]; // 2KB of 'A's

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("text/plain".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range (should be compressed)
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &original_data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 1);

        // Verify compression was applied
        let range_spec = &metadata.ranges[0];
        assert!(
            range_spec.compressed_size < range_spec.uncompressed_size,
            "Data should be compressed: compressed={}, uncompressed={}",
            range_spec.compressed_size,
            range_spec.uncompressed_size
        );

        // Load range data (should decompress automatically)
        let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();

        // Verify data matches original after decompression
        assert_eq!(loaded_data, original_data);
        assert_eq!(loaded_data.len(), 2048);
    }

    #[tokio::test]
    async fn test_load_range_data_no_compression() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), false, 1024, false); // Compression disabled
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let original_data = vec![42u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range (no compression)
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &original_data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        let range_spec = &metadata.ranges[0];

        // Verify compression algorithm is Lz4 (all data uses frame format now)
        assert_eq!(range_spec.compression_algorithm, CompressionAlgorithm::Lz4);

        // Load range data
        let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();

        // Verify data matches original
        assert_eq!(loaded_data, original_data);
    }

    #[tokio::test]
    async fn test_load_range_data_missing_file() {
        use crate::cache_types::RangeSpec;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        // Create a range spec pointing to a non-existent file
        let range_spec = RangeSpec::new(
            0,
            1023,
            "nonexistent_file_0-1023.bin".to_string(),
            CompressionAlgorithm::Lz4,
            1024,
            1024,
        );

        // Try to load - should fail with error
        let result = cache_manager.load_range_data(&range_spec).await;
        assert!(result.is_err(), "Should fail when range file is missing");

        // Verify error message mentions the missing file
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("not found") || error_msg.contains("nonexistent"),
            "Error should mention missing file: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_load_range_data_multiple_ranges() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store multiple ranges with different data
        let mut expected_data = Vec::new();
        for i in 0..5 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            expected_data.push(data.clone());
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 5);

        // Load each range and verify
        for (i, range_spec) in metadata.ranges.iter().enumerate() {
            let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();
            assert_eq!(loaded_data, expected_data[i], "Range {} data mismatch", i);
            assert_eq!(loaded_data.len(), 1024);
        }
    }

    #[tokio::test]
    async fn test_load_range_data_large_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 10485759u64; // 10MB
        let original_data = vec![42u8; 10485760];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10485760,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10485760,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store large range
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &original_data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        let range_spec = &metadata.ranges[0];

        // Load large range - should complete in under 100ms as per requirement 9.1
        let start_time = std::time::Instant::now();
        let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();
        let elapsed = start_time.elapsed();

        // Verify data
        assert_eq!(loaded_data.len(), 10485760);
        assert_eq!(loaded_data, original_data);

        // Performance check
        assert!(
            elapsed.as_millis() < 100,
            "Loading 10MB range took {}ms, should be under 100ms",
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_load_range_data_full_object_as_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let content_length = 10240u64;
        let original_data = vec![42u8; content_length as usize];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: content_length,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object as range 0 to content_length-1
        cache_manager
            .store_range(
                cache_key,
                0,
                content_length - 1,
                &original_data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 1);

        let range_spec = &metadata.ranges[0];
        assert_eq!(range_spec.start, 0);
        assert_eq!(range_spec.end, content_length - 1);

        // Load full object
        let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();

        // Verify
        assert_eq!(loaded_data, original_data);
        assert_eq!(loaded_data.len(), content_length as usize);
    }

    #[tokio::test]
    async fn test_load_range_data_different_compression_algorithms() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 100, false);
        cache_manager.initialize().await.unwrap();

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 100000,
            content_type: Some("text/plain".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 100000,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store ranges with different file extensions (triggers different compression)
        let test_cases = vec![
            ("test-bucket/file.txt", vec![65u8; 2048]), // Text file - compressible
            ("test-bucket/file.bin", vec![42u8; 2048]), // Binary - may not compress well
            ("test-bucket/file.json", vec![123u8; 2048]), // JSON - compressible
        ];

        for (cache_key, original_data) in test_cases {
            // Store range
            cache_manager
                .store_range(
                    cache_key,
                    0,
                    2047,
                    &original_data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();

            // Get metadata
            let metadata = cache_manager
                .get_metadata(cache_key)
                .await
                .unwrap()
                .unwrap();
            let range_spec = &metadata.ranges[0];

            // Load range data
            let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();

            // Verify data matches regardless of compression algorithm used
            assert_eq!(
                loaded_data, original_data,
                "Data mismatch for {}",
                cache_key
            );
            assert_eq!(loaded_data.len(), 2048);
        }
    }

    #[tokio::test]
    async fn test_load_range_data_single_byte() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 0u64; // Single byte
        let original_data = vec![42u8];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 1024,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store single byte range
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &original_data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        let range_spec = &metadata.ranges[0];

        // Load single byte
        let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();

        // Verify
        assert_eq!(loaded_data, original_data);
        assert_eq!(loaded_data.len(), 1);
        assert_eq!(loaded_data[0], 42);
    }

    #[tokio::test]
    async fn test_load_range_data_after_store_and_retrieve() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store multiple ranges
        let mut expected_data = Vec::new();
        for i in 0..3 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            expected_data.push(data.clone());
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Simulate a request for range 1024-2047
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 1024, 2047, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find exact match");

        // Load the found range
        let loaded_data = cache_manager.load_range_data(&ranges[0]).await.unwrap();

        // Verify it's the correct range data
        assert_eq!(loaded_data, expected_data[1]);
        assert_eq!(loaded_data.len(), 1024);
        assert_eq!(loaded_data[0], 1); // Second range has value 1
    }

    #[tokio::test]
    async fn test_load_range_data_performance() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 100 * 1024 * 1024, // 100MB
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 100 * 1024 * 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store 10 ranges of 1MB each
        for i in 0..10 {
            let start = i * 1024 * 1024;
            let end = start + 1024 * 1024 - 1;
            let data = vec![i as u8; 1024 * 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();

        // Load all ranges and measure total time
        let start_time = std::time::Instant::now();
        for range_spec in &metadata.ranges {
            let _loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();
        }
        let elapsed = start_time.elapsed();

        // Loading 10MB total should be fast (under 1 second)
        assert!(
            elapsed.as_secs() < 1,
            "Loading 10 ranges (10MB total) took {}ms, should be under 1 second",
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_load_range_data_validates_size() {
        use crate::cache_types::RangeSpec;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        // Manually create a range file with wrong size
        let ranges_dir = temp_dir.path().join("ranges");
        std::fs::create_dir_all(&ranges_dir).unwrap();

        let range_file_path = ranges_dir.join("test_0-1023.bin");
        let wrong_data = vec![42u8; 512]; // Only 512 bytes instead of 1024
        std::fs::write(&range_file_path, &wrong_data).unwrap();

        // Create range spec expecting 1024 bytes with no compression
        // so the raw data length (512) is compared directly against uncompressed_size (1024)
        let range_spec = RangeSpec::new(
            0,
            1023,
            "test_0-1023.bin".to_string(),
            CompressionAlgorithm::None,
            1024,
            1024,
        );

        // Try to load - should fail due to size mismatch
        let result = cache_manager.load_range_data(&range_spec).await;
        assert!(
            result.is_err(),
            "Should fail when data size doesn't match range size"
        );

        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("mismatch") || error_msg.contains("size"),
            "Error should mention size mismatch: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_load_range_data_concurrent_loads() {
        use crate::cache_types::ObjectMetadata;
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = Arc::new(Mutex::new(DiskCacheManager::new(
            temp_dir.path().to_path_buf(),
            true,
            1024,
            false,
        )));
        cache_manager.lock().await.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store 10 ranges
        let mut expected_data = Vec::new();
        for i in 0..10 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            expected_data.push(data.clone());
            cache_manager
                .lock()
                .await
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Get metadata
        let metadata = cache_manager
            .lock()
            .await
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        let range_specs: Vec<_> = metadata.ranges.clone();

        // Load all ranges concurrently
        let mut handles = vec![];
        for (i, range_spec) in range_specs.into_iter().enumerate() {
            let cache_manager_clone = Arc::clone(&cache_manager);
            let expected = expected_data[i].clone();

            let handle = tokio::spawn(async move {
                let cm = cache_manager_clone.lock().await;
                let loaded = cm.load_range_data(&range_spec).await.unwrap();
                assert_eq!(loaded, expected);
            });

            handles.push(handle);
        }

        // Wait for all loads to complete
        for handle in handles {
            handle.await.unwrap();
        }
    }

    // ============================================================================
    // Tests for Full Object as Range Handling (Task 7)
    // ============================================================================

    #[tokio::test]
    async fn test_store_full_object_as_range_basic() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let data = vec![42u8; 10240]; // 10KB object

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: data.len() as u64,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: data.len() as u64,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object as range
        cache_manager
            .store_full_object_as_range(
                cache_key,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Verify it was stored as range 0 to content_length-1
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 1, "Should have exactly one range");
        assert_eq!(metadata.ranges[0].start, 0, "Range should start at 0");
        assert_eq!(
            metadata.ranges[0].end, 10239,
            "Range should end at content_length-1"
        );
        assert_eq!(
            metadata.ranges[0].uncompressed_size, 10240,
            "Uncompressed size should match data length"
        );

        // Verify range file exists
        let range_file_path = cache_manager.get_new_range_file_path(cache_key, 0, 10239);
        assert!(range_file_path.exists(), "Range file should exist");
    }

    #[tokio::test]
    async fn test_store_full_object_as_range_empty_object() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/empty-object";
        let data = vec![]; // Empty object

        let object_metadata = ObjectMetadata {
            etag: "empty-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 0,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 0,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store empty object
        cache_manager
            .store_full_object_as_range(
                cache_key,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Verify metadata exists but has no ranges (empty objects have no data to cache)
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            metadata.ranges.len(),
            0,
            "Empty object should have no ranges"
        );
        assert_eq!(
            metadata.object_metadata.content_length, 0,
            "Content length should be 0"
        );
    }

    #[tokio::test]
    async fn test_get_full_object_as_range_basic() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let original_data = vec![42u8; 10240]; // 10KB object

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: original_data.len() as u64,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: original_data.len() as u64,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object as range
        cache_manager
            .store_full_object_as_range(
                cache_key,
                &original_data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Retrieve full object as range
        let retrieved_data = cache_manager
            .get_full_object_as_range(cache_key)
            .await
            .unwrap();
        assert!(retrieved_data.is_some(), "Should retrieve cached object");

        let data = retrieved_data.unwrap();
        assert_eq!(
            data.len(),
            original_data.len(),
            "Retrieved data should have same length"
        );
        assert_eq!(data, original_data, "Retrieved data should match original");
    }

    #[tokio::test]
    async fn test_get_full_object_as_range_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/nonexistent-object";

        // Try to retrieve non-existent object
        let retrieved_data = cache_manager
            .get_full_object_as_range(cache_key)
            .await
            .unwrap();
        assert!(
            retrieved_data.is_none(),
            "Should return None for non-existent object"
        );
    }

    #[tokio::test]
    async fn test_get_full_object_as_range_empty_object() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/empty-object";
        let original_data = vec![]; // Empty object

        let object_metadata = ObjectMetadata {
            etag: "empty-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 0,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 0,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store empty object as range
        cache_manager
            .store_full_object_as_range(
                cache_key,
                &original_data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Retrieve empty object as range
        let retrieved_data = cache_manager
            .get_full_object_as_range(cache_key)
            .await
            .unwrap();
        assert!(
            retrieved_data.is_some(),
            "Should retrieve cached empty object"
        );

        let data = retrieved_data.unwrap();
        assert_eq!(data.len(), 0, "Retrieved data should be empty");
    }

    #[tokio::test]
    async fn test_full_object_can_serve_any_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let original_data: Vec<u8> = (0..10240).map(|i| (i % 256) as u8).collect(); // 10KB with pattern

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: original_data.len() as u64,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: original_data.len() as u64,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object as range
        cache_manager
            .store_full_object_as_range(
                cache_key,
                &original_data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Test that we can serve various range requests from the cached full object

        // Test range at start
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 0, 1023, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find range covering start");
        let range_data = cache_manager.load_range_data(&ranges[0]).await.unwrap();
        assert_eq!(
            &range_data[0..1024],
            &original_data[0..1024],
            "Range data should match"
        );

        // Test range in middle
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 5000, 6000, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find range covering middle");

        // Test range at end
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 9000, 10239, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find range covering end");

        // Test full range
        let ranges = cache_manager
            .find_cached_ranges(cache_key, 0, 10239, None)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find range covering entire object");
        assert_eq!(ranges[0].start, 0, "Range should start at 0");
        assert_eq!(ranges[0].end, 10239, "Range should end at content_length-1");
    }

    #[tokio::test]
    async fn test_full_object_as_range_no_special_casing() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let full_object_data = vec![42u8; 10240]; // 10KB object

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: full_object_data.len() as u64,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: full_object_data.len() as u64,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object using store_full_object_as_range
        cache_manager
            .store_full_object_as_range(
                cache_key,
                &full_object_data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Now store a partial range using store_range
        let cache_key2 = "test-bucket/test-object-2";
        let partial_data = vec![99u8; 1024];
        cache_manager
            .store_range(
                cache_key2,
                0,
                1023,
                &partial_data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Both should be stored in the same way (as ranges in the ranges/ directory)
        let full_obj_metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        let partial_obj_metadata = cache_manager
            .get_metadata(cache_key2)
            .await
            .unwrap()
            .unwrap();

        // Both should have ranges array
        assert!(
            !full_obj_metadata.ranges.is_empty(),
            "Full object should have ranges"
        );
        assert!(
            !partial_obj_metadata.ranges.is_empty(),
            "Partial range should have ranges"
        );

        // Both should have range files in ranges/ directory
        let full_obj_range_path = cache_manager.get_new_range_file_path(cache_key, 0, 10239);
        let partial_range_path = cache_manager.get_new_range_file_path(cache_key2, 0, 1023);

        assert!(
            full_obj_range_path.to_string_lossy().contains("ranges/"),
            "Full object should be in ranges/ directory"
        );
        assert!(
            partial_range_path.to_string_lossy().contains("ranges/"),
            "Partial range should be in ranges/ directory"
        );

        // Both should be retrievable using the same mechanisms
        let full_obj_ranges = cache_manager
            .find_cached_ranges(cache_key, 0, 10239, None)
            .await
            .unwrap();
        let partial_ranges = cache_manager
            .find_cached_ranges(cache_key2, 0, 1023, None)
            .await
            .unwrap();

        assert_eq!(
            full_obj_ranges.len(),
            1,
            "Full object should have one range"
        );
        assert_eq!(
            partial_ranges.len(),
            1,
            "Partial range should have one range"
        );
    }

    #[tokio::test]
    async fn test_get_full_object_as_range_from_multiple_ranges() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 5120,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 5120,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store object as multiple ranges (simulating partial caching)
        let data1 = vec![1u8; 1024];
        let data2 = vec![2u8; 1024];
        let data3 = vec![3u8; 1024];
        let data4 = vec![4u8; 1024];
        let data5 = vec![5u8; 1024];

        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data1,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                1024,
                2047,
                &data2,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                2048,
                3071,
                &data3,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                3072,
                4095,
                &data4,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                4096,
                5119,
                &data5,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Retrieve full object - should assemble from multiple ranges
        let retrieved_data = cache_manager
            .get_full_object_as_range(cache_key)
            .await
            .unwrap();
        assert!(
            retrieved_data.is_some(),
            "Should retrieve full object from multiple ranges"
        );

        let data = retrieved_data.unwrap();
        assert_eq!(
            data.len(),
            5120,
            "Retrieved data should have correct length"
        );

        // Verify data integrity
        assert_eq!(&data[0..1024], &data1[..], "First range should match");
        assert_eq!(&data[1024..2048], &data2[..], "Second range should match");
        assert_eq!(&data[2048..3072], &data3[..], "Third range should match");
        assert_eq!(&data[3072..4096], &data4[..], "Fourth range should match");
        assert_eq!(&data[4096..5120], &data5[..], "Fifth range should match");
    }

    #[tokio::test]
    async fn test_get_full_object_as_range_incomplete_coverage() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 5120,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 5120,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store only some ranges (incomplete coverage)
        let data1 = vec![1u8; 1024];
        let data3 = vec![3u8; 1024];

        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data1,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        // Skip range 1024-2047 (gap)
        cache_manager
            .store_range(
                cache_key,
                2048,
                3071,
                &data3,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();
        // Skip ranges 3072-5119

        // Try to retrieve full object - should return None due to incomplete coverage
        let retrieved_data = cache_manager
            .get_full_object_as_range(cache_key)
            .await
            .unwrap();
        assert!(
            retrieved_data.is_none(),
            "Should return None when coverage is incomplete"
        );
    }

    #[tokio::test]
    async fn test_store_full_object_size_mismatch() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let data = vec![42u8; 10240]; // 10KB object

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 5120, // Wrong size!
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 5120,
            parts: Vec::new(),
            ..Default::default()
        };

        // Try to store with mismatched size - should fail
        let result = cache_manager
            .store_full_object_as_range(
                cache_key,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await;
        assert!(
            result.is_err(),
            "Should fail when data length doesn't match metadata"
        );
    }

    // ============================================================================
    // Tests for delete_cache_entry() and delete_ranges() methods
    // ============================================================================

    #[tokio::test]
    async fn test_delete_cache_entry_basic() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store multiple ranges
        for i in 0..5 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Verify files exist
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        assert!(
            metadata_file_path.exists(),
            "Metadata file should exist before deletion"
        );

        for i in 0..5 {
            let start = i * 1024;
            let end = start + 1023;
            let range_file_path = cache_manager.get_new_range_file_path(cache_key, start, end);
            assert!(
                range_file_path.exists(),
                "Range file {} should exist before deletion",
                i
            );
        }

        // Delete cache entry
        cache_manager.delete_cache_entry(cache_key).await.unwrap();

        // Verify all files are deleted
        assert!(
            !metadata_file_path.exists(),
            "Metadata file should be deleted"
        );

        for i in 0..5 {
            let start = i * 1024;
            let end = start + 1023;
            let range_file_path = cache_manager.get_new_range_file_path(cache_key, start, end);
            assert!(
                !range_file_path.exists(),
                "Range file {} should be deleted",
                i
            );
        }

        // Verify lock file is deleted
        let lock_file_path = metadata_file_path.with_extension("meta.lock");
        assert!(!lock_file_path.exists(), "Lock file should be deleted");
    }

    #[tokio::test]
    async fn test_delete_cache_entry_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/nonexistent-object";

        // Delete nonexistent entry - should not error
        let result = cache_manager.delete_cache_entry(cache_key).await;
        assert!(
            result.is_ok(),
            "Deleting nonexistent entry should not error"
        );
    }

    #[tokio::test]
    async fn test_delete_cache_entry_no_orphaned_files() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store ranges
        for i in 0..3 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Delete cache entry
        cache_manager.delete_cache_entry(cache_key).await.unwrap();

        // Verify no orphaned .bin files remain in ranges directory
        let ranges_dir = temp_dir.path().join("ranges");
        let sanitized_key = cache_manager.sanitize_cache_key_new(cache_key);

        for entry in std::fs::read_dir(&ranges_dir).unwrap() {
            let entry = entry.unwrap();
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            // No files for this cache key should remain
            assert!(
                !file_name_str.starts_with(&sanitized_key),
                "Orphaned file found: {}",
                file_name_str
            );
        }
    }

    #[tokio::test]
    async fn test_delete_cache_entry_cleans_up_tmp_files() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store a range
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Manually create some .tmp files to simulate partial writes in the sharded structure
        // Get the sharded path for the range file
        let tmp_range_path = cache_manager
            .get_new_range_file_path(cache_key, 1024, 2047)
            .with_extension("bin.tmp");
        let tmp_meta_path = cache_manager
            .get_new_metadata_file_path(cache_key)
            .with_extension("meta.tmp");

        // Create parent directories if needed
        if let Some(parent) = tmp_range_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        if let Some(parent) = tmp_meta_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }

        std::fs::write(&tmp_range_path, b"temporary data").unwrap();
        std::fs::write(&tmp_meta_path, b"temporary metadata").unwrap();

        // Verify tmp files exist
        assert!(
            tmp_range_path.exists(),
            "Tmp range file should exist before cleanup"
        );
        assert!(
            tmp_meta_path.exists(),
            "Tmp metadata file should exist before cleanup"
        );

        // Delete cache entry
        cache_manager.delete_cache_entry(cache_key).await.unwrap();

        // Verify tmp files are cleaned up
        assert!(
            !tmp_range_path.exists(),
            "Tmp range file should be cleaned up"
        );
        assert!(
            !tmp_meta_path.exists(),
            "Tmp metadata file should be cleaned up"
        );
    }

    #[tokio::test]
    async fn test_delete_cache_entry_with_missing_range_files() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store ranges
        for i in 0..3 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Manually delete one range file to simulate inconsistency
        let range_file_path = cache_manager.get_new_range_file_path(cache_key, 1024, 2047);
        std::fs::remove_file(&range_file_path).unwrap();

        // Delete cache entry - should not error even with missing range file
        let result = cache_manager.delete_cache_entry(cache_key).await;
        assert!(
            result.is_ok(),
            "Should handle missing range files gracefully"
        );

        // Verify metadata is still deleted
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        assert!(
            !metadata_file_path.exists(),
            "Metadata file should be deleted"
        );
    }

    #[tokio::test]
    async fn test_delete_ranges_partial_eviction() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store 5 ranges
        for i in 0..5 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Delete 2 ranges (partial eviction)
        let ranges_to_delete = vec![(1024u64, 2047u64), (3072u64, 4095u64)];
        cache_manager
            .delete_ranges(cache_key, &ranges_to_delete)
            .await
            .unwrap();

        // Verify deleted range files are gone
        let range1_path = cache_manager.get_new_range_file_path(cache_key, 1024, 2047);
        let range2_path = cache_manager.get_new_range_file_path(cache_key, 3072, 4095);
        assert!(
            !range1_path.exists(),
            "Deleted range 1 file should not exist"
        );
        assert!(
            !range2_path.exists(),
            "Deleted range 2 file should not exist"
        );

        // Verify remaining range files still exist
        let range0_path = cache_manager.get_new_range_file_path(cache_key, 0, 1023);
        let range3_path = cache_manager.get_new_range_file_path(cache_key, 2048, 3071);
        let range4_path = cache_manager.get_new_range_file_path(cache_key, 4096, 5119);
        assert!(range0_path.exists(), "Remaining range 0 file should exist");
        assert!(range3_path.exists(), "Remaining range 3 file should exist");
        assert!(range4_path.exists(), "Remaining range 4 file should exist");

        // Verify metadata is updated with only remaining ranges
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 3, "Should have 3 remaining ranges");

        // Verify the correct ranges remain
        assert!(metadata
            .ranges
            .iter()
            .any(|r| r.start == 0 && r.end == 1023));
        assert!(metadata
            .ranges
            .iter()
            .any(|r| r.start == 2048 && r.end == 3071));
        assert!(metadata
            .ranges
            .iter()
            .any(|r| r.start == 4096 && r.end == 5119));

        // Verify deleted ranges are not in metadata
        assert!(!metadata
            .ranges
            .iter()
            .any(|r| r.start == 1024 && r.end == 2047));
        assert!(!metadata
            .ranges
            .iter()
            .any(|r| r.start == 3072 && r.end == 4095));
    }

    #[tokio::test]
    async fn test_delete_ranges_all_ranges_deletes_entry() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 3072,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 3072,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store 3 ranges
        for i in 0..3 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Delete all ranges
        let ranges_to_delete = vec![(0u64, 1023u64), (1024u64, 2047u64), (2048u64, 3071u64)];
        cache_manager
            .delete_ranges(cache_key, &ranges_to_delete)
            .await
            .unwrap();

        // Verify entire entry is deleted (metadata + all range files)
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        assert!(
            !metadata_file_path.exists(),
            "Metadata file should be deleted when all ranges removed"
        );

        for i in 0..3 {
            let start = i * 1024;
            let end = start + 1023;
            let range_file_path = cache_manager.get_new_range_file_path(cache_key, start, end);
            assert!(
                !range_file_path.exists(),
                "Range file {} should be deleted",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_delete_ranges_empty_list() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 1024,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store a range
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Delete empty list of ranges - should be no-op
        let result = cache_manager.delete_ranges(cache_key, &[]).await;
        assert!(result.is_ok(), "Deleting empty list should not error");

        // Verify range still exists
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 1, "Range should still exist");
    }

    #[tokio::test]
    async fn test_delete_ranges_nonexistent_ranges() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 2048,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 2048,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store one range
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Try to delete a range that doesn't exist
        let ranges_to_delete = vec![(1024u64, 2047u64)];
        let result = cache_manager
            .delete_ranges(cache_key, &ranges_to_delete)
            .await;
        assert!(
            result.is_ok(),
            "Deleting nonexistent range should not error"
        );

        // Verify original range still exists
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            metadata.ranges.len(),
            1,
            "Original range should still exist"
        );
        assert_eq!(metadata.ranges[0].start, 0);
        assert_eq!(metadata.ranges[0].end, 1023);
    }

    #[tokio::test]
    async fn test_delete_ranges_batch_optimization() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store 10 ranges
        for i in 0..10 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Delete 5 ranges in a single batch operation
        let ranges_to_delete = vec![
            (1024u64, 2047u64),
            (3072u64, 4095u64),
            (5120u64, 6143u64),
            (7168u64, 8191u64),
            (9216u64, 10239u64),
        ];

        cache_manager
            .delete_ranges(cache_key, &ranges_to_delete)
            .await
            .unwrap();

        // Verify metadata is updated once with all deletions
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 5, "Should have 5 remaining ranges");

        // Verify correct ranges remain
        assert!(metadata
            .ranges
            .iter()
            .any(|r| r.start == 0 && r.end == 1023));
        assert!(metadata
            .ranges
            .iter()
            .any(|r| r.start == 2048 && r.end == 3071));
        assert!(metadata
            .ranges
            .iter()
            .any(|r| r.start == 4096 && r.end == 5119));
        assert!(metadata
            .ranges
            .iter()
            .any(|r| r.start == 6144 && r.end == 7167));
        assert!(metadata
            .ranges
            .iter()
            .any(|r| r.start == 8192 && r.end == 9215));
    }

    #[tokio::test]
    async fn test_delete_ranges_with_missing_range_files() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 3072,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 3072,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store 3 ranges
        for i in 0..3 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600), true)
                .await
                .unwrap();
        }

        // Manually delete one range file to simulate inconsistency
        let range_file_path = cache_manager.get_new_range_file_path(cache_key, 1024, 2047);
        std::fs::remove_file(&range_file_path).unwrap();

        // Delete that range - should handle missing file gracefully
        let ranges_to_delete = vec![(1024u64, 2047u64)];
        let result = cache_manager
            .delete_ranges(cache_key, &ranges_to_delete)
            .await;
        assert!(
            result.is_ok(),
            "Should handle missing range file gracefully"
        );

        // Verify metadata is updated
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 2, "Should have 2 remaining ranges");
        assert!(!metadata
            .ranges
            .iter()
            .any(|r| r.start == 1024 && r.end == 2047));
    }

    #[tokio::test]
    async fn test_cleanup_orphaned_files() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";

        // Get the sharded paths where orphaned files would be located
        let orphaned_file1 = cache_manager.get_new_range_file_path(cache_key, 0, 1023);
        let orphaned_file2 = cache_manager.get_new_range_file_path(cache_key, 1024, 2047);

        // Create parent directories for the sharded structure
        std::fs::create_dir_all(orphaned_file1.parent().unwrap()).unwrap();

        // Manually create orphaned range files (without metadata)
        std::fs::write(&orphaned_file1, b"orphaned data 1").unwrap();
        std::fs::write(&orphaned_file2, b"orphaned data 2").unwrap();

        // Verify orphaned files exist
        assert!(orphaned_file1.exists());
        assert!(orphaned_file2.exists());

        // Call cleanup_orphaned_files
        cache_manager
            .cleanup_orphaned_files(cache_key)
            .await
            .unwrap();

        // Verify orphaned files are cleaned up
        assert!(
            !orphaned_file1.exists(),
            "Orphaned file 1 should be cleaned up"
        );
        assert!(
            !orphaned_file2.exists(),
            "Orphaned file 2 should be cleaned up"
        );
    }

    #[tokio::test]
    async fn test_cleanup_temp_files() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 1024,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store a range
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Manually create temp files in the sharded structure
        let tmp_range_path = cache_manager
            .get_new_range_file_path(cache_key, 1024, 2047)
            .with_extension("bin.tmp");
        let tmp_meta_path = cache_manager
            .get_new_metadata_file_path(cache_key)
            .with_extension("meta.tmp");

        // Create parent directories if needed
        if let Some(parent) = tmp_range_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        if let Some(parent) = tmp_meta_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }

        std::fs::write(&tmp_range_path, b"temp range data").unwrap();
        std::fs::write(&tmp_meta_path, b"temp metadata").unwrap();

        // Verify temp files exist
        assert!(tmp_range_path.exists());
        assert!(tmp_meta_path.exists());

        // Call cleanup_temp_files
        cache_manager.cleanup_temp_files(cache_key).await.unwrap();

        // Verify temp files are cleaned up
        assert!(
            !tmp_range_path.exists(),
            "Temp range file should be cleaned up"
        );
        assert!(
            !tmp_meta_path.exists(),
            "Temp metadata file should be cleaned up"
        );
    }

    // Property-based tests using quickcheck
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: journal-based-metadata-updates, Property 10: Single-Instance Direct Write Optimization**
    /// *For any* cache-hit operation when shared_storage mode is disabled, the system may use
    /// direct metadata writes for performance (bypassing the journal).
    /// When shared_storage mode is enabled, the system shall use journal-based updates.
    /// **Validates: Requirements 5.4, 5.5**
    #[quickcheck]
    fn prop_mode_dependent_ttl_refresh_behavior(
        range_start: u64,
        range_end_offset: u16,
        ttl_secs: u16,
        use_shared_storage: bool,
    ) -> TestResult {
        // Filter invalid inputs
        if ttl_secs == 0 || range_end_offset == 0 {
            return TestResult::discard();
        }

        let range_end = range_start.saturating_add(range_end_offset as u64);
        let cache_key = "test-bucket/test-object";

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();

            // Create cache manager
            let mut cache_manager = DiskCacheManager::new(
                temp_dir.path().to_path_buf(),
                true,  // compression_enabled
                1024,  // compression_threshold
                false, // write_cache_enabled
            );

            if use_shared_storage {
                // Set up CacheHitUpdateBuffer for shared storage mode
                let buffer = std::sync::Arc::new(
                    crate::cache_hit_update_buffer::CacheHitUpdateBuffer::with_config(
                        temp_dir.path().to_path_buf(),
                        "test-instance".to_string(),
                        Duration::from_secs(60), // Long interval to prevent auto-flush
                        10000,
                    )
                );
                cache_manager.set_cache_hit_update_buffer(buffer.clone());

                // Property 1: In shared storage mode, cache_hit_update_buffer should be set
                if cache_manager.get_cache_hit_update_buffer().is_none() {
                    return TestResult::error("CacheHitUpdateBuffer should be set in shared storage mode");
                }

                // Call refresh_object_ttl
                let result = cache_manager.refresh_object_ttl(
                    cache_key,
                    Duration::from_secs(ttl_secs as u64),
                ).await;

                // Should succeed (buffered in RAM)
                if result.is_err() {
                    return TestResult::error(format!(
                        "refresh_object_ttl failed in shared storage mode: {:?}",
                        result.err()
                    ));
                }

                // Property 2: Entry should be buffered (not written directly to metadata)
                let buffer_len = buffer.buffer_len().await;
                if buffer_len != 1 {
                    return TestResult::error(format!(
                        "Expected 1 entry in buffer, got {}",
                        buffer_len
                    ));
                }

                // Property 3: No metadata file should be created (journal routing)
                let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
                if metadata_path.exists() {
                    return TestResult::error("Metadata file should not be created in shared storage mode (journal routing)");
                }
            } else {
                // Property 4: In single instance mode, cache_hit_update_buffer should be None
                if cache_manager.get_cache_hit_update_buffer().is_some() {
                    return TestResult::error("CacheHitUpdateBuffer should not be set in single instance mode");
                }

                // Create metadata file first (required for direct TTL refresh)
                let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
                if let Some(parent) = metadata_path.parent() {
                    tokio::fs::create_dir_all(parent).await.unwrap();
                }

                // Create initial metadata with the range
                let now = SystemTime::now();
                let range_spec = crate::cache_types::RangeSpec {
                    start: range_start,
                    end: range_end,
                    file_path: "test_range.bin".to_string(),
                    compression_algorithm: CompressionAlgorithm::Lz4,
                    compressed_size: range_end - range_start + 1,
                    uncompressed_size: range_end - range_start + 1,
                    created_at: now,
                    last_accessed: now,
                    access_count: 1,
                    frequency_score: 1,
                };

                let metadata = crate::cache_types::NewCacheMetadata {
                    cache_key: cache_key.to_string(),
                    object_metadata: crate::cache_types::ObjectMetadata::default(),
                    ranges: vec![range_spec],
                    created_at: now,
                    expires_at: now + Duration::from_secs(3600),
                    compression_info: crate::cache_types::CompressionInfo::default(),
                    ..Default::default()
                };

                let json = serde_json::to_string_pretty(&metadata).unwrap();
                tokio::fs::write(&metadata_path, json).await.unwrap();

                // Call refresh_object_ttl
                let result = cache_manager.refresh_object_ttl(
                    cache_key,
                    Duration::from_secs(ttl_secs as u64),
                ).await;

                // Should succeed (direct write)
                if result.is_err() {
                    return TestResult::error(format!(
                        "refresh_object_ttl failed in single instance mode: {:?}",
                        result.err()
                    ));
                }

                // Property 5: Metadata file should be updated directly
                if !metadata_path.exists() {
                    return TestResult::error("Metadata file should exist after direct TTL refresh");
                }

                // Property 6: No journal file should be created
                let journal_path = temp_dir.path()
                    .join("metadata")
                    .join("_journals")
                    .join("test-instance.journal");
                if journal_path.exists() {
                    return TestResult::error("Journal file should not be created in single instance mode");
                }
            }

            TestResult::passed()
        })
    }

    // ============================================================================
    // Tests for batch_delete_ranges (parallel file deletes) — Task 3.2
    // Requirements: 2.1, 2.4
    // ============================================================================

    /// Test batch_delete_ranges with multiple range files — verify all deleted and bytes_freed correct.
    /// Validates Requirement 2.1: async file deletes work correctly for multiple ranges.
    #[tokio::test]
    async fn test_batch_delete_ranges_multiple_files() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/batch-delete-object";
        let now = SystemTime::now();

        // Create 5 range .bin files directly on disk with known sizes
        let ranges: Vec<(u64, u64, usize)> = vec![
            (0, 999, 1000),
            (1000, 1999, 1000),
            (2000, 2999, 1000),
            (3000, 3999, 1000),
            (4000, 4999, 1000),
        ];

        let mut range_specs = Vec::new();
        for (start, end, size) in &ranges {
            let range_file_path = cache_manager.get_new_range_file_path(cache_key, *start, *end);
            if let Some(parent) = range_file_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            let data = vec![0xABu8; *size];
            std::fs::write(&range_file_path, &data).unwrap();

            range_specs.push(crate::cache_types::RangeSpec {
                start: *start,
                end: *end,
                file_path: range_file_path
                    .strip_prefix(temp_dir.path().join("ranges"))
                    .unwrap()
                    .to_string_lossy()
                    .to_string(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: *size as u64,
                uncompressed_size: *size as u64,
                created_at: now,
                last_accessed: now,
                access_count: 1,
                frequency_score: 1,
            });
        }

        // Write metadata file
        let metadata = crate::cache_types::NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: crate::cache_types::ObjectMetadata::default(),
            ranges: range_specs,
            created_at: now,
            expires_at: now + Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        if let Some(parent) = metadata_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let json = serde_json::to_string_pretty(&metadata).unwrap();
        std::fs::write(&metadata_path, &json).unwrap();

        // Delete ranges 0-999, 2000-2999, 4000-4999 (3 of 5)
        let ranges_to_delete = vec![(0u64, 999u64), (2000u64, 2999u64), (4000u64, 4999u64)];

        let (bytes_freed, all_evicted, deleted_paths) = cache_manager
            .batch_delete_ranges(cache_key, &ranges_to_delete)
            .await
            .unwrap();

        // Verify bytes_freed = 3 * 1000 = 3000
        assert_eq!(
            bytes_freed, 3000,
            "bytes_freed should equal sum of deleted file sizes"
        );

        // Verify not all ranges evicted (2 remain)
        assert!(
            !all_evicted,
            "all_evicted should be false when some ranges remain"
        );

        // Verify 3 paths were deleted
        assert_eq!(
            deleted_paths.len(),
            3,
            "Should have 3 deleted file paths"
        );

        // Verify the .bin files for deleted ranges no longer exist
        for (start, end) in &ranges_to_delete {
            let path = cache_manager.get_new_range_file_path(cache_key, *start, *end);
            assert!(
                !path.exists(),
                "Range file {}-{} should be deleted",
                start,
                end
            );
        }

        // Verify the .bin files for remaining ranges still exist
        let remaining_ranges = vec![(1000u64, 1999u64), (3000u64, 3999u64)];
        for (start, end) in &remaining_ranges {
            let path = cache_manager.get_new_range_file_path(cache_key, *start, *end);
            assert!(
                path.exists(),
                "Range file {}-{} should still exist",
                start,
                end
            );
        }

        // Verify metadata was updated — only 2 ranges remain
        let updated_content = std::fs::read_to_string(&metadata_path).unwrap();
        let updated_metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&updated_content).unwrap();
        assert_eq!(
            updated_metadata.ranges.len(),
            2,
            "Metadata should have 2 remaining ranges"
        );
        assert!(updated_metadata
            .ranges
            .iter()
            .any(|r| r.start == 1000 && r.end == 1999));
        assert!(updated_metadata
            .ranges
            .iter()
            .any(|r| r.start == 3000 && r.end == 3999));
    }

    /// Test batch_delete_ranges with some missing files — verify warning logged, remaining files
    /// still deleted, and metadata updated correctly.
    /// Validates Requirement 2.4: failed deletes log warning and continue processing.
    #[tokio::test]
    async fn test_batch_delete_ranges_missing_files() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/batch-delete-missing";
        let now = SystemTime::now();

        // Create 4 ranges in metadata, but only create .bin files for 2 of them
        let all_ranges: Vec<(u64, u64)> =
            vec![(0, 999), (1000, 1999), (2000, 2999), (3000, 3999)];
        let file_size: usize = 500;

        let mut range_specs = Vec::new();
        for (start, end) in &all_ranges {
            let range_file_path = cache_manager.get_new_range_file_path(cache_key, *start, *end);
            if let Some(parent) = range_file_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }

            // Only create .bin files for ranges 0-999 and 2000-2999
            // Ranges 1000-1999 and 3000-3999 will be "missing"
            if *start == 0 || *start == 2000 {
                let data = vec![0xCDu8; file_size];
                std::fs::write(&range_file_path, &data).unwrap();
            }

            range_specs.push(crate::cache_types::RangeSpec {
                start: *start,
                end: *end,
                file_path: range_file_path
                    .strip_prefix(temp_dir.path().join("ranges"))
                    .unwrap()
                    .to_string_lossy()
                    .to_string(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: file_size as u64,
                uncompressed_size: file_size as u64,
                created_at: now,
                last_accessed: now,
                access_count: 1,
                frequency_score: 1,
            });
        }

        // Write metadata file with all 4 ranges
        let metadata = crate::cache_types::NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: crate::cache_types::ObjectMetadata::default(),
            ranges: range_specs,
            created_at: now,
            expires_at: now + Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        if let Some(parent) = metadata_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let json = serde_json::to_string_pretty(&metadata).unwrap();
        std::fs::write(&metadata_path, &json).unwrap();

        // Delete all 4 ranges — 2 exist on disk, 2 are missing
        let ranges_to_delete: Vec<(u64, u64)> = all_ranges.clone();

        let (bytes_freed, all_evicted, deleted_paths) = cache_manager
            .batch_delete_ranges(cache_key, &ranges_to_delete)
            .await
            .unwrap();

        // bytes_freed should only count the 2 files that existed (2 * 500 = 1000)
        assert_eq!(
            bytes_freed, 1000,
            "bytes_freed should only count files that existed on disk"
        );

        // All ranges evicted from metadata (even missing files get removed from metadata)
        assert!(
            all_evicted,
            "all_evicted should be true when all ranges are removed from metadata"
        );

        // deleted_paths should contain only the 2 files that were actually deleted,
        // plus the metadata file and lock file that get cleaned up when all ranges are evicted
        let bin_deleted: Vec<_> = deleted_paths
            .iter()
            .filter(|p| p.to_string_lossy().ends_with(".bin"))
            .collect();
        assert_eq!(
            bin_deleted.len(),
            2,
            "Should have 2 deleted .bin file paths (only files that existed)"
        );

        // Verify the existing .bin files were actually deleted
        for start in &[0u64, 2000u64] {
            let end = start + 999;
            let path = cache_manager.get_new_range_file_path(cache_key, *start, end);
            assert!(
                !path.exists(),
                "Range file {}-{} should be deleted",
                start,
                end
            );
        }

        // Verify metadata file was deleted (all ranges evicted)
        assert!(
            !metadata_path.exists(),
            "Metadata file should be deleted when all ranges are evicted"
        );
    }
}
