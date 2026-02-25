//! Orphaned Range Recovery Module
//!
//! Provides detection and recovery of orphaned range files in the shared cache system.
//! Orphaned ranges are range files that exist but have no corresponding metadata entries.

use crate::cache_types::{NewCacheMetadata, RangeSpec};
use crate::compression::CompressionAlgorithm;
use crate::hybrid_metadata_writer::HybridMetadataWriter;
use crate::{ProxyError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use walkdir::WalkDir;

/// Orphaned range recovery manager
pub struct OrphanedRangeRecovery {
    cache_dir: PathBuf,
    metadata_writer: Arc<Mutex<HybridMetadataWriter>>,
    metrics: Arc<RecoveryMetrics>,
}

/// Metrics for orphaned range recovery operations
#[derive(Debug, Default)]
pub struct RecoveryMetrics {
    pub orphans_detected: std::sync::atomic::AtomicU64,
    pub orphans_recovered: std::sync::atomic::AtomicU64,
    pub orphans_cleaned: std::sync::atomic::AtomicU64,
    pub bytes_recovered: std::sync::atomic::AtomicU64,
    pub bytes_cleaned: std::sync::atomic::AtomicU64,
    pub recovery_failures: std::sync::atomic::AtomicU64,
}

/// Represents an orphaned range file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrphanedRange {
    pub cache_key: String,
    pub range_file_path: PathBuf,
    pub range_spec: Option<RangeSpec>,
    pub validation_status: ValidationStatus,
    pub access_frequency: u64,
    pub last_accessed: Option<SystemTime>,
    pub file_size: u64,
}

/// Validation status of an orphaned range
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValidationStatus {
    Valid,
    Invalid,
    Corrupted,
    Unknown,
}

/// Result of recovery operation
#[derive(Debug, Clone)]
pub enum RecoveryResult {
    Recovered(RangeSpec),
    Cleaned(u64), // bytes freed
    Failed(String),
}

impl OrphanedRangeRecovery {
    /// Create a new orphaned range recovery manager
    pub fn new(cache_dir: PathBuf, metadata_writer: Arc<Mutex<HybridMetadataWriter>>) -> Self {
        Self {
            cache_dir,
            metadata_writer,
            metrics: Arc::new(RecoveryMetrics::default()),
        }
    }

    /// Get recovery metrics
    pub fn get_metrics(&self) -> Arc<RecoveryMetrics> {
        self.metrics.clone()
    }

    /// Scan for orphaned ranges for a specific cache key
    pub async fn scan_for_orphans(&self, cache_key: &str) -> Result<Vec<OrphanedRange>> {
        debug!("Scanning for orphaned ranges for cache key: {}", cache_key);

        // Parse cache key to get bucket and object
        let (bucket, object_key) = self.parse_cache_key(cache_key)?;

        // Get the sharded path for this cache key
        let shard_path = self.get_shard_path(&bucket, &object_key);

        // Get metadata file path
        let metadata_path = self
            .cache_dir
            .join("metadata")
            .join(&shard_path)
            .join(format!("{}.meta", self.sanitize_key(&object_key)));

        // Load existing metadata if it exists
        let existing_metadata = if metadata_path.exists() {
            match self.load_metadata(&metadata_path).await {
                Ok(metadata) => Some(metadata),
                Err(e) => {
                    warn!("Failed to load metadata for {}: {}", cache_key, e);
                    None
                }
            }
        } else {
            None
        };

        // Get range files directory
        let ranges_dir = self.cache_dir.join("ranges").join(&shard_path);

        if !ranges_dir.exists() {
            return Ok(Vec::new());
        }

        // Scan for range files
        let mut orphaned_ranges = Vec::new();
        let range_prefix = format!("{}_", self.sanitize_key(&object_key));

        for entry in std::fs::read_dir(&ranges_dir)? {
            let entry = entry?;
            let file_path = entry.path();

            if !file_path.is_file() {
                continue;
            }

            let file_name = match file_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            // Check if this is a range file for our object
            if !file_name.starts_with(&range_prefix) || !file_name.ends_with(".bin") {
                continue;
            }

            // Parse range from filename
            let range_spec = match self.parse_range_from_filename(file_name, &object_key) {
                Ok(spec) => spec,
                Err(e) => {
                    warn!("Failed to parse range from filename {}: {}", file_name, e);
                    continue;
                }
            };

            // Check if this range is referenced in metadata
            let is_orphaned = match &existing_metadata {
                Some(metadata) => !metadata
                    .ranges
                    .iter()
                    .any(|r| r.start == range_spec.start && r.end == range_spec.end),
                None => true, // No metadata means all ranges are orphaned
            };

            if is_orphaned {
                let file_size = entry.metadata()?.len();
                let access_frequency = self.estimate_access_frequency(&file_path).await;
                let last_accessed = self.get_last_accessed(&file_path).await;

                let validation_status = self.validate_range_file(&file_path, &range_spec).await;

                let orphaned_range = OrphanedRange {
                    cache_key: cache_key.to_string(),
                    range_file_path: file_path,
                    range_spec: Some(range_spec),
                    validation_status,
                    access_frequency,
                    last_accessed,
                    file_size,
                };

                orphaned_ranges.push(orphaned_range);
                self.metrics
                    .orphans_detected
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // Sort by priority (access frequency descending, then by file size descending)
        orphaned_ranges.sort_by(|a, b| {
            b.access_frequency
                .cmp(&a.access_frequency)
                .then_with(|| b.file_size.cmp(&a.file_size))
        });

        info!(
            "Found {} orphaned ranges for cache key: {}",
            orphaned_ranges.len(),
            cache_key
        );
        Ok(orphaned_ranges)
    }

    /// Recover an orphaned range by adding it to metadata
    pub async fn recover_orphan(&self, orphan: &OrphanedRange) -> Result<RecoveryResult> {
        debug!(
            "Attempting to recover orphaned range: {:?}",
            orphan.range_file_path
        );

        match orphan.validation_status {
            ValidationStatus::Valid => {
                // Attempt to add the range to metadata
                match self.add_range_to_metadata(orphan).await {
                    Ok(range_spec) => {
                        info!(
                            "Successfully recovered orphaned range: {:?}",
                            orphan.range_file_path
                        );
                        self.metrics
                            .orphans_recovered
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.metrics
                            .bytes_recovered
                            .fetch_add(orphan.file_size, std::sync::atomic::Ordering::Relaxed);
                        Ok(RecoveryResult::Recovered(range_spec))
                    }
                    Err(e) => {
                        error!(
                            "Failed to recover orphaned range {:?}: {}",
                            orphan.range_file_path, e
                        );
                        self.metrics
                            .recovery_failures
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Ok(RecoveryResult::Failed(e.to_string()))
                    }
                }
            }
            ValidationStatus::Invalid | ValidationStatus::Corrupted => {
                // Clean up invalid/corrupted orphaned range
                match self.cleanup_invalid_orphan(orphan).await {
                    Ok(bytes_freed) => Ok(RecoveryResult::Cleaned(bytes_freed)),
                    Err(e) => Ok(RecoveryResult::Failed(e.to_string())),
                }
            }
            ValidationStatus::Unknown => {
                warn!(
                    "Cannot recover orphaned range with unknown validation status: {:?}",
                    orphan.range_file_path
                );
                Ok(RecoveryResult::Failed(
                    "Unknown validation status".to_string(),
                ))
            }
        }
    }

    /// Clean up an invalid or corrupted orphaned range
    pub async fn cleanup_invalid_orphan(&self, orphan: &OrphanedRange) -> Result<u64> {
        debug!(
            "Cleaning up invalid orphaned range: {:?}",
            orphan.range_file_path
        );

        let file_size = orphan.file_size;

        match std::fs::remove_file(&orphan.range_file_path) {
            Ok(()) => {
                info!(
                    "Successfully cleaned up orphaned range: {:?}",
                    orphan.range_file_path
                );
                self.metrics
                    .orphans_cleaned
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.metrics
                    .bytes_cleaned
                    .fetch_add(file_size, std::sync::atomic::Ordering::Relaxed);
                Ok(file_size)
            }
            Err(e) => {
                error!(
                    "Failed to clean up orphaned range {:?}: {}",
                    orphan.range_file_path, e
                );
                self.metrics
                    .recovery_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err(ProxyError::CacheError(format!(
                    "Failed to clean up orphaned range: {}",
                    e
                )))
            }
        }
    }

    /// Scan for orphaned ranges across all cache keys (background operation)
    pub async fn scan_all_orphans(&self) -> Result<HashMap<String, Vec<OrphanedRange>>> {
        debug!("Starting comprehensive orphaned range scan");

        let mut all_orphans = HashMap::new();
        let ranges_dir = self.cache_dir.join("ranges");

        if !ranges_dir.exists() {
            return Ok(all_orphans);
        }

        // Walk through all range files
        for entry in WalkDir::new(&ranges_dir).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                continue;
            }

            let file_path = entry.path();
            let file_name = match file_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            if !file_name.ends_with(".bin") {
                continue;
            }

            // Extract cache key from file path
            let cache_key = match self.extract_cache_key_from_path(file_path) {
                Ok(key) => key,
                Err(e) => {
                    warn!(
                        "Failed to extract cache key from path {:?}: {}",
                        file_path, e
                    );
                    continue;
                }
            };

            // Check if we've already scanned this cache key
            if all_orphans.contains_key(&cache_key) {
                continue;
            }

            // Scan for orphans for this cache key
            match self.scan_for_orphans(&cache_key).await {
                Ok(orphans) => {
                    if !orphans.is_empty() {
                        all_orphans.insert(cache_key, orphans);
                    }
                }
                Err(e) => {
                    warn!("Failed to scan orphans for cache key {}: {}", cache_key, e);
                }
            }
        }

        info!(
            "Comprehensive scan found orphaned ranges for {} cache keys",
            all_orphans.len()
        );
        Ok(all_orphans)
    }

    /// Scan a single shard for orphaned ranges (scalable approach for large caches)
    ///
    /// Instead of scanning all files at once, this scans one shard (XX/YYY directory)
    /// at a time. Call this repeatedly with different shards to cover the entire cache.
    ///
    /// # Arguments
    /// * `bucket` - Bucket name to scan within
    /// * `shard_prefix` - Two-character hex prefix (e.g., "a3") for the shard
    /// * `timeout` - Maximum time to spend scanning this shard
    /// * `max_orphans` - Maximum orphans to return (for bounded processing)
    ///
    /// # Returns
    /// HashMap of cache_key -> Vec<OrphanedRange> for orphans found in this shard
    pub async fn scan_shard_for_orphans(
        &self,
        bucket: &str,
        shard_prefix: &str,
        timeout: Duration,
        max_orphans: usize,
    ) -> Result<HashMap<String, Vec<OrphanedRange>>> {
        let start_time = std::time::Instant::now();
        let mut all_orphans = HashMap::new();
        let mut total_orphans_found = 0;

        // Construct shard directory path: ranges/bucket/XX/
        let shard_dir = self
            .cache_dir
            .join("ranges")
            .join(bucket)
            .join(shard_prefix);

        if !shard_dir.exists() {
            debug!("Shard directory does not exist: {:?}", shard_dir);
            return Ok(all_orphans);
        }

        debug!("Scanning shard for orphans: {:?}", shard_dir);

        // Iterate through sub-shards (YYY directories)
        let sub_shard_entries = match std::fs::read_dir(&shard_dir) {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to read shard directory {:?}: {}", shard_dir, e);
                return Ok(all_orphans);
            }
        };

        for sub_shard_entry in sub_shard_entries {
            // Check timeout
            if start_time.elapsed() > timeout {
                info!(
                    "Shard scan timeout reached after {:?}, processed {} orphans",
                    start_time.elapsed(),
                    total_orphans_found
                );
                break;
            }

            // Check max orphans limit
            if total_orphans_found >= max_orphans {
                info!(
                    "Max orphans limit ({}) reached, stopping shard scan",
                    max_orphans
                );
                break;
            }

            let sub_shard_entry = match sub_shard_entry {
                Ok(e) => e,
                Err(_) => continue,
            };

            let sub_shard_path = sub_shard_entry.path();
            if !sub_shard_path.is_dir() {
                continue;
            }

            // Scan .bin files in this sub-shard
            let bin_files = match std::fs::read_dir(&sub_shard_path) {
                Ok(entries) => entries,
                Err(_) => continue,
            };

            // Group files by cache key to batch metadata lookups
            let mut files_by_cache_key: HashMap<String, Vec<std::path::PathBuf>> = HashMap::new();

            for file_entry in bin_files {
                let file_entry = match file_entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                let file_path = file_entry.path();
                if !file_path.is_file() {
                    continue;
                }

                let _file_name = match file_path.file_name().and_then(|n| n.to_str()) {
                    Some(name) if name.ends_with(".bin") => name,
                    _ => continue,
                };

                // Extract cache key from filename
                let cache_key = match self.extract_cache_key_from_path(&file_path) {
                    Ok(key) => key,
                    Err(_) => continue,
                };

                files_by_cache_key
                    .entry(cache_key)
                    .or_default()
                    .push(file_path);
            }

            // Process each cache key
            for (cache_key, bin_files) in files_by_cache_key {
                if total_orphans_found >= max_orphans {
                    break;
                }

                // Load metadata once per cache key
                let metadata = self.load_metadata_for_cache_key(&cache_key).await;

                for bin_file_path in bin_files {
                    if total_orphans_found >= max_orphans {
                        break;
                    }

                    // Check if this range is in metadata
                    let file_name = match bin_file_path.file_name().and_then(|n| n.to_str()) {
                        Some(name) => name,
                        None => continue,
                    };

                    let (_, object_key) = match self.parse_cache_key(&cache_key) {
                        Ok(parts) => parts,
                        Err(_) => continue,
                    };

                    let range_spec = match self.parse_range_from_filename(file_name, &object_key) {
                        Ok(spec) => spec,
                        Err(_) => continue,
                    };

                    // Check if orphaned
                    let is_orphaned = match &metadata {
                        Some(meta) => !meta
                            .ranges
                            .iter()
                            .any(|r| r.start == range_spec.start && r.end == range_spec.end),
                        None => true,
                    };

                    if is_orphaned {
                        let file_size = std::fs::metadata(&bin_file_path)
                            .map(|m| m.len())
                            .unwrap_or(0);

                        let validation_status =
                            self.validate_range_file(&bin_file_path, &range_spec).await;
                        let access_frequency = self.estimate_access_frequency(&bin_file_path).await;
                        let last_accessed = self.get_last_accessed(&bin_file_path).await;

                        let orphan = OrphanedRange {
                            cache_key: cache_key.clone(),
                            range_file_path: bin_file_path,
                            range_spec: Some(range_spec),
                            validation_status,
                            access_frequency,
                            last_accessed,
                            file_size,
                        };

                        all_orphans
                            .entry(cache_key.clone())
                            .or_insert_with(Vec::new)
                            .push(orphan);

                        total_orphans_found += 1;
                        self.metrics
                            .orphans_detected
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }

        if total_orphans_found > 0 {
            info!(
                "Shard scan complete: bucket={}, shard={}, orphans_found={}, duration={:?}",
                bucket,
                shard_prefix,
                total_orphans_found,
                start_time.elapsed()
            );
        }

        Ok(all_orphans)
    }

    /// Get list of all shards (XX prefixes) for a bucket
    ///
    /// Returns a list of two-character hex prefixes that exist in the ranges directory.
    /// Use this to iterate through shards for incremental scanning.
    pub fn list_shards(&self, bucket: &str) -> Result<Vec<String>> {
        let bucket_dir = self.cache_dir.join("ranges").join(bucket);

        if !bucket_dir.exists() {
            return Ok(Vec::new());
        }

        let mut shards = Vec::new();

        for entry in std::fs::read_dir(&bucket_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    // Validate it's a 2-character hex prefix
                    if name.len() == 2 && name.chars().all(|c| c.is_ascii_hexdigit()) {
                        shards.push(name.to_string());
                    }
                }
            }
        }

        // Sort for deterministic iteration
        shards.sort();

        Ok(shards)
    }

    /// Get list of all buckets in the cache
    pub fn list_buckets(&self) -> Result<Vec<String>> {
        let ranges_dir = self.cache_dir.join("ranges");

        if !ranges_dir.exists() {
            return Ok(Vec::new());
        }

        let mut buckets = Vec::new();

        for entry in std::fs::read_dir(&ranges_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    buckets.push(name.to_string());
                }
            }
        }

        buckets.sort();
        Ok(buckets)
    }

    /// Load metadata for a cache key (helper for shard scanning)
    async fn load_metadata_for_cache_key(&self, cache_key: &str) -> Option<NewCacheMetadata> {
        let (bucket, object_key) = match self.parse_cache_key(cache_key) {
            Ok(parts) => parts,
            Err(_) => return None,
        };

        let shard_path = self.get_shard_path(&bucket, &object_key);
        let metadata_path = self
            .cache_dir
            .join("metadata")
            .join(&shard_path)
            .join(format!("{}.meta", self.sanitize_key(&object_key)));

        if !metadata_path.exists() {
            return None;
        }

        match self.load_metadata(&metadata_path).await {
            Ok(metadata) => Some(metadata),
            Err(e) => {
                debug!("Failed to load metadata for {}: {}", cache_key, e);
                None
            }
        }
    }

    /// Recover orphaned ranges with priority-based processing
    pub async fn recover_with_priority(
        &self,
        orphans: &mut [OrphanedRange],
    ) -> Result<Vec<RecoveryResult>> {
        // Sort by priority: access frequency (desc), then file size (desc)
        orphans.sort_by(|a, b| {
            b.access_frequency
                .cmp(&a.access_frequency)
                .then_with(|| b.file_size.cmp(&a.file_size))
        });

        let mut results = Vec::new();

        for orphan in orphans {
            match self.recover_orphan(orphan).await {
                Ok(result) => results.push(result),
                Err(e) => {
                    error!(
                        "Failed to recover orphan {:?}: {}",
                        orphan.range_file_path, e
                    );
                    results.push(RecoveryResult::Failed(e.to_string()));
                }
            }
        }

        Ok(results)
    }

    // Private helper methods

    /// Parse cache key into bucket and object key
    fn parse_cache_key(&self, cache_key: &str) -> Result<(String, String)> {
        let parts: Vec<&str> = cache_key.splitn(2, '/').collect();
        if parts.len() != 2 {
            return Err(ProxyError::CacheError(format!(
                "Invalid cache key format: {}",
                cache_key
            )));
        }
        Ok((parts[0].to_string(), parts[1].to_string()))
    }

    /// Get sharded path for bucket and object key
    fn get_shard_path(&self, bucket: &str, object_key: &str) -> String {
        use blake3::Hasher;

        let mut hasher = Hasher::new();
        hasher.update(bucket.as_bytes());
        hasher.update(b"/");
        hasher.update(object_key.as_bytes());
        let hash = hasher.finalize();
        let hash_hex = hash.to_hex();

        format!("{}/{}/{}", bucket, &hash_hex[0..2], &hash_hex[2..5])
    }

    /// Sanitize object key for filename use
    fn sanitize_key(&self, key: &str) -> String {
        key.replace('/', "%2F")
            .replace('\\', "%5C")
            .replace(':', "%3A")
            .replace('*', "%2A")
            .replace('?', "%3F")
            .replace('"', "%22")
            .replace('<', "%3C")
            .replace('>', "%3E")
            .replace('|', "%7C")
    }

    /// Load metadata from file
    async fn load_metadata(&self, metadata_path: &Path) -> Result<NewCacheMetadata> {
        let content = tokio::fs::read_to_string(metadata_path).await?;
        let metadata: NewCacheMetadata = serde_json::from_str(&content)?;
        Ok(metadata)
    }

    /// Parse range specification from filename
    fn parse_range_from_filename(&self, filename: &str, object_key: &str) -> Result<RangeSpec> {
        let sanitized_key = self.sanitize_key(object_key);
        let prefix = format!("{}_", sanitized_key);

        if !filename.starts_with(&prefix) || !filename.ends_with(".bin") {
            return Err(ProxyError::CacheError(format!(
                "Invalid range filename format: {}",
                filename
            )));
        }

        // Extract range part: "start-end.bin"
        let range_part = &filename[prefix.len()..filename.len() - 4]; // Remove ".bin"
        let parts: Vec<&str> = range_part.split('-').collect();

        if parts.len() != 2 {
            return Err(ProxyError::CacheError(format!(
                "Invalid range format in filename: {}",
                filename
            )));
        }

        let start: u64 = parts[0]
            .parse()
            .map_err(|_| ProxyError::CacheError(format!("Invalid start range: {}", parts[0])))?;
        let end: u64 = parts[1]
            .parse()
            .map_err(|_| ProxyError::CacheError(format!("Invalid end range: {}", parts[1])))?;

        if start > end {
            return Err(ProxyError::CacheError(format!(
                "Invalid range: start {} > end {}",
                start, end
            )));
        }

        // Create a basic RangeSpec - we'll fill in more details during validation
        let now = SystemTime::now();

        Ok(RangeSpec {
            start,
            end,
            file_path: filename.to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4, // All data uses frame format
            compressed_size: 0, // Will be determined during validation
            uncompressed_size: end - start + 1,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            frequency_score: 1,
        })
    }

    /// Validate a range file
    async fn validate_range_file(
        &self,
        file_path: &Path,
        range_spec: &RangeSpec,
    ) -> ValidationStatus {
        // Check if file exists and is readable
        if !file_path.exists() {
            return ValidationStatus::Invalid;
        }

        // Check file size
        let file_size = match std::fs::metadata(file_path) {
            Ok(metadata) => metadata.len(),
            Err(_) => return ValidationStatus::Invalid,
        };

        if file_size == 0 {
            return ValidationStatus::Invalid;
        }

        // Basic validation: file size should be reasonable for the range
        let expected_min_size = (range_spec.end - range_spec.start + 1) / 10; // Allow 10:1 compression
        let expected_max_size = (range_spec.end - range_spec.start + 1) * 2; // Allow some overhead

        if file_size < expected_min_size || file_size > expected_max_size {
            warn!(
                "Range file size {} is outside expected range [{}, {}] for range {}-{}",
                file_size, expected_min_size, expected_max_size, range_spec.start, range_spec.end
            );
            return ValidationStatus::Invalid;
        }

        // Try to read first few bytes to check if file is corrupted
        match std::fs::File::open(file_path) {
            Ok(mut file) => {
                use std::io::Read;
                let mut buffer = [0u8; 1024];
                match file.read(&mut buffer) {
                    Ok(bytes_read) if bytes_read > 0 => ValidationStatus::Valid,
                    Ok(_) => ValidationStatus::Corrupted, // Empty file
                    Err(_) => ValidationStatus::Corrupted,
                }
            }
            Err(_) => ValidationStatus::Invalid,
        }
    }

    /// Estimate access frequency based on file metadata
    async fn estimate_access_frequency(&self, file_path: &Path) -> u64 {
        // Simple heuristic: use file access time if available
        match std::fs::metadata(file_path) {
            Ok(metadata) => {
                if let Ok(accessed) = metadata.accessed() {
                    if let Ok(duration) = SystemTime::now().duration_since(accessed) {
                        // More recent access = higher frequency score
                        let days_since_access = duration.as_secs() / 86400;
                        return (100_u64).saturating_sub(days_since_access);
                    }
                }
            }
            Err(_) => {}
        }

        // Default frequency
        1
    }

    /// Get last accessed time for a file
    async fn get_last_accessed(&self, file_path: &Path) -> Option<SystemTime> {
        std::fs::metadata(file_path)
            .ok()
            .and_then(|metadata| metadata.accessed().ok())
    }

    /// Add orphaned range to metadata
    async fn add_range_to_metadata(&self, orphan: &OrphanedRange) -> Result<RangeSpec> {
        let range_spec = orphan
            .range_spec
            .as_ref()
            .ok_or_else(|| ProxyError::CacheError("No range spec for orphan".to_string()))?;

        // Update range spec with actual file information
        let file_size = orphan.file_size;
        let mut updated_range_spec = range_spec.clone();
        updated_range_spec.compressed_size = file_size;
        updated_range_spec.file_path = orphan
            .range_file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&range_spec.file_path)
            .to_string();

        // Determine compression algorithm from file content
        updated_range_spec.compression_algorithm =
            self.detect_compression(&orphan.range_file_path).await;

        // Use hybrid metadata writer to add the range
        // Note: For orphaned range recovery, we don't have the original object_metadata,
        // so we pass None. The consolidator will use existing .meta if available.
        let mut writer = self.metadata_writer.lock().await;
        match writer
            .write_range_metadata(
                &orphan.cache_key,
                updated_range_spec.clone(),
                crate::hybrid_metadata_writer::WriteMode::Hybrid,
                None, // No object_metadata available for orphaned ranges
                std::time::Duration::ZERO, // Force revalidation since original TTL is unknown
            )
            .await
        {
            Ok(()) => Ok(updated_range_spec),
            Err(e) => Err(e),
        }
    }

    /// Detect compression algorithm from file content
    async fn detect_compression(&self, file_path: &Path) -> CompressionAlgorithm {
        // Read first few bytes to detect compression
        match std::fs::File::open(file_path) {
            Ok(mut file) => {
                use std::io::Read;
                let mut buffer = [0u8; 16];
                if let Ok(bytes_read) = file.read(&mut buffer) {
                    if bytes_read >= 4 {
                        // Check for LZ4 magic number
                        if &buffer[0..4] == b"\x04\"M\x18" {
                            return CompressionAlgorithm::Lz4;
                        }
                    }
                }
            }
            Err(_) => {}
        }

        CompressionAlgorithm::Lz4
    }

    /// Extract cache key from file path
    fn extract_cache_key_from_path(&self, file_path: &Path) -> Result<String> {
        // Path structure: cache_dir/ranges/bucket/XX/YYY/object_start-end.bin
        let ranges_dir = self.cache_dir.join("ranges");
        let relative_path = file_path
            .strip_prefix(&ranges_dir)
            .map_err(|_| ProxyError::CacheError("Invalid range file path".to_string()))?;

        let components: Vec<&std::ffi::OsStr> = relative_path.iter().collect();
        if components.len() < 4 {
            return Err(ProxyError::CacheError(
                "Invalid range file path structure".to_string(),
            ));
        }

        // Extract bucket from first component
        let bucket = components[0]
            .to_str()
            .ok_or_else(|| ProxyError::CacheError("Invalid bucket name".to_string()))?;

        // Extract object key from filename
        let filename = components[components.len() - 1]
            .to_str()
            .ok_or_else(|| ProxyError::CacheError("Invalid filename".to_string()))?;

        // Parse object key from filename (remove range suffix and .bin extension)
        let object_key = self.extract_object_key_from_filename(filename)?;

        Ok(format!("{}/{}", bucket, object_key))
    }

    /// Extract object key from range filename
    fn extract_object_key_from_filename(&self, filename: &str) -> Result<String> {
        if !filename.ends_with(".bin") {
            return Err(ProxyError::CacheError("Invalid range filename".to_string()));
        }

        // Remove .bin extension
        let name_without_ext = &filename[..filename.len() - 4];

        // Find the last underscore followed by range pattern (digits-digits)
        if let Some(last_underscore) = name_without_ext.rfind('_') {
            let range_part = &name_without_ext[last_underscore + 1..];

            // Check if this looks like a range (digits-digits)
            if range_part.contains('-')
                && range_part.chars().all(|c| c.is_ascii_digit() || c == '-')
            {
                let object_key_sanitized = &name_without_ext[..last_underscore];

                // Unsanitize the object key
                let object_key = object_key_sanitized
                    .replace("%2F", "/")
                    .replace("%5C", "\\")
                    .replace("%3A", ":")
                    .replace("%2A", "*")
                    .replace("%3F", "?")
                    .replace("%22", "\"")
                    .replace("%3C", "<")
                    .replace("%3E", ">")
                    .replace("%7C", "|");

                return Ok(object_key);
            }
        }

        Err(ProxyError::CacheError(format!(
            "Cannot extract object key from filename: {}",
            filename
        )))
    }
}

impl RecoveryMetrics {
    /// Get total orphans detected
    pub fn get_orphans_detected(&self) -> u64 {
        self.orphans_detected
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get total orphans recovered
    pub fn get_orphans_recovered(&self) -> u64 {
        self.orphans_recovered
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get total orphans cleaned
    pub fn get_orphans_cleaned(&self) -> u64 {
        self.orphans_cleaned
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get total bytes recovered
    pub fn get_bytes_recovered(&self) -> u64 {
        self.bytes_recovered
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get total bytes cleaned
    pub fn get_bytes_cleaned(&self) -> u64 {
        self.bytes_cleaned
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get total recovery failures
    pub fn get_recovery_failures(&self) -> u64 {
        self.recovery_failures
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hybrid_metadata_writer::{ConsolidationTrigger, HybridMetadataWriter};
    use crate::journal_manager::JournalManager;
    use crate::metadata_lock_manager::MetadataLockManager;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    fn create_test_recovery(temp_dir: &TempDir) -> OrphanedRangeRecovery {
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            std::time::Duration::from_secs(30),
            3,
        ));
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let consolidation_trigger = Arc::new(ConsolidationTrigger::new(1024 * 1024, 100));
        let metadata_writer = Arc::new(Mutex::new(HybridMetadataWriter::new(
            temp_dir.path().to_path_buf(),
            lock_manager,
            journal_manager,
            consolidation_trigger,
        )));

        OrphanedRangeRecovery::new(temp_dir.path().to_path_buf(), metadata_writer)
    }

    #[test]
    fn test_parse_cache_key() {
        let temp_dir = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp_dir);

        let (bucket, object) = recovery
            .parse_cache_key("test-bucket/path/to/object.txt")
            .unwrap();
        assert_eq!(bucket, "test-bucket");
        assert_eq!(object, "path/to/object.txt");

        // Test invalid cache key
        assert!(recovery.parse_cache_key("invalid-key").is_err());
    }

    #[test]
    fn test_sanitize_key() {
        let temp_dir = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp_dir);

        let sanitized = recovery.sanitize_key("path/to/file:with*special?chars");
        assert_eq!(sanitized, "path%2Fto%2Ffile%3Awith%2Aspecial%3Fchars");
    }

    #[test]
    fn test_parse_range_from_filename() {
        let temp_dir = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp_dir);

        let range_spec = recovery
            .parse_range_from_filename("test-object_0-8388607.bin", "test-object")
            .unwrap();

        assert_eq!(range_spec.start, 0);
        assert_eq!(range_spec.end, 8388607);
        assert_eq!(range_spec.uncompressed_size, 8388608);
    }

    #[test]
    fn test_extract_object_key_from_filename() {
        let temp_dir = TempDir::new().unwrap();
        let recovery = create_test_recovery(&temp_dir);

        let object_key = recovery
            .extract_object_key_from_filename("path%2Fto%2Fobject.txt_0-8388607.bin")
            .unwrap();

        assert_eq!(object_key, "path/to/object.txt");
    }

    #[test]
    fn test_validation_status() {
        assert_eq!(ValidationStatus::Valid, ValidationStatus::Valid);
        assert_ne!(ValidationStatus::Valid, ValidationStatus::Invalid);
    }

    #[test]
    fn test_recovery_metrics() {
        let metrics = RecoveryMetrics::default();

        assert_eq!(metrics.get_orphans_detected(), 0);
        assert_eq!(metrics.get_orphans_recovered(), 0);
        assert_eq!(metrics.get_orphans_cleaned(), 0);

        metrics
            .orphans_detected
            .fetch_add(5, std::sync::atomic::Ordering::Relaxed);
        metrics
            .orphans_recovered
            .fetch_add(3, std::sync::atomic::Ordering::Relaxed);
        metrics
            .orphans_cleaned
            .fetch_add(2, std::sync::atomic::Ordering::Relaxed);

        assert_eq!(metrics.get_orphans_detected(), 5);
        assert_eq!(metrics.get_orphans_recovered(), 3);
        assert_eq!(metrics.get_orphans_cleaned(), 2);
    }
}
