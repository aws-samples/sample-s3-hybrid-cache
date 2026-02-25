//! Hybrid Metadata Writer Module
//!
//! Provides immediate metadata updates with journaling backup for the atomic metadata writes system.
//! Combines immediate availability with resilience through write-ahead journaling.

use crate::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use crate::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use crate::metadata_lock_manager::{MetadataLock, MetadataLockManager};
use crate::{ProxyError, Result};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, error, info, warn};

/// Write modes for hybrid metadata writer
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WriteMode {
    /// Update metadata file immediately (no journal)
    Immediate,
    /// Journal-only under high contention
    JournalOnly,
    /// Both immediate + journal (default for range operations)
    Hybrid,
}

/// Consolidation trigger for immediate consolidation
pub struct ConsolidationTrigger {
    size_threshold: u64,
    entry_count_threshold: usize,
}

impl ConsolidationTrigger {
    pub fn new(size_threshold: u64, entry_count_threshold: usize) -> Self {
        Self {
            size_threshold,
            entry_count_threshold,
        }
    }

    /// Check if immediate consolidation should be triggered
    pub fn should_trigger(&self, journal_size: u64, entry_count: usize) -> bool {
        journal_size >= self.size_threshold || entry_count >= self.entry_count_threshold
    }
}

/// Hybrid metadata writer combining immediate updates with journaling
pub struct HybridMetadataWriter {
    cache_dir: PathBuf,
    lock_manager: Arc<MetadataLockManager>,
    journal_manager: Arc<JournalManager>,
    consolidation_trigger: Arc<ConsolidationTrigger>,
}

impl HybridMetadataWriter {
    /// Create a new hybrid metadata writer
    pub fn new(
        cache_dir: PathBuf,
        lock_manager: Arc<MetadataLockManager>,
        journal_manager: Arc<JournalManager>,
        consolidation_trigger: Arc<ConsolidationTrigger>,
    ) -> Self {
        Self {
            cache_dir,
            lock_manager,
            journal_manager,
            consolidation_trigger,
        }
    }

    /// Get journal manager reference for metadata reconstruction
    pub fn get_journal_manager(&self) -> Option<&Arc<JournalManager>> {
        Some(&self.journal_manager)
    }

    /// Write range metadata using the specified write mode
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `range_spec` - The range specification to write
    /// * `write_mode` - The write mode (Immediate, JournalOnly, or Hybrid)
    /// * `object_metadata` - Optional object metadata from S3 response (needed for JournalOnly mode
    ///   when .meta file doesn't exist yet, to preserve response_headers)
    pub async fn write_range_metadata(
        &mut self,
        cache_key: &str,
        range_spec: RangeSpec,
        write_mode: WriteMode,
        object_metadata: Option<crate::cache_types::ObjectMetadata>,
        ttl: std::time::Duration,
    ) -> Result<()> {
        debug!(
            "Writing range metadata: cache_key={}, range={}-{}, mode={:?}",
            cache_key, range_spec.start, range_spec.end, write_mode
        );

        match write_mode {
            WriteMode::Immediate => self.write_immediate_only(cache_key, range_spec, ttl).await,
            WriteMode::JournalOnly => {
                self.write_journal_only(cache_key, range_spec, object_metadata, ttl)
                    .await
            }
            WriteMode::Hybrid => self.write_hybrid(cache_key, range_spec, ttl).await,
        }
    }

    /// Write full object metadata with optimized write mode
    pub async fn write_full_object_metadata(
        &mut self,
        cache_key: &str,
        object_metadata: ObjectMetadata,
        ttl: std::time::Duration,
    ) -> Result<()> {
        debug!(
            "Writing full object metadata: cache_key={}, content_length={}",
            cache_key, object_metadata.content_length
        );

        // Optimization: Full objects rarely need journaling since they don't have
        // the concurrent partial write issues that journals are designed to solve
        let write_mode = if self.is_high_contention(cache_key) {
            WriteMode::Hybrid // Fall back to journaling under extreme contention
        } else {
            WriteMode::Immediate // Default: immediate-only for better performance
        };

        // Create range spec for full object
        let range_spec = if object_metadata.content_length == 0 {
            // Empty objects have no ranges
            return self
                .write_empty_object_metadata(cache_key, object_metadata, ttl)
                .await;
        } else {
            RangeSpec::new(
                0,
                object_metadata.content_length.saturating_sub(1),
                format!(
                    "{}_{}-{}.bin",
                    sanitize_cache_key_for_filename(cache_key),
                    0,
                    object_metadata.content_length.saturating_sub(1)
                ),
                crate::compression::CompressionAlgorithm::Lz4,
                object_metadata.content_length,
                object_metadata.content_length,
            )
        };

        self.write_range_metadata(cache_key, range_spec, write_mode, Some(object_metadata), ttl)
            .await
    }

    /// Write empty object metadata (no ranges)
    async fn write_empty_object_metadata(
        &mut self,
        cache_key: &str,
        object_metadata: ObjectMetadata,
        ttl: std::time::Duration,
    ) -> Result<()> {
        debug!("Writing empty object metadata: cache_key={}", cache_key);

        let lock = self.lock_manager.acquire_lock(cache_key).await?;

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata,
            ranges: Vec::new(), // Empty objects have no ranges
            created_at: SystemTime::now(),
            expires_at: SystemTime::now() + ttl,
            compression_info: crate::cache_types::CompressionInfo::default(),
            ..Default::default()
        };

        self.write_metadata_to_disk(&metadata, &lock).await?;

        info!(
            "Successfully wrote empty object metadata: cache_key={}",
            cache_key
        );

        Ok(())
    }

    /// Write metadata immediately only (no journal)
    async fn write_immediate_only(&mut self, cache_key: &str, range_spec: RangeSpec, ttl: std::time::Duration) -> Result<()> {
        let lock = match self.lock_manager.acquire_lock(cache_key).await {
            Ok(lock) => lock,
            Err(e) => {
                error!(
                    "Failed to acquire metadata lock: cache_key={}, error={}",
                    cache_key, e
                );
                // Ensure no temporary files are left behind on lock failure
                self.cleanup_temp_files_for_cache_key(cache_key).await?;
                return Err(e);
            }
        };

        // Load existing metadata or create new
        let mut metadata = match self.load_or_create_metadata(cache_key, &lock, ttl).await {
            Ok(metadata) => metadata,
            Err(e) => {
                error!(
                    "Failed to load metadata: cache_key={}, error={}",
                    cache_key, e
                );
                // Cleanup on metadata load failure
                self.cleanup_temp_files_for_cache_key(cache_key).await?;
                return Err(e);
            }
        };

        // Add or update range
        self.add_or_update_range(&mut metadata, range_spec);

        // Write to disk with cleanup on failure
        match self.write_metadata_to_disk(&metadata, &lock).await {
            Ok(()) => {
                debug!(
                    "Successfully wrote metadata immediately: cache_key={}, ranges={}",
                    cache_key,
                    metadata.ranges.len()
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to write metadata to disk: cache_key={}, error={}",
                    cache_key, e
                );
                // Cleanup temporary files on write failure
                self.cleanup_temp_files_for_cache_key(cache_key).await?;
                Err(e)
            }
        }
    }

    /// Write to journal only (high contention fallback)
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `range_spec` - The range specification to write
    /// * `object_metadata` - Optional object metadata from S3 response (needed when .meta file
    ///   doesn't exist yet, to preserve response_headers for serving cached responses)
    async fn write_journal_only(
        &mut self,
        cache_key: &str,
        range_spec: RangeSpec,
        object_metadata: Option<crate::cache_types::ObjectMetadata>,
        ttl: std::time::Duration,
    ) -> Result<()> {
        let journal_entry = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: self.get_instance_id(),
            cache_key: cache_key.to_string(),
            range_spec,
            operation: JournalOperation::Add,
            range_file_path: "".to_string(), // Will be filled by consolidation
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(ttl.as_secs()),
            access_increment: None,
            object_metadata, // Include object metadata for journal consolidation
            metadata_written: false, // Range NOT written to .meta - consolidation SHOULD count size
        };

        match self
            .journal_manager
            .append_range_entry(cache_key, journal_entry)
            .await
        {
            Ok(()) => {
                debug!(
                    "Successfully wrote to journal only: cache_key={}",
                    cache_key
                );
            }
            Err(e) => {
                error!(
                    "Failed to write to journal: cache_key={}, error={}",
                    cache_key, e
                );
                // Journal write failure in journal-only mode is critical
                return Err(ProxyError::CacheError(format!(
                    "Journal write failed in journal-only mode: {}",
                    e
                )));
            }
        }

        // Check if immediate consolidation should be triggered
        let pending_entries = match self.journal_manager.get_pending_entries(cache_key).await {
            Ok(entries) => entries,
            Err(e) => {
                warn!(
                    "Failed to get pending entries for consolidation check: cache_key={}, error={}",
                    cache_key, e
                );
                // Don't fail the operation if we can't check for consolidation
                return Ok(());
            }
        };

        if self.consolidation_trigger.should_trigger(
            self.estimate_journal_size(&pending_entries),
            pending_entries.len(),
        ) {
            debug!(
                "Journal entries accumulated for large object, consolidation will merge: cache_key={}, entries={}",
                cache_key, pending_entries.len()
            );
            // JournalConsolidator handles consolidation on its periodic schedule
        }

        Ok(())
    }

    /// Write both immediate and journal (hybrid mode)
    async fn write_hybrid(&mut self, cache_key: &str, range_spec: RangeSpec, ttl: std::time::Duration) -> Result<()> {
        // Try immediate write first
        match self
            .write_immediate_only(cache_key, range_spec.clone(), ttl)
            .await
        {
            Ok(()) => {
                // Immediate write succeeded, also write to journal for redundancy
                // Set metadata_written: true because range is already in .meta
                let journal_entry = JournalEntry {
                    timestamp: SystemTime::now(),
                    instance_id: self.get_instance_id(),
                    cache_key: cache_key.to_string(),
                    range_spec,
                    operation: JournalOperation::Add,
                    range_file_path: "".to_string(),
                    metadata_version: 1,
                    new_ttl_secs: None,
                    object_ttl_secs: Some(ttl.as_secs()),
                    access_increment: None,
                    object_metadata: None, // Not needed in hybrid mode - immediate write already created .meta
                    metadata_written: true, // Range already in .meta - consolidation should NOT count size
                };

                // Journal write failure is not critical in hybrid mode since immediate write succeeded
                if let Err(e) = self
                    .journal_manager
                    .append_range_entry(cache_key, journal_entry)
                    .await
                {
                    warn!(
                        "Journal write failed in hybrid mode (immediate write succeeded): cache_key={}, error={}",
                        cache_key, e
                    );
                    // Continue - immediate write succeeded, so operation is successful
                }

                debug!(
                    "Successfully wrote metadata in hybrid mode: cache_key={}",
                    cache_key
                );

                Ok(())
            }
            Err(e) => {
                warn!(
                    "Immediate write failed in hybrid mode, falling back to journal-only: cache_key={}, error={}",
                    cache_key, e
                );

                // Fall back to journal-only with proper error handling
                // Note: In hybrid fallback, we don't have object_metadata since immediate write failed
                match self.write_journal_only(cache_key, range_spec, None, ttl).await {
                    Ok(()) => {
                        info!(
                            "Successfully fell back to journal-only mode: cache_key={}",
                            cache_key
                        );
                        Ok(())
                    }
                    Err(journal_err) => {
                        error!(
                            "Both immediate and journal writes failed in hybrid mode: cache_key={}, immediate_error={}, journal_error={}",
                            cache_key, e, journal_err
                        );
                        // Cleanup any temporary files from failed operations
                        self.cleanup_temp_files_for_cache_key(cache_key).await?;
                        Err(ProxyError::CacheError(format!(
                            "Hybrid write failed: immediate={}, journal={}",
                            e, journal_err
                        )))
                    }
                }
            }
        }
    }

    /// Check if cache key is experiencing high contention
    fn is_high_contention(&self, _cache_key: &str) -> bool {
        // Returns false (no contention) - full objects use immediate writes for performance
        false
    }

    /// Load existing metadata or create new metadata structure
    async fn load_or_create_metadata(
        &self,
        cache_key: &str,
        _lock: &MetadataLock,
        ttl: std::time::Duration,
    ) -> Result<NewCacheMetadata> {
        let metadata_path = self.get_metadata_file_path(cache_key);

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
            Ok(NewCacheMetadata {
                cache_key: cache_key.to_string(),
                object_metadata: ObjectMetadata::default(),
                ranges: Vec::new(),
                created_at: now,
                expires_at: now + ttl,
                compression_info: crate::cache_types::CompressionInfo::default(),
                ..Default::default()
            })
        }
    }

    /// Add or update range in metadata
    fn add_or_update_range(&self, metadata: &mut NewCacheMetadata, range_spec: RangeSpec) {
        // Check if range already exists (same start and end)
        if let Some(existing_range) = metadata
            .ranges
            .iter_mut()
            .find(|r| r.start == range_spec.start && r.end == range_spec.end)
        {
            // Update existing range
            *existing_range = range_spec;
            debug!(
                "Updated existing range: cache_key={}, range={}-{}",
                metadata.cache_key, existing_range.start, existing_range.end
            );
        } else {
            // Add new range
            debug!(
                "Adding new range: cache_key={}, range={}-{}",
                metadata.cache_key, range_spec.start, range_spec.end
            );
            metadata.ranges.push(range_spec);
        }

        // Update object metadata if this is a full object
        if metadata.ranges.len() == 1
            && metadata.ranges[0].start == 0
            && metadata.object_metadata.content_length == 0
        {
            // This looks like a full object, update content_length
            metadata.object_metadata.content_length = metadata.ranges[0].end + 1;
        }
    }

    /// Write metadata to disk atomically with enhanced error handling
    async fn write_metadata_to_disk(
        &self,
        metadata: &NewCacheMetadata,
        _lock: &MetadataLock,
    ) -> Result<()> {
        let metadata_path = self.get_metadata_file_path(&metadata.cache_key);

        // Ensure parent directory exists
        if let Some(parent) = metadata_path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                error!(
                    "Failed to create metadata directory: cache_key={}, path={:?}, error={}",
                    metadata.cache_key, parent, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to create metadata directory: {}",
                    e
                )));
            }
        }

        // Serialize metadata
        let json_content = serde_json::to_string_pretty(metadata).map_err(|e| {
            error!(
                "Failed to serialize metadata: cache_key={}, error={}",
                metadata.cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        // Atomic write: write to temp file then rename
        // Use instance-specific tmp file to avoid race conditions on shared storage
        // Format: {key}.meta.tmp.{hostname}.{pid}
        let instance_suffix = format!(
            "{}.{}",
            gethostname::gethostname().to_string_lossy(),
            std::process::id()
        );
        let tmp_extension = format!("meta.tmp.{}", instance_suffix);
        let temp_path = metadata_path.with_extension(&tmp_extension);

        // Write to temporary file
        if let Err(e) = tokio::fs::write(&temp_path, &json_content).await {
            error!(
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
            error!(
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
            "Successfully wrote metadata to disk: cache_key={}, path={:?}",
            metadata.cache_key, metadata_path
        );

        Ok(())
    }

    /// Clean up temporary files for a specific cache key
    async fn cleanup_temp_files_for_cache_key(&self, cache_key: &str) -> Result<()> {
        debug!(
            "Starting temp file cleanup for cache key: cache_key={}",
            cache_key
        );

        let mut cleanup_count = 0;
        let mut cleanup_errors = Vec::new();

        // Clean up metadata temp files (both old .meta.tmp and new instance-specific patterns)
        let metadata_path = self.get_metadata_file_path(cache_key);

        // Clean up old-style .meta.tmp
        let metadata_temp_path = metadata_path.with_extension("meta.tmp");
        if metadata_temp_path.exists() {
            match tokio::fs::remove_file(&metadata_temp_path).await {
                Ok(()) => {
                    cleanup_count += 1;
                    debug!(
                        "Cleaned up metadata temp file: cache_key={}, path={:?}",
                        cache_key, metadata_temp_path
                    );
                }
                Err(e) => {
                    let error_msg = format!(
                        "Failed to remove metadata temp file: path={:?}, error={}",
                        metadata_temp_path, e
                    );
                    warn!("{}", error_msg);
                    cleanup_errors.push(error_msg);
                }
            }
        }

        // Clean up instance-specific .meta.tmp.{instance} files in the same directory
        if let Some(parent_dir) = metadata_path.parent() {
            if let Some(file_stem) = metadata_path.file_stem().and_then(|s| s.to_str()) {
                if let Ok(mut entries) = tokio::fs::read_dir(parent_dir).await {
                    while let Ok(Some(entry)) = entries.next_entry().await {
                        if let Some(name) = entry.file_name().to_str() {
                            // Match pattern: {file_stem}.meta.tmp.{instance}
                            if name.starts_with(file_stem) && name.contains(".meta.tmp.") {
                                let path = entry.path();
                                match tokio::fs::remove_file(&path).await {
                                    Ok(()) => {
                                        cleanup_count += 1;
                                        debug!(
                                            "Cleaned up instance-specific metadata temp file: cache_key={}, path={:?}",
                                            cache_key, path
                                        );
                                    }
                                    Err(e) => {
                                        let error_msg = format!(
                                            "Failed to remove instance-specific metadata temp file: path={:?}, error={}",
                                            path, e
                                        );
                                        warn!("{}", error_msg);
                                        cleanup_errors.push(error_msg);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Clean up range temp files (simplified - in production this would use proper sharding)
        let ranges_dir = self.cache_dir.join("ranges");
        if ranges_dir.exists() {
            if let Ok(entries) = tokio::fs::read_dir(&ranges_dir).await {
                let mut entries = entries;
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let path = entry.path();
                    if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                        // Check if this is a temp file for our cache key
                        let sanitized_key = sanitize_cache_key_for_filename(cache_key);
                        if file_name.starts_with(&sanitized_key) && file_name.ends_with(".tmp") {
                            match tokio::fs::remove_file(&path).await {
                                Ok(()) => {
                                    cleanup_count += 1;
                                    debug!(
                                        "Cleaned up range temp file: cache_key={}, path={:?}",
                                        cache_key, path
                                    );
                                }
                                Err(e) => {
                                    let error_msg = format!(
                                        "Failed to remove range temp file: path={:?}, error={}",
                                        path, e
                                    );
                                    warn!("{}", error_msg);
                                    cleanup_errors.push(error_msg);
                                }
                            }
                        }
                    }
                }
            }
        }

        if cleanup_count > 0 {
            info!(
                "Temp file cleanup completed: cache_key={}, files_cleaned={}, errors={}",
                cache_key,
                cleanup_count,
                cleanup_errors.len()
            );
        }

        // Don't fail the operation if cleanup has errors - log them but continue
        if !cleanup_errors.is_empty() {
            warn!(
                "Temp file cleanup completed with errors: cache_key={}, errors={:?}",
                cache_key, cleanup_errors
            );
        }

        Ok(())
    }

    /// Get metadata file path for a cache key
    ///
    /// # Panics
    /// Panics if cache_key is malformed (missing bucket/object separator).
    fn get_metadata_file_path(&self, cache_key: &str) -> PathBuf {
        let base_dir = self.cache_dir.join("metadata");

        // Use the same sharding logic as DiskCacheManager for consistency
        match crate::disk_cache::get_sharded_path(&base_dir, cache_key, ".meta") {
            Ok(path) => path,
            Err(e) => {
                panic!(
                    "Malformed cache key '{}': {}. Cache keys must be in 'bucket/object' format.",
                    cache_key, e
                );
            }
        }
    }

    /// Get instance ID for journal entries
    fn get_instance_id(&self) -> String {
        format!(
            "{}:{}",
            hostname::get().unwrap_or_default().to_string_lossy(),
            std::process::id()
        )
    }

    /// Estimate journal size for consolidation triggering
    fn estimate_journal_size(&self, entries: &[JournalEntry]) -> u64 {
        entries
            .iter()
            .map(|entry| {
                // Rough estimate: JSON serialization size
                serde_json::to_string(entry)
                    .map(|s| s.len() as u64)
                    .unwrap_or(1024) // Default estimate
            })
            .sum()
    }
}

/// Sanitize cache key for safe filesystem usage
fn sanitize_cache_key_for_filename(cache_key: &str) -> String {
    use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

    // Define filesystem-unsafe ASCII characters that need encoding
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

    let sanitized = utf8_percent_encode(cache_key, FRAGMENT).to_string();

    // Filesystem filename limit is typically 255 bytes
    // Use 200 as threshold to leave room for extensions
    if sanitized.len() > 200 {
        let hash = blake3::hash(cache_key.as_bytes());
        format!("long_key_{}", hash.to_hex())
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test_write_mode_enum() {
        assert_eq!(WriteMode::Immediate, WriteMode::Immediate);
        assert_eq!(WriteMode::JournalOnly, WriteMode::JournalOnly);
        assert_eq!(WriteMode::Hybrid, WriteMode::Hybrid);
        assert_ne!(WriteMode::Immediate, WriteMode::Hybrid);
    }

    #[test]
    fn test_consolidation_trigger() {
        let trigger = ConsolidationTrigger::new(1024, 10);

        // Should not trigger below thresholds
        assert!(!trigger.should_trigger(512, 5));

        // Should trigger when size threshold exceeded
        assert!(trigger.should_trigger(1024, 5));

        // Should trigger when entry count threshold exceeded
        assert!(trigger.should_trigger(512, 10));

        // Should trigger when both thresholds exceeded
        assert!(trigger.should_trigger(1024, 10));
    }

    #[tokio::test]
    async fn test_hybrid_metadata_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let consolidation_trigger = Arc::new(ConsolidationTrigger::new(1024, 10));

        let writer = HybridMetadataWriter::new(
            temp_dir.path().to_path_buf(),
            lock_manager,
            journal_manager,
            consolidation_trigger,
        );

        assert_eq!(writer.cache_dir, temp_dir.path());
    }

    #[tokio::test]
    async fn test_write_empty_object_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let consolidation_trigger = Arc::new(ConsolidationTrigger::new(1024, 10));

        let mut writer = HybridMetadataWriter::new(
            temp_dir.path().to_path_buf(),
            lock_manager,
            journal_manager,
            consolidation_trigger,
        );

        let cache_key = "test-bucket/empty-object";
        let object_metadata = ObjectMetadata {
            etag: "empty-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 0,
            content_type: Some("text/plain".to_string()),
            ..Default::default()
        };

        // Write empty object metadata
        writer
            .write_full_object_metadata(cache_key, object_metadata, std::time::Duration::from_secs(3600))
            .await
            .unwrap();

        // Verify metadata file was created
        let metadata_path = writer.get_metadata_file_path(cache_key);
        assert!(metadata_path.exists());

        // Verify metadata content
        let content = tokio::fs::read_to_string(&metadata_path).await.unwrap();
        let metadata: NewCacheMetadata = serde_json::from_str(&content).unwrap();

        assert_eq!(metadata.cache_key, cache_key);
        assert_eq!(metadata.object_metadata.content_length, 0);
        assert_eq!(metadata.ranges.len(), 0); // Empty objects have no ranges
    }

    #[test]
    fn test_sanitize_cache_key_for_filename() {
        // Test normal key
        let sanitized = sanitize_cache_key_for_filename("normal-key");
        assert_eq!(sanitized, "normal-key");

        // Test key with unsafe characters
        let sanitized = sanitize_cache_key_for_filename("path/with:unsafe*chars");
        assert!(!sanitized.contains('/'));
        assert!(!sanitized.contains(':'));
        assert!(!sanitized.contains('*'));

        // Test long key (should be hashed)
        let long_key = "a".repeat(250);
        let sanitized = sanitize_cache_key_for_filename(&long_key);
        assert!(sanitized.starts_with("long_key_"));
        assert!(sanitized.len() < 200);
    }
}
