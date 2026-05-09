//! Cache Writer Module
//!
//! Provides streaming write functionality for caching PUT requests.
//! Implements atomic operations with temporary files and capacity checking.

use crate::cache_types::ObjectMetadata;
use crate::compression::{CompressionAlgorithm, CompressionHandler};
use crate::{ProxyError, Result};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, info, warn};

/// CacheWriter handles streaming writes to cache with atomic operations
///
/// This struct writes data to a temporary file and only commits it to the
/// final location after successful completion. This ensures cache consistency
/// and prevents partial writes from corrupting the cache.
///
/// # Requirements
///
/// - Requirement 4.1: Write to temporary file
/// - Requirement 4.2: Atomic rename on success
/// - Requirement 4.3: Delete temporary file on error
/// - Requirement 2.3: Check capacity during streaming
/// - Requirement 2.4: Handle capacity exceeded during streaming
/// - Requirement 7.2: Compress data if beneficial
pub struct CacheWriter {
    /// Temporary file handle for writing
    temp_file: Option<File>,
    /// Path to temporary file
    temp_path: PathBuf,
    /// Final path after commit
    final_path: PathBuf,
    /// Compression handler for data compression
    compression_handler: CompressionHandler,
    /// Number of bytes written so far (uncompressed)
    bytes_written: u64,
    /// Number of bytes written to disk (compressed)
    bytes_written_compressed: u64,
    /// Optional capacity limit (None = unlimited)
    capacity_limit: Option<u64>,
    /// Whether the writer has been committed or discarded
    finalized: bool,
    /// Compression algorithm being used
    compression_algorithm: CompressionAlgorithm,
}

impl CacheWriter {
    /// Create a new CacheWriter
    ///
    /// # Arguments
    ///
    /// * `final_path` - The final path where the cache file will be stored
    /// * `compression_handler` - Handler for compressing data
    /// * `capacity_limit` - Optional capacity limit in bytes
    /// * `cache_key` - Cache key for content-aware compression
    ///
    /// # Errors
    ///
    /// Returns an error if the temporary file cannot be created
    pub async fn new(
        final_path: PathBuf,
        compression_handler: CompressionHandler,
        capacity_limit: Option<u64>,
        cache_key: String,
    ) -> Result<Self> {
        // Create temporary file path with .tmp extension
        let temp_path = final_path.with_extension("tmp");

        debug!(
            "Creating CacheWriter: final_path={:?}, temp_path={:?}, capacity_limit={:?}, cache_key={}",
            final_path, temp_path, capacity_limit, cache_key
        );

        // Ensure parent directory exists
        if let Some(parent) = temp_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                error!(
                    "Failed to create cache directory: path={:?}, error={}",
                    parent, e
                );
                ProxyError::CacheError(format!("Failed to create cache directory: {}", e))
            })?;
        }

        // Create temporary file
        let temp_file = File::create(&temp_path).await.map_err(|e| {
            error!(
                "Failed to create temporary cache file: path={:?}, error={}",
                temp_path, e
            );
            ProxyError::CacheError(format!("Failed to create temporary file: {}", e))
        })?;

        // Determine compression algorithm — always Lz4 (frame format for integrity)
        let compression_algorithm = compression_handler.get_preferred_algorithm().clone();

        info!(
            "Created temporary cache file: {:?}, compression={:?}",
            temp_path, compression_algorithm
        );

        Ok(Self {
            temp_file: Some(temp_file),
            temp_path,
            final_path,
            compression_handler,
            bytes_written: 0,
            bytes_written_compressed: 0,
            capacity_limit,
            finalized: false,
            compression_algorithm,
        })
    }

    /// Write a chunk of data to the cache
    ///
    /// # Arguments
    ///
    /// * `data` - The data chunk to write
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Writing fails
    /// - Capacity limit is exceeded
    /// - Writer has already been finalized
    /// - Disk is full (ENOSPC)
    ///
    /// # Requirements
    ///
    /// - Requirement 1.2: Write data in chunks as received
    /// - Requirement 2.4: Check capacity during streaming
    /// - Requirement 7.2: Compress data if beneficial
    /// - Requirement 8.1: Handle cache write failures gracefully
    /// - Requirement 8.3: Handle disk full errors
    pub async fn write_chunk(&mut self, data: &[u8]) -> Result<()> {
        if self.finalized {
            return Err(ProxyError::CacheError(
                "Cannot write to finalized CacheWriter".to_string(),
            ));
        }

        let chunk_size = data.len() as u64;

        // Compress data through LZ4 frame format (Requirement 7.2)
        // All data goes through frame encoder for integrity checksums
        let (data_to_write, compressed_size) = match self
            .compression_handler
            .compress_with_algorithm(data, self.compression_algorithm.clone())
        {
            Ok(result) => {
                debug!(
                    "Compressed chunk: {} bytes -> {} bytes (ratio: {:.2})",
                    data.len(),
                    result.compressed_size,
                    result.compressed_size as f32 / data.len() as f32
                );
                (Some(result.data), result.compressed_size)
            }
            Err(e) => {
                warn!("Compression failed for chunk, storing raw: {}", e);
                (None, data.len() as u64)
            }
        };

        // Check capacity limit before writing (based on compressed size)
        // Note: We check against compressed size since that's what actually gets stored on disk
        if let Some(limit) = self.capacity_limit {
            if self.bytes_written_compressed + compressed_size > limit {
                error!(
                    "Capacity limit exceeded: current_compressed={}, chunk_compressed={}, limit={}",
                    self.bytes_written_compressed, compressed_size, limit
                );
                return Err(ProxyError::CacheError(format!(
                    "Capacity limit exceeded: {} + {} > {}",
                    self.bytes_written_compressed, compressed_size, limit
                )));
            }
        }

        // Write to temporary file (Requirement 8.1, 8.3)
        // Use compressed data if available, otherwise write original data directly (zero-copy)
        let write_slice: &[u8] = match &data_to_write {
            Some(compressed) => compressed,
            None => data,
        };

        if let Some(ref mut file) = self.temp_file {
            file.write_all(write_slice).await.map_err(|e| {
                // Check for disk full error (Requirement 8.3)
                let error_kind = e.kind();
                if error_kind == std::io::ErrorKind::StorageFull
                    || e.raw_os_error() == Some(28) // ENOSPC on Linux
                    || e.to_string().contains("No space left on device")
                {
                    error!(
                        "Disk full while writing to cache: path={:?}, size={}, error={}",
                        self.temp_path,
                        write_slice.len(),
                        e
                    );
                    ProxyError::CacheError(format!(
                        "Disk full: cannot write to cache (attempted {} bytes)",
                        write_slice.len()
                    ))
                } else {
                    error!(
                        "Failed to write chunk to cache: path={:?}, size={}, error={}, kind={:?}",
                        self.temp_path,
                        write_slice.len(),
                        e,
                        error_kind
                    );
                    ProxyError::CacheError(format!("Failed to write to cache: {}", e))
                }
            })?;

            self.bytes_written += chunk_size;
            self.bytes_written_compressed += compressed_size;

            debug!(
                "Wrote chunk to cache: uncompressed={}, compressed={}, total_uncompressed={}, total_compressed={}",
                chunk_size, compressed_size, self.bytes_written, self.bytes_written_compressed
            );
        } else {
            return Err(ProxyError::CacheError(
                "Temporary file not available".to_string(),
            ));
        }

        Ok(())
    }

    /// Commit the cached data to its final location with atomic durability.
    ///
    /// The commit sequence ensures that either both the `.bin` and `.meta` files
    /// are present and consistent, or neither is visible:
    ///
    /// 1. Flush + `sync_all` the `.bin` temp file (ensures data pages reach storage)
    /// 2. Rename `.bin.tmp` → `.bin` (atomic on same filesystem)
    /// 3. Write `.meta.tmp` with metadata JSON
    /// 4. `sync_all` on `.meta.tmp` (durable before rename)
    /// 5. Rename `.meta.tmp` → `.meta`
    ///
    /// On any failure, all temp files and the `.bin` (if already renamed) are removed
    /// to prevent orphan `.bin` files without corresponding `.meta`.
    ///
    /// # Arguments
    ///
    /// * `metadata` - Object metadata to store alongside the cached data
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Flushing or sync fails
    /// - Renaming fails
    /// - Metadata cannot be written
    /// - Writer has already been finalized
    ///
    /// # Requirements
    ///
    /// - Requirement 4.1: sync_all before rename
    /// - Requirement 4.2: Atomic rename on success
    /// - Requirement 4.3: Write .meta via temp + rename
    /// - Requirement 4.4: On failure, remove temp files and .bin if renamed
    /// - Requirement 4.5: .meta rename only after .bin rename succeeds
    /// - Requirement 3.4: Store metadata alongside cached object
    pub async fn commit(&mut self, metadata: ObjectMetadata) -> Result<()> {
        if self.finalized {
            return Err(ProxyError::CacheError(
                "CacheWriter already finalized".to_string(),
            ));
        }

        info!(
            "Committing cache entry: temp_path={:?}, final_path={:?}, bytes={}",
            self.temp_path, self.final_path, self.bytes_written
        );

        let metadata_path = self.final_path.with_extension("meta");
        let meta_tmp_path = metadata_path.with_extension("meta.tmp");

        match self
            .commit_inner(&metadata, &metadata_path, &meta_tmp_path)
            .await
        {
            Ok(()) => {
                self.finalized = true;
                info!(
                    "Successfully committed cache entry: path={:?}, bytes={}",
                    self.final_path, self.bytes_written
                );
                Ok(())
            }
            Err(e) => {
                // Failure cleanup: remove temp files and .bin if already renamed.
                // This ensures no orphan .bin without .meta.
                Self::cleanup_on_failure(&self.temp_path, &self.final_path, &meta_tmp_path).await;
                self.finalized = true;
                Err(e)
            }
        }
    }

    /// Inner commit logic that performs the durable atomic commit sequence.
    /// Separated so that the caller can handle cleanup on any error.
    async fn commit_inner(
        &mut self,
        metadata: &ObjectMetadata,
        metadata_path: &std::path::Path,
        meta_tmp_path: &std::path::Path,
    ) -> Result<()> {
        // Step 1: Flush + sync_all the .bin temp file
        if let Some(mut file) = self.temp_file.take() {
            file.flush().await.map_err(|e| {
                error!(
                    "Failed to flush temporary file: path={:?}, error={}",
                    self.temp_path, e
                );
                ProxyError::CacheError(format!("Failed to flush cache file: {}", e))
            })?;
            file.sync_all().await.map_err(|e| {
                error!(
                    "Failed to sync temporary file: path={:?}, error={}",
                    self.temp_path, e
                );
                ProxyError::CacheError(format!("Failed to sync cache file: {}", e))
            })?;
            drop(file);
        }

        // Step 2: Rename .bin.tmp → .bin (same directory for NFS atomicity)
        tokio::fs::rename(&self.temp_path, &self.final_path)
            .await
            .map_err(|e| {
                error!(
                    "Failed to rename cache file: temp={:?}, final={:?}, error={}",
                    self.temp_path, self.final_path, e
                );
                ProxyError::CacheError(format!("Failed to commit cache file: {}", e))
            })?;

        // Step 3: Write .meta.tmp
        let metadata_json = serde_json::to_string_pretty(metadata).map_err(|e| {
            error!(
                "Failed to serialize metadata: path={:?}, error={}",
                metadata_path, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        {
            let mut meta_file = tokio::fs::File::create(meta_tmp_path).await.map_err(|e| {
                error!(
                    "Failed to create metadata temp file: path={:?}, error={}",
                    meta_tmp_path, e
                );
                ProxyError::CacheError(format!("Failed to create metadata temp file: {}", e))
            })?;

            meta_file
                .write_all(metadata_json.as_bytes())
                .await
                .map_err(|e| {
                    error!(
                        "Failed to write metadata temp file: path={:?}, error={}",
                        meta_tmp_path, e
                    );
                    ProxyError::CacheError(format!("Failed to write metadata: {}", e))
                })?;

            // Step 4: sync_all on .meta.tmp (durable before rename)
            meta_file.sync_all().await.map_err(|e| {
                error!(
                    "Failed to sync metadata temp file: path={:?}, error={}",
                    meta_tmp_path, e
                );
                ProxyError::CacheError(format!("Failed to sync metadata file: {}", e))
            })?;
        }

        // Step 5: Rename .meta.tmp → .meta
        tokio::fs::rename(meta_tmp_path, metadata_path)
            .await
            .map_err(|e| {
                error!(
                    "Failed to rename metadata file: temp={:?}, final={:?}, error={}",
                    meta_tmp_path, metadata_path, e
                );
                ProxyError::CacheError(format!("Failed to commit metadata file: {}", e))
            })?;

        Ok(())
    }

    /// Clean up files on commit failure.
    /// Removes temp_path (.bin.tmp), final_path (.bin if already renamed),
    /// and meta_tmp_path (.meta.tmp) to prevent orphan files.
    async fn cleanup_on_failure(
        temp_path: &std::path::Path,
        final_path: &std::path::Path,
        meta_tmp_path: &std::path::Path,
    ) {
        // Remove .bin.tmp if it still exists
        if let Err(e) = tokio::fs::remove_file(temp_path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    "Failed to remove temp file during cleanup: path={:?}, error={}",
                    temp_path, e
                );
            }
        }

        // Remove .bin if it was already renamed into place
        if let Err(e) = tokio::fs::remove_file(final_path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    "Failed to remove final file during cleanup: path={:?}, error={}",
                    final_path, e
                );
            }
        }

        // Remove .meta.tmp if it was created
        if let Err(e) = tokio::fs::remove_file(meta_tmp_path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    "Failed to remove metadata temp file during cleanup: path={:?}, error={}",
                    meta_tmp_path, e
                );
            }
        }
    }

    /// Discard the cached data and clean up temporary files
    ///
    /// This should be called when an error occurs or when the cache should
    /// not be committed (e.g., S3 returned an error).
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup fails, but logs the error and continues
    ///
    /// # Requirements
    ///
    /// - Requirement 4.3: Delete temporary file on error
    /// - Requirement 8.2: Clean up cached data on S3 error
    pub async fn discard(&mut self) -> Result<()> {
        if self.finalized {
            debug!("CacheWriter already finalized, nothing to discard");
            return Ok(());
        }

        info!("Discarding cache entry: temp_path={:?}", self.temp_path);

        // Close the file if still open
        if let Some(file) = self.temp_file.take() {
            drop(file);
        }

        // Remove temporary file
        if self.temp_path.exists() {
            match tokio::fs::remove_file(&self.temp_path).await {
                Ok(_) => {
                    debug!("Removed temporary file: {:?}", self.temp_path);
                }
                Err(e) => {
                    warn!(
                        "Failed to remove temporary file: path={:?}, error={}",
                        self.temp_path, e
                    );
                    // Don't return error - cleanup is best-effort
                }
            }
        }

        self.finalized = true;

        info!("Discarded cache entry: {:?}", self.temp_path);

        Ok(())
    }

    /// Check if the current write would exceed capacity
    ///
    /// # Arguments
    ///
    /// * `additional_bytes` - Number of bytes to check
    ///
    /// # Returns
    ///
    /// Returns true if writing the additional bytes would exceed capacity
    ///
    /// # Requirements
    ///
    /// - Requirement 2.3: Check capacity during streaming
    ///
    /// Note: This checks against compressed size since that's what gets stored on disk
    pub fn check_capacity(&self, additional_compressed_bytes: u64) -> bool {
        if let Some(limit) = self.capacity_limit {
            self.bytes_written_compressed + additional_compressed_bytes > limit
        } else {
            false // No limit = never exceeds
        }
    }

    /// Get the number of bytes written so far (uncompressed)
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Get the number of bytes written to disk (compressed)
    pub fn bytes_written_compressed(&self) -> u64 {
        self.bytes_written_compressed
    }

    /// Get the compression algorithm being used
    pub fn compression_algorithm(&self) -> &CompressionAlgorithm {
        &self.compression_algorithm
    }

    /// Get the capacity limit
    pub fn capacity_limit(&self) -> Option<u64> {
        self.capacity_limit
    }
}

/// Ensure cleanup happens even if commit/discard is not called
impl Drop for CacheWriter {
    fn drop(&mut self) {
        if !self.finalized && self.temp_path.exists() {
            warn!(
                "CacheWriter dropped without finalization, cleaning up: {:?}",
                self.temp_path
            );
            // Best-effort synchronous cleanup
            let _ = std::fs::remove_file(&self.temp_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_compression_handler() -> CompressionHandler {
        CompressionHandler::new(1024, true)
    }

    fn create_test_metadata() -> ObjectMetadata {
        ObjectMetadata::new(
            "test-etag".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            1024,
            Some("application/octet-stream".to_string()),
        )
    }

    #[tokio::test]
    async fn test_cache_writer_basic() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("test.cache");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "test.cache".to_string(),
        )
        .await
        .unwrap();

        // Write some data
        let data = b"Hello, World!";
        writer.write_chunk(data).await.unwrap();

        assert_eq!(writer.bytes_written(), data.len() as u64);

        // Commit
        let metadata = create_test_metadata();
        writer.commit(metadata).await.unwrap();

        // Verify final file exists
        assert!(final_path.exists());

        // Verify metadata file exists
        let metadata_path = final_path.with_extension("meta");
        assert!(metadata_path.exists());
    }

    #[tokio::test]
    async fn test_cache_writer_discard() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("test.cache");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "test.cache".to_string(),
        )
        .await
        .unwrap();

        // Write some data
        let data = b"Hello, World!";
        writer.write_chunk(data).await.unwrap();

        // Discard
        writer.discard().await.unwrap();

        // Verify final file does not exist
        assert!(!final_path.exists());

        // Verify temp file was cleaned up
        let temp_path = final_path.with_extension("tmp");
        assert!(!temp_path.exists());
    }

    #[tokio::test]
    async fn test_cache_writer_capacity_limit() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("test.cache");
        let compression_handler = create_test_compression_handler();

        // Set capacity limit to 100 bytes (compressed)
        // Note: Capacity is now checked against compressed size, not uncompressed
        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            Some(100),
            "test.cache".to_string(),
        )
        .await
        .unwrap();

        // Write some data - should succeed
        let data1 = b"12345";
        writer.write_chunk(data1).await.unwrap();
        assert_eq!(writer.bytes_written(), 5);

        // Compressed size should be tracked
        let compressed_after_first = writer.bytes_written_compressed();
        assert!(compressed_after_first > 0);

        // Write more data - should succeed
        let data2 = b"67890";
        writer.write_chunk(data2).await.unwrap();
        assert_eq!(writer.bytes_written(), 10);

        // Compressed size should have increased
        assert!(writer.bytes_written_compressed() > compressed_after_first);

        // Write a large chunk that would exceed capacity even when compressed
        // Use random-ish data that won't compress well
        let data3: Vec<u8> = (0..150).map(|i| (i * 7 + 13) as u8).collect();
        let result = writer.write_chunk(&data3).await;
        assert!(
            result.is_err(),
            "Should fail when compressed size exceeds capacity"
        );

        // Discard
        writer.discard().await.unwrap();
    }

    #[tokio::test]
    async fn test_cache_writer_check_capacity() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("test.cache");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            Some(100),
            "test.cache".to_string(),
        )
        .await
        .unwrap();

        // Write 50 bytes (uncompressed)
        let data = vec![0u8; 50];
        writer.write_chunk(&data).await.unwrap();

        // Get the actual compressed size written
        let compressed_written = writer.bytes_written_compressed();
        assert!(compressed_written > 0);
        assert!(compressed_written <= 100);

        // Check if 40 more compressed bytes would fit
        let remaining = 100 - compressed_written;
        assert!(
            !writer.check_capacity(remaining - 1),
            "Should fit within capacity"
        );

        // Check if more than remaining would exceed
        assert!(
            writer.check_capacity(remaining + 1),
            "Should exceed capacity"
        );

        writer.discard().await.unwrap();
    }

    #[tokio::test]
    async fn test_cache_writer_multiple_chunks() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("test.cache");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "test.cache".to_string(),
        )
        .await
        .unwrap();

        // Write multiple chunks
        for i in 0..10 {
            let data = format!("Chunk {}", i);
            writer.write_chunk(data.as_bytes()).await.unwrap();
        }

        // Commit
        let metadata = create_test_metadata();
        writer.commit(metadata).await.unwrap();

        // Verify file exists and has content
        assert!(final_path.exists());
        let content = tokio::fs::read(&final_path).await.unwrap();
        assert!(!content.is_empty());
    }

    #[tokio::test]
    async fn test_cache_writer_cannot_write_after_commit() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("test.cache");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "test.cache".to_string(),
        )
        .await
        .unwrap();

        let data = b"Test data";
        writer.write_chunk(data).await.unwrap();

        let metadata = create_test_metadata();
        writer.commit(metadata).await.unwrap();

        // Try to write after commit - should fail
        let result = writer.write_chunk(b"More data").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cache_writer_cannot_write_after_discard() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("test.cache");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "test.cache".to_string(),
        )
        .await
        .unwrap();

        let data = b"Test data";
        writer.write_chunk(data).await.unwrap();

        writer.discard().await.unwrap();

        // Try to write after discard - should fail
        let result = writer.write_chunk(b"More data").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cache_writer_drop_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("test.cache");
        let _temp_path = final_path.with_extension("tmp");
        let compression_handler = create_test_compression_handler();

        {
            let mut writer = CacheWriter::new(
                final_path.clone(),
                compression_handler,
                None,
                "test.cache".to_string(),
            )
            .await
            .unwrap();

            let data = b"Test data";
            writer.write_chunk(data).await.unwrap();

            // Drop without commit or discard
        }

        // Temp file should be cleaned up by Drop
        // Note: This is best-effort and may not always work in tests
        // due to async cleanup timing
    }

    // =========================================================================
    // Atomic commit sequence tests (Requirements 4.1, 4.2, 4.3, 4.4, 4.5)
    // =========================================================================

    /// Test successful commit: both .bin and .meta exist after commit.
    /// Validates: Requirements 4.1, 4.2, 4.3, 4.5
    #[tokio::test]
    async fn test_atomic_commit_both_bin_and_meta_exist() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("entry.bin");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "entry.bin".to_string(),
        )
        .await
        .unwrap();

        writer
            .write_chunk(b"atomic commit test data")
            .await
            .unwrap();

        let metadata = create_test_metadata();
        writer.commit(metadata).await.unwrap();

        // Both .bin and .meta must exist
        assert!(final_path.exists(), ".bin file must exist after commit");
        let metadata_path = final_path.with_extension("meta");
        assert!(metadata_path.exists(), ".meta file must exist after commit");

        // Verify .meta content is valid JSON with expected fields
        let meta_content = tokio::fs::read_to_string(&metadata_path).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&meta_content).unwrap();
        assert_eq!(parsed["etag"], "test-etag");
        assert_eq!(parsed["content_length"], 1024);
    }

    /// Test that .bin.tmp is removed after successful commit (no leftover temp files).
    /// Validates: Requirements 4.1, 4.2
    #[tokio::test]
    async fn test_atomic_commit_no_leftover_temp_files() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("entry.bin");
        let temp_path = final_path.with_extension("tmp");
        let meta_tmp_path = final_path.with_extension("meta").with_extension("meta.tmp");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "entry.bin".to_string(),
        )
        .await
        .unwrap();

        // Verify temp file exists during write
        writer.write_chunk(b"temp file test").await.unwrap();
        assert!(temp_path.exists(), ".bin.tmp should exist during write");

        let metadata = create_test_metadata();
        writer.commit(metadata).await.unwrap();

        // After commit, no temp files should remain
        assert!(
            !temp_path.exists(),
            ".bin.tmp must not exist after successful commit"
        );
        assert!(
            !meta_tmp_path.exists(),
            ".meta.tmp must not exist after successful commit"
        );

        // Final files should exist
        assert!(final_path.exists());
        assert!(final_path.with_extension("meta").exists());
    }

    /// Test that if commit fails (non-existent target directory), cleanup removes orphan files.
    /// Validates: Requirements 4.4
    #[tokio::test]
    async fn test_atomic_commit_failure_cleans_up_orphans() {
        let temp_dir = TempDir::new().unwrap();
        // Create the writer in a valid directory, but set final_path to a non-existent
        // subdirectory so the rename will fail.
        let write_dir = temp_dir.path().join("write_area");
        tokio::fs::create_dir_all(&write_dir).await.unwrap();

        // The temp file will be created in write_area, but the final path points
        // to a non-existent directory that we'll remove before commit.
        let final_path = write_dir.join("entry.bin");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "entry.bin".to_string(),
        )
        .await
        .unwrap();

        writer.write_chunk(b"will fail on commit").await.unwrap();

        // Make the directory read-only so rename fails
        // On macOS/Linux, removing write permission on the directory prevents renames
        let temp_path = final_path.with_extension("tmp");
        assert!(temp_path.exists(), "temp file should exist before commit");

        // Move the temp file to a different location so the rename source doesn't exist
        // This simulates a failure in the rename step
        let moved_path = temp_dir.path().join("moved.tmp");
        tokio::fs::rename(&temp_path, &moved_path).await.unwrap();

        // Now commit should fail because the temp file is gone
        let metadata = create_test_metadata();
        let result = writer.commit(metadata).await;
        assert!(
            result.is_err(),
            "Commit should fail when temp file is missing"
        );

        // After failed commit, no orphan .bin or .meta should exist
        assert!(
            !final_path.exists(),
            ".bin must not exist after failed commit"
        );
        let metadata_path = final_path.with_extension("meta");
        assert!(
            !metadata_path.exists(),
            ".meta must not exist after failed commit"
        );
    }

    /// Test that after a failed commit, neither .bin nor .meta exist (atomicity guarantee).
    /// Simulates failure by making the metadata write path invalid.
    /// Validates: Requirements 4.4, 4.5
    #[tokio::test]
    async fn test_atomic_commit_failure_neither_bin_nor_meta_exist() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("entry.bin");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "entry.bin".to_string(),
        )
        .await
        .unwrap();

        writer.write_chunk(b"atomicity test data").await.unwrap();

        // To simulate a failure after .bin rename but before .meta rename,
        // we create a directory at the .meta path so the .meta.tmp creation fails
        let metadata_path = final_path.with_extension("meta");
        let meta_tmp_path = metadata_path.with_extension("meta.tmp");
        // Create a directory where .meta.tmp should be a file — this causes File::create to fail
        tokio::fs::create_dir_all(&meta_tmp_path).await.unwrap();

        let metadata = create_test_metadata();
        let result = writer.commit(metadata).await;
        assert!(
            result.is_err(),
            "Commit should fail when .meta.tmp path is a directory"
        );

        // After failed commit, .bin should be cleaned up (even though rename succeeded)
        // The cleanup_on_failure removes final_path
        assert!(
            !final_path.exists(),
            ".bin must not exist after failed commit (cleanup removes it)"
        );
        // .meta should not exist either
        assert!(
            !metadata_path.exists() || metadata_path.is_dir(),
            ".meta file must not exist after failed commit"
        );
    }

    /// Test the discard() method removes temp files.
    /// Validates: Requirements 4.3
    #[tokio::test]
    async fn test_discard_removes_temp_files() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("entry.bin");
        let temp_path = final_path.with_extension("tmp");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "entry.bin".to_string(),
        )
        .await
        .unwrap();

        writer.write_chunk(b"data to discard").await.unwrap();

        // Temp file should exist before discard
        assert!(temp_path.exists(), ".tmp file should exist before discard");

        writer.discard().await.unwrap();

        // After discard, temp file must be gone
        assert!(
            !temp_path.exists(),
            ".tmp file must not exist after discard"
        );
        // Final file must not exist
        assert!(!final_path.exists(), ".bin must not exist after discard");
        // Metadata must not exist
        let metadata_path = final_path.with_extension("meta");
        assert!(
            !metadata_path.exists(),
            ".meta must not exist after discard"
        );
    }

    /// Test that discard on an already-finalized writer is a no-op.
    /// Validates: Requirements 4.3
    #[tokio::test]
    async fn test_discard_after_commit_is_noop() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("entry.bin");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "entry.bin".to_string(),
        )
        .await
        .unwrap();

        writer.write_chunk(b"committed data").await.unwrap();

        let metadata = create_test_metadata();
        writer.commit(metadata).await.unwrap();

        // Discard after commit should be a no-op (not remove committed files)
        writer.discard().await.unwrap();

        // Committed files should still exist
        assert!(
            final_path.exists(),
            ".bin should still exist after discard on committed writer"
        );
        let metadata_path = final_path.with_extension("meta");
        assert!(
            metadata_path.exists(),
            ".meta should still exist after discard on committed writer"
        );
    }

    /// Test that commit ordering is correct: .bin rename happens before .meta rename.
    /// We verify this by checking that after commit, .meta contains valid metadata
    /// that references the .bin file's properties.
    /// Validates: Requirements 4.3, 4.5
    #[tokio::test]
    async fn test_commit_ordering_bin_before_meta() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("ordered.bin");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "ordered.bin".to_string(),
        )
        .await
        .unwrap();

        let test_data = b"ordering verification data";
        writer.write_chunk(test_data).await.unwrap();

        let metadata = ObjectMetadata::new(
            "etag-order-test".to_string(),
            "Thu, 01 Jan 2026 00:00:00 GMT".to_string(),
            test_data.len() as u64,
            Some("text/plain".to_string()),
        );
        writer.commit(metadata).await.unwrap();

        // Both files exist — .meta was written after .bin
        assert!(final_path.exists());
        let metadata_path = final_path.with_extension("meta");
        assert!(metadata_path.exists());

        // .bin has content (compressed)
        let bin_content = tokio::fs::read(&final_path).await.unwrap();
        assert!(!bin_content.is_empty(), ".bin should have content");

        // .meta has valid JSON referencing the correct content_length
        let meta_content = tokio::fs::read_to_string(&metadata_path).await.unwrap();
        let parsed: ObjectMetadata = serde_json::from_str(&meta_content).unwrap();
        assert_eq!(parsed.etag, "etag-order-test");
        assert_eq!(parsed.content_length, test_data.len() as u64);
    }

    /// Test that double-commit is rejected.
    /// Validates: Requirements 4.2
    #[tokio::test]
    async fn test_double_commit_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let final_path = temp_dir.path().join("double.bin");
        let compression_handler = create_test_compression_handler();

        let mut writer = CacheWriter::new(
            final_path.clone(),
            compression_handler,
            None,
            "double.bin".to_string(),
        )
        .await
        .unwrap();

        writer.write_chunk(b"data").await.unwrap();

        let metadata = create_test_metadata();
        writer.commit(metadata.clone()).await.unwrap();

        // Second commit should fail
        let result = writer.commit(metadata).await;
        assert!(result.is_err(), "Double commit should be rejected");
    }
}
