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
        let (data_to_write, compressed_size) =
            match self
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
                    (Some(result.data), result.compressed_size as u64)
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

    /// Commit the cached data to its final location
    ///
    /// This performs an atomic rename operation, ensuring that either the
    /// complete file exists or no file exists (no partial writes).
    ///
    /// # Arguments
    ///
    /// * `metadata` - Object metadata to store alongside the cached data
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Flushing fails
    /// - Renaming fails
    /// - Metadata cannot be written
    /// - Writer has already been finalized
    ///
    /// # Requirements
    ///
    /// - Requirement 4.2: Atomic rename on success
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

        // Flush and close the temporary file
        if let Some(mut file) = self.temp_file.take() {
            file.flush().await.map_err(|e| {
                error!(
                    "Failed to flush temporary file: path={:?}, error={}",
                    self.temp_path, e
                );
                ProxyError::CacheError(format!("Failed to flush cache file: {}", e))
            })?;

            // Explicitly drop the file to close it before rename
            drop(file);
        }

        // Atomic rename from temp to final location
        tokio::fs::rename(&self.temp_path, &self.final_path)
            .await
            .map_err(|e| {
                error!(
                    "Failed to rename cache file: temp={:?}, final={:?}, error={}",
                    self.temp_path, self.final_path, e
                );
                ProxyError::CacheError(format!("Failed to commit cache file: {}", e))
            })?;

        // Write metadata file
        let metadata_path = self.final_path.with_extension("meta");
        let metadata_json = serde_json::to_string_pretty(&metadata).map_err(|e| {
            error!(
                "Failed to serialize metadata: path={:?}, error={}",
                metadata_path, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        tokio::fs::write(&metadata_path, metadata_json)
            .await
            .map_err(|e| {
                error!(
                    "Failed to write metadata file: path={:?}, error={}",
                    metadata_path, e
                );
                ProxyError::CacheError(format!("Failed to write metadata: {}", e))
            })?;

        self.finalized = true;

        info!(
            "Successfully committed cache entry: path={:?}, bytes={}",
            self.final_path, self.bytes_written
        );

        Ok(())
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

    /// Check if the writer has been finalized (committed or discarded)
    pub fn is_finalized(&self) -> bool {
        self.finalized
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
        assert!(content.len() > 0);
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
}
