//! Compression Module
//!
//! Provides LZ4 compression and decompression for cached objects and ranges.
//! Handles compression thresholds, failure recovery, and statistics tracking.

use crate::{ProxyError, Result};
use lz4_flex::frame::{BlockMode, FrameDecoder, FrameEncoder, FrameInfo};
use std::io::{Read, Write};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};

/// Compression statistics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStats {
    pub total_objects_compressed: u64,
    pub total_objects_uncompressed: u64,
    pub total_bytes_before: u64,
    pub total_bytes_after: u64,
    pub compression_failures: u64,
    pub decompression_failures: u64,
    pub average_compression_ratio: f32,
    pub compression_time_ms: u64,
}

impl Default for CompressionStats {
    fn default() -> Self {
        Self {
            total_objects_compressed: 0,
            total_objects_uncompressed: 0,
            total_bytes_before: 0,
            total_bytes_after: 0,
            compression_failures: 0,
            compression_time_ms: 0,
            decompression_failures: 0,
            average_compression_ratio: 1.0,
        }
    }
}

/// Compression algorithm identifier
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    Lz4,  // LZ4 compression (frame format with content checksum)
    None, // No compression (data stored as-is, used when per-bucket compression is disabled)
          // Future algorithms can be added here:
          // Zstd,     // Zstandard compression
          // Brotli,   // Brotli compression
          // Lz4Hc,    // LZ4 High Compression
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        CompressionAlgorithm::Lz4
    }
}

/// Compression result with metadata
#[derive(Debug, Clone)]
pub struct CompressionResult {
    pub data: Vec<u8>,
    pub algorithm: CompressionAlgorithm,
    pub original_size: u64,
    pub compressed_size: u64,
    pub was_compressed: bool,
}

/// Compression handler for multiple algorithms
#[derive(Clone)]
pub struct CompressionHandler {
    compression_threshold: usize,
    compression_enabled: bool,
    content_aware_compression: bool,
    preferred_algorithm: CompressionAlgorithm,
    stats: CompressionStats,
}

impl CompressionHandler {
    /// Create a new compression handler
    pub fn new(compression_threshold: usize, compression_enabled: bool) -> Self {
        Self {
            compression_threshold,
            compression_enabled,
            content_aware_compression: true, // Enable content-aware compression by default
            preferred_algorithm: CompressionAlgorithm::Lz4,
            stats: CompressionStats::default(),
        }
    }

    /// Create a new compression handler with content-aware option
    pub fn new_with_content_aware(
        compression_threshold: usize,
        compression_enabled: bool,
        content_aware_compression: bool,
    ) -> Self {
        Self {
            compression_threshold,
            compression_enabled,
            content_aware_compression,
            preferred_algorithm: CompressionAlgorithm::Lz4,
            stats: CompressionStats::default(),
        }
    }

    /// Create a new compression handler with algorithm preference
    pub fn new_with_algorithm(
        compression_threshold: usize,
        compression_enabled: bool,
        content_aware_compression: bool,
        preferred_algorithm: CompressionAlgorithm,
    ) -> Self {
        Self {
            compression_threshold,
            compression_enabled,
            content_aware_compression,
            preferred_algorithm,
            stats: CompressionStats::default(),
        }
    }

    /// Check if data should be compressed based on size and configuration
    pub fn should_compress(&self, data_size: usize) -> bool {
        self.compression_enabled && data_size >= self.compression_threshold
    }

    /// Compress data unconditionally using LZ4 frame format.
    /// Used by RAM cache to save memory regardless of the global compression.enabled flag.
    pub fn compress_always(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        if data.len() < 64 {
            return Self::wrap_in_frame(data);
        }
        let mut frame_info = FrameInfo::new();
        frame_info.content_checksum = true;
        frame_info.block_mode = BlockMode::Independent;
        let mut output = Vec::new();
        let mut encoder = FrameEncoder::with_frame_info(frame_info, &mut output);
        encoder.write_all(data).map_err(|e| {
            ProxyError::CompressionError(format!("LZ4 frame encode failed: {}", e))
        })?;
        encoder.finish().map_err(|e| {
            ProxyError::CompressionError(format!("LZ4 frame finish failed: {}", e))
        })?;
        Ok(output)
    }

    /// Check if a file path/key should be compressed based on content type
    pub fn should_compress_content(&self, path: &str, data_size: usize) -> bool {
        if !self.compression_enabled || data_size < self.compression_threshold {
            return false;
        }

        if !self.content_aware_compression {
            return true; // Fall back to size-only check if content-aware is disabled
        }

        // Extract file extension from path
        let extension = Self::extract_file_extension(path);

        // Check if this is an already-compressed format that won't benefit from LZ4
        !Self::is_already_compressed_format(&extension)
    }

    /// Extract file extension from a path or S3 key
    fn extract_file_extension(path: &str) -> String {
        if let Some(last_segment) = path.split('/').last() {
            if let Some(dot_pos) = last_segment.rfind('.') {
                return last_segment[dot_pos + 1..].to_lowercase();
            }
        }
        String::new()
    }

    /// Check if a file extension represents an already-compressed format
    fn is_already_compressed_format(extension: &str) -> bool {
        match extension {
            // Image formats (already compressed)
            "jpg" | "jpeg" | "png" | "gif" | "webp" | "avif" | "heic" | "heif" => true,

            // Video formats (already compressed)
            "mp4" | "avi" | "mkv" | "mov" | "wmv" | "flv" | "webm" | "m4v" => true,

            // Audio formats (already compressed)
            "mp3" | "aac" | "ogg" | "flac" | "m4a" | "wma" | "opus" => true,

            // Archive formats (already compressed)
            "zip" | "rar" | "7z" | "gz" | "bz2" | "xz" | "lz4" | "zst" | "tar.gz" | "tgz" => true,

            // Document formats (already compressed)
            "pdf" | "docx" | "xlsx" | "pptx" | "odt" | "ods" | "odp" => true,

            // Application formats (already compressed)
            "apk" | "ipa" | "jar" | "war" | "ear" => true,

            // Font formats (already compressed)
            "woff" | "woff2" => true,

            // Database formats (often compressed)
            "sqlite" | "db" => true,

            // Executable formats (often compressed)
            "exe" | "msi" | "dmg" | "pkg" => true,

            // Everything else should be compressed
            _ => false,
        }
    }

    /// Get list of file extensions that are skipped for compression
    pub fn get_skipped_extensions() -> Vec<&'static str> {
        vec![
            // Images
            "jpg", "jpeg", "png", "gif", "webp", "avif", "heic", "heif", // Video
            "mp4", "avi", "mkv", "mov", "wmv", "flv", "webm", "m4v", // Audio
            "mp3", "aac", "ogg", "flac", "m4a", "wma", "opus", // Archives
            "zip", "rar", "7z", "gz", "bz2", "xz", "lz4", "zst", "tar.gz", "tgz",
            // Documents
            "pdf", "docx", "xlsx", "pptx", "odt", "ods", "odp", // Applications
            "apk", "ipa", "jar", "war", "ear", // Fonts
            "woff", "woff2", // Database
            "sqlite", "db", // Executables
            "exe", "msi", "dmg", "pkg",
        ]
    }

    /// Wrap data in LZ4 frame format without compression (uncompressed blocks).
    /// Used for non-compressible data and when compression is disabled,
    /// to provide integrity checksums via the frame content checksum.
    fn wrap_in_frame(data: &[u8]) -> Result<Vec<u8>> {
        let mut frame_info = FrameInfo::new();
        frame_info.content_checksum = true;
        frame_info.block_mode = BlockMode::Independent;

        let mut output = Vec::new();
        let mut encoder = FrameEncoder::with_frame_info(frame_info, &mut output);
        encoder.write_all(data).map_err(|e| {
            ProxyError::CompressionError(format!("Failed to write data to LZ4 frame encoder: {}", e))
        })?;
        encoder.finish().map_err(|e| {
            ProxyError::CompressionError(format!("Failed to finish LZ4 frame encoding: {}", e))
        })?;
        Ok(output)
    }

    /// Compress data using LZ4
    pub fn compress_data(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        if !self.should_compress(data.len()) {
            debug!(
                "Wrapping {} bytes in frame without compression (below threshold or disabled)",
                data.len()
            );
            self.stats.total_objects_uncompressed += 1;
            return Self::wrap_in_frame(data);
        }

        let start_time = std::time::Instant::now();

        let mut frame_info = FrameInfo::new();
        frame_info.content_checksum = true;
        frame_info.block_mode = BlockMode::Independent;

        let mut output = Vec::new();
        let mut encoder = FrameEncoder::with_frame_info(frame_info, &mut output);
        encoder.write_all(data).map_err(|e| {
            ProxyError::CompressionError(format!("Failed to write data to LZ4 frame encoder: {}", e))
        })?;
        encoder.finish().map_err(|e| {
            ProxyError::CompressionError(format!("Failed to finish LZ4 frame encoding: {}", e))
        })?;

        let duration_ms = start_time.elapsed().as_millis() as u64;
        let original_size = data.len() as u64;
        let compressed_size = output.len() as u64;

        self.stats.total_objects_compressed += 1;
        self.stats.total_bytes_before += original_size;
        self.stats.total_bytes_after += compressed_size;
        self.stats.compression_time_ms += duration_ms;

        // Update average compression ratio
        if self.stats.total_bytes_before > 0 {
            self.stats.average_compression_ratio =
                self.stats.total_bytes_after as f32 / self.stats.total_bytes_before as f32;
        }

        debug!(
            "Compressed {} bytes to {} bytes (ratio: {:.2}, time: {}ms)",
            original_size,
            compressed_size,
            compressed_size as f32 / original_size as f32,
            duration_ms
        );

        Ok(output)
    }

    /// Compress data with failure handling - stores uncompressed on failure
    pub fn compress_data_with_fallback(&mut self, data: &[u8]) -> (Vec<u8>, bool) {
        match self.compress_data(data) {
            Ok(compressed_data) => {
                // Check if data was actually compressed (not just wrapped in frame)
                let was_compressed = self.should_compress(data.len());
                (compressed_data, was_compressed)
            }
            Err(e) => {
                warn!("Compression failed, wrapping in frame: {}", e);
                self.stats.compression_failures += 1;
                self.stats.total_objects_uncompressed += 1;
                // Wrap in frame even on failure for integrity
                match Self::wrap_in_frame(data) {
                    Ok(framed_data) => (framed_data, false),
                    Err(_) => (data.to_vec(), false),
                }
            }
        }
    }

    /// Compress data with content-aware logic based on file path
    pub fn compress_data_content_aware(&mut self, data: &[u8], path: &str) -> Result<Vec<u8>> {
        if !self.should_compress_content(path, data.len()) {
            let reason = if !self.compression_enabled {
                "compression disabled"
            } else if data.len() < self.compression_threshold {
                "below size threshold"
            } else {
                "already-compressed format detected"
            };

            debug!(
                "Wrapping in frame without compression for {} ({} bytes): {}",
                path,
                data.len(),
                reason
            );
            self.stats.total_objects_uncompressed += 1;
            return Self::wrap_in_frame(data);
        }

        // Use regular compression logic
        self.compress_data(data)
    }

    /// Compress data with content-aware logic and failure handling
    pub fn compress_data_content_aware_with_fallback(
        &mut self,
        data: &[u8],
        path: &str,
    ) -> (Vec<u8>, bool) {
        match self.compress_data_content_aware(data, path) {
            Ok(compressed_data) => {
                // Data is always in LZ4 frame format (compressed or uncompressed blocks),
                // so it always needs decompression via FrameDecoder on read.
                (compressed_data, true)
            }
            Err(e) => {
                warn!(
                    "Compression failed for {}, wrapping in frame: {}",
                    path, e
                );
                self.stats.compression_failures += 1;
                self.stats.total_objects_uncompressed += 1;
                // Wrap in frame even on failure for integrity
                match Self::wrap_in_frame(data) {
                    Ok(framed_data) => (framed_data, true),
                    Err(_) => (data.to_vec(), false),
                }
            }
        }
    }

    /// Compress data with content-aware logic returning full metadata
    pub fn compress_content_aware_with_metadata(
        &mut self,
        data: &[u8],
        path: &str,
    ) -> CompressionResult {
        let original_size = data.len() as u64;

        // Always use Lz4 algorithm — non-compressible data gets wrapped in frame
        // with uncompressed blocks, compressible data gets compressed normally
        let algorithm = CompressionAlgorithm::Lz4;

        match self.compress_with_algorithm(data, algorithm) {
            Ok(result) => result,
            Err(e) => {
                warn!(
                    "Compression failed for {}, wrapping in frame: {}",
                    path, e
                );
                self.stats.compression_failures += 1;

                // Even on failure, wrap in frame format for integrity
                match Self::wrap_in_frame(data) {
                    Ok(framed_data) => {
                        let compressed_size = framed_data.len() as u64;
                        CompressionResult {
                            data: framed_data,
                            algorithm: CompressionAlgorithm::Lz4,
                            original_size,
                            compressed_size,
                            was_compressed: false,
                        }
                    }
                    Err(_) => {
                        // Last resort: return raw data (should not happen in practice)
                        self.stats.total_objects_uncompressed += 1;
                        CompressionResult {
                            data: data.to_vec(),
                            algorithm: CompressionAlgorithm::Lz4,
                            original_size,
                            compressed_size: original_size,
                            was_compressed: false,
                        }
                    }
                }
            }
        }
    }

    /// Decompress data using LZ4 frame format
    pub fn decompress_data(&self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = FrameDecoder::new(compressed_data);
        let mut decompressed = Vec::new();
        match decoder.read_to_end(&mut decompressed) {
            Ok(_) => {
                debug!(
                    "Decompressed {} bytes to {} bytes",
                    compressed_data.len(),
                    decompressed.len()
                );
                Ok(decompressed)
            }
            Err(e) => {
                error!("Decompression failed: {}", e);
                Err(ProxyError::CompressionError(format!(
                    "Failed to decompress cached data: {}",
                    e
                )))
            }
        }
    }

    /// Get compression statistics
    pub fn get_stats(&self) -> &CompressionStats {
        &self.stats
    }

    /// Get compression statistics (async version for consistency with other components)
    pub async fn get_compression_statistics(&self) -> Result<CompressionStats> {
        Ok(self.stats.clone())
    }

    /// Reset compression statistics
    pub fn reset_stats(&mut self) {
        self.stats = CompressionStats::default();
    }

    /// Get compression threshold
    pub fn get_compression_threshold(&self) -> usize {
        self.compression_threshold
    }

    /// Set compression threshold
    pub fn set_compression_threshold(&mut self, threshold: usize) {
        self.compression_threshold = threshold;
    }

    /// Check if compression is enabled
    pub fn is_compression_enabled(&self) -> bool {
        self.compression_enabled
    }

    /// Enable or disable compression
    pub fn set_compression_enabled(&mut self, enabled: bool) {
        self.compression_enabled = enabled;
    }

    /// Check if content-aware compression is enabled
    pub fn is_content_aware_compression_enabled(&self) -> bool {
        self.content_aware_compression
    }

    /// Enable or disable content-aware compression
    pub fn set_content_aware_compression(&mut self, enabled: bool) {
        self.content_aware_compression = enabled;
    }

    /// Get preferred compression algorithm
    pub fn get_preferred_algorithm(&self) -> &CompressionAlgorithm {
        &self.preferred_algorithm
    }

    /// Set preferred compression algorithm
    pub fn set_preferred_algorithm(&mut self, algorithm: CompressionAlgorithm) {
        self.preferred_algorithm = algorithm;
    }

    /// Compress data with algorithm-specific logic
    pub fn compress_with_algorithm(
        &mut self,
        data: &[u8],
        algorithm: CompressionAlgorithm,
    ) -> Result<CompressionResult> {
        let original_size = data.len() as u64;

        let compressed_data = match algorithm {
            CompressionAlgorithm::Lz4 => {
                let mut frame_info = FrameInfo::new();
                frame_info.content_checksum = true;
                frame_info.block_mode = BlockMode::Independent;

                let mut output = Vec::new();
                let mut encoder = FrameEncoder::with_frame_info(frame_info, &mut output);
                encoder.write_all(data).map_err(|e| {
                    ProxyError::CompressionError(format!(
                        "Failed to write data to LZ4 frame encoder: {}",
                        e
                    ))
                })?;
                encoder.finish().map_err(|e| {
                    ProxyError::CompressionError(format!(
                        "Failed to finish LZ4 frame encoding: {}",
                        e
                    ))
                })?;
                output
            }
            CompressionAlgorithm::None => {
                // No compression — return data as-is
                return Ok(CompressionResult {
                    data: data.to_vec(),
                    algorithm: CompressionAlgorithm::None,
                    original_size,
                    compressed_size: original_size,
                    was_compressed: false,
                });
            }
            // Future algorithms would be handled here
        };

        let compressed_size = compressed_data.len() as u64;

        // All data goes through frame format now
        self.stats.total_objects_compressed += 1;
        self.stats.total_bytes_before += original_size;
        self.stats.total_bytes_after += compressed_size;

        // Update average compression ratio
        if self.stats.total_bytes_before > 0 {
            self.stats.average_compression_ratio =
                self.stats.total_bytes_after as f32 / self.stats.total_bytes_before as f32;
        }

        Ok(CompressionResult {
            data: compressed_data,
            algorithm: CompressionAlgorithm::Lz4,
            original_size,
            compressed_size,
            was_compressed: true,
        })
    }

    /// Decompress data based on algorithm
    pub fn decompress_with_algorithm(
        &self,
        data: &[u8],
        algorithm: CompressionAlgorithm,
    ) -> Result<Vec<u8>> {
        match algorithm {
            CompressionAlgorithm::Lz4 => self.decompress_data(data),
            CompressionAlgorithm::None => Ok(data.to_vec()),
            // Future algorithms would be handled here
        }
    }

    /// Try to decompress data, return error if decompression fails
    pub fn decompress_data_with_fallback(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Use frame decoder directly — it handles empty/small inputs
        let mut decoder = FrameDecoder::new(data);
        let mut decompressed = Vec::new();
        match decoder.read_to_end(&mut decompressed) {
            Ok(_) => {
                debug!(
                    "Decompressed {} bytes to {} bytes",
                    data.len(),
                    decompressed.len()
                );
                Ok(decompressed)
            }
            Err(e) => {
                error!("Decompression failed: {}", e);
                Err(ProxyError::CompressionError(format!(
                    "Failed to decompress cached data: {}",
                    e
                )))
            }
        }
    }

    /// Calculate compression ratio for given data sizes
    pub fn calculate_compression_ratio(original_size: u64, compressed_size: u64) -> f32 {
        if original_size == 0 {
            1.0
        } else {
            compressed_size as f32 / original_size as f32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_threshold() {
        let handler = CompressionHandler::new(100, true);

        // Small data should not be compressed
        let small_data = b"small";
        assert!(!handler.should_compress(small_data.len()));

        // Large data should be compressed
        let large_data = vec![b'x'; 200];
        assert!(handler.should_compress(large_data.len()));
    }

    #[test]
    fn test_compression_disabled() {
        let handler = CompressionHandler::new(10, false);

        // Even large data should not be compressed when disabled
        let large_data = vec![b'x'; 200];
        assert!(!handler.should_compress(large_data.len()));
    }

    #[test]
    fn test_compression_round_trip() {
        let mut handler = CompressionHandler::new(10, true);
        let original_data = b"This is some test data that should be compressed because it's longer than the threshold";

        // Compress the data
        let compressed = handler.compress_data(original_data).unwrap();

        // Decompress the data
        let decompressed = handler.decompress_data(&compressed).unwrap();

        // Should match original
        assert_eq!(original_data, decompressed.as_slice());
    }

    #[test]
    fn test_compression_with_fallback() {
        let mut handler = CompressionHandler::new(10, true);
        let test_data = b"This is test data for compression";

        let (compressed_data, was_compressed) = handler.compress_data_with_fallback(test_data);
        assert!(was_compressed);
        assert_ne!(compressed_data, test_data);

        // Should be able to decompress
        let decompressed = handler
            .decompress_data_with_fallback(&compressed_data)
            .unwrap();
        assert_eq!(test_data, decompressed.as_slice());
    }

    #[test]
    fn test_small_data_passthrough() {
        let mut handler = CompressionHandler::new(100, true);
        let small_data = b"small";

        let (result_data, was_compressed) = handler.compress_data_with_fallback(small_data);
        assert!(!was_compressed);
        // Small data is now frame-wrapped (not raw passthrough)
        assert_ne!(result_data, small_data);
        // Round-trip: decompress should recover original data
        let decompressed = handler.decompress_data(&result_data).unwrap();
        assert_eq!(decompressed, small_data);
    }

    #[test]
    fn test_compression_statistics() {
        let mut handler = CompressionHandler::new(10, true);
        let test_data = vec![b'A'; 100]; // Repeating data compresses well

        let initial_stats = handler.get_stats().clone();
        assert_eq!(initial_stats.total_objects_compressed, 0);

        // Compress some data
        let _compressed = handler.compress_data(&test_data).unwrap();

        let stats = handler.get_stats();
        assert_eq!(stats.total_objects_compressed, 1);
        assert_eq!(stats.total_bytes_before, 100);
        assert!(stats.total_bytes_after < 100); // Should be compressed
        assert!(stats.average_compression_ratio < 1.0);
    }

    #[test]
    fn test_compression_ratio_calculation() {
        assert_eq!(
            CompressionHandler::calculate_compression_ratio(100, 50),
            0.5
        );
        assert_eq!(CompressionHandler::calculate_compression_ratio(0, 0), 1.0);
        assert_eq!(
            CompressionHandler::calculate_compression_ratio(100, 100),
            1.0
        );
    }

    #[test]
    fn test_file_extension_extraction() {
        assert_eq!(
            CompressionHandler::extract_file_extension("file.txt"),
            "txt"
        );
        assert_eq!(
            CompressionHandler::extract_file_extension("path/to/file.json"),
            "json"
        );
        assert_eq!(
            CompressionHandler::extract_file_extension("bucket/folder/image.jpg"),
            "jpg"
        );
        assert_eq!(
            CompressionHandler::extract_file_extension("file.tar.gz"),
            "gz"
        );
        assert_eq!(
            CompressionHandler::extract_file_extension("noextension"),
            ""
        );
        assert_eq!(CompressionHandler::extract_file_extension(""), "");
    }

    #[test]
    fn test_already_compressed_format_detection() {
        // Images (should skip compression)
        assert!(CompressionHandler::is_already_compressed_format("jpg"));
        assert!(CompressionHandler::is_already_compressed_format("png"));
        assert!(CompressionHandler::is_already_compressed_format("gif"));
        assert!(CompressionHandler::is_already_compressed_format("webp"));

        // Video (should skip compression)
        assert!(CompressionHandler::is_already_compressed_format("mp4"));
        assert!(CompressionHandler::is_already_compressed_format("avi"));
        assert!(CompressionHandler::is_already_compressed_format("mkv"));

        // Audio (should skip compression)
        assert!(CompressionHandler::is_already_compressed_format("mp3"));
        assert!(CompressionHandler::is_already_compressed_format("aac"));
        assert!(CompressionHandler::is_already_compressed_format("ogg"));

        // Archives (should skip compression)
        assert!(CompressionHandler::is_already_compressed_format("zip"));
        assert!(CompressionHandler::is_already_compressed_format("gz"));
        assert!(CompressionHandler::is_already_compressed_format("7z"));

        // Documents (should skip compression)
        assert!(CompressionHandler::is_already_compressed_format("pdf"));
        assert!(CompressionHandler::is_already_compressed_format("docx"));

        // Text files (should compress)
        assert!(!CompressionHandler::is_already_compressed_format("txt"));
        assert!(!CompressionHandler::is_already_compressed_format("json"));
        assert!(!CompressionHandler::is_already_compressed_format("xml"));
        assert!(!CompressionHandler::is_already_compressed_format("html"));
        assert!(!CompressionHandler::is_already_compressed_format("css"));
        assert!(!CompressionHandler::is_already_compressed_format("js"));
    }

    #[test]
    fn test_content_aware_compression() {
        let handler = CompressionHandler::new(10, true);
        let test_data = vec![b'A'; 100]; // Repeating data that compresses well

        // Text file should be compressed
        assert!(handler.should_compress_content("file.txt", test_data.len()));
        assert!(handler.should_compress_content("data.json", test_data.len()));
        assert!(handler.should_compress_content("style.css", test_data.len()));

        // Already compressed files should not be compressed
        assert!(!handler.should_compress_content("image.jpg", test_data.len()));
        assert!(!handler.should_compress_content("video.mp4", test_data.len()));
        assert!(!handler.should_compress_content("archive.zip", test_data.len()));
        assert!(!handler.should_compress_content("document.pdf", test_data.len()));
    }

    #[test]
    fn test_content_aware_compression_with_paths() {
        let handler = CompressionHandler::new(10, true);
        let test_data = vec![b'A'; 100];

        // Test with S3-style paths
        assert!(handler.should_compress_content("bucket/folder/data.txt", test_data.len()));
        assert!(!handler.should_compress_content("bucket/images/photo.jpg", test_data.len()));
        assert!(!handler.should_compress_content("bucket/videos/movie.mp4", test_data.len()));

        // Test with nested paths
        assert!(handler.should_compress_content("deep/nested/path/config.json", test_data.len()));
        assert!(!handler.should_compress_content("deep/nested/path/archive.zip", test_data.len()));
    }

    #[test]
    fn test_content_aware_compression_disabled() {
        let handler = CompressionHandler::new_with_content_aware(10, true, false);
        let test_data = vec![b'A'; 100];

        // When content-aware is disabled, should compress based on size only
        assert!(handler.should_compress_content("image.jpg", test_data.len()));
        assert!(handler.should_compress_content("video.mp4", test_data.len()));
        assert!(handler.should_compress_content("file.txt", test_data.len()));
    }

    #[test]
    fn test_content_aware_compression_with_fallback() {
        let mut handler = CompressionHandler::new(10, true);
        let test_data = b"This is some test data that should be compressed";

        // Text file should be compressed
        let (compressed_txt, was_compressed_txt) =
            handler.compress_data_content_aware_with_fallback(test_data, "file.txt");
        assert!(was_compressed_txt);
        assert_ne!(compressed_txt.len(), test_data.len());

        // Image file is frame-wrapped with uncompressed blocks — still needs decompression
        let (compressed_jpg, was_compressed_jpg) =
            handler.compress_data_content_aware_with_fallback(test_data, "image.jpg");
        assert!(was_compressed_jpg); // always true: data is in LZ4 frame format
        assert_ne!(compressed_jpg, test_data); // frame-wrapped, not raw
        // Round-trip: decompress should recover original data
        let decompressed = handler.decompress_data(&compressed_jpg).unwrap();
        assert_eq!(decompressed, test_data);
    }

    #[test]
    fn test_skipped_extensions_list() {
        let skipped = CompressionHandler::get_skipped_extensions();

        // Should include common compressed formats
        assert!(skipped.contains(&"jpg"));
        assert!(skipped.contains(&"mp4"));
        assert!(skipped.contains(&"zip"));
        assert!(skipped.contains(&"pdf"));

        // Should not include text formats
        assert!(!skipped.contains(&"txt"));
        assert!(!skipped.contains(&"json"));
        assert!(!skipped.contains(&"html"));
    }

    #[test]
    fn test_compression_algorithm_handling() {
        let mut handler = CompressionHandler::new(10, true);

        // Default algorithm should be LZ4
        assert_eq!(
            *handler.get_preferred_algorithm(),
            CompressionAlgorithm::Lz4
        );

        // Algorithm should remain Lz4 (only variant now)
        handler.set_preferred_algorithm(CompressionAlgorithm::Lz4);
        assert_eq!(
            *handler.get_preferred_algorithm(),
            CompressionAlgorithm::Lz4
        );
    }

    #[test]
    fn test_compression_handler_with_algorithm() {
        let handler =
            CompressionHandler::new_with_algorithm(1024, true, true, CompressionAlgorithm::Lz4);

        assert_eq!(handler.get_compression_threshold(), 1024);
        assert!(handler.is_compression_enabled());
        assert!(handler.is_content_aware_compression_enabled());
        assert_eq!(
            *handler.get_preferred_algorithm(),
            CompressionAlgorithm::Lz4
        );
    }

    #[test]
    fn test_compression_with_metadata() {
        let mut handler = CompressionHandler::new(10, true);
        // Use repeating data that compresses well
        let test_data = "This is some test data for compression with metadata. ".repeat(10);
        let test_bytes = test_data.as_bytes();

        let result = handler.compress_content_aware_with_metadata(test_bytes, "file.txt");

        assert!(result.was_compressed);
        assert_eq!(result.algorithm, CompressionAlgorithm::Lz4);
        assert_eq!(result.original_size, test_bytes.len() as u64);
        assert!(result.compressed_size < result.original_size); // Should compress well due to repetition
        assert_ne!(result.data, test_bytes);
    }

    #[test]
    fn test_compression_with_metadata_skip_jpg() {
        let mut handler = CompressionHandler::new(10, true);
        let test_data = b"This is fake JPEG data that should not be compressed";

        let result = handler.compress_content_aware_with_metadata(test_data, "image.jpg");

        // JPEG data is wrapped in frame with uncompressed blocks, not compressed
        assert!(result.was_compressed); // was_compressed is true because data went through frame encoder
        assert_eq!(result.algorithm, CompressionAlgorithm::Lz4);
        assert_eq!(result.original_size, test_data.len() as u64);
        // Frame-wrapped data should be decompressible back to original
        let handler2 = CompressionHandler::new(10, true);
        let decompressed = handler2.decompress_data(&result.data).unwrap();
        assert_eq!(decompressed, test_data);
    }

    #[test]
    fn test_corrupted_frame_data_returns_error() {
        let handler = CompressionHandler::new(10, true);

        // Corrupt data that starts with LZ4 frame magic but has invalid content
        let mut corrupt_data = vec![0x04, 0x22, 0x4D, 0x18]; // LZ4 frame magic number
        corrupt_data.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01, 0x02]);

        let result = handler.decompress_data(&corrupt_data);
        assert!(result.is_err());
        match result {
            Err(ProxyError::CompressionError(msg)) => {
                assert!(msg.contains("Failed to decompress"), "Unexpected error message: {}", msg);
            }
            other => panic!("Expected ProxyError::CompressionError, got: {:?}", other),
        }
    }

    #[test]
    fn test_corrupt_cache_entry_decompression_error() {
        // Decompression of corrupt data returns an error at the compression layer.
        // The cache layer (disk_cache.rs) handles auto-deletion of corrupt entries
        // and falls back to S3 on checksum failure.
        let handler = CompressionHandler::new(10, true);

        // Create valid frame data, then corrupt the content bytes
        let mut compressor = CompressionHandler::new(10, true);
        let original = b"Some data to compress and then corrupt";
        let mut compressed = compressor.compress_data(original).unwrap();

        // Corrupt a byte in the middle of the frame (after the header)
        if compressed.len() > 15 {
            compressed[15] ^= 0xFF;
        }

        let result = handler.decompress_data(&compressed);
        assert!(result.is_err(), "Decompression of corrupted frame data should fail");
    }

    #[test]
    fn test_frame_uncompressed_blocks_round_trip() {
        let mut handler = CompressionHandler::new(1000, true); // High threshold
        let small_data = b"Below threshold data";

        // Data below threshold gets frame-wrapped with uncompressed blocks
        let compressed = handler.compress_data(small_data).unwrap();

        // Frame-wrapped data should be different from raw data
        assert_ne!(compressed.as_slice(), small_data.as_slice());

        // Round-trip: decompress should recover original data
        let decompressed = handler.decompress_data(&compressed).unwrap();
        assert_eq!(decompressed, small_data);
    }

    #[test]
    fn test_algorithm_based_decompression() {
        let mut handler = CompressionHandler::new(10, true);
        let test_data = b"This is test data for algorithm-based decompression";

        // Compress with LZ4
        let compressed = handler
            .compress_with_algorithm(test_data, CompressionAlgorithm::Lz4)
            .unwrap();
        assert_eq!(compressed.algorithm, CompressionAlgorithm::Lz4);

        // Decompress with LZ4
        let decompressed = handler
            .decompress_with_algorithm(&compressed.data, CompressionAlgorithm::Lz4)
            .unwrap();
        assert_eq!(decompressed, test_data);
    }
}
