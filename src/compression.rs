//! Compression Module
//!
//! Provides LZ4 compression and decompression for cached objects and ranges.
//! Handles compression thresholds, failure recovery, and statistics tracking.
//!
//! ## Content-aware compression decision
//!
//! Whether a given write is actually compressed is decided by the caller
//! (`CacheManager::effective_compression` in `cache.rs`), which combines:
//! per-key cache-rule overrides (rules win), the global size threshold, and
//! — only when no rule explicitly set `compression_enabled` — the built-in
//! extension denylist (`is_denylisted_extension`). This module no longer
//! makes that decision itself; `compress_with_metadata` takes the decision
//! as a parameter. See the compression-content-aware-fix spec.
//!
//! Writes that skip compression are NOT stored as raw bytes. They go through
//! [`CompressionHandler::encode_store_mode_frame`], which produces a valid
//! LZ4 frame with uncompressed ("stored") blocks and a real xxhash32 content
//! checksum, without invoking the LZ4 block compressor. This gives every
//! cache entry — compressed or not — the same integrity guarantee and read
//! path (`lz4_flex::frame::FrameDecoder`), while still skipping the CPU cost
//! of compression for content that won't benefit from it.
//! `CompressionAlgorithm::None` remains a valid tag for entries written by
//! older proxy versions (raw bytes, no checksum) and is still readable, but
//! no write path in this version produces it.

use crate::{ProxyError, Result};
use lz4_flex::frame::{BlockMode, FrameDecoder, FrameEncoder, FrameInfo};
use serde::{Deserialize, Serialize};
use std::hash::Hasher;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, warn};
use twox_hash::XxHash32;

/// Maximum uncompressed size of a single stored block in a store-mode frame.
/// Matches lz4_flex's `BlockSize::Max4MB` so the BD byte encodes block-size
/// code `7` (see the LZ4 frame format table in `lz4_flex`'s
/// `frame/header.rs`), keeping store-mode frames decodable by any standard
/// LZ4 frame reader, not just `lz4_flex`.
const STORE_MODE_MAX_BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// The high bit of the 4-byte little-endian block-size word marks a block as
/// stored (uncompressed) rather than compressed. Mirrors
/// `lz4_flex::frame::header`'s private `BLOCK_UNCOMPRESSED_SIZE_BIT`.
const BLOCK_UNCOMPRESSED_SIZE_BIT: u32 = 0x8000_0000;

/// LZ4 frame magic number (little-endian on the wire).
const LZ4F_MAGIC_NUMBER: u32 = 0x184D_2204;

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
}

impl Default for CompressionStats {
    fn default() -> Self {
        Self {
            total_objects_compressed: 0,
            total_objects_uncompressed: 0,
            total_bytes_before: 0,
            total_bytes_after: 0,
            compression_failures: 0,
            decompression_failures: 0,
            average_compression_ratio: 1.0,
        }
    }
}

/// Atomic-backed counters shared across every clone of a `CompressionHandler`.
///
/// `CompressionHandler` is `Clone` and is cloned per fresh `DiskCacheManager`
/// construction (see `CacheManager::create_configured_disk_cache_manager`),
/// while `main.rs` takes a *separate* `Arc<CompressionHandler>` snapshot once
/// at startup for health/metrics reporting. Without a shared backing store,
/// mutations on any clone after that point are invisible to the snapshot, so
/// `/metrics` and OTLP report frozen startup-time (zero) stats forever.
/// Wrapping the counters in `Arc<AtomicU64>` and sharing that `Arc` across
/// clones fixes this: every clone updates the same counters.
#[derive(Debug, Default)]
pub(crate) struct CompressionStatsAtomic {
    total_objects_compressed: AtomicU64,
    total_objects_uncompressed: AtomicU64,
    total_bytes_before: AtomicU64,
    total_bytes_after: AtomicU64,
    compression_failures: AtomicU64,
    decompression_failures: AtomicU64,
}

impl CompressionStatsAtomic {
    /// Record one flushed batch's byte counts (uncompressed in, framed out).
    /// Used by the streaming incremental writers in `disk_cache.rs`, which
    /// encode frames inline rather than through `CompressionHandler` methods
    /// (compression-content-aware-fix spec, Requirement 5.2). Store-mode
    /// batches are included, so `average_compression_ratio` reflects the real
    /// on-disk footprint across all writes.
    pub(crate) fn record_batch_bytes(&self, bytes_before: u64, bytes_after: u64) {
        self.total_bytes_before
            .fetch_add(bytes_before, Ordering::Relaxed);
        self.total_bytes_after
            .fetch_add(bytes_after, Ordering::Relaxed);
    }

    /// Record one finalized object/range write from the streaming path.
    pub(crate) fn record_object(&self, compressed: bool) {
        if compressed {
            self.total_objects_compressed
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.total_objects_uncompressed
                .fetch_add(1, Ordering::Relaxed);
        }
    }
    fn snapshot(&self) -> CompressionStats {
        let total_bytes_before = self.total_bytes_before.load(Ordering::Relaxed);
        let total_bytes_after = self.total_bytes_after.load(Ordering::Relaxed);
        let average_compression_ratio = if total_bytes_before > 0 {
            total_bytes_after as f32 / total_bytes_before as f32
        } else {
            1.0
        };
        CompressionStats {
            total_objects_compressed: self.total_objects_compressed.load(Ordering::Relaxed),
            total_objects_uncompressed: self.total_objects_uncompressed.load(Ordering::Relaxed),
            total_bytes_before,
            total_bytes_after,
            compression_failures: self.compression_failures.load(Ordering::Relaxed),
            decompression_failures: self.decompression_failures.load(Ordering::Relaxed),
            average_compression_ratio,
        }
    }
}

/// Compression algorithm identifier
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum CompressionAlgorithm {
    #[default]
    Lz4, // LZ4 compression (frame format with content checksum; may be
    // compressed blocks or store-mode/stored blocks — both are valid
    // LZ4 frames with the same integrity guarantee)
    None, // Legacy: raw bytes with no frame/checksum. No write path in this
          // version produces this tag; retained only so entries written by
          // older proxy versions remain readable.
          // Future algorithms can be added here:
          // Zstd,     // Zstandard compression
          // Brotli,   // Brotli compression
          // Lz4Hc,    // LZ4 High Compression
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
    // The handler's configured size threshold. Currently not read: the effective
    // "compress if size >= threshold" decision lives in
    // `CacheManager::effective_compression`, which owns its own threshold and
    // passes an explicit `should_compress` bool into `compress_with_metadata`.
    // Retained as part of the handler's configuration surface alongside the
    // future-use `preferred_algorithm` (see Requirement 5) — a future
    // multi-algorithm decision made inside the handler would consult it. Kept on
    // the constructor signature to avoid churning every call site.
    // Spec: compression-followup-fixes Requirement 4/5.
    #[allow(dead_code)]
    compression_threshold: usize,
    compression_enabled: bool,
    preferred_algorithm: CompressionAlgorithm,
    /// Shared across every clone so stats mutated on one clone (e.g. a fresh
    /// `DiskCacheManager`'s handler) are visible through any other clone
    /// (e.g. the snapshot `main.rs` hands to health/metrics at startup).
    stats: Arc<CompressionStatsAtomic>,
}

impl CompressionHandler {
    /// Create a new compression handler
    pub fn new(compression_threshold: usize, compression_enabled: bool) -> Self {
        Self {
            compression_threshold,
            compression_enabled,
            preferred_algorithm: CompressionAlgorithm::Lz4,
            stats: Arc::new(CompressionStatsAtomic::default()),
        }
    }

    /// Create a new compression handler with a specific algorithm preference.
    ///
    /// FUTURE USE — intentionally retained, not dead code. Production handlers
    /// are built via `new` (which hardcodes Lz4); this constructor is the
    /// forward-looking surface for planned multi-algorithm support (e.g. zstd),
    /// paired with `get_preferred_algorithm` and the `preferred_algorithm`
    /// config field. Do not remove in dead-code sweeps.
    /// Spec: compression-followup-fixes Requirement 5.
    pub fn new_with_algorithm(
        compression_threshold: usize,
        compression_enabled: bool,
        preferred_algorithm: CompressionAlgorithm,
    ) -> Self {
        Self {
            compression_threshold,
            compression_enabled,
            preferred_algorithm,
            stats: Arc::new(CompressionStatsAtomic::default()),
        }
    }

    /// Create a new compression handler sharing another handler's stats Arc.
    /// Used when constructing a fresh `DiskCacheManager` (e.g. from
    /// `CacheManager::create_configured_disk_cache_manager`) so its handler's
    /// activity still counts toward the stats snapshot exposed via
    /// `CacheManager::get_compression_handler()`.
    pub fn new_with_shared_stats(
        compression_threshold: usize,
        compression_enabled: bool,
        stats_source: &CompressionHandler,
    ) -> Self {
        Self {
            compression_threshold,
            compression_enabled,
            preferred_algorithm: stats_source.preferred_algorithm.clone(),
            stats: stats_source.stats.clone(),
        }
    }

    /// Shared stats handle for write paths that encode frames inline rather
    /// than through this handler's methods (the streaming incremental writers
    /// in `disk_cache.rs`). Lets them contribute to the same live counters.
    pub(crate) fn shared_stats(&self) -> Arc<CompressionStatsAtomic> {
        self.stats.clone()
    }

    /// Check whether a file path/key's extension is in the built-in
    /// already-compressed-format denylist (the *default* layer of the
    /// compression decision; per-key cache rules that explicitly set
    /// `compression_enabled` bypass this check entirely — see
    /// `CacheManager::effective_compression`).
    pub fn is_denylisted_extension(path: &str) -> bool {
        let extension = Self::extract_file_extension(path);
        Self::is_already_compressed_format(&extension)
    }

    /// Extract file extension from a path or S3 key
    fn extract_file_extension(path: &str) -> String {
        if let Some(last_segment) = path.split('/').next_back() {
            if let Some(dot_pos) = last_segment.rfind('.') {
                return last_segment[dot_pos + 1..].to_lowercase();
            }
        }
        String::new()
    }

    /// Check if a file extension represents an already-compressed format.
    ///
    /// Note: `extract_file_extension` returns only the final dot-suffix
    /// (e.g. `"gz"` for `file.tar.gz`, not `"tar.gz"`), so a multi-part
    /// suffix can never reach this match. `tar.gz` files are still
    /// correctly denylisted via the `"gz"` arm. Operators needing exact
    /// multi-part-suffix precision should use a `cache_rules.json` glob
    /// rule (e.g. `**/*.tar.gz`) instead, which matches the full key and
    /// has no extraction step.
    fn is_already_compressed_format(extension: &str) -> bool {
        match extension {
            // Image formats (already compressed)
            "jpg" | "jpeg" | "png" | "gif" | "webp" | "avif" | "heic" | "heif" => true,

            // Video formats (already compressed)
            "mp4" | "avi" | "mkv" | "mov" | "wmv" | "flv" | "webm" | "m4v" => true,

            // Audio formats (already compressed)
            "mp3" | "aac" | "ogg" | "flac" | "m4a" | "wma" | "opus" => true,

            // Archive formats (already compressed)
            "zip" | "rar" | "7z" | "gz" | "bz2" | "xz" | "lz4" | "zst" | "tgz" => true,

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

    /// Encode `data` as a valid LZ4 frame using stored (uncompressed) blocks
    /// and an xxhash32 content checksum, without invoking the LZ4 block
    /// compressor at all.
    ///
    /// This is the real "skip compression but keep integrity" mechanism —
    /// see the compression-content-aware-fix spec. The wire format here
    /// matches `lz4_flex::frame`'s encoder exactly (verified
    /// against `lz4_flex-0.11.6/src/frame/{header,compress}.rs`): magic
    /// number, FLG/BD bytes (independent blocks, content checksum, 4 MiB
    /// block size), one-byte header checksum (`xxh32(FLG..BD) >> 8`), one or
    /// more stored blocks (4-byte LE size with the high bit set, followed by
    /// the raw bytes), a 4-byte end mark (`0`), and a 4-byte little-endian
    /// content checksum (`xxh32` over the *uncompressed* bytes, seed 0).
    /// `lz4_flex::frame::FrameDecoder` requires no changes to read frames
    /// produced by this function — stored blocks are a standard part of the
    /// LZ4 frame format, not an lz4_flex extension.
    pub fn encode_store_mode_frame(data: &[u8]) -> Result<Vec<u8>> {
        // Frame descriptor: FLG byte (version 01, independent blocks,
        // content checksum) + BD byte (block size code 7 == 4 MiB).
        const FLG_SUPPORTED_VERSION_BITS: u8 = 0b0100_0000;
        const FLG_INDEPENDENT_BLOCKS: u8 = 0b0010_0000;
        const FLG_CONTENT_CHECKSUM: u8 = 0b0000_0100;
        const BD_BLOCK_SIZE_MAX_4MB: u8 = 7 << 4;

        let flg = FLG_SUPPORTED_VERSION_BITS | FLG_INDEPENDENT_BLOCKS | FLG_CONTENT_CHECKSUM;
        let bd = BD_BLOCK_SIZE_MAX_4MB;

        // Header checksum is computed over FLG..BD (no content-size / dict-id
        // fields are written here, matching `FrameInfo::write` with those
        // fields unset).
        let mut header_hasher = XxHash32::with_seed(0);
        header_hasher.write(&[flg, bd]);
        let header_checksum = (header_hasher.finish() >> 8) as u8;

        let block_count = data.len() / STORE_MODE_MAX_BLOCK_SIZE + 1;
        let mut output = Vec::with_capacity(data.len() + block_count * 4 + 15);
        output.extend_from_slice(&LZ4F_MAGIC_NUMBER.to_le_bytes());
        output.push(flg);
        output.push(bd);
        output.push(header_checksum);

        // One stored block per STORE_MODE_MAX_BLOCK_SIZE chunk. An empty
        // input produces zero data blocks (matches lz4_flex's own behavior
        // for an empty write — flush() is a no-op when src_start == src_end).
        for chunk in data.chunks(STORE_MODE_MAX_BLOCK_SIZE.max(1)) {
            let block_size_word = (chunk.len() as u32) | BLOCK_UNCOMPRESSED_SIZE_BIT;
            output.extend_from_slice(&block_size_word.to_le_bytes());
            output.extend_from_slice(chunk);
        }

        // End mark: a 4-byte zero block-size word.
        output.extend_from_slice(&0u32.to_le_bytes());

        // Content checksum over the full uncompressed payload.
        let content_checksum = XxHash32::oneshot(0, data);
        output.extend_from_slice(&content_checksum.to_le_bytes());

        Ok(output)
    }

    /// Compress data with the effective compression decision already made by
    /// the caller (see `CacheManager::effective_compression`), returning full
    /// metadata. `should_compress` controls whether the LZ4 block compressor
    /// runs at all: when true, standard compression; when false, a
    /// store-mode frame (no compressor invocation, still checksummed). Both
    /// outcomes are tagged `CompressionAlgorithm::Lz4` and decode uniformly.
    pub fn compress_with_metadata(
        &mut self,
        data: &[u8],
        path: &str,
        should_compress: bool,
    ) -> CompressionResult {
        let original_size = data.len() as u64;

        if !should_compress {
            return match Self::encode_store_mode_frame(data) {
                Ok(framed_data) => {
                    let compressed_size = framed_data.len() as u64;
                    self.stats
                        .total_objects_uncompressed
                        .fetch_add(1, Ordering::Relaxed);
                    CompressionResult {
                        data: framed_data,
                        algorithm: CompressionAlgorithm::Lz4,
                        original_size,
                        compressed_size,
                        was_compressed: false,
                    }
                }
                Err(e) => {
                    warn!(
                        "Store-mode frame encoding failed for {}, falling back to raw: {}",
                        path, e
                    );
                    self.stats
                        .compression_failures
                        .fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .total_objects_uncompressed
                        .fetch_add(1, Ordering::Relaxed);
                    CompressionResult {
                        data: data.to_vec(),
                        algorithm: CompressionAlgorithm::None,
                        original_size,
                        compressed_size: original_size,
                        was_compressed: false,
                    }
                }
            };
        }

        match self.compress_with_algorithm(data, CompressionAlgorithm::Lz4) {
            Ok(result) => result,
            Err(e) => {
                warn!(
                    "Compression failed for {}, falling back to store-mode frame: {}",
                    path, e
                );
                self.stats
                    .compression_failures
                    .fetch_add(1, Ordering::Relaxed);

                // Even on failure, use a store-mode frame for integrity.
                match Self::encode_store_mode_frame(data) {
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
                        self.stats
                            .total_objects_uncompressed
                            .fetch_add(1, Ordering::Relaxed);
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
        use std::io::Cursor;

        let mut decompressed = Vec::new();
        let mut cursor = Cursor::new(compressed_data);
        let total_len = compressed_data.len() as u64;

        // Loop to handle concatenated LZ4 frames (produced by incremental writes,
        // including a mix of compressed-block and store-mode frames).
        // FrameDecoder::read_to_end stops at the end of each frame, so we create
        // a new decoder for each frame until the cursor is exhausted.
        loop {
            if cursor.position() >= total_len {
                break;
            }

            let mut decoder = FrameDecoder::new(&mut cursor);
            match decoder.read_to_end(&mut decompressed) {
                Ok(0) => break, // No more data
                Ok(_) => continue,
                Err(e) => {
                    error!("Decompression failed: {}", e);
                    self.stats
                        .decompression_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(ProxyError::CompressionError(format!(
                        "Failed to decompress cached data: {}",
                        e
                    )));
                }
            }
        }

        debug!(
            "Decompressed {} bytes to {} bytes",
            compressed_data.len(),
            decompressed.len()
        );
        Ok(decompressed)
    }

    /// Get compression statistics (live snapshot, shared across handler clones)
    pub fn get_stats(&self) -> CompressionStats {
        self.stats.snapshot()
    }

    /// Get compression statistics (async version for consistency with other components)
    pub async fn get_compression_statistics(&self) -> Result<CompressionStats> {
        Ok(self.stats.snapshot())
    }

    /// Check if compression is enabled
    pub fn is_compression_enabled(&self) -> bool {
        self.compression_enabled
    }

    /// Get the preferred compression algorithm.
    ///
    /// FUTURE USE — intentionally retained, not dead code. Reads the
    /// `preferred_algorithm` set via `new_with_algorithm`; part of the planned
    /// multi-algorithm surface. Do not remove in dead-code sweeps.
    /// Spec: compression-followup-fixes Requirement 5.
    pub fn get_preferred_algorithm(&self) -> &CompressionAlgorithm {
        &self.preferred_algorithm
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
            } // Future algorithms would be handled here
        };

        let compressed_size = compressed_data.len() as u64;

        // All data goes through frame format now
        self.stats
            .total_objects_compressed
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_bytes_before
            .fetch_add(original_size, Ordering::Relaxed);
        self.stats
            .total_bytes_after
            .fetch_add(compressed_size, Ordering::Relaxed);

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_round_trip() {
        let mut handler = CompressionHandler::new(10, true);
        let original_data = b"This is some test data that should be compressed because it's longer than the threshold";

        let compressed = handler
            .compress_with_algorithm(original_data, CompressionAlgorithm::Lz4)
            .unwrap();
        let decompressed = handler.decompress_data(&compressed.data).unwrap();

        assert_eq!(original_data, decompressed.as_slice());
    }

    #[test]
    fn test_store_mode_round_trip() {
        let test_data = b"This data will be stored, not compressed";
        let framed = CompressionHandler::encode_store_mode_frame(test_data).unwrap();

        assert_ne!(framed.as_slice(), test_data.as_slice());

        let handler = CompressionHandler::new(10, true);
        let decompressed = handler.decompress_data(&framed).unwrap();
        assert_eq!(decompressed, test_data);
    }

    #[test]
    fn test_store_mode_empty_input() {
        let framed = CompressionHandler::encode_store_mode_frame(&[]).unwrap();
        let handler = CompressionHandler::new(10, true);
        let decompressed = handler.decompress_data(&framed).unwrap();
        assert_eq!(decompressed, Vec::<u8>::new());
    }

    #[test]
    fn test_store_mode_exact_block_boundary() {
        // Exactly one full block: verify the chunk-boundary logic doesn't
        // emit a spurious empty trailing block.
        let test_data = vec![b'Z'; STORE_MODE_MAX_BLOCK_SIZE];
        let framed = CompressionHandler::encode_store_mode_frame(&test_data).unwrap();
        let handler = CompressionHandler::new(10, true);
        let decompressed = handler.decompress_data(&framed).unwrap();
        assert_eq!(decompressed, test_data);
    }

    #[test]
    fn test_store_mode_spans_multiple_blocks() {
        // One byte over the block size forces a second stored block.
        let test_data = vec![b'Y'; STORE_MODE_MAX_BLOCK_SIZE + 1];
        let framed = CompressionHandler::encode_store_mode_frame(&test_data).unwrap();
        let handler = CompressionHandler::new(10, true);
        let decompressed = handler.decompress_data(&framed).unwrap();
        assert_eq!(decompressed, test_data);
    }

    #[test]
    fn test_store_mode_corruption_detected() {
        let test_data = b"Data that will be corrupted after store-mode encoding";
        let mut framed = CompressionHandler::encode_store_mode_frame(test_data).unwrap();

        // Flip a byte inside the stored block's data region (after the
        // frame header and the 4-byte block-size word).
        let corrupt_index = framed.len() - 6; // inside the payload, before the trailer
        framed[corrupt_index] ^= 0xFF;

        let handler = CompressionHandler::new(10, true);
        let result = handler.decompress_data(&framed);
        assert!(
            result.is_err(),
            "Corrupted store-mode frame should fail the content checksum"
        );
    }

    #[test]
    fn test_mixed_compressed_and_store_mode_frames_concatenate() {
        let mut handler = CompressionHandler::new(10, true);
        let compressible = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".repeat(5);
        let compressed = handler
            .compress_with_algorithm(compressible.as_bytes(), CompressionAlgorithm::Lz4)
            .unwrap();
        let stored =
            CompressionHandler::encode_store_mode_frame(b"incompressible-ish chunk").unwrap();

        let mut concatenated = compressed.data.clone();
        concatenated.extend_from_slice(&stored);

        let decompressed = handler.decompress_data(&concatenated).unwrap();
        let mut expected = compressible.as_bytes().to_vec();
        expected.extend_from_slice(b"incompressible-ish chunk");
        assert_eq!(decompressed, expected);
    }

    #[test]
    fn test_small_data_passthrough() {
        let handler = CompressionHandler::new(100, true);
        let small_data = b"small";

        // Small/below-threshold data is store-mode-framed (not raw passthrough).
        let framed = CompressionHandler::encode_store_mode_frame(small_data).unwrap();
        assert_ne!(framed, small_data);
        let decompressed = handler.decompress_data(&framed).unwrap();
        assert_eq!(decompressed, small_data);
    }

    #[test]
    fn test_compression_statistics_live_across_clones() {
        let mut handler = CompressionHandler::new(10, true);
        let clone = handler.clone();
        let test_data = vec![b'A'; 100]; // Repeating data compresses well

        let initial_stats = clone.get_stats();
        assert_eq!(initial_stats.total_objects_compressed, 0);

        // Mutate via the original handler...
        let _compressed = handler
            .compress_with_algorithm(&test_data, CompressionAlgorithm::Lz4)
            .unwrap();

        // ...and observe it through the independently-obtained clone, proving
        // the stats Arc is shared rather than duplicated per clone.
        let stats = clone.get_stats();
        assert_eq!(stats.total_objects_compressed, 1);
        assert_eq!(stats.total_bytes_before, 100);
        assert!(stats.total_bytes_after < 100); // Should be compressed
        assert!(stats.average_compression_ratio < 1.0);
    }

    #[test]
    fn test_decompression_failure_increments_stats() {
        let handler = CompressionHandler::new(10, true);
        let mut corrupt_data = vec![0x04, 0x22, 0x4D, 0x18]; // LZ4 frame magic number
        corrupt_data.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01, 0x02]);

        assert_eq!(handler.get_stats().decompression_failures, 0);
        let result = handler.decompress_data(&corrupt_data);
        assert!(result.is_err());
        assert_eq!(handler.get_stats().decompression_failures, 1);
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

        // Archives (should skip compression) — including .tar.gz via the "gz" arm
        assert!(CompressionHandler::is_already_compressed_format("zip"));
        assert!(CompressionHandler::is_already_compressed_format("gz"));
        assert!(CompressionHandler::is_already_compressed_format("tgz"));
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
    fn test_is_denylisted_extension_with_paths() {
        assert!(!CompressionHandler::is_denylisted_extension(
            "bucket/folder/data.txt"
        ));
        assert!(CompressionHandler::is_denylisted_extension(
            "bucket/images/photo.jpg"
        ));
        assert!(CompressionHandler::is_denylisted_extension(
            "bucket/videos/movie.mp4"
        ));
        assert!(!CompressionHandler::is_denylisted_extension(
            "deep/nested/path/config.json"
        ));
        assert!(CompressionHandler::is_denylisted_extension(
            "deep/nested/path/archive.zip"
        ));
        // file.tar.gz matches via the "gz" suffix (documented caveat).
        assert!(CompressionHandler::is_denylisted_extension(
            "bucket/data/archive.tar.gz"
        ));
    }

    #[test]
    fn test_denylisted_extensions() {
        // Should treat common already-compressed formats as denylisted.
        assert!(CompressionHandler::is_denylisted_extension("photo.jpg"));
        assert!(CompressionHandler::is_denylisted_extension("clip.mp4"));
        assert!(CompressionHandler::is_denylisted_extension("bundle.zip"));
        assert!(CompressionHandler::is_denylisted_extension("doc.pdf"));

        // Should not denylist compressible text formats.
        assert!(!CompressionHandler::is_denylisted_extension("notes.txt"));
        assert!(!CompressionHandler::is_denylisted_extension("data.json"));
        assert!(!CompressionHandler::is_denylisted_extension("index.html"));
    }

    #[test]
    fn test_compression_handler_with_algorithm() {
        let handler = CompressionHandler::new_with_algorithm(1024, true, CompressionAlgorithm::Lz4);

        assert!(handler.is_compression_enabled());
        assert_eq!(
            *handler.get_preferred_algorithm(),
            CompressionAlgorithm::Lz4
        );
    }

    #[test]
    fn test_new_with_shared_stats() {
        let mut source = CompressionHandler::new(10, true);
        let shared = CompressionHandler::new_with_shared_stats(20, false, &source);

        assert!(!shared.is_compression_enabled());

        let test_data = vec![b'A'; 100];
        let _ = source
            .compress_with_algorithm(&test_data, CompressionAlgorithm::Lz4)
            .unwrap();

        // Stats mutated via `source` are visible via `shared`, proving the
        // stats Arc was actually shared, not duplicated.
        assert_eq!(shared.get_stats().total_objects_compressed, 1);
    }

    #[test]
    fn test_compress_with_metadata_compresses_when_should_compress_true() {
        let mut handler = CompressionHandler::new(10, true);
        let test_data = "This is some test data for compression with metadata. ".repeat(10);
        let test_bytes = test_data.as_bytes();

        let result = handler.compress_with_metadata(test_bytes, "file.txt", true);

        assert!(result.was_compressed);
        assert_eq!(result.algorithm, CompressionAlgorithm::Lz4);
        assert_eq!(result.original_size, test_bytes.len() as u64);
        assert!(result.compressed_size < result.original_size);
        assert_ne!(result.data, test_bytes);
    }

    #[test]
    fn test_compress_with_metadata_store_mode_when_should_compress_false() {
        let mut handler = CompressionHandler::new(10, true);
        let test_data = b"This is fake JPEG data that should not be compressed";

        let result = handler.compress_with_metadata(test_data, "image.jpg", false);

        // Store-mode: no LZ4 block compression ran, but the result is still
        // a valid, checksummed LZ4 frame tagged Lz4.
        assert!(!result.was_compressed);
        assert_eq!(result.algorithm, CompressionAlgorithm::Lz4);
        assert_eq!(result.original_size, test_data.len() as u64);
        assert_ne!(result.data, test_data);

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
                assert!(
                    msg.contains("Failed to decompress"),
                    "Unexpected error message: {}",
                    msg
                );
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
        let compressed_result = compressor
            .compress_with_algorithm(original, CompressionAlgorithm::Lz4)
            .unwrap();
        let mut compressed = compressed_result.data;

        // Corrupt a byte in the middle of the frame (after the header)
        if compressed.len() > 15 {
            compressed[15] ^= 0xFF;
        }

        let result = handler.decompress_data(&compressed);
        assert!(
            result.is_err(),
            "Decompression of corrupted frame data should fail"
        );
    }

    #[test]
    fn test_frame_uncompressed_blocks_round_trip() {
        let handler = CompressionHandler::new(1000, true); // High threshold
        let small_data = b"Below threshold data";

        // Data below threshold gets store-mode-framed.
        let framed = CompressionHandler::encode_store_mode_frame(small_data).unwrap();

        // Frame-wrapped data should be different from raw data
        assert_ne!(framed.as_slice(), small_data.as_slice());

        // Round-trip: decompress should recover original data
        let decompressed = handler.decompress_data(&framed).unwrap();
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
