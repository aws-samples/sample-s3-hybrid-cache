//! Property-based tests for the batched incremental write path in
//! `IncrementalRangeWriter`.
//!
//! **Property 1: Batched-write byte-identity**
//!
//! *For any* byte sequence of length N, *for any* partition into non-empty
//! chunks summing to N, *for any* `batch_size` in the valid range
//! [64 KiB, 16 MiB], writing the chunks via
//! `begin_incremental_range_write` → `write_range_chunk` per chunk →
//! `commit_incremental_range` and reading back via `load_range_data` must
//! return the original bytes.
//!
//! **Validates: Requirements 1.4, 2.7, 6.1**
//!
//! **Property 2: Batched-write equivalence to single-shot**
//!
//! *For any* input, partition, and `batch_size`, the file produced by the
//! batched incremental path must decompress to the same bytes as the file
//! produced by `store_range`.
//!
//! **Validates: Requirements 2.7, 6.3**
//!
//! **Property 6: Residual flush on commit**
//!
//! *For any* length N that is NOT an exact multiple of `batch_size`, the
//! residual `N mod batch_size` bytes must be flushed as a final LZ4 frame by
//! `commit_incremental_range`, and the cached file must decompress to all
//! N bytes.
//!
//! **Validates: Requirements 2.3, 1.4, 6.1**
//!
//! WARNING: This file contains property-based tests (PBT). Running it may
//! take longer than unit tests due to random input generation, multiple
//! async round-trips per case, and temp-dir setup per case.

use quickcheck::{quickcheck, TestResult};
use s3_proxy::cache_types::{ObjectMetadata, RangeSpec, UploadState};
use s3_proxy::compression::{CompressionAlgorithm, CompressionHandler};
use s3_proxy::disk_cache::DiskCacheManager;
use std::time::Duration;

/// Minimum accepted `compression_batch_size` (64 KiB).
const MIN_BATCH_SIZE: usize = 64 * 1024;
/// Maximum accepted `compression_batch_size` (16 MiB).
const MAX_BATCH_SIZE: usize = 16 * 1024 * 1024;
/// Upper bound on generated input size. Keeps property runs fast while still
/// exercising multi-batch flushing at the smallest valid `batch_size`.
const MAX_INPUT_SIZE: usize = 2 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

/// Map a `u32` seed into the valid `compression_batch_size` range
/// `[MIN_BATCH_SIZE, MAX_BATCH_SIZE]`.
fn batch_size_from_seed(seed: u32) -> usize {
    let span = MAX_BATCH_SIZE - MIN_BATCH_SIZE + 1;
    MIN_BATCH_SIZE + (seed as usize % span)
}

/// Split `data` into non-empty chunks using `split_seeds` to determine chunk
/// boundaries. Chunks sum to exactly `data.len()`. Borrowed from the existing
/// `incremental_write_property_test.rs` to keep partitioning behavior aligned
/// across property suites.
fn split_into_chunks(data: &[u8], split_seeds: &[u8]) -> Vec<Vec<u8>> {
    if data.is_empty() {
        return vec![];
    }
    if split_seeds.is_empty() {
        return vec![data.to_vec()];
    }

    let mut chunks = Vec::new();
    let mut offset = 0;
    let total_seeds = split_seeds.len();

    for (i, &seed) in split_seeds.iter().enumerate() {
        if offset >= data.len() {
            break;
        }
        let remaining_data = data.len() - offset;
        let remaining_chunks = total_seeds - i;

        if remaining_chunks <= 1 || remaining_data <= remaining_chunks {
            chunks.push(data[offset..].to_vec());
            offset = data.len();
            break;
        }

        let max_chunk = remaining_data - (remaining_chunks - 1);
        let chunk_size = 1 + (seed as usize % max_chunk);
        chunks.push(data[offset..offset + chunk_size].to_vec());
        offset += chunk_size;
    }

    if offset < data.len() {
        chunks.push(data[offset..].to_vec());
    }

    chunks
}

/// Expand a seed byte vector into a byte sequence of length up to
/// `MAX_INPUT_SIZE`. Using a non-constant repeating seed keeps round-trip
/// checks meaningful (constant data would round-trip trivially).
fn expand_input(data_seed: &[u8]) -> Vec<u8> {
    let target_size = if data_seed.len() <= 64 {
        data_seed.len()
    } else {
        (data_seed.len() * 128).min(MAX_INPUT_SIZE)
    };
    data_seed
        .iter()
        .cycle()
        .take(target_size)
        .copied()
        .collect()
}

fn test_object_metadata(len: u64) -> ObjectMetadata {
    ObjectMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: len,
        content_type: Some("application/octet-stream".to_string()),
        upload_state: UploadState::Complete,
        cumulative_size: len,
        parts: Vec::new(),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Property 1: Batched-write byte-identity
// **Validates: Requirements 1.4, 2.7, 6.1**
// ---------------------------------------------------------------------------

/// Property: writing any byte sequence via the batched incremental path with
/// any valid batch size round-trips through `load_range_data` byte-for-byte.
#[test]
fn prop_batched_write_byte_identity() {
    fn property(
        data_seed: Vec<u8>,
        split_seeds: Vec<u8>,
        batch_size_seed: u32,
    ) -> TestResult {
        if data_seed.is_empty() {
            return TestResult::discard();
        }

        let original_data = expand_input(&data_seed);
        let chunks = split_into_chunks(&original_data, &split_seeds);
        let batch_size = batch_size_from_seed(batch_size_seed);

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let cache_manager = DiskCacheManager::new(
                temp_dir.path().to_path_buf(),
                true,
                1024,
                false,
                batch_size,
            );
            cache_manager.initialize().await.unwrap();

            let cache_key = "test-bucket/prop-batched-roundtrip";
            let start = 0u64;
            let end = original_data.len() as u64 - 1;

            let mut writer = match cache_manager
                .begin_incremental_range_write(cache_key, start, end, true)
                .await
            {
                Ok(w) => w,
                Err(e) => {
                    return TestResult::error(format!(
                        "begin_incremental_range_write failed: {}",
                        e
                    ));
                }
            };

            for (i, chunk) in chunks.iter().enumerate() {
                if let Err(e) = DiskCacheManager::write_range_chunk(&mut writer, chunk) {
                    return TestResult::error(format!(
                        "write_range_chunk failed on chunk {}: {}",
                        i, e
                    ));
                }
            }

            // Capture compressed bytes before the writer is consumed by commit.
            // commit will add the residual frame (if any) after this snapshot,
                // so we read the final compressed length from disk below.
            let _pre_commit_compressed = writer.compressed_bytes_written;

            if let Err(e) = cache_manager
                .commit_incremental_range(
                    writer,
                    test_object_metadata(original_data.len() as u64),
                    Duration::from_secs(3600),
                )
                .await
            {
                return TestResult::error(format!("commit_incremental_range failed: {}", e));
            }

            // Build a RangeSpec from the committed .bin file; read it back.
            let final_path =
                cache_manager.get_new_range_file_path(cache_key, start, end);
            let ranges_dir = temp_dir.path().join("ranges");
            let relative_path = final_path
                .strip_prefix(&ranges_dir)
                .unwrap()
                .to_string_lossy()
                .to_string();
            let bin_len = match std::fs::metadata(&final_path) {
                Ok(m) => m.len(),
                Err(e) => {
                    return TestResult::error(format!(
                        "Failed to stat committed .bin file: {}",
                        e
                    ));
                }
            };

            let range_spec = RangeSpec::new(
                start,
                end,
                relative_path,
                CompressionAlgorithm::Lz4,
                bin_len,
                original_data.len() as u64,
            );

            let loaded = match cache_manager.load_range_data(&range_spec).await {
                Ok(d) => d,
                Err(e) => {
                    return TestResult::error(format!("load_range_data failed: {}", e));
                }
            };

            if loaded != original_data {
                return TestResult::error(format!(
                    "Batched round-trip mismatch: original {} bytes, loaded {} bytes, \
                     {} chunks, batch_size={}",
                    original_data.len(),
                    loaded.len(),
                    chunks.len(),
                    batch_size
                ));
            }

            TestResult::passed()
        })
    }

    quickcheck(property as fn(Vec<u8>, Vec<u8>, u32) -> TestResult);
}

// ---------------------------------------------------------------------------
// Property 2: Batched-write equivalence to single-shot
// **Validates: Requirements 2.7, 6.3**
// ---------------------------------------------------------------------------

/// Property: the concatenated-frame file produced by the batched incremental
/// path decompresses to the same bytes as the single-frame file produced by
/// `store_range`, for any input, partition, and valid `batch_size`.
#[test]
fn prop_batched_write_equivalence_to_single_shot() {
    fn property(
        data_seed: Vec<u8>,
        split_seeds: Vec<u8>,
        batch_size_seed: u32,
    ) -> TestResult {
        if data_seed.is_empty() {
            return TestResult::discard();
        }

        let original_data = expand_input(&data_seed);
        let chunks = split_into_chunks(&original_data, &split_seeds);
        let batch_size = batch_size_from_seed(batch_size_seed);

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            // --- Batched incremental path ---
            let incr_dir = tempfile::TempDir::new().unwrap();
            let incr_cache = DiskCacheManager::new(
                incr_dir.path().to_path_buf(),
                true,
                1024,
                false,
                batch_size,
            );
            incr_cache.initialize().await.unwrap();

            let cache_key = "test-bucket/prop-batched-equivalence";
            let start = 0u64;
            let end = original_data.len() as u64 - 1;

            let mut writer = match incr_cache
                .begin_incremental_range_write(cache_key, start, end, true)
                .await
            {
                Ok(w) => w,
                Err(e) => {
                    return TestResult::error(format!(
                        "begin_incremental_range_write failed: {}",
                        e
                    ));
                }
            };

            for (i, chunk) in chunks.iter().enumerate() {
                if let Err(e) = DiskCacheManager::write_range_chunk(&mut writer, chunk) {
                    return TestResult::error(format!(
                        "write_range_chunk failed on chunk {}: {}",
                        i, e
                    ));
                }
            }

            let metadata = test_object_metadata(original_data.len() as u64);

            if let Err(e) = incr_cache
                .commit_incremental_range(writer, metadata.clone(), Duration::from_secs(3600))
                .await
            {
                return TestResult::error(format!("commit_incremental_range failed: {}", e));
            }

            // --- Single-shot store_range path ---
            let shot_dir = tempfile::TempDir::new().unwrap();
            let mut shot_cache = DiskCacheManager::new(
                shot_dir.path().to_path_buf(),
                true,
                1024,
                false,
                1_048_576,
            );
            shot_cache.initialize().await.unwrap();

            if let Err(e) = shot_cache
                .store_range(
                    cache_key,
                    start,
                    end,
                    &original_data,
                    metadata,
                    Duration::from_secs(3600),
                    true,
                )
                .await
            {
                return TestResult::error(format!("store_range failed: {}", e));
            }

            // --- Read both committed files and decompress ---
            let incr_bin_path = incr_cache.get_new_range_file_path(cache_key, start, end);
            let shot_bin_path = shot_cache.get_new_range_file_path(cache_key, start, end);

            let incr_compressed = match std::fs::read(&incr_bin_path) {
                Ok(d) => d,
                Err(e) => {
                    return TestResult::error(format!(
                        "Failed to read incremental .bin file: {}",
                        e
                    ));
                }
            };
            let shot_compressed = match std::fs::read(&shot_bin_path) {
                Ok(d) => d,
                Err(e) => {
                    return TestResult::error(format!(
                        "Failed to read single-shot .bin file: {}",
                        e
                    ));
                }
            };

            let handler = CompressionHandler::new(0, true);

            let incr_decompressed = match handler
                .decompress_with_algorithm(&incr_compressed, CompressionAlgorithm::Lz4)
            {
                Ok(d) => d,
                Err(e) => {
                    return TestResult::error(format!(
                        "Failed to decompress incremental file: {}",
                        e
                    ));
                }
            };
            let shot_decompressed = match handler
                .decompress_with_algorithm(&shot_compressed, CompressionAlgorithm::Lz4)
            {
                Ok(d) => d,
                Err(e) => {
                    return TestResult::error(format!(
                        "Failed to decompress single-shot file: {}",
                        e
                    ));
                }
            };

            if incr_decompressed != original_data {
                return TestResult::error(format!(
                    "Batched decompressed does not match original: got {} bytes vs {} \
                     (batch_size={}, {} chunks)",
                    incr_decompressed.len(),
                    original_data.len(),
                    batch_size,
                    chunks.len()
                ));
            }
            if shot_decompressed != original_data {
                return TestResult::error(format!(
                    "Single-shot decompressed does not match original: got {} bytes vs {}",
                    shot_decompressed.len(),
                    original_data.len()
                ));
            }
            if incr_decompressed != shot_decompressed {
                return TestResult::error(format!(
                    "Decompressed outputs differ: batched {} bytes vs single-shot {} bytes",
                    incr_decompressed.len(),
                    shot_decompressed.len()
                ));
            }

            TestResult::passed()
        })
    }

    quickcheck(property as fn(Vec<u8>, Vec<u8>, u32) -> TestResult);
}

// ---------------------------------------------------------------------------
// Property 6: Residual flush on commit
// **Validates: Requirements 2.3, 1.4, 6.1**
// ---------------------------------------------------------------------------

/// Property: when `N mod batch_size != 0`, `commit_incremental_range` SHALL
/// flush the residual bytes as a final LZ4 frame, and `load_range_data`
/// SHALL return all N bytes.
///
/// Discards cases where N is an exact multiple of batch_size to focus the
/// property on the residual-flush arm. Picks a small `batch_size` so typical
/// generated inputs exercise at least one full flush plus a residual.
#[test]
fn prop_residual_flush_on_commit() {
    fn property(data_seed: Vec<u8>, split_seeds: Vec<u8>) -> TestResult {
        if data_seed.is_empty() {
            return TestResult::discard();
        }

        // Fix batch_size at the minimum valid value so that typical expanded
        // inputs (up to 2 MiB) span multiple batches with a non-zero residual
        // on almost every case.
        let batch_size = MIN_BATCH_SIZE;

        let original_data = expand_input(&data_seed);

        // Focus the property on non-exact-multiple lengths. If we happened
        // to land on a multiple, discard — other properties already cover
        // the exact-multiple path.
        if original_data.len() % batch_size == 0 {
            return TestResult::discard();
        }

        let chunks = split_into_chunks(&original_data, &split_seeds);

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let cache_manager = DiskCacheManager::new(
                temp_dir.path().to_path_buf(),
                true,
                1024,
                false,
                batch_size,
            );
            cache_manager.initialize().await.unwrap();

            let cache_key = "test-bucket/prop-residual-flush";
            let start = 0u64;
            let end = original_data.len() as u64 - 1;

            let mut writer = match cache_manager
                .begin_incremental_range_write(cache_key, start, end, true)
                .await
            {
                Ok(w) => w,
                Err(e) => {
                    return TestResult::error(format!(
                        "begin_incremental_range_write failed: {}",
                        e
                    ));
                }
            };

            for (i, chunk) in chunks.iter().enumerate() {
                if let Err(e) = DiskCacheManager::write_range_chunk(&mut writer, chunk) {
                    return TestResult::error(format!(
                        "write_range_chunk failed on chunk {}: {}",
                        i, e
                    ));
                }
            }

            // Before commit: whatever is still buffered is strictly less than
            // one full batch (any >= batch_size threshold would have already
            // triggered a flush). It's the residual that commit must flush.
            if writer.batch_buf_len() >= batch_size {
                return TestResult::error(format!(
                    "Pre-commit batch_buf_len() = {} >= batch_size {} (should have flushed)",
                    writer.batch_buf_len(),
                    batch_size
                ));
            }
            // And there must be *some* residual in this property — since we
            // discard exact-multiple lengths above, if the last chunk didn't
            // exactly complete a batch, some bytes must remain. But depending
            // on chunk alignment, bytes_written could have partially overshot
            // a batch boundary and the residual could even be zero on some
            // partitions. Only assert it's smaller than one batch.
            let pre_commit_compressed = writer.compressed_bytes_written;
            let writer_batch_buf_len_pre_commit = writer.batch_buf_len();

            if let Err(e) = cache_manager
                .commit_incremental_range(
                    writer,
                    test_object_metadata(original_data.len() as u64),
                    Duration::from_secs(3600),
                )
                .await
            {
                return TestResult::error(format!("commit_incremental_range failed: {}", e));
            }

            let final_path =
                cache_manager.get_new_range_file_path(cache_key, start, end);
            let ranges_dir = temp_dir.path().join("ranges");
            let relative_path = final_path
                .strip_prefix(&ranges_dir)
                .unwrap()
                .to_string_lossy()
                .to_string();
            let bin_len = match std::fs::metadata(&final_path) {
                Ok(m) => m.len(),
                Err(e) => {
                    return TestResult::error(format!(
                        "Failed to stat committed .bin file: {}",
                        e
                    ));
                }
            };

            // The final .bin must be strictly larger than the bytes that
            // were already flushed before commit — i.e. commit actually
            // appended a residual frame. This is the core of Property 6:
            // the residual must not be dropped, it must land in the file.
            if bin_len <= pre_commit_compressed && writer_batch_buf_len_pre_commit > 0 {
                return TestResult::error(format!(
                    "Residual frame was not appended on commit: \
                     bin_len={} <= pre_commit_compressed={}",
                    bin_len, pre_commit_compressed
                ));
            }

            let range_spec = RangeSpec::new(
                start,
                end,
                relative_path,
                CompressionAlgorithm::Lz4,
                bin_len,
                original_data.len() as u64,
            );

            let loaded = match cache_manager.load_range_data(&range_spec).await {
                Ok(d) => d,
                Err(e) => {
                    return TestResult::error(format!("load_range_data failed: {}", e));
                }
            };

            if loaded != original_data {
                return TestResult::error(format!(
                    "Residual-flush round-trip mismatch: original {} bytes vs loaded {} \
                     (pre-commit batch_buf={} bytes at batch_size {})",
                    original_data.len(),
                    loaded.len(),
                    writer_batch_buf_len_pre_commit,
                    batch_size
                ));
            }

            TestResult::passed()
        })
    }

    quickcheck(property as fn(Vec<u8>, Vec<u8>) -> TestResult);
}

// ---------------------------------------------------------------------------
// Explicit residual-flush cases
//
// These lock in the contract for concrete, well-known non-multiple
// (length, batch_size) pairs. They complement Property 6 by providing
// deterministic reproductions that survive quickcheck seed changes.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_residual_flush_100k_bytes_64k_batch() {
    // 100_000 bytes at 64 KiB batch_size => one full batch (65_536) + residual 34_464 bytes.
    let batch_size: usize = MIN_BATCH_SIZE;
    let total_len: usize = 100_000;
    assert_ne!(
        total_len % batch_size,
        0,
        "test must use a non-multiple length"
    );

    let temp_dir = tempfile::TempDir::new().unwrap();
    let cache_manager = DiskCacheManager::new(
        temp_dir.path().to_path_buf(),
        true,
        1024,
        false,
        batch_size,
    );
    cache_manager.initialize().await.unwrap();

    // Pseudo-random but deterministic pattern so the round-trip is meaningful.
    let input: Vec<u8> = (0..total_len).map(|i| (i % 251) as u8).collect();

    let cache_key = "test-bucket/residual-100k-64k";
    let start = 0u64;
    let end = (total_len as u64) - 1;

    let mut writer = cache_manager
        .begin_incremental_range_write(cache_key, start, end, true)
        .await
        .unwrap();

    // Feed in uneven chunks to exercise mixed-threshold batching.
    for chunk in input.chunks(4099) {
        DiskCacheManager::write_range_chunk(&mut writer, chunk).unwrap();
    }

    let expected_residual = total_len % batch_size;
    // With 4099-byte chunks and 65_536-byte batch size, the flush threshold
    // is crossed mid-chunk — the implementation flushes whatever is in
    // batch_buf at that point (which exceeds batch_size by up to chunk_size).
    // So the residual is NOT strictly `total_len % batch_size`; it's
    // `total_len - (bytes flushed)`. What we can assert: (a) the residual is
    // strictly less than one batch (no pending flush), and (b) something
    // non-zero remains buffered so commit's residual flush is exercised.
    let residual = writer.batch_buf_len();
    assert!(
        residual < batch_size,
        "batch_buf must be under batch_size pre-commit: residual={}, batch_size={}",
        residual,
        batch_size
    );
    assert!(
        residual > 0,
        "residual must be > 0 to exercise commit's residual flush"
    );
    // Sanity: the "naive" residual is still informative for debugging.
    let _ = expected_residual;
    let pre_commit_compressed = writer.compressed_bytes_written;

    cache_manager
        .commit_incremental_range(
            writer,
            test_object_metadata(total_len as u64),
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

    let final_path = cache_manager.get_new_range_file_path(cache_key, start, end);
    let bin_len = std::fs::metadata(&final_path).unwrap().len();
    assert!(
        bin_len > pre_commit_compressed,
        "commit must append a residual frame: bin={} > pre={}",
        bin_len,
        pre_commit_compressed
    );

    let ranges_dir = temp_dir.path().join("ranges");
    let relative_path = final_path
        .strip_prefix(&ranges_dir)
        .unwrap()
        .to_string_lossy()
        .to_string();

    let range_spec = RangeSpec::new(
        start,
        end,
        relative_path,
        CompressionAlgorithm::Lz4,
        bin_len,
        total_len as u64,
    );

    let loaded = cache_manager.load_range_data(&range_spec).await.unwrap();
    assert_eq!(loaded, input, "decoded bytes must match original input");
}

#[tokio::test]
async fn test_residual_flush_3mib_plus_1_byte_at_1mib_batch() {
    // 3 MiB + 1 byte with a 1 MiB batch_size => three full batches + 1-byte residual.
    // Catches the edge case where the residual is a single byte.
    let batch_size: usize = 1_048_576;
    let total_len: usize = 3 * 1_048_576 + 1;

    let temp_dir = tempfile::TempDir::new().unwrap();
    let cache_manager = DiskCacheManager::new(
        temp_dir.path().to_path_buf(),
        true,
        1024,
        false,
        batch_size,
    );
    cache_manager.initialize().await.unwrap();

    let input: Vec<u8> = (0..total_len).map(|i| (i as u8).wrapping_mul(31)).collect();

    let cache_key = "test-bucket/residual-3mib-plus1";
    let start = 0u64;
    let end = (total_len as u64) - 1;

    let mut writer = cache_manager
        .begin_incremental_range_write(cache_key, start, end, true)
        .await
        .unwrap();
    for chunk in input.chunks(8192) {
        DiskCacheManager::write_range_chunk(&mut writer, chunk).unwrap();
    }
    assert_eq!(writer.batch_buf_len(), 1, "one-byte residual expected");

    cache_manager
        .commit_incremental_range(
            writer,
            test_object_metadata(total_len as u64),
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

    let final_path = cache_manager.get_new_range_file_path(cache_key, start, end);
    let ranges_dir = temp_dir.path().join("ranges");
    let relative_path = final_path
        .strip_prefix(&ranges_dir)
        .unwrap()
        .to_string_lossy()
        .to_string();
    let bin_len = std::fs::metadata(&final_path).unwrap().len();

    let range_spec = RangeSpec::new(
        start,
        end,
        relative_path,
        CompressionAlgorithm::Lz4,
        bin_len,
        total_len as u64,
    );

    let loaded = cache_manager.load_range_data(&range_spec).await.unwrap();
    assert_eq!(loaded, input);
}
