// Feature: throughput-optimization, Property 2: Incremental write round-trip
//
// For any byte sequence of length N, and for any partitioning of that sequence into a list of
// non-empty chunks whose lengths sum to N, writing those chunks via begin_incremental_range_write
// → write_range_chunk (for each chunk) → commit_incremental_range, then reading back via
// load_range_data, must return the original byte sequence.
//
// **Validates: Requirements 2.1, 2.2**

use quickcheck::{quickcheck, TestResult};
use s3_proxy::cache_types::{ObjectMetadata, RangeSpec, UploadState};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::DiskCacheManager;
use std::time::Duration;

/// Split `data` into chunks using `split_seeds` to determine chunk boundaries.
/// Each seed value determines the size of one chunk (at least 1 byte).
/// The last chunk gets whatever remains.
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
            // Last seed or not enough data for remaining chunks: take everything
            chunks.push(data[offset..].to_vec());
            offset = data.len();
            break;
        }

        // Reserve at least 1 byte for each remaining chunk after this one
        let max_chunk = remaining_data - (remaining_chunks - 1);
        let chunk_size = 1 + (seed as usize % max_chunk);
        chunks.push(data[offset..offset + chunk_size].to_vec());
        offset += chunk_size;
    }

    // If there's leftover data, append it as a final chunk
    if offset < data.len() {
        chunks.push(data[offset..].to_vec());
    }

    chunks
}

/// Property 2: Incremental write round-trip
///
/// Generate random byte vectors of varying sizes, split into random chunk sizes that sum to
/// the total length, write via begin_incremental_range_write → write_range_chunk (each chunk)
/// → commit_incremental_range, then read back via load_range_data and compare against the
/// original byte sequence.
#[test]
fn prop_incremental_write_round_trip() {
    fn property(data_seed: Vec<u8>, split_seeds: Vec<u8>) -> TestResult {
        // Need at least 1 byte of data
        if data_seed.is_empty() {
            return TestResult::discard();
        }

        // Scale data: repeat seed to get meaningful sizes, capped at 2 MiB
        let target_size = if data_seed.len() <= 64 {
            data_seed.len()
        } else {
            (data_seed.len() * 128).min(2 * 1024 * 1024)
        };

        let original_data: Vec<u8> = data_seed
            .iter()
            .cycle()
            .take(target_size)
            .copied()
            .collect();

        // Split into chunks using split_seeds
        let chunks = split_into_chunks(&original_data, &split_seeds);

        // Sanity: chunks must reconstruct the original
        let reconstructed: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
        assert_eq!(reconstructed, original_data);

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let cache_manager =
                DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
            cache_manager.initialize().await.unwrap();

            let cache_key = "test-bucket/prop-test-incremental";
            let start = 0u64;
            let end = original_data.len() as u64 - 1;

            // Begin incremental write with compression enabled
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

            // Write each chunk
            for (i, chunk) in chunks.iter().enumerate() {
                if let Err(e) = DiskCacheManager::write_range_chunk(&mut writer, chunk) {
                    return TestResult::error(format!(
                        "write_range_chunk failed on chunk {}: {}",
                        i, e
                    ));
                }
            }

            // Capture compressed_bytes_written before commit consumes the writer
            let compressed_bytes = writer.compressed_bytes_written;

            // Commit with metadata
            let object_metadata = ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: original_data.len() as u64,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: UploadState::Complete,
                cumulative_size: original_data.len() as u64,
                parts: Vec::new(),
                ..Default::default()
            };

            if let Err(e) = cache_manager
                .commit_incremental_range(writer, object_metadata, Duration::from_secs(3600))
                .await
            {
                return TestResult::error(format!("commit_incremental_range failed: {}", e));
            }

            // Construct the RangeSpec manually using the known file path.
            // get_new_range_file_path computes the final .bin path; we need the
            // relative portion under the "ranges" directory.
            let final_path = cache_manager.get_new_range_file_path(cache_key, start, end);
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
                compressed_bytes,
                original_data.len() as u64,
            );

            // Read back via load_range_data
            let loaded_data = match cache_manager.load_range_data(&range_spec).await {
                Ok(data) => data,
                Err(e) => {
                    return TestResult::error(format!("load_range_data failed: {}", e));
                }
            };

            // Property: loaded data must be byte-identical to original
            if loaded_data != original_data {
                return TestResult::error(format!(
                    "Round-trip mismatch: original {} bytes, loaded {} bytes, {} chunks",
                    original_data.len(),
                    loaded_data.len(),
                    chunks.len()
                ));
            }

            TestResult::passed()
        })
    }

    quickcheck(property as fn(Vec<u8>, Vec<u8>) -> TestResult);
}

// Feature: throughput-optimization, Property 3: Incremental write equivalence to single-shot write
//
// For any byte sequence and for any partitioning into chunks, the file produced by incremental
// writing (concatenated LZ4 frames) must decompress to the same bytes as the file produced by
// the existing single-shot store_range (single LZ4 frame). That is,
// decompress(incremental_file) == decompress(single_shot_file).
//
// **Validates: Requirements 2.4**

/// Property 3: Incremental write equivalence to single-shot write
///
/// Generate random byte vectors of varying sizes, produce both an incremental file (random
/// chunks via begin_incremental_range_write → write_range_chunk → commit_incremental_range)
/// and a single-shot file (via store_range). Read back the raw .bin files from disk,
/// decompress both, and compare byte-for-byte.
#[test]
fn prop_incremental_write_equivalence_to_single_shot() {
    fn property(data_seed: Vec<u8>, split_seeds: Vec<u8>) -> TestResult {
        if data_seed.is_empty() {
            return TestResult::discard();
        }

        // Scale data: repeat seed to get meaningful sizes, capped at 2 MiB
        let target_size = if data_seed.len() <= 64 {
            data_seed.len()
        } else {
            (data_seed.len() * 128).min(2 * 1024 * 1024)
        };

        let original_data: Vec<u8> = data_seed
            .iter()
            .cycle()
            .take(target_size)
            .copied()
            .collect();

        let chunks = split_into_chunks(&original_data, &split_seeds);

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            // --- Incremental write ---
            let incr_dir = tempfile::TempDir::new().unwrap();
            let incr_cache =
                DiskCacheManager::new(incr_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
            incr_cache.initialize().await.unwrap();

            let cache_key = "test-bucket/prop-test-equivalence";
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

            let object_metadata = ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: original_data.len() as u64,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: UploadState::Complete,
                cumulative_size: original_data.len() as u64,
                parts: Vec::new(),
                ..Default::default()
            };

            if let Err(e) = incr_cache
                .commit_incremental_range(writer, object_metadata.clone(), Duration::from_secs(3600))
                .await
            {
                return TestResult::error(format!("commit_incremental_range failed: {}", e));
            }

            // --- Single-shot write ---
            let shot_dir = tempfile::TempDir::new().unwrap();
            let mut shot_cache =
                DiskCacheManager::new(shot_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
            shot_cache.initialize().await.unwrap();

            if let Err(e) = shot_cache
                .store_range(
                    cache_key,
                    start,
                    end,
                    &original_data,
                    object_metadata,
                    Duration::from_secs(3600),
                    true,
                )
                .await
            {
                return TestResult::error(format!("store_range failed: {}", e));
            }

            // --- Read raw .bin files from disk ---
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

            // --- Decompress both ---
            let compression_handler =
                s3_proxy::compression::CompressionHandler::new(0, true);

            let incr_decompressed = match compression_handler.decompress_with_algorithm(
                &incr_compressed,
                s3_proxy::compression::CompressionAlgorithm::Lz4,
            ) {
                Ok(d) => d,
                Err(e) => {
                    return TestResult::error(format!(
                        "Failed to decompress incremental file: {}",
                        e
                    ));
                }
            };

            let shot_decompressed = match compression_handler.decompress_with_algorithm(
                &shot_compressed,
                s3_proxy::compression::CompressionAlgorithm::Lz4,
            ) {
                Ok(d) => d,
                Err(e) => {
                    return TestResult::error(format!(
                        "Failed to decompress single-shot file: {}",
                        e
                    ));
                }
            };

            // --- Property: decompressed outputs must be byte-identical ---
            if incr_decompressed != shot_decompressed {
                return TestResult::error(format!(
                    "Decompressed mismatch: incremental {} bytes vs single-shot {} bytes (original {} bytes, {} chunks)",
                    incr_decompressed.len(),
                    shot_decompressed.len(),
                    original_data.len(),
                    chunks.len()
                ));
            }

            // Also verify both match the original data
            if incr_decompressed != original_data {
                return TestResult::error(format!(
                    "Decompressed data does not match original: got {} bytes vs original {} bytes",
                    incr_decompressed.len(),
                    original_data.len()
                ));
            }

            TestResult::passed()
        })
    }

    quickcheck(property as fn(Vec<u8>, Vec<u8>) -> TestResult);
}
