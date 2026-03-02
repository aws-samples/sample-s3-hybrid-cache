// Feature: large-file-cache-regression, Property 2: Preservation
//
// Non-Range-Write Behavior Unchanged
//
// These tests validate baseline behavior that MUST be preserved after the bugfix.
// They run on UNFIXED code and MUST PASS, confirming the properties we need to protect.
//
// Property tests:
// 1. store_range produces a valid .bin file with correct LZ4 compressed content
// 2. store_range leaves zero .tmp files on disk after completion
// 3. Single-task IncrementalRangeWriter produces output equivalent to store_range
//
// **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5**

use quickcheck::{quickcheck, TestResult};
use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::DiskCacheManager;
use std::time::Duration;
use walkdir::WalkDir;

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

fn make_object_metadata(content_length: u64) -> ObjectMetadata {
    ObjectMetadata {
        etag: "test-etag-preservation".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length,
        content_type: Some("application/octet-stream".to_string()),
        upload_state: UploadState::Complete,
        cumulative_size: content_length,
        parts: Vec::new(),
        ..Default::default()
    }
}

/// Scale data_seed to a target size between 1 byte and 2 MiB.
/// Uses cycle repetition for seeds > 64 bytes, capped at 2 MiB.
fn scale_data(data_seed: &[u8]) -> Vec<u8> {
    let target_size = if data_seed.len() <= 64 {
        data_seed.len()
    } else {
        (data_seed.len() * 128).min(2 * 1024 * 1024)
    };
    data_seed.iter().cycle().take(target_size).copied().collect()
}

/// Count files with a given extension recursively under a directory.
fn count_files_with_ext(dir: &std::path::Path, ext: &str) -> usize {
    if !dir.exists() {
        return 0;
    }
    WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .map_or(false, |n| n.ends_with(ext))
        })
        .count()
}

/// Property 2.1: store_range produces a valid .bin file with correct LZ4 compressed content
///
/// For random range data (1 byte to 2 MiB) and random chunk splits, store_range produces
/// a .bin file on disk whose LZ4-decompressed content matches the original input data.
///
/// **Validates: Requirements 3.1, 3.2**
#[test]
fn prop_store_range_produces_valid_bin_file() {
    fn property(data_seed: Vec<u8>, split_seeds: Vec<u8>) -> TestResult {
        if data_seed.is_empty() {
            return TestResult::discard();
        }

        let original_data = scale_data(&data_seed);
        // split_seeds unused for store_range (it writes all at once), but we keep
        // the signature consistent with the other properties for quickcheck generation
        let _ = split_seeds;

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let mut cache_manager =
                DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
            cache_manager.initialize().await.unwrap();

            let cache_key = "test-bucket/preservation-store-range";
            let start = 0u64;
            let end = original_data.len() as u64 - 1;
            let metadata = make_object_metadata(original_data.len() as u64);

            // Write via store_range
            if let Err(e) = cache_manager
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

            // Verify .bin file exists
            let bin_path = cache_manager.get_new_range_file_path(cache_key, start, end);
            if !bin_path.exists() {
                return TestResult::error(format!(
                    "Expected .bin file at {:?} but it does not exist",
                    bin_path
                ));
            }

            // Read raw compressed data from .bin file
            let compressed_data = match std::fs::read(&bin_path) {
                Ok(d) => d,
                Err(e) => {
                    return TestResult::error(format!("Failed to read .bin file: {}", e));
                }
            };

            // Decompress and verify content matches original
            let compression_handler =
                s3_proxy::compression::CompressionHandler::new(0, true);
            let decompressed = match compression_handler.decompress_with_algorithm(
                &compressed_data,
                CompressionAlgorithm::Lz4,
            ) {
                Ok(d) => d,
                Err(e) => {
                    return TestResult::error(format!(
                        "Failed to decompress .bin file: {}",
                        e
                    ));
                }
            };

            if decompressed != original_data {
                return TestResult::error(format!(
                    "Decompressed data mismatch: original {} bytes, decompressed {} bytes",
                    original_data.len(),
                    decompressed.len()
                ));
            }

            TestResult::passed()
        })
    }

    quickcheck(property as fn(Vec<u8>, Vec<u8>) -> TestResult);
}

/// Property 2.2: store_range leaves zero .tmp files on disk after completion
///
/// For random range data, after store_range completes successfully, there are zero
/// .tmp files anywhere in the cache directory tree.
///
/// **Validates: Requirements 3.4**
#[test]
fn prop_store_range_leaves_zero_tmp_files() {
    fn property(data_seed: Vec<u8>) -> TestResult {
        if data_seed.is_empty() {
            return TestResult::discard();
        }

        let original_data = scale_data(&data_seed);

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let mut cache_manager =
                DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
            cache_manager.initialize().await.unwrap();

            let cache_key = "test-bucket/preservation-tmp-cleanup";
            let start = 0u64;
            let end = original_data.len() as u64 - 1;
            let metadata = make_object_metadata(original_data.len() as u64);

            if let Err(e) = cache_manager
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

            // Count .tmp files in the entire cache directory
            let tmp_count = count_files_with_ext(temp_dir.path(), ".tmp");
            if tmp_count != 0 {
                return TestResult::error(format!(
                    "Expected zero .tmp files after store_range, found {}",
                    tmp_count
                ));
            }

            TestResult::passed()
        })
    }

    quickcheck(property as fn(Vec<u8>) -> TestResult);
}

/// Property 2.3: Single-task IncrementalRangeWriter equivalence to store_range
///
/// For random range data and random chunk splits, a single-task IncrementalRangeWriter
/// (begin → write_chunks → commit) produces a .bin file whose decompressed content is
/// byte-identical to the .bin file produced by store_range for the same input.
///
/// This validates that the existing prop_incremental_write_equivalence_to_single_shot
/// property still holds, specifically in the context of the preservation guarantee.
///
/// **Validates: Requirements 3.2, 3.5**
#[test]
fn prop_single_task_incremental_equivalent_to_store_range() {
    fn property(data_seed: Vec<u8>, split_seeds: Vec<u8>) -> TestResult {
        if data_seed.is_empty() {
            return TestResult::discard();
        }

        let original_data = scale_data(&data_seed);
        let chunks = split_into_chunks(&original_data, &split_seeds);

        // Sanity: chunks must reconstruct the original
        let reconstructed: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
        assert_eq!(reconstructed, original_data);

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            // --- Incremental write (single task, no contention) ---
            let incr_dir = tempfile::TempDir::new().unwrap();
            let mut incr_cache =
                DiskCacheManager::new(incr_dir.path().to_path_buf(), true, 1024, false);
            incr_cache.initialize().await.unwrap();

            let cache_key = "test-bucket/preservation-equivalence";
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

            let metadata = make_object_metadata(original_data.len() as u64);

            if let Err(e) = incr_cache
                .commit_incremental_range(writer, metadata.clone(), Duration::from_secs(3600))
                .await
            {
                return TestResult::error(format!("commit_incremental_range failed: {}", e));
            }

            // --- Single-shot store_range ---
            let shot_dir = tempfile::TempDir::new().unwrap();
            let mut shot_cache =
                DiskCacheManager::new(shot_dir.path().to_path_buf(), true, 1024, false);
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

            // --- Read and decompress both .bin files ---
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
                        "Failed to read store_range .bin file: {}",
                        e
                    ));
                }
            };

            let compression_handler =
                s3_proxy::compression::CompressionHandler::new(0, true);

            let incr_decompressed = match compression_handler.decompress_with_algorithm(
                &incr_compressed,
                CompressionAlgorithm::Lz4,
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
                CompressionAlgorithm::Lz4,
            ) {
                Ok(d) => d,
                Err(e) => {
                    return TestResult::error(format!(
                        "Failed to decompress store_range file: {}",
                        e
                    ));
                }
            };

            // Property: both decompressed outputs must be byte-identical
            if incr_decompressed != shot_decompressed {
                return TestResult::error(format!(
                    "Decompressed mismatch: incremental {} bytes vs store_range {} bytes \
                     (original {} bytes, {} chunks)",
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
