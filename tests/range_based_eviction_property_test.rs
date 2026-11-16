//! Consolidated property-based tests for range-based disk cache eviction
//!
//! This file consolidates all property tests for the range-based eviction feature:
//! - Property 1: Range Independence (Requirements 1.1, 1.5)
//! - Property 2: Equal-frequency ranges sort oldest first under TinyLFU (was LRU ordering,
//!   Requirements 1.2; retargeted when LRU was deleted, cache-eviction-at-scale R10.1)
//! - Property 3: TinyLFU Scoring by Range (Requirements 1.3)
//! - Property 4: Single Range Deletion Isolation (Requirements 2.1)
//! - Property 5: Metadata Update After Range Eviction (Requirements 2.2)
//! - Property 6: Metadata Cleanup Condition (Requirements 2.3)
//! - Property 7: Cache Size Tracking (Requirements 3.1, 3.2)
//! - Property 8: Lock Coordination (Requirements 4.1, 4.2)
//! - Property 9: Directory Cleanup (Requirements 5.1, 5.2)

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::{get_sharded_path, DiskCacheManager};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::runtime::Runtime;

// ============================================================================
// Test Configuration Types
// ============================================================================

#[derive(Debug, Clone)]
struct MetadataUpdateConfig {
    num_ranges: usize,
    range_sizes: Vec<u64>,
    ranges_to_delete: Vec<usize>,
}

impl Arbitrary for MetadataUpdateConfig {
    fn arbitrary(g: &mut Gen) -> Self {
        let num_ranges = 2 + (usize::arbitrary(g) % 9);
        let range_sizes: Vec<u64> = (0..num_ranges)
            .map(|_| 1024 + (u64::arbitrary(g) % (50 * 1024)))
            .collect();
        let num_to_delete = 1 + (usize::arbitrary(g) % (num_ranges - 1));
        let mut indices: Vec<usize> = (0..num_ranges).collect();
        for i in (1..indices.len()).rev() {
            let j = usize::arbitrary(g) % (i + 1);
            indices.swap(i, j);
        }
        let ranges_to_delete: Vec<usize> = indices.into_iter().take(num_to_delete).collect();
        MetadataUpdateConfig {
            num_ranges,
            range_sizes,
            ranges_to_delete,
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn create_test_cache_dir() -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();
    std::fs::create_dir_all(cache_dir.join("locks")).ok();
    std::fs::create_dir_all(cache_dir.join("metadata")).ok();
    std::fs::create_dir_all(cache_dir.join("ranges")).ok();
    std::fs::create_dir_all(cache_dir.join("size_tracking")).ok();
    (temp_dir, cache_dir)
}

fn create_test_metadata_with_ranges(
    cache_dir: &Path,
    cache_key: &str,
    range_sizes: &[u64],
) -> Result<(Vec<(u64, u64)>, PathBuf), String> {
    let now = SystemTime::now();
    let disk_cache = DiskCacheManager::new(cache_dir.to_path_buf(), false, 0, false, 1_048_576);

    let object_metadata = ObjectMetadata::new(
        format!("etag-{}", cache_key),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        range_sizes.iter().sum(),
        Some("application/octet-stream".to_string()),
    );

    let mut ranges = Vec::with_capacity(range_sizes.len());
    let mut range_bounds: Vec<(u64, u64)> = Vec::with_capacity(range_sizes.len());
    let mut current_offset = 0u64;

    for size in range_sizes {
        let start = current_offset;
        let end = current_offset + size - 1;
        let bin_path = disk_cache.get_new_range_file_path(cache_key, start, end);
        let file_path = format!("{}_{}-{}.bin", cache_key.replace('/', "%2F"), start, end);

        let range_spec = RangeSpec::new(
            start,
            end,
            file_path,
            CompressionAlgorithm::Lz4,
            *size,
            *size,
        );
        ranges.push(range_spec);

        if let Some(parent) = bin_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("Failed to create dirs: {}", e))?;
        }
        let data = vec![0u8; *size as usize];
        std::fs::write(&bin_path, &data).map_err(|e| format!("Failed to write bin file: {}", e))?;

        range_bounds.push((start, end));
        current_offset = end + 1;
    }

    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata,
        ranges,
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        ..Default::default()
    };

    let meta_path = get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
        .map_err(|e| format!("Failed to get sharded path: {}", e))?;

    if let Some(parent) = meta_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("Failed to create dirs: {}", e))?;
    }

    let json = serde_json::to_string_pretty(&metadata)
        .map_err(|e| format!("Failed to serialize metadata: {}", e))?;
    std::fs::write(&meta_path, json).map_err(|e| format!("Failed to write metadata: {}", e))?;

    Ok((range_bounds, meta_path))
}

fn read_metadata(meta_path: &PathBuf) -> Result<NewCacheMetadata, String> {
    let content = std::fs::read_to_string(meta_path)
        .map_err(|e| format!("Failed to read metadata: {}", e))?;
    serde_json::from_str(&content).map_err(|e| format!("Failed to parse metadata: {}", e))
}

// ============================================================================
// Property 1: Range Independence
// **Validates: Requirements 1.1, 1.5**
// ============================================================================

// ============================================================================
// Property 5: Metadata Update After Range Eviction
// **Validates: Requirements 2.2**
// ============================================================================

fn prop_metadata_removes_evicted_ranges(config: MetadataUpdateConfig) -> TestResult {
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let disk_cache = DiskCacheManager::new(cache_dir.clone(), false, 0, false, 1_048_576);

        if disk_cache.initialize().await.is_err() {
            return TestResult::discard();
        }

        let cache_key = "test-bucket/metadata-update-object";
        let (range_bounds, meta_path) =
            match create_test_metadata_with_ranges(&cache_dir, cache_key, &config.range_sizes) {
                Ok(info) => info,
                Err(_) => return TestResult::discard(),
            };

        let initial_metadata = match read_metadata(&meta_path) {
            Ok(m) => m,
            Err(_) => return TestResult::discard(),
        };

        if initial_metadata.ranges.len() != config.num_ranges {
            return TestResult::discard();
        }

        let ranges_to_delete: Vec<(u64, u64)> = config
            .ranges_to_delete
            .iter()
            .filter_map(|&idx| range_bounds.get(idx).copied())
            .collect();

        let delete_set: HashSet<(u64, u64)> = ranges_to_delete.iter().cloned().collect();

        let result = disk_cache
            .batch_delete_ranges(cache_key, &ranges_to_delete)
            .await;

        match result {
            Ok(deletion) => {
                let all_evicted = deletion.all_ranges_evicted;
                if !all_evicted {
                    let updated_metadata = match read_metadata(&meta_path) {
                        Ok(m) => m,
                        Err(_) => return TestResult::failed(),
                    };

                    let expected_remaining = config.num_ranges - config.ranges_to_delete.len();
                    if updated_metadata.ranges.len() != expected_remaining {
                        return TestResult::failed();
                    }

                    for range in &updated_metadata.ranges {
                        let range_key = (range.start, range.end);
                        if delete_set.contains(&range_key) {
                            return TestResult::failed();
                        }
                    }

                    let remaining_set: HashSet<(u64, u64)> = updated_metadata
                        .ranges
                        .iter()
                        .map(|r| (r.start, r.end))
                        .collect();

                    for (i, &(start, end)) in range_bounds.iter().enumerate() {
                        if !config.ranges_to_delete.contains(&i)
                            && !remaining_set.contains(&(start, end))
                        {
                            return TestResult::failed();
                        }
                    }
                }
                TestResult::passed()
            }
            Err(_) => TestResult::failed(),
        }
    })
}

fn prop_metadata_preserves_object_metadata(config: MetadataUpdateConfig) -> TestResult {
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let disk_cache = DiskCacheManager::new(cache_dir.clone(), false, 0, false, 1_048_576);

        if disk_cache.initialize().await.is_err() {
            return TestResult::discard();
        }

        let cache_key = "test-bucket/preserve-object-metadata";
        let (range_bounds, meta_path) =
            match create_test_metadata_with_ranges(&cache_dir, cache_key, &config.range_sizes) {
                Ok(info) => info,
                Err(_) => return TestResult::discard(),
            };

        let initial_metadata = match read_metadata(&meta_path) {
            Ok(m) => m,
            Err(_) => return TestResult::discard(),
        };

        let original_etag = initial_metadata.object_metadata.etag.clone();
        let original_last_modified = initial_metadata.object_metadata.last_modified.clone();
        let original_content_length = initial_metadata.object_metadata.content_length;
        let original_cache_key = initial_metadata.cache_key.clone();

        let ranges_to_delete: Vec<(u64, u64)> = config
            .ranges_to_delete
            .iter()
            .filter_map(|&idx| range_bounds.get(idx).copied())
            .collect();

        let result = disk_cache
            .batch_delete_ranges(cache_key, &ranges_to_delete)
            .await;

        match result {
            Ok(deletion) => {
                let all_evicted = deletion.all_ranges_evicted;
                if !all_evicted {
                    let updated_metadata = match read_metadata(&meta_path) {
                        Ok(m) => m,
                        Err(_) => return TestResult::failed(),
                    };

                    if updated_metadata.cache_key != original_cache_key
                        || updated_metadata.object_metadata.etag != original_etag
                        || updated_metadata.object_metadata.last_modified != original_last_modified
                        || updated_metadata.object_metadata.content_length
                            != original_content_length
                    {
                        return TestResult::failed();
                    }
                }
                TestResult::passed()
            }
            Err(_) => TestResult::failed(),
        }
    })
}

#[test]
fn test_property_metadata_update_removes_evicted_ranges() {
    QuickCheck::new()
        .tests(20)
        .quickcheck(prop_metadata_removes_evicted_ranges as fn(MetadataUpdateConfig) -> TestResult);
}

#[test]
fn test_property_metadata_update_preserves_object_metadata() {
    QuickCheck::new().tests(20).quickcheck(
        prop_metadata_preserves_object_metadata as fn(MetadataUpdateConfig) -> TestResult,
    );
}
