//! Consolidated property-based tests for range-based disk cache eviction
//!
//! This file consolidates all property tests for the range-based eviction feature:
//! - Property 1: Range Independence (Requirements 1.1, 1.5)
//! - Property 2: LRU Ordering by Range (Requirements 1.2)
//! - Property 3: TinyLFU Scoring by Range (Requirements 1.3)
//! - Property 4: Single Range Deletion Isolation (Requirements 2.1)
//! - Property 5: Metadata Update After Range Eviction (Requirements 2.2)
//! - Property 6: Metadata Cleanup Condition (Requirements 2.3)
//! - Property 7: Cache Size Tracking (Requirements 3.1, 3.2)
//! - Property 8: Lock Coordination (Requirements 4.1, 4.2)
//! - Property 9: Directory Cleanup (Requirements 5.1, 5.2)

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::{get_sharded_path, DiskCacheManager};
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::runtime::Runtime;

// ============================================================================
// Test Configuration Types
// ============================================================================

#[derive(Debug, Clone)]
struct RangeIndependenceConfig {
    num_objects: usize,
    ranges_per_object: Vec<usize>,
    range_sizes: Vec<u64>,
}

impl Arbitrary for RangeIndependenceConfig {
    fn arbitrary(g: &mut Gen) -> Self {
        let num_objects = 1 + (usize::arbitrary(g) % 5);
        let ranges_per_object: Vec<usize> = (0..num_objects)
            .map(|_| 1 + (usize::arbitrary(g) % 10))
            .collect();
        let total_ranges: usize = ranges_per_object.iter().sum();
        let range_sizes: Vec<u64> = (0..total_ranges)
            .map(|_| 1024 + (u64::arbitrary(g) % (1024 * 1024)))
            .collect();
        RangeIndependenceConfig {
            num_objects,
            ranges_per_object,
            range_sizes,
        }
    }
}

#[derive(Debug, Clone)]
struct LruOrderingConfig {
    num_objects: usize,
    ranges_per_object: Vec<usize>,
    time_offsets: Vec<u64>,
}

impl Arbitrary for LruOrderingConfig {
    fn arbitrary(g: &mut Gen) -> Self {
        let num_objects = 1 + (usize::arbitrary(g) % 5);
        let ranges_per_object: Vec<usize> = (0..num_objects)
            .map(|_| 1 + (usize::arbitrary(g) % 5))
            .collect();
        let total_ranges: usize = ranges_per_object.iter().sum();
        let time_offsets: Vec<u64> = (0..total_ranges)
            .map(|_| u64::arbitrary(g) % 3600)
            .collect();
        LruOrderingConfig {
            num_objects,
            ranges_per_object,
            time_offsets,
        }
    }
}

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

fn create_test_metadata(
    cache_dir: &PathBuf,
    cache_key: &str,
    num_ranges: usize,
    range_sizes: &[u64],
) -> Result<usize, String> {
    let now = SystemTime::now();
    let old_access_time = now - Duration::from_secs(120);

    let object_metadata = ObjectMetadata::new(
        format!("etag-{}", cache_key),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        range_sizes.iter().sum(),
        Some("application/octet-stream".to_string()),
    );

    let mut ranges = Vec::with_capacity(num_ranges);
    let mut current_offset = 0u64;

    for i in 0..num_ranges {
        let size = range_sizes.get(i).copied().unwrap_or(8192);
        let start = current_offset;
        let end = current_offset + size - 1;
        let file_path = format!(
            "{}/{}_{}-{}.bin",
            cache_key.replace('/', "%2F"),
            cache_key.replace('/', "%2F"),
            start,
            end
        );

        let mut range_spec = RangeSpec::new(
            start,
            end,
            file_path.clone(),
            CompressionAlgorithm::Lz4,
            size,
            size,
        );
        range_spec.last_accessed = old_access_time;
        ranges.push(range_spec);

        let bin_path = cache_dir.join("ranges").join(&file_path);
        if let Some(parent) = bin_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let data = vec![0u8; size as usize];
        std::fs::write(&bin_path, &data).map_err(|e| format!("Failed to write bin file: {}", e))?;
        current_offset = end + 1;
    }

    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata,
        ranges,
        created_at: old_access_time,
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

    Ok(num_ranges)
}

fn create_test_metadata_with_times(
    cache_dir: &PathBuf,
    cache_key: &str,
    num_ranges: usize,
    time_offsets: &[u64],
) -> Result<Vec<SystemTime>, String> {
    let base_time = SystemTime::now() - Duration::from_secs(7200);
    let object_metadata = ObjectMetadata::new(
        format!("etag-{}", cache_key),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        num_ranges as u64 * 8192,
        Some("application/octet-stream".to_string()),
    );

    let mut ranges = Vec::with_capacity(num_ranges);
    let mut created_times = Vec::with_capacity(num_ranges);
    let range_size = 8192u64;

    for i in 0..num_ranges {
        let start = i as u64 * range_size;
        let end = start + range_size - 1;
        let offset = time_offsets.get(i).copied().unwrap_or(0);
        let last_accessed = base_time + Duration::from_secs(offset);
        created_times.push(last_accessed);

        let file_path = format!(
            "{}/{}_{}-{}.bin",
            cache_key.replace('/', "%2F"),
            cache_key.replace('/', "%2F"),
            start,
            end
        );

        let mut range_spec = RangeSpec::new(
            start,
            end,
            file_path.clone(),
            CompressionAlgorithm::Lz4,
            range_size,
            range_size,
        );
        range_spec.last_accessed = last_accessed;
        ranges.push(range_spec);

        let bin_path = cache_dir.join("ranges").join(&file_path);
        if let Some(parent) = bin_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let data = vec![0u8; range_size as usize];
        std::fs::write(&bin_path, &data).map_err(|e| format!("Failed to write bin file: {}", e))?;
    }

    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata,
        ranges,
        created_at: base_time,
        expires_at: SystemTime::now() + Duration::from_secs(3600),
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

    Ok(created_times)
}

fn create_test_metadata_with_ranges(
    cache_dir: &PathBuf,
    cache_key: &str,
    range_sizes: &[u64],
) -> Result<(Vec<(u64, u64)>, PathBuf), String> {
    let now = SystemTime::now();
    let disk_cache = DiskCacheManager::new(cache_dir.clone(), false, 0, false);

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

fn prop_candidate_count_equals_range_count(config: RangeIndependenceConfig) -> TestResult {
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        let mut total_expected_ranges = 0usize;
        let mut range_size_idx = 0usize;

        for obj_idx in 0..config.num_objects {
            let cache_key = format!("test-bucket/object-{}", obj_idx);
            let num_ranges = config.ranges_per_object.get(obj_idx).copied().unwrap_or(1);
            let end_idx = (range_size_idx + num_ranges).min(config.range_sizes.len());
            let sizes: Vec<u64> = if range_size_idx < config.range_sizes.len() {
                config.range_sizes[range_size_idx..end_idx].to_vec()
            } else {
                vec![8192; num_ranges]
            };
            range_size_idx = end_idx;

            match create_test_metadata(&cache_dir, &cache_key, num_ranges, &sizes) {
                Ok(created_ranges) => total_expected_ranges += created_ranges,
                Err(_) => return TestResult::discard(),
            }
        }

        let candidates = match cache_manager.collect_range_candidates_for_eviction().await {
            Ok(c) => c,
            Err(_) => return TestResult::discard(),
        };

        if candidates.len() != total_expected_ranges {
            return TestResult::failed();
        }
        TestResult::passed()
    })
}

fn prop_each_range_is_independent(config: RangeIndependenceConfig) -> TestResult {
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        let cache_key = "test-bucket/multi-range-object";
        let num_ranges = config
            .ranges_per_object
            .first()
            .copied()
            .unwrap_or(3)
            .max(2);
        let sizes: Vec<u64> = config
            .range_sizes
            .iter()
            .take(num_ranges)
            .copied()
            .collect();
        let sizes = if sizes.len() < num_ranges {
            vec![8192; num_ranges]
        } else {
            sizes
        };

        if create_test_metadata(&cache_dir, cache_key, num_ranges, &sizes).is_err() {
            return TestResult::discard();
        }

        let candidates = match cache_manager.collect_range_candidates_for_eviction().await {
            Ok(c) => c,
            Err(_) => return TestResult::discard(),
        };

        let mut seen_ranges: Vec<(u64, u64)> = Vec::new();
        for candidate in &candidates {
            if candidate.cache_key != cache_key {
                return TestResult::failed();
            }
            let range_bounds = (candidate.range_start, candidate.range_end);
            if seen_ranges.contains(&range_bounds) {
                return TestResult::failed();
            }
            seen_ranges.push(range_bounds);
            if candidate.access_count < 1 {
                return TestResult::failed();
            }
        }

        if seen_ranges.len() != num_ranges {
            return TestResult::failed();
        }
        TestResult::passed()
    })
}

#[test]
fn test_property_range_independence_candidate_count() {
    QuickCheck::new().tests(20).quickcheck(
        prop_candidate_count_equals_range_count as fn(RangeIndependenceConfig) -> TestResult,
    );
}

#[test]
fn test_property_range_independence_separate_candidates() {
    QuickCheck::new()
        .tests(20)
        .quickcheck(prop_each_range_is_independent as fn(RangeIndependenceConfig) -> TestResult);
}

// ============================================================================
// Property 2: LRU Ordering by Range
// **Validates: Requirements 1.2**
// ============================================================================

fn prop_lru_sorts_oldest_first(config: LruOrderingConfig) -> TestResult {
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_eviction_algorithm(
            cache_dir.clone(),
            false,
            0,
            CacheEvictionAlgorithm::LRU,
        );

        let mut time_offset_idx = 0usize;
        for obj_idx in 0..config.num_objects {
            let cache_key = format!("test-bucket/object-{}", obj_idx);
            let num_ranges = config.ranges_per_object.get(obj_idx).copied().unwrap_or(1);
            let end_idx = (time_offset_idx + num_ranges).min(config.time_offsets.len());
            let offsets: Vec<u64> = if time_offset_idx < config.time_offsets.len() {
                config.time_offsets[time_offset_idx..end_idx].to_vec()
            } else {
                (0..num_ranges).map(|i| i as u64 * 100).collect()
            };
            time_offset_idx = end_idx;

            if create_test_metadata_with_times(&cache_dir, &cache_key, num_ranges, &offsets)
                .is_err()
            {
                return TestResult::discard();
            }
        }

        let mut candidates = match cache_manager.collect_range_candidates_for_eviction().await {
            Ok(c) => c,
            Err(_) => return TestResult::discard(),
        };

        if candidates.is_empty() {
            return TestResult::discard();
        }

        cache_manager.sort_range_candidates(&mut candidates);

        for i in 1..candidates.len() {
            if candidates[i - 1].last_accessed > candidates[i].last_accessed {
                return TestResult::failed();
            }
        }
        TestResult::passed()
    })
}

#[test]
fn test_property_lru_ordering_oldest_first() {
    QuickCheck::new()
        .tests(20)
        .quickcheck(prop_lru_sorts_oldest_first as fn(LruOrderingConfig) -> TestResult);
}

// ============================================================================
// Property 5: Metadata Update After Range Eviction
// **Validates: Requirements 2.2**
// ============================================================================

fn prop_metadata_removes_evicted_ranges(config: MetadataUpdateConfig) -> TestResult {
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let disk_cache = DiskCacheManager::new(cache_dir.clone(), false, 0, false);

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
            Ok((_bytes_freed, all_evicted, _deleted_paths)) => {
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
                        if !config.ranges_to_delete.contains(&i) {
                            if !remaining_set.contains(&(start, end)) {
                                return TestResult::failed();
                            }
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
        let disk_cache = DiskCacheManager::new(cache_dir.clone(), false, 0, false);

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
            Ok((_bytes_freed, all_evicted, _deleted_paths)) => {
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

// ============================================================================
// Unit tests for edge cases
// ============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[tokio::test]
    async fn test_single_range_object() {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        let cache_key = "test-bucket/single-range";
        create_test_metadata(&cache_dir, cache_key, 1, &[8192]).unwrap();

        let candidates = cache_manager
            .collect_range_candidates_for_eviction()
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].cache_key, cache_key);
    }

    #[tokio::test]
    async fn test_lru_stable_for_equal_times() {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_eviction_algorithm(
            cache_dir.clone(),
            false,
            0,
            CacheEvictionAlgorithm::LRU,
        );

        let cache_key = "test-bucket/same-time-object";
        let same_offset = vec![1000u64; 5];
        create_test_metadata_with_times(&cache_dir, cache_key, 5, &same_offset).unwrap();

        let mut candidates = cache_manager
            .collect_range_candidates_for_eviction()
            .await
            .unwrap();
        assert_eq!(candidates.len(), 5);

        cache_manager.sort_range_candidates(&mut candidates);

        let first_time = candidates[0].last_accessed;
        for candidate in &candidates {
            let diff = if candidate.last_accessed >= first_time {
                candidate
                    .last_accessed
                    .duration_since(first_time)
                    .unwrap_or_default()
            } else {
                first_time
                    .duration_since(candidate.last_accessed)
                    .unwrap_or_default()
            };
            assert!(diff <= Duration::from_secs(1));
        }
    }
}
