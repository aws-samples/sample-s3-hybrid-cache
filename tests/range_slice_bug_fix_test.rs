use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::config::SharedStorageConfig;
/// Test for range slice bug fix
///
/// This test verifies that when a cached range is larger than the requested range,
/// the proxy correctly slices the cached data to return only the requested bytes.
///
/// Bug scenario: Client requests bytes=0-8388607 (8388608 bytes), but cache contains
/// 0-8388661 (8388662 bytes). The proxy should return exactly 8388608 bytes, not 8388662.
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use std::sync::Arc;
use tempfile::TempDir;

fn create_test_cache_manager(cache_dir: std::path::PathBuf) -> Arc<CacheManager> {
    Arc::new(CacheManager::new_with_shared_storage(
        cache_dir,
        false, // RAM cache disabled for tests
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        1024,
        true,
        std::time::Duration::from_secs(3600), // GET TTL
        std::time::Duration::from_secs(3600), // HEAD TTL
        std::time::Duration::from_secs(3600), // PUT TTL
        false,                                // Don't actively remove cached data
        SharedStorageConfig::default(),
        10.0,                                  // write_cache_percent
        false,                                 // write_cache_enabled - disabled for these tests
        std::time::Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    ))
}

#[tokio::test]
async fn test_range_slice_single_cached_range_larger_than_requested() {
    // Create temporary directory for cache
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = create_test_cache_manager(cache_dir.clone());

    // Create disk cache manager
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir.clone(), true, 1024, false),
    ));

    // Create range handler
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));

    let cache_key = "test-object";

    // Simulate the bug scenario: cache contains 0-8388661 (54 extra bytes)
    let cached_start = 0u64;
    let cached_end = 8388661u64; // 8388662 bytes (54 extra)
    let cached_data = vec![0xAB; (cached_end - cached_start + 1) as usize];

    // Store the cached range
    let metadata = ObjectMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: cached_end + 1,
        content_type: Some("application/octet-stream".to_string()),
        upload_state: UploadState::Complete,
        cumulative_size: cached_end + 1,
        parts: Vec::new(),
        ..Default::default()
    };

    range_handler
        .store_range_new_storage(
            cache_key,
            cached_start,
            cached_end,
            &cached_data,
            metadata,
            std::time::Duration::from_secs(315360000), // 10 years TTL
        )
        .await
        .unwrap();

    // Client requests bytes=0-8388607 (8388608 bytes - the correct size)
    let requested_start = 0u64;
    let requested_end = 8388607u64;
    let requested_range = RangeSpec {
        start: requested_start,
        end: requested_end,
    };

    // Find cached ranges
    let overlap = range_handler
        .find_cached_ranges(cache_key, &requested_range, None, None)
        .await
        .unwrap();

    // Verify we found the cached range
    assert_eq!(overlap.cached_ranges.len(), 1, "Should find 1 cached range");
    assert_eq!(
        overlap.missing_ranges.len(),
        0,
        "Should have no missing ranges"
    );
    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve from cache"
    );

    let cached_range = &overlap.cached_ranges[0];
    assert_eq!(cached_range.start, cached_start);
    assert_eq!(cached_range.end, cached_end);

    // Extract bytes using the range handler's extract function
    // This simulates what serve_range_from_cache does
    let (extracted_bytes, _ram_hit) = range_handler
        .extract_bytes_from_cached_range(cache_key, cached_range, requested_start, requested_end)
        .await
        .unwrap();

    // Verify the extracted bytes are exactly the requested size
    let expected_size = (requested_end - requested_start + 1) as usize;
    assert_eq!(
        extracted_bytes.len(),
        expected_size,
        "Should return exactly {} bytes (not {} bytes)",
        expected_size,
        cached_data.len()
    );

    // Verify we got the correct bytes (first 8388608 bytes of the cached data)
    assert_eq!(
        extracted_bytes,
        &cached_data[0..expected_size],
        "Should return the first {} bytes of cached data",
        expected_size
    );
}

#[tokio::test]
async fn test_range_slice_middle_of_cached_range() {
    // Create temporary directory for cache
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = create_test_cache_manager(cache_dir.clone());

    // Create disk cache manager
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir.clone(), true, 1024, false),
    ));

    // Create range handler
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));

    let cache_key = "test-object-2";

    // Cache contains 0-1000
    let cached_start = 0u64;
    let cached_end = 1000u64;
    let mut cached_data = Vec::new();
    for i in 0..=1000 {
        cached_data.push((i % 256) as u8);
    }

    // Store the cached range
    let metadata = ObjectMetadata {
        etag: "test-etag-2".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: cached_end + 1,
        content_type: Some("application/octet-stream".to_string()),
        upload_state: UploadState::Complete,
        cumulative_size: cached_end + 1,
        parts: Vec::new(),
        ..Default::default()
    };

    range_handler
        .store_range_new_storage(
            cache_key,
            cached_start,
            cached_end,
            &cached_data,
            metadata,
            std::time::Duration::from_secs(315360000), // 10 years TTL
        )
        .await
        .unwrap();

    // Client requests bytes=100-200 (middle of cached range)
    let requested_start = 100u64;
    let requested_end = 200u64;
    let requested_range = RangeSpec {
        start: requested_start,
        end: requested_end,
    };

    // Find cached ranges
    let overlap = range_handler
        .find_cached_ranges(cache_key, &requested_range, None, None)
        .await
        .unwrap();

    // Verify we found the cached range
    assert_eq!(overlap.cached_ranges.len(), 1);
    assert_eq!(overlap.missing_ranges.len(), 0);

    let cached_range = &overlap.cached_ranges[0];

    // Extract bytes
    let (extracted_bytes, _ram_hit) = range_handler
        .extract_bytes_from_cached_range(cache_key, cached_range, requested_start, requested_end)
        .await
        .unwrap();

    // Verify the extracted bytes are exactly the requested size
    let expected_size = (requested_end - requested_start + 1) as usize;
    assert_eq!(extracted_bytes.len(), expected_size);

    // Verify we got the correct bytes (bytes 100-200 of the cached data)
    assert_eq!(
        extracted_bytes,
        &cached_data[100..=200],
        "Should return bytes 100-200 of cached data"
    );
}

#[tokio::test]
async fn test_range_slice_end_of_cached_range() {
    // Create temporary directory for cache
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = create_test_cache_manager(cache_dir.clone());

    // Create disk cache manager
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir.clone(), true, 1024, false),
    ));

    // Create range handler
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));

    let cache_key = "test-object-3";

    // Cache contains 0-8388661 (like the bug scenario)
    let cached_start = 0u64;
    let cached_end = 8388661u64;
    let cached_data = vec![0xFF; (cached_end - cached_start + 1) as usize];

    // Store the cached range
    let metadata = ObjectMetadata {
        etag: "test-etag-3".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: cached_end + 1,
        content_type: Some("application/octet-stream".to_string()),
        upload_state: UploadState::Complete,
        cumulative_size: cached_end + 1,
        parts: Vec::new(),
        ..Default::default()
    };

    range_handler
        .store_range_new_storage(
            cache_key,
            cached_start,
            cached_end,
            &cached_data,
            metadata,
            std::time::Duration::from_secs(315360000), // 10 years TTL
        )
        .await
        .unwrap();

    // Client requests the last 100 bytes: 8388561-8388660
    let requested_start = 8388561u64;
    let requested_end = 8388660u64;
    let requested_range = RangeSpec {
        start: requested_start,
        end: requested_end,
    };

    // Find cached ranges
    let overlap = range_handler
        .find_cached_ranges(cache_key, &requested_range, None, None)
        .await
        .unwrap();

    // Verify we found the cached range
    assert_eq!(overlap.cached_ranges.len(), 1);
    assert_eq!(overlap.missing_ranges.len(), 0);

    let cached_range = &overlap.cached_ranges[0];

    // Extract bytes
    let (extracted_bytes, _ram_hit) = range_handler
        .extract_bytes_from_cached_range(cache_key, cached_range, requested_start, requested_end)
        .await
        .unwrap();

    // Verify the extracted bytes are exactly the requested size
    let expected_size = (requested_end - requested_start + 1) as usize;
    assert_eq!(extracted_bytes.len(), expected_size);
    assert_eq!(extracted_bytes.len(), 100);

    // Verify we got the correct bytes
    let start_idx = (requested_start - cached_start) as usize;
    let end_idx = (requested_end - cached_start + 1) as usize;
    assert_eq!(
        extracted_bytes,
        &cached_data[start_idx..end_idx],
        "Should return the correct slice of cached data"
    );
}

#[tokio::test]
async fn test_range_slice_exact_match_no_slicing() {
    // Create temporary directory for cache
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = create_test_cache_manager(cache_dir.clone());

    // Create disk cache manager
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir.clone(), true, 1024, false),
    ));

    // Create range handler
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));

    let cache_key = "test-object-4";

    // Cache contains exactly 0-8388607 (correct size)
    let cached_start = 0u64;
    let cached_end = 8388607u64;
    let cached_data = vec![0xCC; (cached_end - cached_start + 1) as usize];

    // Store the cached range
    let metadata = ObjectMetadata {
        etag: "test-etag-4".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: cached_end + 1,
        content_type: Some("application/octet-stream".to_string()),
        upload_state: UploadState::Complete,
        cumulative_size: cached_end + 1,
        parts: Vec::new(),
        ..Default::default()
    };

    range_handler
        .store_range_new_storage(
            cache_key,
            cached_start,
            cached_end,
            &cached_data,
            metadata,
            std::time::Duration::from_secs(315360000), // 10 years TTL
        )
        .await
        .unwrap();

    // Client requests exactly the same range: 0-8388607
    let requested_start = 0u64;
    let requested_end = 8388607u64;
    let requested_range = RangeSpec {
        start: requested_start,
        end: requested_end,
    };

    // Find cached ranges
    let overlap = range_handler
        .find_cached_ranges(cache_key, &requested_range, None, None)
        .await
        .unwrap();

    // Verify we found the cached range
    assert_eq!(overlap.cached_ranges.len(), 1);
    assert_eq!(overlap.missing_ranges.len(), 0);

    let cached_range = &overlap.cached_ranges[0];

    // Extract bytes
    let (extracted_bytes, _ram_hit) = range_handler
        .extract_bytes_from_cached_range(cache_key, cached_range, requested_start, requested_end)
        .await
        .unwrap();

    // Verify the extracted bytes are exactly the requested size
    let expected_size = (requested_end - requested_start + 1) as usize;
    assert_eq!(extracted_bytes.len(), expected_size);

    // Verify we got all the cached data (no slicing needed)
    assert_eq!(
        extracted_bytes, cached_data,
        "Should return entire cached data when ranges match exactly"
    );
}

#[tokio::test]
async fn test_range_slice_multiple_cached_ranges() {
    // Create temporary directory for cache
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = create_test_cache_manager(cache_dir.clone());

    // Create disk cache manager
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir.clone(), true, 1024, false),
    ));

    // Create range handler
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));

    let cache_key = "test-object-multiple";

    // Scenario: Client requests bytes 0-2999 (3000 bytes)
    // Cache contains three ranges that need to be sliced and concatenated:
    // - Range 1: 0-1099 (1100 bytes, but client only needs 0-999, so slice 1000 bytes)
    // - Range 2: 1000-2099 (1100 bytes, but client only needs 1000-1999, so slice 1000 bytes)
    // - Range 3: 2000-3099 (1100 bytes, but client only needs 2000-2999, so slice 1000 bytes)

    // Create test data for each cached range
    let mut range1_data = Vec::new();
    for i in 0..1100 {
        range1_data.push((i % 256) as u8);
    }

    let mut range2_data = Vec::new();
    for i in 1000..2100 {
        range2_data.push((i % 256) as u8);
    }

    let mut range3_data = Vec::new();
    for i in 2000..3100 {
        range3_data.push((i % 256) as u8);
    }

    // Store the three cached ranges
    let metadata1 = ObjectMetadata {
        etag: "test-etag-multi-1".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: 3100,
        content_type: Some("application/octet-stream".to_string()),
        upload_state: UploadState::Complete,
        cumulative_size: 3100,
        parts: Vec::new(),
        ..Default::default()
    };

    range_handler
        .store_range_new_storage(
            cache_key,
            0,
            1099,
            &range1_data,
            metadata1.clone(),
            std::time::Duration::from_secs(315360000), // 10 years TTL
        )
        .await
        .unwrap();

    range_handler
        .store_range_new_storage(
            cache_key,
            1000,
            2099,
            &range2_data,
            metadata1.clone(),
            std::time::Duration::from_secs(315360000), // 10 years TTL
        )
        .await
        .unwrap();

    range_handler
        .store_range_new_storage(
            cache_key,
            2000,
            3099,
            &range3_data,
            metadata1,
            std::time::Duration::from_secs(315360000), // 10 years TTL
        )
        .await
        .unwrap();

    // Client requests bytes=0-2999 (3000 bytes)
    let requested_start = 0u64;
    let requested_end = 2999u64;
    let requested_range = RangeSpec {
        start: requested_start,
        end: requested_end,
    };

    // Find cached ranges
    let overlap = range_handler
        .find_cached_ranges(cache_key, &requested_range, None, None)
        .await
        .unwrap();

    // Verify we found all three cached ranges
    assert_eq!(
        overlap.cached_ranges.len(),
        3,
        "Should find 3 cached ranges"
    );
    assert_eq!(
        overlap.missing_ranges.len(),
        0,
        "Should have no missing ranges"
    );
    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve from cache"
    );

    // Verify the cached ranges
    assert_eq!(overlap.cached_ranges[0].start, 0);
    assert_eq!(overlap.cached_ranges[0].end, 1099);
    assert_eq!(overlap.cached_ranges[1].start, 1000);
    assert_eq!(overlap.cached_ranges[1].end, 2099);
    assert_eq!(overlap.cached_ranges[2].start, 2000);
    assert_eq!(overlap.cached_ranges[2].end, 3099);

    // Use merge_range_segments to merge the cached ranges
    // This simulates what serve_range_from_cache does for multiple ranges
    let merge_result = range_handler
        .merge_range_segments(
            cache_key,
            &requested_range,
            &overlap.cached_ranges,
            &[], // No fetched ranges - all data is from cache
        )
        .await
        .unwrap();

    // Verify the merged data is exactly the requested size
    let expected_size = (requested_end - requested_start + 1) as usize;
    assert_eq!(
        merge_result.data.len(),
        expected_size,
        "Merged data should be exactly {} bytes",
        expected_size
    );

    // Verify the merged data is correct
    // Expected data: bytes 0-999 from range1, bytes 1000-1999 from range2, bytes 2000-2999 from range3
    let mut expected_data = Vec::new();
    expected_data.extend_from_slice(&range1_data[0..1000]); // First 1000 bytes from range1
    expected_data.extend_from_slice(&range2_data[0..1000]); // First 1000 bytes from range2 (which is bytes 1000-1999 of the object)
    expected_data.extend_from_slice(&range3_data[0..1000]); // First 1000 bytes from range3 (which is bytes 2000-2999 of the object)

    assert_eq!(
        merge_result.data, expected_data,
        "Merged data should match expected sliced and concatenated data"
    );

    // Verify metrics
    assert_eq!(
        merge_result.segments_merged, 3,
        "Should have merged 3 segments"
    );
    assert_eq!(
        merge_result.bytes_from_cache, 3000,
        "All 3000 bytes should be from cache"
    );
    assert_eq!(merge_result.bytes_from_s3, 0, "No bytes should be from S3");
    assert_eq!(
        merge_result.cache_efficiency, 100.0,
        "Cache efficiency should be 100%"
    );
}

#[tokio::test]
async fn test_range_slice_multiple_cached_ranges_with_gaps() {
    // Create temporary directory for cache
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = create_test_cache_manager(cache_dir.clone());

    // Create disk cache manager
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir.clone(), true, 1024, false),
    ));

    // Create range handler
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));

    let cache_key = "test-object-gaps";

    // Scenario: Client requests bytes 0-4999 (5000 bytes)
    // Cache contains two ranges with a gap:
    // - Range 1: 0-1999 (2000 bytes)
    // - Gap: 2000-2999 (1000 bytes) - NOT cached
    // - Range 2: 3000-5099 (2100 bytes, but client only needs 3000-4999, so slice 2000 bytes)

    // Create test data for each cached range
    let range1_data = vec![0xAA; 2000];
    let range2_data = vec![0xBB; 2100];

    // Store the two cached ranges
    let metadata = ObjectMetadata {
        etag: "test-etag-gaps".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: 5100,
        content_type: Some("application/octet-stream".to_string()),
        upload_state: UploadState::Complete,
        cumulative_size: 5100,
        parts: Vec::new(),
        ..Default::default()
    };

    range_handler
        .store_range_new_storage(
            cache_key,
            0,
            1999,
            &range1_data,
            metadata.clone(),
            std::time::Duration::from_secs(315360000), // 10 years TTL
        )
        .await
        .unwrap();

    range_handler
        .store_range_new_storage(
            cache_key,
            3000,
            5099,
            &range2_data,
            metadata,
            std::time::Duration::from_secs(315360000), // 10 years TTL
        )
        .await
        .unwrap();

    // Client requests bytes=0-4999 (5000 bytes)
    let requested_start = 0u64;
    let requested_end = 4999u64;
    let requested_range = RangeSpec {
        start: requested_start,
        end: requested_end,
    };

    // Find cached ranges
    let overlap = range_handler
        .find_cached_ranges(cache_key, &requested_range, None, None)
        .await
        .unwrap();

    // Verify we found two cached ranges and one missing range
    assert_eq!(
        overlap.cached_ranges.len(),
        2,
        "Should find 2 cached ranges"
    );
    assert_eq!(
        overlap.missing_ranges.len(),
        1,
        "Should have 1 missing range"
    );
    assert!(
        !overlap.can_serve_from_cache,
        "Should NOT be able to serve entirely from cache due to gap"
    );

    // Verify the missing range
    assert_eq!(overlap.missing_ranges[0].start, 2000);
    assert_eq!(overlap.missing_ranges[0].end, 2999);

    // This test verifies that the system correctly identifies gaps
    // In a real scenario, the missing range would be fetched from S3 and merged
}
