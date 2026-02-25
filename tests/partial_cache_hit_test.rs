//! Integration test for partial cache hit scenario
//!
//! Tests Requirements 1.1, 1.2, 1.4, 1.5:
//! - Create test with specific cached ranges (0-8MB, 16-24MB, 32-40MB)
//! - Request full range (0-40MB)
//! - Verify only missing ranges (8-16MB, 24-32MB) are fetched from S3
//! - Verify response is complete and correct
//! - Verify missing ranges are now cached
//! - Verify cache efficiency is approximately 60%

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper function to create a test cache manager
fn create_test_cache_manager() -> (
    Arc<CacheManager>,
    Arc<tokio::sync::RwLock<DiskCacheManager>>,
    TempDir,
) {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = Arc::new(CacheManager::new_with_all_ttls(
        cache_dir.clone(),
        false, // RAM cache disabled for these tests
        0,
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        1024,                                 // compression threshold
        true,                                 // compression enabled
        std::time::Duration::from_secs(3600), // GET_TTL
        std::time::Duration::from_secs(1800), // HEAD_TTL
        std::time::Duration::from_secs(900),  // PUT_TTL
        false,                                // actively_remove_cached_data
    ));

    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(DiskCacheManager::new(
        cache_dir, true, 1024, false,
    )));

    (cache_manager, disk_cache_manager, temp_dir)
}

#[tokio::test]
async fn test_partial_cache_hit_scenario() {
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/partial-cache-object.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Step 1: Create 40MB of test data with a predictable pattern
    let mb = 1024 * 1024;
    let total_size = 40 * mb;

    let full_data: Vec<u8> = (0..total_size)
        .map(|i| ((i / 1024 + i % 256) % 256) as u8)
        .collect();

    // Step 2: Cache specific ranges (0-8MB, 16-24MB, 32-40MB)
    // This gives us 24MB cached out of 40MB = 60% cache efficiency
    let cached_ranges = vec![
        (0, 8 * mb - 1),        // 0-8MB (8MB)
        (16 * mb, 24 * mb - 1), // 16-24MB (8MB)
        (32 * mb, 40 * mb - 1), // 32-40MB (8MB)
    ];

    // Create object metadata
    let object_metadata = ObjectMetadata {
        etag: "test-etag-partial".to_string(),
        last_modified: "Wed, 21 Nov 2025 12:00:00 GMT".to_string(),
        content_length: total_size as u64,
        content_type: Some("application/octet-stream".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: UploadState::Complete,
        cumulative_size: total_size as u64,
        parts: Vec::new(),
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::Lz4,
        compressed_size: total_size as u64,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
    };

    // Store each cached range
    for (start, end) in &cached_ranges {
        let range_data = full_data[*start..=*end].to_vec();
        disk_cache_manager
            .write()
            .await
            .store_range(
                &cache_key,
                *start as u64,
                *end as u64,
                &range_data,
                object_metadata.clone(),
                std::time::Duration::from_secs(315360000), // 10 years TTL
                true)
            .await
            .unwrap();
    }

    // Step 3: Request full range (0-40MB)
    let requested_range = RangeSpec {
        start: 0,
        end: (total_size - 1) as u64,
    };

    // Create RangeHandler
    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Find cached ranges that overlap with the request
    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    // Requirement 1.1: Verify we cannot serve entirely from cache
    assert!(
        !overlap.can_serve_from_cache,
        "Should NOT be able to serve entirely from cache - we have missing ranges"
    );

    // Requirement 1.1: Verify missing ranges are identified correctly
    assert_eq!(
        overlap.missing_ranges.len(),
        2,
        "Should have 2 missing ranges"
    );

    // Verify the missing ranges are correct (8-16MB and 24-32MB)
    let expected_missing = vec![
        RangeSpec {
            start: 8 * mb as u64,
            end: 16 * mb as u64 - 1,
        },
        RangeSpec {
            start: 24 * mb as u64,
            end: 32 * mb as u64 - 1,
        },
    ];

    assert_eq!(
        overlap.missing_ranges[0].start, expected_missing[0].start,
        "First missing range should start at 8MB"
    );
    assert_eq!(
        overlap.missing_ranges[0].end, expected_missing[0].end,
        "First missing range should end at 16MB-1"
    );
    assert_eq!(
        overlap.missing_ranges[1].start, expected_missing[1].start,
        "Second missing range should start at 24MB"
    );
    assert_eq!(
        overlap.missing_ranges[1].end, expected_missing[1].end,
        "Second missing range should end at 32MB-1"
    );

    // Verify cached ranges
    assert_eq!(
        overlap.cached_ranges.len(),
        3,
        "Should have 3 cached ranges"
    );

    // Step 4: Simulate fetching missing ranges from S3
    // In a real scenario, these would be fetched from S3
    // For this test, we extract them from our full_data
    let fetched_ranges: Vec<(RangeSpec, Vec<u8>, HashMap<String, String>)> = overlap
        .missing_ranges
        .iter()
        .map(|range_spec| {
            let data = full_data[range_spec.start as usize..=range_spec.end as usize].to_vec();
            (range_spec.clone(), data, HashMap::new())
        })
        .collect();

    // Requirement 1.1: Verify only missing ranges are "fetched"
    assert_eq!(
        fetched_ranges.len(),
        2,
        "Should only fetch 2 missing ranges"
    );
    assert_eq!(
        fetched_ranges[0].1.len(),
        8 * mb,
        "First fetched range should be 8MB"
    );
    assert_eq!(
        fetched_ranges[1].1.len(),
        8 * mb,
        "Second fetched range should be 8MB"
    );

    // Step 5: Merge cached and fetched ranges
    let merge_result = range_handler
        .merge_range_segments(
            &cache_key,
            &requested_range,
            &overlap.cached_ranges,
            &fetched_ranges,
        )
        .await
        .unwrap();

    // Requirement 1.5: Verify response is complete and correct
    assert_eq!(
        merge_result.data.len(),
        total_size,
        "Merged data should be 40MB"
    );
    assert_eq!(
        merge_result.data, full_data,
        "Merged data should be byte-identical to original data"
    );

    // Requirement 1.4 & 1.5: Verify cache efficiency is approximately 60%
    // We had 24MB cached out of 40MB = 60%
    assert_eq!(
        merge_result.bytes_from_cache,
        24 * mb as u64,
        "Should have 24MB from cache"
    );
    assert_eq!(
        merge_result.bytes_from_s3,
        16 * mb as u64,
        "Should have 16MB from S3"
    );

    // Allow for floating point precision
    assert!(
        (merge_result.cache_efficiency - 60.0).abs() < 0.1,
        "Cache efficiency should be approximately 60%, got {}",
        merge_result.cache_efficiency
    );

    assert_eq!(
        merge_result.segments_merged, 5,
        "Should have merged 5 segments (3 cached + 2 fetched)"
    );

    // Step 6: Cache the fetched ranges (simulating what the proxy would do)
    for (range_spec, data, _headers) in &fetched_ranges {
        disk_cache_manager
            .write()
            .await
            .store_range(
                &cache_key,
                range_spec.start,
                range_spec.end,
                data,
                object_metadata.clone(),
                std::time::Duration::from_secs(315360000), // 10 years TTL
                true)
            .await
            .unwrap();
    }

    // Step 7: Verify missing ranges are now cached
    // Request the same range again
    let overlap_after = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    // Requirement 1.4: Verify missing ranges are now cached
    assert!(
        overlap_after.can_serve_from_cache,
        "Should now be able to serve entirely from cache"
    );
    assert!(
        overlap_after.missing_ranges.is_empty(),
        "Should have no missing ranges after caching"
    );
    assert_eq!(
        overlap_after.cached_ranges.len(),
        5,
        "Should now have 5 cached ranges"
    );

    // Verify we can serve the full request from cache with 100% efficiency
    let merge_result_after = range_handler
        .merge_range_segments(
            &cache_key,
            &requested_range,
            &overlap_after.cached_ranges,
            &[],
        )
        .await
        .unwrap();

    assert_eq!(
        merge_result_after.cache_efficiency, 100.0,
        "Cache efficiency should now be 100%"
    );
    assert_eq!(
        merge_result_after.bytes_from_s3, 0,
        "Should have 0 bytes from S3 on second request"
    );

    println!("✓ Partial cache hit scenario test passed!");
    println!("  - Initial state: 3 cached ranges (0-8MB, 16-24MB, 32-40MB)");
    println!("  - Requested: 0-40MB");
    println!("  - Missing ranges identified: 8-16MB, 24-32MB");
    println!("  - Initial cache efficiency: 60%");
    println!("  - After caching missing ranges: 100% efficiency");
}
