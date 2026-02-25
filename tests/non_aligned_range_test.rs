//! Integration test for non-aligned range request
//!
//! Tests Requirements 1.3, 2.2, 2.3:
//! - Create test with cached ranges at 8MB boundaries
//! - Request range that crosses boundaries (e.g., 1MB-10MB)
//! - Verify request is served from cached ranges without S3 fetch
//! - Verify correct bytes are extracted from each cached range
//! - Verify response is byte-identical to S3 response

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
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
async fn test_non_aligned_range_request() {
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/non-aligned-object.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Step 1: Create 16MB of test data with a predictable pattern
    // We'll cache this in 2 ranges at 8MB boundaries: 0-8MB and 8-16MB
    let mb = 1024 * 1024;
    let total_size = 16 * mb;

    // Generate unique data so we can verify correctness
    let full_data: Vec<u8> = (0..total_size)
        .map(|i| ((i / 1024 + i % 256) % 256) as u8)
        .collect();

    // Step 2: Cache ranges at 8MB boundaries
    // Range 1: 0 to 8MB-1 (8388607)
    // Range 2: 8MB (8388608) to 16MB-1 (16777215)
    let cached_ranges = vec![
        (0, 8 * mb - 1),       // 0-8388607
        (8 * mb, 16 * mb - 1), // 8388608-16777215
    ];

    // Create object metadata
    let object_metadata = ObjectMetadata {
        etag: "test-etag-non-aligned".to_string(),
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

    // Step 3: Request a non-aligned range that crosses the 8MB boundary
    // Request: 1MB (1048576) to 10MB-1 (10485759)
    // This crosses the boundary at 8MB (8388608)
    let requested_start = 1 * mb;
    let requested_end = 10 * mb - 1;
    let requested_range = RangeSpec {
        start: requested_start as u64,
        end: requested_end as u64,
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

    // Requirement 1.3: Verify request can be served entirely from cache (no S3 fetch)
    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve entirely from cache - all requested bytes are cached"
    );
    assert!(
        overlap.missing_ranges.is_empty(),
        "Should have no missing ranges - request is fully covered by cached ranges"
    );

    // Verify we have 2 cached ranges that overlap with our request
    assert_eq!(
        overlap.cached_ranges.len(),
        2,
        "Should have 2 cached ranges overlapping with the request"
    );

    // Verify the cached ranges are correct
    assert_eq!(overlap.cached_ranges[0].start, 0);
    assert_eq!(overlap.cached_ranges[0].end, 8 * mb as u64 - 1);
    assert_eq!(overlap.cached_ranges[1].start, 8 * mb as u64);
    assert_eq!(overlap.cached_ranges[1].end, 16 * mb as u64 - 1);

    // Step 4: Merge the cached ranges to serve the request
    // No fetched ranges since everything is cached
    let merge_result = range_handler
        .merge_range_segments(
            &cache_key,
            &requested_range,
            &overlap.cached_ranges,
            &[], // No fetched ranges
        )
        .await
        .unwrap();

    // Requirement 2.2, 2.3: Verify correct bytes are extracted from each cached range
    let expected_size = (requested_end - requested_start + 1) as usize;
    assert_eq!(
        merge_result.data.len(),
        expected_size,
        "Merged data should be {} bytes (1MB to 10MB)",
        expected_size
    );

    // Requirement 2.2, 2.3: Verify response is byte-identical to original data
    let expected_data = &full_data[requested_start..=requested_end];
    assert_eq!(
        merge_result.data, expected_data,
        "Merged data should be byte-identical to original data"
    );

    // Requirement 1.3: Verify no S3 fetch occurred (100% cache efficiency)
    assert_eq!(
        merge_result.bytes_from_cache, expected_size as u64,
        "All bytes should come from cache"
    );
    assert_eq!(
        merge_result.bytes_from_s3, 0,
        "No bytes should come from S3"
    );
    assert_eq!(
        merge_result.cache_efficiency, 100.0,
        "Cache efficiency should be 100%"
    );

    // Verify we merged 2 segments (one from each cached range)
    assert_eq!(
        merge_result.segments_merged, 2,
        "Should have merged 2 segments (one from each cached range)"
    );

    println!("✓ Non-aligned range request test passed!");
    println!("  - Cached ranges: 0-8MB, 8-16MB (at 8MB boundaries)");
    println!("  - Requested: 1MB-10MB (crosses 8MB boundary)");
    println!("  - Served entirely from cache without S3 fetch");
    println!("  - Correct bytes extracted from each cached range");
    println!("  - Response is byte-identical to original data");
    println!("  - Cache efficiency: 100%");
}

#[tokio::test]
async fn test_non_aligned_range_multiple_boundaries() {
    // Test a range that crosses multiple boundaries
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/multi-boundary-object.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Create 32MB of test data
    let mb = 1024 * 1024;
    let total_size = 32 * mb;

    let full_data: Vec<u8> = (0..total_size)
        .map(|i| ((i / 512 + i % 256) % 256) as u8)
        .collect();

    // Cache in 4 ranges at 8MB boundaries
    let cached_ranges = vec![
        (0, 8 * mb - 1),        // 0-8MB
        (8 * mb, 16 * mb - 1),  // 8-16MB
        (16 * mb, 24 * mb - 1), // 16-24MB
        (24 * mb, 32 * mb - 1), // 24-32MB
    ];

    let object_metadata = ObjectMetadata {
        etag: "test-etag-multi-boundary".to_string(),
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

    // Request a range that crosses 3 boundaries: 2MB to 26MB
    // This crosses boundaries at 8MB, 16MB, and 24MB
    let requested_start = 2 * mb;
    let requested_end = 26 * mb - 1;
    let requested_range = RangeSpec {
        start: requested_start as u64,
        end: requested_end as u64,
    };

    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    // Should be able to serve from cache
    assert!(overlap.can_serve_from_cache);
    assert!(overlap.missing_ranges.is_empty());
    assert_eq!(
        overlap.cached_ranges.len(),
        4,
        "Should overlap with 4 cached ranges"
    );

    // Merge the ranges
    let merge_result = range_handler
        .merge_range_segments(&cache_key, &requested_range, &overlap.cached_ranges, &[])
        .await
        .unwrap();

    // Verify correctness
    let expected_size = (requested_end - requested_start + 1) as usize;
    assert_eq!(merge_result.data.len(), expected_size);

    let expected_data = &full_data[requested_start..=requested_end];
    assert_eq!(merge_result.data, expected_data);

    assert_eq!(merge_result.bytes_from_cache, expected_size as u64);
    assert_eq!(merge_result.bytes_from_s3, 0);
    assert_eq!(merge_result.cache_efficiency, 100.0);
    assert_eq!(merge_result.segments_merged, 4, "Should merge 4 segments");

    println!("✓ Non-aligned range crossing multiple boundaries test passed!");
    println!("  - Cached ranges: 0-8MB, 8-16MB, 16-24MB, 24-32MB");
    println!("  - Requested: 2MB-26MB (crosses 3 boundaries)");
    println!("  - Served entirely from cache");
    println!("  - Response is byte-identical to original data");
}

#[tokio::test]
async fn test_non_aligned_range_at_start() {
    // Test a range that starts at 0 but doesn't align with end boundary
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/start-aligned-object.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    let mb = 1024 * 1024;
    let total_size = 16 * mb;

    let full_data: Vec<u8> = (0..total_size).map(|i| ((i / 256) % 256) as u8).collect();

    // Cache in 2 ranges at 8MB boundaries
    let cached_ranges = vec![(0, 8 * mb - 1), (8 * mb, 16 * mb - 1)];

    let object_metadata = ObjectMetadata {
        etag: "test-etag-start-aligned".to_string(),
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

    // Request from 0 to 12MB (not aligned with 8MB boundary at end)
    let requested_start = 0;
    let requested_end = 12 * mb - 1;
    let requested_range = RangeSpec {
        start: requested_start as u64,
        end: requested_end as u64,
    };

    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    assert!(overlap.can_serve_from_cache);
    assert!(overlap.missing_ranges.is_empty());

    let merge_result = range_handler
        .merge_range_segments(&cache_key, &requested_range, &overlap.cached_ranges, &[])
        .await
        .unwrap();

    let expected_size = (requested_end - requested_start + 1) as usize;
    assert_eq!(merge_result.data.len(), expected_size);

    let expected_data = &full_data[requested_start..=requested_end];
    assert_eq!(merge_result.data, expected_data);

    assert_eq!(merge_result.cache_efficiency, 100.0);

    println!("✓ Non-aligned range at start test passed!");
    println!("  - Requested: 0-12MB (start aligned, end not aligned)");
    println!("  - Served entirely from cache");
}

#[tokio::test]
async fn test_non_aligned_range_at_end() {
    // Test a range that ends at the file end but doesn't align with start boundary
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/end-aligned-object.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    let mb = 1024 * 1024;
    let total_size = 16 * mb;

    let full_data: Vec<u8> = (0..total_size).map(|i| ((i / 128) % 256) as u8).collect();

    // Cache in 2 ranges at 8MB boundaries
    let cached_ranges = vec![(0, 8 * mb - 1), (8 * mb, 16 * mb - 1)];

    let object_metadata = ObjectMetadata {
        etag: "test-etag-end-aligned".to_string(),
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

    // Request from 4MB to end (16MB) - not aligned with 8MB boundary at start
    let requested_start = 4 * mb;
    let requested_end = 16 * mb - 1;
    let requested_range = RangeSpec {
        start: requested_start as u64,
        end: requested_end as u64,
    };

    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    assert!(overlap.can_serve_from_cache);
    assert!(overlap.missing_ranges.is_empty());

    let merge_result = range_handler
        .merge_range_segments(&cache_key, &requested_range, &overlap.cached_ranges, &[])
        .await
        .unwrap();

    let expected_size = (requested_end - requested_start + 1) as usize;
    assert_eq!(merge_result.data.len(), expected_size);

    let expected_data = &full_data[requested_start..=requested_end];
    assert_eq!(merge_result.data, expected_data);

    assert_eq!(merge_result.cache_efficiency, 100.0);

    println!("✓ Non-aligned range at end test passed!");
    println!("  - Requested: 4MB-16MB (start not aligned, end aligned)");
    println!("  - Served entirely from cache");
}
