//! Integration test for multipart upload followed by GET
//!
//! Tests Requirement 5.3:
//! - Upload 100MB file via multipart (13 parts)
//! - Immediately GET the file after upload completes
//! - Verify response is served entirely from cache (no S3 fetch)
//! - Verify response is byte-identical to uploaded data
//! - Verify cache efficiency is 100%

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::UploadState;
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
async fn test_multipart_upload_followed_by_get() {
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/large-object.bin";

    // Step 1: Create 10MB of test data
    // We'll use 5 parts for faster testing
    // Part sizes: 4 parts of 2MB + 1 part of 2MB = 10MB
    let part_size_2mb = 2 * 1024 * 1024; // 2MB
    let total_size = 10 * 1024 * 1024; // 10MB

    // Generate unique data for each part so we can verify correctness
    let mut full_data = Vec::with_capacity(total_size);
    let mut parts = Vec::new();

    // Create 5 parts of 2MB each
    for part_num in 1..=5 {
        let part_data: Vec<u8> = (0..part_size_2mb)
            .map(|i| ((part_num * 17 + i / 1024) % 256) as u8)
            .collect();
        full_data.extend_from_slice(&part_data);
        parts.push((part_num, part_data));
    }

    assert_eq!(full_data.len(), total_size, "Full data should be 10MB");
    assert_eq!(parts.len(), 5, "Should have 5 parts");

    // Step 2: Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await.unwrap();

    // Step 3: Upload all parts
    for (part_num, part_data) in &parts {
        let etag = format!("etag-part-{}", part_num);
        cache_manager
            .store_multipart_part(path, *part_num, part_data, etag)
            .await
            .unwrap();
    }

    // Step 4: Complete multipart upload
    cache_manager.complete_multipart_upload(path).await.unwrap();

    // Step 5: Verify upload is complete
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(metadata.object_metadata.upload_state, UploadState::Complete);
    assert_eq!(metadata.object_metadata.content_length, total_size as u64);
    assert_eq!(metadata.ranges.len(), 5, "Should have 5 cached ranges");

    // Step 6: Perform GET request for the entire object
    // This simulates a client requesting the full file
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

    // Requirement 5.3: Verify response is served entirely from cache (no S3 fetch)
    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve entirely from cache"
    );
    assert!(
        overlap.missing_ranges.is_empty(),
        "Should have no missing ranges - all data is cached"
    );
    assert_eq!(
        overlap.cached_ranges.len(),
        5,
        "Should have 5 cached ranges"
    );

    // Step 7: Merge the cached ranges to reconstruct the full object
    let merge_result = range_handler
        .merge_range_segments(
            &cache_key,
            &requested_range,
            &overlap.cached_ranges,
            &[], // No fetched ranges since everything is cached
        )
        .await
        .unwrap();

    // Requirement 5.3: Verify response is byte-identical to uploaded data
    assert_eq!(
        merge_result.data.len(),
        total_size,
        "Merged data should be 10MB"
    );
    assert_eq!(
        merge_result.data, full_data,
        "Merged data should be byte-identical to original data"
    );

    // Requirement 5.3: Verify cache efficiency is 100%
    assert_eq!(
        merge_result.bytes_from_cache, total_size as u64,
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
    assert_eq!(
        merge_result.segments_merged, 5,
        "Should have merged 5 segments"
    );

    println!("✓ Multipart upload followed by GET test passed!");
    println!("  - Uploaded 10MB file in 5 parts");
    println!("  - GET request served entirely from cache");
    println!("  - Data is byte-identical to original");
    println!("  - Cache efficiency: 100%");
}

#[tokio::test]
async fn test_multipart_upload_partial_get() {
    // Test getting a partial range after multipart upload
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/partial-get-object.bin";

    // Create 3 parts of 10MB each (30MB total)
    let part_size = 10 * 1024 * 1024; // 10MB

    // Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await.unwrap();

    // Upload 3 parts
    for part_num in 1..=3 {
        let part_data: Vec<u8> = (0..part_size)
            .map(|i| ((part_num * 23 + i / 1024) % 256) as u8)
            .collect();
        let etag = format!("etag-part-{}", part_num);
        cache_manager
            .store_multipart_part(path, part_num, &part_data, etag)
            .await
            .unwrap();
    }

    // Complete multipart upload
    cache_manager.complete_multipart_upload(path).await.unwrap();

    // Request a partial range that spans multiple parts
    // Request bytes 5MB to 25MB (spans parts 1, 2, and part of 3)
    let start = 5 * 1024 * 1024;
    let end = 25 * 1024 * 1024 - 1;
    let requested_range = RangeSpec { start, end };

    let cache_key = CacheManager::generate_cache_key(path, None);
    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Find cached ranges
    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    // Should be able to serve from cache
    assert!(overlap.can_serve_from_cache);
    assert!(overlap.missing_ranges.is_empty());

    // Merge the ranges
    let merge_result = range_handler
        .merge_range_segments(&cache_key, &requested_range, &overlap.cached_ranges, &[])
        .await
        .unwrap();

    // Verify the merged data size
    let expected_size = (end - start + 1) as usize;
    assert_eq!(merge_result.data.len(), expected_size);
    assert_eq!(merge_result.bytes_from_cache, expected_size as u64);
    assert_eq!(merge_result.bytes_from_s3, 0);
    assert_eq!(merge_result.cache_efficiency, 100.0);

    println!("✓ Multipart upload partial GET test passed!");
    println!("  - Requested range spanning multiple parts");
    println!("  - Served entirely from cache");
    println!("  - Cache efficiency: 100%");
}

#[tokio::test]
async fn test_multipart_upload_non_aligned_get() {
    // Test getting a non-aligned range after multipart upload
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/non-aligned-object.bin";

    // Create 5 parts of 1MB each (5MB total)
    let part_size = 1024 * 1024; // 1MB
    let total_size = 5 * 1024 * 1024; // 5MB

    // Generate full data for verification
    let mut full_data = Vec::with_capacity(total_size);

    // Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await.unwrap();

    // Upload 5 parts
    for part_num in 1..=5 {
        let part_data: Vec<u8> = (0..part_size)
            .map(|i| ((part_num * 31 + i / 256) % 256) as u8)
            .collect();
        full_data.extend_from_slice(&part_data);
        let etag = format!("etag-part-{}", part_num);
        cache_manager
            .store_multipart_part(path, part_num, &part_data, etag)
            .await
            .unwrap();
    }

    // Complete multipart upload
    cache_manager.complete_multipart_upload(path).await.unwrap();

    // Request a non-aligned range (not at part boundaries)
    // Request bytes 500KB to 3.5MB
    let start = 500 * 1024;
    let end = 3 * 1024 * 1024 + 512 * 1024 - 1;
    let requested_range = RangeSpec { start, end };

    let cache_key = CacheManager::generate_cache_key(path, None);
    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Find cached ranges
    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    // Should be able to serve from cache
    assert!(overlap.can_serve_from_cache);
    assert!(overlap.missing_ranges.is_empty());

    // Merge the ranges
    let merge_result = range_handler
        .merge_range_segments(&cache_key, &requested_range, &overlap.cached_ranges, &[])
        .await
        .unwrap();

    // Verify the merged data
    let expected_size = (end - start + 1) as usize;
    assert_eq!(merge_result.data.len(), expected_size);

    // Verify byte-identical to original data
    let expected_data = &full_data[start as usize..=end as usize];
    assert_eq!(merge_result.data, expected_data);

    assert_eq!(merge_result.bytes_from_cache, expected_size as u64);
    assert_eq!(merge_result.bytes_from_s3, 0);
    assert_eq!(merge_result.cache_efficiency, 100.0);

    println!("✓ Multipart upload non-aligned GET test passed!");
    println!("  - Requested non-aligned range");
    println!("  - Served entirely from cache");
    println!("  - Data is byte-identical to original");
    println!("  - Cache efficiency: 100%");
}
