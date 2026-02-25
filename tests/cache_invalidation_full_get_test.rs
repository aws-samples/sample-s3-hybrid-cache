//! Integration test for cache invalidation and full object GET
//!
//! Tests Requirements 2.4, 2.5:
//! - PUT an object (which caches it in write cache)
//! - Perform full object GET (without Range header)
//! - Verify that cached ranges from PUT are utilized
//! - Verify that the proxy doesn't unnecessarily fetch from S3

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{CacheMetadata, UploadState};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper function to create a test cache manager
async fn create_test_cache_manager() -> (
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

    // Must call create_configured_disk_cache_manager() to set up the JournalConsolidator
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();

    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(DiskCacheManager::new(
        cache_dir, true, 1024, false,
    )));

    (cache_manager, disk_cache_manager, temp_dir)
}

/// Test that full object GET utilizes cached ranges from a previous PUT
///
/// This test reproduces the issue where:
/// 1. A PUT operation caches an object in the write cache
/// 2. A subsequent full object GET (without Range header) should utilize the cached data
/// 3. Currently, the proxy may not be finding/using the cached ranges
#[tokio::test]
async fn test_put_then_full_get_uses_cache() {
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager().await;
    cache_manager.initialize().await.unwrap();
    disk_cache_manager.write().await.initialize().await.unwrap();

    // Use the test bucket and prefix
    let path = "/egummett-testing-source-1/s3-transparent-proxy-testing/test-cache-lookup.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Step 1: Simulate a PUT operation by storing data in write cache
    let test_data = b"This is test data for cache invalidation testing. It should be found when we do a full GET.";
    let data_size = test_data.len() as u64;

    let headers = HashMap::from([
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("etag".to_string(), "test-etag-12345".to_string()),
        (
            "last-modified".to_string(),
            "2024-01-01T00:00:00Z".to_string(),
        ),
    ]);

    let metadata = CacheMetadata {
        etag: "test-etag-12345".to_string(),
        last_modified: "2024-01-01T00:00:00Z".to_string(),
        content_length: data_size,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    println!("[TEST] Step 1: Storing PUT data in write cache");
    cache_manager
        .store_write_cache_entry(&cache_key, test_data, headers.clone(), metadata.clone())
        .await
        .unwrap();

    // Verify the data was cached
    let cached_metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap();
    assert!(
        cached_metadata.is_some(),
        "Metadata should be cached after PUT"
    );

    let cached_metadata = cached_metadata.unwrap();
    println!(
        "[TEST] Cached metadata: upload_state={:?}, content_length={}, ranges={}",
        cached_metadata.object_metadata.upload_state,
        cached_metadata.object_metadata.content_length,
        cached_metadata.ranges.len()
    );

    assert_eq!(
        cached_metadata.object_metadata.upload_state,
        UploadState::Complete
    );
    assert_eq!(cached_metadata.object_metadata.content_length, data_size);
    assert!(
        !cached_metadata.ranges.is_empty(),
        "Should have at least one cached range"
    );

    // Step 2: Simulate a full object GET request
    // This is what happens when a client does GET without a Range header
    // The proxy converts it to a range request for bytes 0-(size-1)
    println!(
        "\n[TEST] Step 2: Simulating full object GET (converted to range 0-{})",
        data_size - 1
    );

    let requested_range = RangeSpec {
        start: 0,
        end: data_size - 1,
    };

    // Create RangeHandler
    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Step 3: Find cached ranges that overlap with the request
    println!(
        "[TEST] Step 3: Calling find_cached_ranges for range {}-{}",
        requested_range.start, requested_range.end
    );

    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    println!("[TEST] find_cached_ranges result:");
    println!("  - cached_ranges: {}", overlap.cached_ranges.len());
    println!("  - missing_ranges: {}", overlap.missing_ranges.len());
    println!("  - can_serve_from_cache: {}", overlap.can_serve_from_cache);

    for (i, cached_range) in overlap.cached_ranges.iter().enumerate() {
        println!(
            "  - Cached range {}: start={}, end={}, etag={}",
            i, cached_range.start, cached_range.end, cached_range.etag
        );
    }

    for (i, missing_range) in overlap.missing_ranges.iter().enumerate() {
        println!(
            "  - Missing range {}: start={}, end={}",
            i, missing_range.start, missing_range.end
        );
    }

    // Step 4: Verify that cached ranges are found and utilized
    // Requirement 2.4: WHEN the Proxy splits a full object GET into parallel range fetches
    // THEN the Proxy SHALL call find_cached_ranges for each range to identify cache hits
    assert!(
        !overlap.cached_ranges.is_empty(),
        "Should find cached ranges from the PUT operation"
    );

    // Requirement 2.5: WHEN cached ranges are found THEN the Proxy SHALL serve those ranges
    // from cache and only fetch missing ranges from S3
    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve the full object entirely from cache (no S3 fetch needed)"
    );

    assert!(
        overlap.missing_ranges.is_empty(),
        "Should have no missing ranges - all data should be cached from PUT"
    );

    // Step 5: Merge the cached ranges to reconstruct the full object
    println!("\n[TEST] Step 5: Merging cached ranges to reconstruct full object");

    let merge_result = range_handler
        .merge_range_segments(
            &cache_key,
            &requested_range,
            &overlap.cached_ranges,
            &[], // No fetched ranges since everything should be cached
        )
        .await
        .unwrap();

    println!("[TEST] Merge result:");
    println!("  - data size: {} bytes", merge_result.data.len());
    println!("  - segments merged: {}", merge_result.segments_merged);
    println!("  - bytes from cache: {}", merge_result.bytes_from_cache);
    println!("  - bytes from S3: {}", merge_result.bytes_from_s3);
    println!(
        "  - cache efficiency: {:.2}%",
        merge_result.cache_efficiency
    );

    // Verify the merged data matches the original PUT data
    assert_eq!(
        merge_result.data.len(),
        data_size as usize,
        "Merged data should be the same size as original"
    );

    assert_eq!(
        merge_result.data, test_data,
        "Merged data should be byte-identical to original PUT data"
    );

    // Verify cache efficiency is 100% (all data from cache, none from S3)
    assert_eq!(
        merge_result.bytes_from_s3, 0,
        "Should fetch 0 bytes from S3 - everything should come from cache"
    );

    assert_eq!(
        merge_result.bytes_from_cache, data_size,
        "Should serve all bytes from cache"
    );

    assert_eq!(
        merge_result.cache_efficiency, 100.0,
        "Cache efficiency should be 100%"
    );

    println!(
        "\n[TEST] ✓ Test passed: Full object GET successfully utilized cached ranges from PUT"
    );
}

/// Test with a larger object to verify range handling
#[tokio::test]
async fn test_put_then_full_get_large_object() {
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager().await;
    cache_manager.initialize().await.unwrap();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path =
        "/egummett-testing-source-1/s3-transparent-proxy-testing/test-cache-lookup-large.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Create a 1MB test file with a pattern
    let data_size = 1024 * 1024; // 1MB
    let test_data: Vec<u8> = (0..data_size).map(|i| ((i / 1024) % 256) as u8).collect();

    let headers = HashMap::from([
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("etag".to_string(), "test-etag-large-67890".to_string()),
        (
            "last-modified".to_string(),
            "2024-01-01T00:00:00Z".to_string(),
        ),
    ]);

    let metadata = CacheMetadata {
        etag: "test-etag-large-67890".to_string(),
        last_modified: "2024-01-01T00:00:00Z".to_string(),
        content_length: data_size as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    println!("[TEST] Storing 1MB PUT data in write cache");
    cache_manager
        .store_write_cache_entry(&cache_key, &test_data, headers, metadata)
        .await
        .unwrap();

    // Simulate full object GET
    let requested_range = RangeSpec {
        start: 0,
        end: (data_size - 1) as u64,
    };

    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    println!("[TEST] Finding cached ranges for full object GET");
    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    println!(
        "[TEST] Result: cached={}, missing={}, can_serve={}",
        overlap.cached_ranges.len(),
        overlap.missing_ranges.len(),
        overlap.can_serve_from_cache
    );

    // Verify cached ranges are found
    assert!(
        !overlap.cached_ranges.is_empty(),
        "Should find cached ranges from PUT"
    );

    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve entirely from cache"
    );

    // Merge and verify
    let merge_result = range_handler
        .merge_range_segments(&cache_key, &requested_range, &overlap.cached_ranges, &[])
        .await
        .unwrap();

    assert_eq!(merge_result.data.len(), data_size);
    assert_eq!(merge_result.data, test_data);
    assert_eq!(merge_result.bytes_from_s3, 0);
    assert_eq!(merge_result.cache_efficiency, 100.0);

    println!("[TEST] ✓ Large object test passed");
}

/// Test that has_cached_ranges optimization works correctly
///
/// This test verifies Requirement 2.2:
/// "WHEN object size is known THEN the Proxy SHALL check if any ranges are cached for that object"
///
/// The optimization should:
/// 1. Check for cached object metadata BEFORE doing HEAD request to S3
/// 2. Return content_length from cached metadata if available
/// 3. Avoid unnecessary HEAD requests when we already have the data
#[tokio::test]
async fn test_has_cached_ranges_optimization() {
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager().await;
    cache_manager.initialize().await.unwrap();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/egummett-testing-source-1/s3-transparent-proxy-testing/test-optimization.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Step 1: Store data via PUT (simulating a PUT operation)
    let test_data = b"Test data for optimization check";
    let data_size = test_data.len() as u64;

    let headers = HashMap::from([
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("etag".to_string(), "test-etag-opt-123".to_string()),
        (
            "last-modified".to_string(),
            "2024-01-01T00:00:00Z".to_string(),
        ),
    ]);

    let metadata = CacheMetadata {
        etag: "test-etag-opt-123".to_string(),
        last_modified: "2024-01-01T00:00:00Z".to_string(),
        content_length: data_size,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    println!("[TEST] Step 1: Storing PUT data");
    cache_manager
        .store_write_cache_entry(&cache_key, test_data, headers, metadata)
        .await
        .unwrap();

    // Step 2: Call has_cached_ranges to check if optimization works
    println!("[TEST] Step 2: Checking has_cached_ranges");
    let result = cache_manager.has_cached_ranges(&cache_key, None).await.unwrap();

    // Verify the result
    assert!(result.is_some(), "Should find cached metadata");

    let (has_ranges, content_length) = result.unwrap();
    println!(
        "[TEST] has_cached_ranges result: has_ranges={}, content_length={}",
        has_ranges, content_length
    );

    assert!(has_ranges, "Should indicate that ranges are cached");
    assert_eq!(
        content_length, data_size,
        "Should return correct content length"
    );

    // Step 3: Verify that we can use this information to skip HEAD request
    // In the actual HTTP flow, this would mean:
    // - No HEAD request to S3 needed
    // - Can immediately convert to range request
    // - Can call find_cached_ranges with the known size
    println!("[TEST] Step 3: Verifying we have all info needed to skip HEAD request");

    let requested_range = RangeSpec {
        start: 0,
        end: content_length - 1,
    };

    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve entirely from cache"
    );
    assert_eq!(
        overlap.missing_ranges.len(),
        0,
        "Should have no missing ranges"
    );

    println!(
        "[TEST] ✓ Optimization test passed: has_cached_ranges correctly identifies cached data"
    );
}

/// Test that has_cached_ranges returns None for non-existent objects
#[tokio::test]
async fn test_has_cached_ranges_no_metadata() {
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager().await;
    cache_manager.initialize().await.unwrap();
    disk_cache_manager.write().await.initialize().await.unwrap();

    // Use a valid cache key format (bucket/object) for a non-existent object
    let cache_key = CacheManager::generate_cache_key("/test-bucket/non-existent-object", None);

    println!("[TEST] Checking has_cached_ranges for non-existent object");
    let result = cache_manager.has_cached_ranges(&cache_key, None).await.unwrap();

    assert!(
        result.is_none(),
        "Should return None for non-existent object"
    );
    println!("[TEST] ✓ Correctly returns None for non-existent object");
}

/// Test that HEAD cache stores entries correctly in unified format
///
/// This test verifies that HEAD cache entries are stored and can be retrieved.
/// With the unified format, HEAD cache entries create a .meta file with HEAD fields
/// but without any cached ranges.
#[tokio::test]
async fn test_head_cache_stores_content_length_in_metadata() {
    let (cache_manager, _disk_cache_manager, _temp_dir) = create_test_cache_manager().await;
    cache_manager.initialize().await.unwrap();

    let path = "/egummett-testing-source-1/s3-transparent-proxy-testing/test-head-metadata.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Step 1: Store HEAD cache entry (simulating a HEAD request response)
    let content_length = 12345u64;
    let headers = HashMap::from([
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("content-length".to_string(), content_length.to_string()),
        ("etag".to_string(), "test-etag-head-456".to_string()),
        (
            "last-modified".to_string(),
            "2024-01-01T00:00:00Z".to_string(),
        ),
    ]);

    let metadata = CacheMetadata {
        etag: "test-etag-head-456".to_string(),
        last_modified: "2024-01-01T00:00:00Z".to_string(),
        content_length,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    println!(
        "[TEST] Step 1: Storing HEAD cache entry with content_length={}",
        content_length
    );
    cache_manager
        .store_head_cache_entry_unified(&cache_key, headers.clone(), metadata.clone())
        .await
        .unwrap();

    // Step 2: Verify HEAD cache entry can be retrieved using unified method
    println!("[TEST] Step 2: Checking if HEAD cache entry can be retrieved");
    let head_entry = cache_manager
        .get_head_cache_entry_unified(&cache_key)
        .await
        .unwrap();

    assert!(head_entry.is_some(), "Should have HEAD cache entry");
    let head_entry = head_entry.unwrap();

    assert_eq!(head_entry.metadata.content_length, content_length);
    assert_eq!(head_entry.metadata.etag, "test-etag-head-456");

    // Step 3: With unified format, HEAD cache creates a .meta file with HEAD fields
    // but without any cached ranges. Verify the metadata exists but has no ranges.
    println!("[TEST] Step 3: Verifying HEAD cache creates metadata without ranges");
    let obj_metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap();

    // With unified format, HEAD cache entries DO create metadata files
    // but they should have no cached ranges (only HEAD metadata)
    assert!(
        obj_metadata.is_some(),
        "Unified HEAD cache should create metadata file"
    );

    let obj_metadata = obj_metadata.unwrap();
    assert!(
        obj_metadata.ranges.is_empty(),
        "HEAD-only metadata should have no cached ranges"
    );
    assert_eq!(
        obj_metadata.object_metadata.content_length, content_length,
        "Metadata should have correct content_length from HEAD"
    );
    assert_eq!(
        obj_metadata.object_metadata.etag, "test-etag-head-456",
        "Metadata should have correct etag from HEAD"
    );

    println!("[TEST] ✓ HEAD cache correctly stores unified metadata without ranges");
}

/// Test that full GET response also stores content_length in object metadata
///
/// This test verifies Requirement 1.2:
/// "WHEN the Proxy caches a full object GET response THEN the Proxy SHALL store the total object size in the object metadata"
#[tokio::test]
async fn test_get_response_stores_content_length_in_metadata() {
    let (cache_manager, _disk_cache_manager, _temp_dir) = create_test_cache_manager().await;
    cache_manager.initialize().await.unwrap();

    let path = "/egummett-testing-source-1/s3-transparent-proxy-testing/test-get-metadata.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Step 1: Store full GET response (simulating a full object GET)
    let test_data = b"This is a full GET response that should be cached with metadata";
    let content_length = test_data.len() as u64;

    let headers = HashMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
        ("content-length".to_string(), content_length.to_string()),
        ("etag".to_string(), "test-etag-get-789".to_string()),
        (
            "last-modified".to_string(),
            "2024-01-01T00:00:00Z".to_string(),
        ),
    ]);

    let metadata = CacheMetadata {
        etag: "test-etag-get-789".to_string(),
        last_modified: "2024-01-01T00:00:00Z".to_string(),
        content_length,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    println!(
        "[TEST] Step 1: Storing full GET response with content_length={}",
        content_length
    );
    cache_manager
        .store_response_with_headers(&cache_key, test_data, headers, metadata)
        .await
        .unwrap();

    // Step 2: Verify that object metadata was created with content_length
    println!("[TEST] Step 2: Checking if object metadata was created");
    let result = cache_manager.has_cached_ranges(&cache_key, None).await.unwrap();

    assert!(
        result.is_some(),
        "Should have object metadata from GET response"
    );

    let (has_ranges, stored_length) = result.unwrap();
    println!(
        "[TEST] Object metadata: has_ranges={}, content_length={}",
        has_ranges, stored_length
    );

    assert_eq!(
        stored_length, content_length,
        "Should store correct content_length in object metadata"
    );

    // Step 3: Verify we can retrieve the metadata directly
    println!("[TEST] Step 3: Verifying object metadata details");
    let obj_metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap();

    assert!(obj_metadata.is_some(), "Should have object metadata");
    let obj_metadata = obj_metadata.unwrap();

    assert_eq!(obj_metadata.object_metadata.content_length, content_length);
    assert_eq!(obj_metadata.object_metadata.etag, "test-etag-get-789");
    assert_eq!(
        obj_metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::Complete
    );

    println!("[TEST] ✓ GET response correctly stores content_length in object metadata");
}
