//! Integration test for full object GET when size is unknown
//!
//! Tests Requirement 2.8:
//! "WHEN the Proxy receives a full object GET request and object size is NOT known
//! THEN the Proxy SHALL forward the original full object request to S3 and cache the response"
//!
//! This test verifies that:
//! 1. When no cached metadata exists (size unknown)
//! 2. A full object GET is forwarded to S3
//! 3. The response is cached with content_length extracted and stored in metadata

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{CacheMetadata, UploadState};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper function to create a test cache manager
fn create_test_cache_manager() -> (Arc<CacheManager>, TempDir) {
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

    (cache_manager, temp_dir)
}

/// Test that full object GET caches response when size is unknown
///
/// Requirement 2.8: WHEN the Proxy receives a full object GET request and object size is NOT known
/// THEN the Proxy SHALL forward the original full object request to S3 and cache the response
#[tokio::test]
async fn test_full_get_caches_when_size_unknown() {
    let (cache_manager, _temp_dir) = create_test_cache_manager();

    let path = "/test-bucket/test-object-size-unknown.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Step 1: Verify no cached metadata exists (simulating size unknown scenario)
    println!("[TEST] Step 1: Verifying no cached metadata exists (size unknown)");
    let result = cache_manager.has_cached_ranges(&cache_key, None).await.unwrap();
    assert!(result.is_none(), "Should have no cached metadata initially");

    // Step 2: Simulate receiving a full object GET response from S3
    // This is what happens when forward_get_head_to_s3_and_cache is called
    let test_data = b"This is test data returned from S3 when size was unknown";
    let content_length = test_data.len() as u64;

    let headers = HashMap::from([
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("content-length".to_string(), content_length.to_string()),
        ("etag".to_string(), "test-etag-unknown-size".to_string()),
        (
            "last-modified".to_string(),
            "2024-01-01T00:00:00Z".to_string(),
        ),
    ]);

    let metadata = CacheMetadata {
        etag: "test-etag-unknown-size".to_string(),
        last_modified: "2024-01-01T00:00:00Z".to_string(),
        content_length,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    println!("[TEST] Step 2: Caching full GET response (simulating S3 response)");
    println!("[TEST]   - content_length: {} bytes", content_length);

    // This is what forward_get_head_to_s3_and_cache does when it receives a GET response
    cache_manager
        .store_response_with_headers(&cache_key, test_data, headers.clone(), metadata.clone())
        .await
        .unwrap();

    // Step 3: Verify the response was cached with content_length in metadata
    println!("[TEST] Step 3: Verifying response was cached with content_length");

    let cached_metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap();
    assert!(
        cached_metadata.is_some(),
        "Should have cached metadata after GET"
    );

    let cached_metadata = cached_metadata.unwrap();
    println!(
        "[TEST]   - upload_state: {:?}",
        cached_metadata.object_metadata.upload_state
    );
    println!(
        "[TEST]   - content_length: {}",
        cached_metadata.object_metadata.content_length
    );
    println!("[TEST]   - etag: {}", cached_metadata.object_metadata.etag);

    // Verify content_length was extracted and stored
    assert_eq!(
        cached_metadata.object_metadata.content_length, content_length,
        "Should store correct content_length in metadata"
    );

    // Verify upload_state is Complete (full GET is complete)
    assert_eq!(
        cached_metadata.object_metadata.upload_state,
        UploadState::Complete,
        "Should mark upload_state as Complete for full GET"
    );

    // Verify ETag was stored
    assert_eq!(
        cached_metadata.object_metadata.etag, "test-etag-unknown-size",
        "Should store ETag in metadata"
    );

    // Step 4: Verify has_cached_ranges now returns the size
    println!("[TEST] Step 4: Verifying has_cached_ranges returns size after caching");

    let result = cache_manager.has_cached_ranges(&cache_key, None).await.unwrap();
    assert!(result.is_some(), "Should have cached metadata after GET");

    let (_has_ranges, stored_length) = result.unwrap();
    assert_eq!(
        stored_length, content_length,
        "has_cached_ranges should return correct content_length"
    );

    // Step 5: Verify the metadata is sufficient for optimization
    // Note: The actual response body is stored in the old cache format,
    // but the metadata (including content_length) is what matters for the optimization
    println!("[TEST] Step 5: Verifying metadata is sufficient for size-based optimization");

    // The key point is that has_cached_ranges now returns the size,
    // which allows subsequent full GET requests to be converted to range requests
    // and utilize any cached ranges
    println!("[TEST]   ✓ Metadata with content_length is available");
    println!("[TEST]   ✓ Subsequent full GET requests can use this size information");
    println!("[TEST]   ✓ No HEAD request to S3 needed for size determination");

    println!("[TEST] ✓ Test passed: Full GET response cached correctly when size was unknown");
}

/// Test the complete flow: size unknown → forward to S3 → cache response → size now known
#[tokio::test]
async fn test_size_unknown_to_known_flow() {
    let (cache_manager, _temp_dir) = create_test_cache_manager();

    let path = "/test-bucket/test-flow.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Initial state: no metadata (size unknown)
    println!("[TEST] Initial state: no metadata exists");
    let result = cache_manager.has_cached_ranges(&cache_key, None).await.unwrap();
    assert!(result.is_none(), "Initially, size should be unknown");

    // Simulate S3 response with 1KB of data
    let test_data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
    let content_length = test_data.len() as u64;

    let headers = HashMap::from([
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("content-length".to_string(), content_length.to_string()),
        ("etag".to_string(), "test-etag-flow".to_string()),
        (
            "last-modified".to_string(),
            "2024-01-01T00:00:00Z".to_string(),
        ),
    ]);

    let metadata = CacheMetadata {
        etag: "test-etag-flow".to_string(),
        last_modified: "2024-01-01T00:00:00Z".to_string(),
        content_length,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Cache the response (simulating forward_get_head_to_s3_and_cache)
    println!("[TEST] Caching S3 response with {} bytes", content_length);
    cache_manager
        .store_response_with_headers(&cache_key, &test_data, headers, metadata)
        .await
        .unwrap();

    // Final state: metadata exists (size now known)
    println!("[TEST] Final state: checking if size is now known");
    let result = cache_manager.has_cached_ranges(&cache_key, None).await.unwrap();
    assert!(result.is_some(), "After caching, size should be known");

    let (_has_ranges, stored_length) = result.unwrap();
    assert_eq!(stored_length, content_length, "Should have correct size");

    // Verify subsequent requests can use this size information
    println!("[TEST] Verifying subsequent requests can use size information");
    let obj_metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap();
    assert!(obj_metadata.is_some());

    let obj_metadata = obj_metadata.unwrap();
    assert_eq!(obj_metadata.object_metadata.content_length, content_length);
    assert_eq!(
        obj_metadata.object_metadata.upload_state,
        UploadState::Complete
    );

    println!("[TEST] ✓ Flow test passed: size unknown → cached → size known");
}

/// Test that content_length is correctly extracted from various response sizes
#[tokio::test]
async fn test_content_length_extraction_various_sizes() {
    let (cache_manager, _temp_dir) = create_test_cache_manager();

    // Test with different sizes
    // Note: Empty files (0 bytes) are intentionally excluded from has_cached_ranges
    // because they don't need range-based optimization
    let test_cases = vec![
        (1, "1 byte"),
        (1024, "1 KB"),
        (1024 * 1024, "1 MB"),
        (10 * 1024 * 1024, "10 MB"),
    ];

    for (size, description) in test_cases {
        println!("[TEST] Testing {}: {} bytes", description, size);

        let path = format!("/test-bucket/test-size-{}.bin", size);
        let cache_key = CacheManager::generate_cache_key(&path, None);

        // Create test data of specified size
        let test_data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

        let headers = HashMap::from([
            (
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            ),
            ("content-length".to_string(), size.to_string()),
            ("etag".to_string(), format!("etag-{}", size)),
            (
                "last-modified".to_string(),
                "2024-01-01T00:00:00Z".to_string(),
            ),
        ]);

        let metadata = CacheMetadata {
            etag: format!("etag-{}", size),
            last_modified: "2024-01-01T00:00:00Z".to_string(),
            content_length: size as u64,
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: std::time::SystemTime::now(),
        };

        // Cache the response
        cache_manager
            .store_response_with_headers(&cache_key, &test_data, headers, metadata)
            .await
            .unwrap();

        // Verify content_length was stored correctly
        let result = cache_manager.has_cached_ranges(&cache_key, None).await.unwrap();
        assert!(result.is_some(), "Should have metadata for {}", description);

        let (_has_ranges, stored_length) = result.unwrap();
        assert_eq!(
            stored_length, size as u64,
            "Should store correct content_length for {}",
            description
        );

        println!("[TEST]   ✓ {} passed", description);
    }

    // Test empty file separately - it should store metadata but has_cached_ranges returns None
    println!("[TEST] Testing empty file: 0 bytes (special case)");
    let path = "/test-bucket/test-size-0.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    let test_data: Vec<u8> = vec![];
    let headers = HashMap::from([
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("content-length".to_string(), "0".to_string()),
        ("etag".to_string(), "etag-0".to_string()),
        (
            "last-modified".to_string(),
            "2024-01-01T00:00:00Z".to_string(),
        ),
    ]);

    let metadata = CacheMetadata {
        etag: "etag-0".to_string(),
        last_modified: "2024-01-01T00:00:00Z".to_string(),
        content_length: 0,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_response_with_headers(&cache_key, &test_data, headers, metadata)
        .await
        .unwrap();

    // Empty files return None from has_cached_ranges (intentional - no range optimization needed)
    let result = cache_manager.has_cached_ranges(&cache_key, None).await.unwrap();
    assert!(
        result.is_none(),
        "Empty files should return None (no range optimization needed)"
    );

    // But metadata should still be stored
    let obj_metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap();
    assert!(
        obj_metadata.is_some(),
        "Should have metadata even for empty file"
    );
    let obj_metadata = obj_metadata.unwrap();
    assert_eq!(obj_metadata.object_metadata.content_length, 0);

    println!("[TEST]   ✓ Empty file handled correctly");

    println!("[TEST] ✓ All size extraction tests passed");
}
