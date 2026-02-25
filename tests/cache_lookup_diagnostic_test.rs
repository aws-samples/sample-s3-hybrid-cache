use s3_proxy::cache_types::ObjectMetadata;
use s3_proxy::disk_cache::DiskCacheManager;
use std::time::Duration;
use tempfile::TempDir;

/// Diagnostic integration test for cache lookup
///
/// This test reproduces the cache lookup bug where find_cached_ranges returns 0 cached ranges
/// even though the metadata file exists with the correct cache key, contains valid ranges,
/// the ranges are not expired, and the range binary files exist on disk.
///
/// Requirements: 4.1, 4.2, 4.3, 4.4, 4.5
#[tokio::test]
async fn test_cache_lookup_diagnostic_basic() {
    // Initialize test environment with debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(
        temp_dir.path().to_path_buf(),
        true,  // compression enabled
        1024,  // compression threshold
        false, // write cache disabled
    );

    // Initialize cache directory structure
    cache_manager.initialize().await.unwrap();

    // Test data
    let cache_key = "test-bucket/path/to/test-object.txt";
    let test_data = b"This is test data for cache lookup diagnostic test. It should be found when we look it up.";
    let range_start = 0u64;
    let range_end = (test_data.len() - 1) as u64;

    let object_metadata = ObjectMetadata::new(
        "test-etag-12345".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        test_data.len() as u64,
        Some("text/plain".to_string()),
    );

    // Requirement 4.1: Store a range using store_range with a known cache key
    println!("\n=== STEP 1: Storing range ===");
    println!("Cache key: {}", cache_key);
    println!("Range: {}-{}", range_start, range_end);
    println!("Data size: {} bytes", test_data.len());

    cache_manager
        .store_range(
            cache_key,
            range_start,
            range_end,
            test_data,
            object_metadata.clone(),
            Duration::from_secs(3600), // 1 hour TTL
            true)
        .await
        .unwrap();

    println!("✓ Range stored successfully");

    // Requirement 4.2: Verify the metadata file exists at the expected path
    println!("\n=== STEP 2: Verifying metadata file ===");
    let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
    println!("Expected metadata path: {:?}", metadata_path);
    assert!(
        metadata_path.exists(),
        "Metadata file should exist at {:?}",
        metadata_path
    );
    println!("✓ Metadata file exists");

    // Requirement 4.3: Verify the range binary file exists at the expected path
    println!("\n=== STEP 3: Verifying range binary file ===");
    let range_path = cache_manager.get_new_range_file_path(cache_key, range_start, range_end);
    println!("Expected range path: {:?}", range_path);
    assert!(
        range_path.exists(),
        "Range binary file should exist at {:?}",
        range_path
    );
    println!("✓ Range binary file exists");

    // Verify sharded path structure
    println!("\n=== STEP 4: Verifying sharded path structure ===");
    let metadata_path_str = metadata_path.to_string_lossy();
    let range_path_str = range_path.to_string_lossy();

    // Extract bucket name
    let bucket = cache_key.split('/').next().unwrap();
    println!("Bucket: {}", bucket);

    // Verify bucket appears in paths
    assert!(
        metadata_path_str.contains(bucket),
        "Metadata path should contain bucket '{}': {:?}",
        bucket,
        metadata_path
    );
    assert!(
        range_path_str.contains(bucket),
        "Range path should contain bucket '{}': {:?}",
        bucket,
        range_path
    );
    println!("✓ Bucket appears in paths");

    // Requirement 4.4: Call find_cached_ranges with the same cache key
    println!("\n=== STEP 5: Looking up cached ranges ===");
    println!("Calling find_cached_ranges with:");
    println!("  cache_key: {}", cache_key);
    println!("  requested_range: {}-{}", range_start, range_end);

    let found_ranges = cache_manager
        .find_cached_ranges(cache_key, range_start, range_end, None)
        .await
        .unwrap();

    println!("Found {} cached range(s)", found_ranges.len());
    for (i, range) in found_ranges.iter().enumerate() {
        println!("  Range {}: {}-{}", i, range.start, range.end);
    }

    // Requirement 4.4: Assert that the stored range is found and returned
    assert!(
        !found_ranges.is_empty(),
        "find_cached_ranges should return at least one range, but returned 0. \
        This indicates a cache lookup bug where the metadata file exists but ranges are not found."
    );

    assert_eq!(
        found_ranges.len(),
        1,
        "Should find exactly one cached range"
    );

    let found_range = &found_ranges[0];
    assert_eq!(
        found_range.start, range_start,
        "Found range start should match stored range start"
    );
    assert_eq!(
        found_range.end, range_end,
        "Found range end should match stored range end"
    );

    println!("✓ Cached range found successfully");

    // Verify we can load the range data
    println!("\n=== STEP 6: Loading range data ===");
    let loaded_data = cache_manager.load_range_data(found_range).await.unwrap();
    assert_eq!(
        loaded_data.len(),
        test_data.len(),
        "Loaded data size should match original data size"
    );
    assert_eq!(
        &loaded_data[..],
        &test_data[..],
        "Loaded data should match original data"
    );
    println!("✓ Range data loaded and verified");

    println!("\n=== TEST PASSED ===");
    println!("All cache lookup operations completed successfully!");
}

/// Test cache lookup with various cache key formats
///
/// Tests with/without leading slash, special characters, and long keys
/// Requirements: 4.5
#[tokio::test]
async fn test_cache_lookup_diagnostic_key_formats() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

    cache_manager.initialize().await.unwrap();

    let test_data = b"Test data for key format variations";
    let range_start = 0u64;
    let range_end = (test_data.len() - 1) as u64;

    // Test cases with different cache key formats
    let test_cases = vec![
        // Without leading slash
        (
            "bucket1/simple/path.txt",
            "Simple path without leading slash",
        ),
        // With leading slash (should be normalized)
        ("/bucket2/with/leading/slash.txt", "Path with leading slash"),
        // Special characters that need encoding
        ("bucket3/path with spaces.txt", "Path with spaces"),
        ("bucket4/path:with:colons.txt", "Path with colons"),
        // Long path
        (
            "bucket5/very/long/path/that/has/many/segments/to/test/sharding/behavior.txt",
            "Long path",
        ),
    ];

    for (cache_key, description) in test_cases {
        println!("\n=== Testing: {} ===", description);
        println!("Cache key: {}", cache_key);

        let object_metadata = ObjectMetadata::new(
            format!("etag-{}", cache_key),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            test_data.len() as u64,
            Some("text/plain".to_string()),
        );

        // Store range
        cache_manager
            .store_range(
                cache_key,
                range_start,
                range_end,
                test_data,
                object_metadata.clone(),
                Duration::from_secs(3600), true)
            .await
            .unwrap();

        // Verify files exist
        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        let range_path = cache_manager.get_new_range_file_path(cache_key, range_start, range_end);

        assert!(
            metadata_path.exists(),
            "Metadata file should exist for key: {}",
            cache_key
        );
        assert!(
            range_path.exists(),
            "Range file should exist for key: {}",
            cache_key
        );

        // Find cached ranges
        let found_ranges = cache_manager
            .find_cached_ranges(cache_key, range_start, range_end, None)
            .await
            .unwrap();

        assert!(
            !found_ranges.is_empty(),
            "Should find cached range for key: {}",
            cache_key
        );

        assert_eq!(
            found_ranges[0].start, range_start,
            "Range start should match for key: {}",
            cache_key
        );
        assert_eq!(
            found_ranges[0].end, range_end,
            "Range end should match for key: {}",
            cache_key
        );

        println!("✓ Cache lookup successful for: {}", description);
    }

    println!("\n=== ALL KEY FORMAT TESTS PASSED ===");
}

/// Test cache lookup with multiple ranges
///
/// Verifies that multiple non-overlapping ranges can be stored and retrieved
#[tokio::test]
async fn test_cache_lookup_diagnostic_multiple_ranges() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/multi-range-object.bin";

    // Store multiple non-overlapping ranges
    // Create data that matches the range sizes
    let data1 = vec![b'A'; 100]; // 100 bytes for range 0-99
    let data2 = vec![b'B'; 100]; // 100 bytes for range 200-299
    let data3 = vec![b'C'; 100]; // 100 bytes for range 400-499

    let ranges = vec![
        (0u64, 99u64, data1.as_slice()),
        (200u64, 299u64, data2.as_slice()),
        (400u64, 499u64, data3.as_slice()),
    ];

    println!("\n=== Storing multiple ranges ===");
    for (start, end, data) in &ranges {
        println!("Storing range: {}-{}", start, end);

        let object_metadata = ObjectMetadata::new(
            "multi-range-etag".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            500, // Total object size
            Some("application/octet-stream".to_string()),
        );

        cache_manager
            .store_range(
                cache_key,
                *start,
                *end,
                data,
                object_metadata,
                Duration::from_secs(3600), true)
            .await
            .unwrap();
    }

    println!("✓ All ranges stored");

    // Test finding each range individually
    println!("\n=== Finding individual ranges ===");
    for (start, end, _) in &ranges {
        println!("Looking up range: {}-{}", start, end);

        let found_ranges = cache_manager
            .find_cached_ranges(cache_key, *start, *end, None)
            .await
            .unwrap();

        assert!(
            !found_ranges.is_empty(),
            "Should find cached range {}-{}",
            start,
            end
        );

        // Should find exact match
        assert_eq!(found_ranges.len(), 1, "Should find exactly one range");
        assert_eq!(found_ranges[0].start, *start);
        assert_eq!(found_ranges[0].end, *end);

        println!("✓ Found range: {}-{}", start, end);
    }

    // Test finding a range that spans multiple cached ranges
    println!("\n=== Finding range spanning multiple cached ranges ===");
    let found_ranges = cache_manager
        .find_cached_ranges(cache_key, 0, 499, None)
        .await
        .unwrap();

    println!("Found {} overlapping ranges", found_ranges.len());
    assert_eq!(
        found_ranges.len(),
        3,
        "Should find all three cached ranges when requesting full span"
    );

    println!("✓ Multiple range lookup successful");

    println!("\n=== MULTIPLE RANGE TEST PASSED ===");
}

/// Test cache lookup with partial overlap
///
/// Verifies that partially overlapping ranges are correctly identified
#[tokio::test]
async fn test_cache_lookup_diagnostic_partial_overlap() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/partial-overlap-object.bin";
    let cached_data = vec![0u8; 1000]; // 1000 bytes

    println!("\n=== Storing cached range ===");
    let object_metadata = ObjectMetadata::new(
        "partial-overlap-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        2000, // Total object size
        Some("application/octet-stream".to_string()),
    );

    // Store range 0-999
    cache_manager
        .store_range(
            cache_key,
            0,
            999,
            &cached_data,
            object_metadata.clone(),
            Duration::from_secs(3600), true)
        .await
        .unwrap();

    println!("✓ Stored range: 0-999");

    // Test cases for partial overlap
    let test_cases = vec![
        (0, 499, true, "Start of cached range"),
        (500, 999, true, "End of cached range"),
        (0, 999, true, "Exact match"),
        (100, 200, true, "Fully contained"),
        (900, 1100, true, "Partial overlap at end"),
        (1000, 1999, false, "No overlap (after cached range)"),
    ];

    println!("\n=== Testing partial overlap scenarios ===");
    for (start, end, should_find, description) in test_cases {
        println!("\nTest: {}", description);
        println!("Requesting range: {}-{}", start, end);

        let found_ranges = cache_manager
            .find_cached_ranges(cache_key, start, end, None)
            .await
            .unwrap();

        if should_find {
            assert!(
                !found_ranges.is_empty(),
                "Should find overlap for range {}-{} ({})",
                start,
                end,
                description
            );
            println!("✓ Found {} overlapping range(s)", found_ranges.len());
        } else {
            assert!(
                found_ranges.is_empty(),
                "Should not find overlap for range {}-{} ({})",
                start,
                end,
                description
            );
            println!("✓ Correctly found no overlap");
        }
    }

    println!("\n=== PARTIAL OVERLAP TEST PASSED ===");
}

/// Test cache lookup with expired ranges
///
/// Verifies that expired ranges are not returned by find_cached_ranges
#[tokio::test]
async fn test_cache_lookup_diagnostic_expired_ranges() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/expired-range-object.bin";
    let test_data = b"This range will expire immediately";

    println!("\n=== Storing range with very short TTL ===");
    let object_metadata = ObjectMetadata::new(
        "expired-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        test_data.len() as u64,
        Some("text/plain".to_string()),
    );

    // Store range with 1 millisecond TTL
    cache_manager
        .store_range(
            cache_key,
            0,
            (test_data.len() - 1) as u64,
            test_data,
            object_metadata,
            Duration::from_millis(1), // Very short TTL
            true)
        .await
        .unwrap();

    println!("✓ Range stored with 1ms TTL");

    // Wait for expiration
    println!("\n=== Waiting for range to expire ===");
    tokio::time::sleep(Duration::from_millis(10)).await;
    println!("✓ Waited 10ms");

    // Try to find the expired range
    println!("\n=== Looking up expired range ===");
    let found_ranges = cache_manager
        .find_cached_ranges(cache_key, 0, (test_data.len() - 1) as u64, None)
        .await
        .unwrap();

    assert!(
        found_ranges.is_empty(),
        "Should not find expired range, but found {} range(s)",
        found_ranges.len()
    );

    println!("✓ Expired range correctly not returned");

    println!("\n=== EXPIRED RANGE TEST PASSED ===");
}
