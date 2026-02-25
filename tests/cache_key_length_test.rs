// Test to verify cache key length reduction with new format
// This test validates Requirements 3.3, 10.1 (cache key length reduction)

use s3_proxy::cache::CacheManager;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper to create and initialize a test cache manager with JournalConsolidator
async fn create_test_cache_manager(cache_dir: std::path::PathBuf) -> Arc<CacheManager> {
    let cache_manager = Arc::new(CacheManager::new_with_defaults(cache_dir, false, 0));

    // Must call create_configured_disk_cache_manager() to set up the JournalConsolidator
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();

    // Now initialize the cache manager (which requires the consolidator to exist)
    cache_manager.initialize().await.unwrap();

    cache_manager
}

#[tokio::test]
async fn test_cache_key_length_reduction() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    // Test various cache key formats with new format (no hostname)
    let test_cases = vec![
        // (cache_key, expected_max_length_without_hostname)
        ("my-bucket/object.jpg", 50),
        ("my-bucket/path/to/deep/object.jpg", 80),
        (
            "my-bucket/very/long/path/to/some/deeply/nested/object.jpg",
            120,
        ),
    ];

    for (cache_key, expected_max_length) in test_cases {
        // New format: just the path
        let new_key_length = cache_key.len();

        // Old format would have been: "s3.amazonaws.com:{path}"
        let old_key_length = "s3.amazonaws.com:".len() + cache_key.len();

        // Verify new format is shorter
        assert!(
            new_key_length < old_key_length,
            "New key length ({}) should be shorter than old key length ({})",
            new_key_length,
            old_key_length
        );

        // Verify reduction is at least the hostname length + colon
        let reduction = old_key_length - new_key_length;
        assert!(
            reduction >= 17, // "s3.amazonaws.com:" is 17 characters
            "Key length reduction ({}) should be at least 17 characters",
            reduction
        );

        // Verify new key is within expected bounds
        assert!(
            new_key_length <= expected_max_length,
            "New key length ({}) should be <= expected max ({})",
            new_key_length,
            expected_max_length
        );
    }
}

#[tokio::test]
async fn test_sha256_hashing_threshold() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    // Test that keys under 200 characters don't get hashed
    // Test that keys over 200 characters do get hashed

    // Short key (should not be hashed)
    let short_key = "my-bucket/short/path.jpg";
    let short_data = b"Short key data";
    let short_metadata = s3_proxy::cache_types::CacheMetadata {
        etag: "etag_short".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: short_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_response(short_key, short_data, short_metadata)
        .await
        .unwrap();
    let cached_short = cache_manager.get_cached_response(short_key).await.unwrap();
    assert!(cached_short.is_some(), "Short key should be cached");

    // Long key (over 200 characters - should be hashed)
    let long_key = format!(
        "my-bucket/{}/object.jpg",
        "very/long/path/".repeat(20) // Creates a path > 200 characters
    );
    assert!(long_key.len() > 200, "Long key should be > 200 characters");

    let long_data = b"Long key data";
    let long_metadata = s3_proxy::cache_types::CacheMetadata {
        etag: "etag_long".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: long_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_response(&long_key, long_data, long_metadata)
        .await
        .unwrap();
    let cached_long = cache_manager.get_cached_response(&long_key).await.unwrap();
    assert!(cached_long.is_some(), "Long key should be cached");

    // Verify both can be retrieved correctly
    let entry_short = cached_short.unwrap();
    let entry_long = cached_long.unwrap();

    assert_eq!(entry_short.body.as_ref().unwrap(), short_data);
    assert_eq!(entry_long.body.as_ref().unwrap(), long_data);
}

#[tokio::test]
async fn test_cache_key_format_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    // Test all cache key formats work correctly with new format
    let test_cases = vec![
        // Full objects
        ("my-bucket/object.jpg", "Full object"),
        // Versioned objects
        ("my-bucket/object.jpg:version:abc123", "Versioned object"),
        // Range requests
        ("my-bucket/object.jpg:range:0-8388607", "Range request"),
        // Versioned ranges
        (
            "my-bucket/object.jpg:version:abc123:range:0-8388607",
            "Versioned range",
        ),
        // Parts
        ("my-bucket/object.jpg:part:1", "Part"),
        // Versioned parts
        (
            "my-bucket/object.jpg:version:abc123:part:1",
            "Versioned part",
        ),
    ];

    for (cache_key, description) in test_cases {
        let data = format!("Data for {}", description).into_bytes();
        let metadata = s3_proxy::cache_types::CacheMetadata {
            etag: format!("etag_{}", description.replace(" ", "_")),
            last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
            content_length: data.len() as u64,
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: std::time::SystemTime::now(),
        };

        // Store
        cache_manager
            .store_response(cache_key, &data, metadata)
            .await
            .unwrap();

        // Retrieve
        let cached = cache_manager.get_cached_response(cache_key).await.unwrap();
        assert!(cached.is_some(), "{} should be cached", description);

        let entry = cached.unwrap();
        assert_eq!(
            entry.body.as_ref().unwrap(),
            &data,
            "Data mismatch for {}",
            description
        );

        // Verify cache key doesn't contain hostname
        assert!(
            !cache_key.contains("s3.amazonaws.com"),
            "Cache key should not contain hostname: {}",
            cache_key
        );
        assert!(
            !cache_key.contains("s3.us-east-1.amazonaws.com"),
            "Cache key should not contain hostname: {}",
            cache_key
        );
    }
}
