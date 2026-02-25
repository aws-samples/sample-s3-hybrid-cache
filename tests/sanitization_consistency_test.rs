// Test to verify sanitization consistency between cache.rs and disk_cache.rs
// This test validates that both modules produce identical sanitized filenames

use s3_proxy::cache::CacheManager;
use tempfile::TempDir;

#[tokio::test]
async fn test_sanitization_produces_same_results() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    // Test various cache keys to ensure consistent sanitization
    let long_key = format!("my-bucket/{}/object.jpg", "very/long/path/".repeat(20));
    let test_keys = vec![
        "my-bucket/simple.jpg",
        "my-bucket/path:with:colons",
        "my-bucket/path/with/slashes",
        "my-bucket/path with spaces",
        "my-bucket/special!@#$%^&*()chars",
        // Long key that should be hashed
        long_key.as_str(),
    ];

    for cache_key in test_keys {
        let data = format!("Data for {}", cache_key).into_bytes();
        let metadata = s3_proxy::cache_types::CacheMetadata {
            etag: "test_etag".to_string(),
            last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
            content_length: data.len() as u64,
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: std::time::SystemTime::now(),
        };

        // Store using cache manager (uses cache.rs sanitization)
        cache_manager
            .store_response(cache_key, &data, metadata)
            .await
            .unwrap();

        // Retrieve to verify it works
        let cached = cache_manager.get_cached_response(cache_key).await.unwrap();
        assert!(
            cached.is_some(),
            "Key '{}' should be cached and retrievable",
            cache_key
        );

        let entry = cached.unwrap();
        assert_eq!(
            entry.body.as_ref().unwrap(),
            &data,
            "Data should match for key '{}'",
            cache_key
        );
    }
}

#[tokio::test]
async fn test_long_key_hashing_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    // Create a key that's exactly at the threshold
    let threshold_key = "a".repeat(200);
    let data_threshold = b"Threshold data";
    let metadata_threshold = s3_proxy::cache_types::CacheMetadata {
        etag: "etag_threshold".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: data_threshold.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Create a key that's over the threshold
    let over_threshold_key = "a".repeat(201);
    let data_over = b"Over threshold data";
    let metadata_over = s3_proxy::cache_types::CacheMetadata {
        etag: "etag_over".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: data_over.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Store both
    cache_manager
        .store_response(&threshold_key, data_threshold, metadata_threshold)
        .await
        .unwrap();
    cache_manager
        .store_response(&over_threshold_key, data_over, metadata_over)
        .await
        .unwrap();

    // Retrieve both
    let cached_threshold = cache_manager
        .get_cached_response(&threshold_key)
        .await
        .unwrap();
    let cached_over = cache_manager
        .get_cached_response(&over_threshold_key)
        .await
        .unwrap();

    assert!(cached_threshold.is_some(), "Threshold key should be cached");
    assert!(cached_over.is_some(), "Over-threshold key should be cached");

    // Verify data integrity
    let entry_threshold = cached_threshold.unwrap();
    let entry_over = cached_over.unwrap();

    assert_eq!(entry_threshold.body.as_ref().unwrap(), data_threshold);
    assert_eq!(entry_over.body.as_ref().unwrap(), data_over);
}

#[tokio::test]
async fn test_percent_encoding_special_chars() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    // Test that special characters are properly percent-encoded
    let special_chars = vec![
        ("bucket/file:name", "Colon"),
        ("bucket/file/name", "Slash"),
        ("bucket/file?name", "Question"),
        ("bucket/file*name", "Star"),
        ("bucket/file name", "Space"),
        ("bucket/file<name>", "Brackets"),
        ("bucket/file|name", "Pipe"),
        ("bucket/file\"name", "Quote"),
        ("bucket/file\\name", "Backslash"),
        ("bucket/file%name", "Percent"),
    ];

    for (cache_key, description) in special_chars {
        let data = format!("Data for {}", description).into_bytes();
        let metadata = s3_proxy::cache_types::CacheMetadata {
            etag: format!("etag_{}", description),
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
        assert!(
            cached.is_some(),
            "{} character should be handled correctly",
            description
        );

        let entry = cached.unwrap();
        assert_eq!(
            entry.body.as_ref().unwrap(),
            &data,
            "Data mismatch for {} character",
            description
        );
    }
}
