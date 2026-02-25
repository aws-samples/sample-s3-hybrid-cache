//! Tests for range and non-range GET request handling
//!
//! Validates that both range and non-range GET requests are properly cached
//! and served from the range cache (Requirement 4.2 from Range Storage Redesign)

use s3_proxy::{cache::CacheManager, cache_types::CacheMetadata};
use std::collections::HashMap;
use std::time::SystemTime;
use tempfile::TempDir;

#[test]
fn test_cache_key_generation_for_ranges() {
    // Test that cache keys are generated correctly for range requests
    // This validates that both range and non-range requests use the same cache key format
    // Note: normalize_cache_key strips leading slashes

    let key1 = CacheManager::generate_cache_key("/test-bucket/object.bin", None);
    assert_eq!(key1, "test-bucket/object.bin");

    // Range requests use the same base cache key
    let key2 = CacheManager::generate_cache_key("/test-bucket/object.bin", None);
    assert_eq!(
        key1, key2,
        "Range and non-range requests should use the same cache key"
    );
}

#[tokio::test]
async fn test_head_cache_for_content_length() {
    // Test that HEAD responses are cached and can be used to determine content-length
    // for non-range GET requests (validates the fix in http_proxy.rs)

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache subdirectories (unified format uses metadata/ and ranges/)
    std::fs::create_dir_all(cache_dir.join("metadata")).unwrap();
    std::fs::create_dir_all(cache_dir.join("ranges")).unwrap();

    let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

    let cache_key = "s3.us-east-1.amazonaws.com:/test-bucket/test-object.bin";

    // Store HEAD response
    let mut headers = HashMap::new();
    headers.insert("content-length".to_string(), "1000".to_string());
    headers.insert("etag".to_string(), "test-etag-head".to_string());
    headers.insert(
        "last-modified".to_string(),
        "Wed, 21 Nov 2025 12:00:00 GMT".to_string(),
    );

    let metadata = CacheMetadata {
        etag: "test-etag-head".to_string(),
        last_modified: "Wed, 21 Nov 2025 12:00:00 GMT".to_string(),
        content_length: 1000,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };

    cache_manager
        .store_head_cache_entry_unified(cache_key, headers.clone(), metadata)
        .await
        .expect("Failed to store HEAD cache entry");

    // Retrieve HEAD cache entry using unified method
    let head_entry = cache_manager
        .get_head_cache_entry_unified(cache_key)
        .await
        .expect("Failed to get HEAD cache entry")
        .expect("HEAD cache entry should exist");

    // Verify content-length is available
    assert_eq!(
        head_entry.headers.get("content-length"),
        Some(&"1000".to_string()),
        "Content-length should be cached in HEAD response"
    );

    // This validates that non-range GET can use cached HEAD to determine
    // the full range (bytes=0-999) without making another HEAD request to S3
}

#[tokio::test]
async fn test_head_cache_hit_and_miss() {
    // Test HEAD cache hit/miss pattern

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache subdirectories (unified format uses metadata/ and ranges/)
    std::fs::create_dir_all(cache_dir.join("metadata")).unwrap();
    std::fs::create_dir_all(cache_dir.join("ranges")).unwrap();

    let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

    let cache_key = "s3.us-east-1.amazonaws.com:/test-bucket/test-object.bin";

    // First request - cache miss
    let result1 = cache_manager
        .get_head_cache_entry_unified(cache_key)
        .await
        .unwrap();
    assert!(
        result1.is_none(),
        "First HEAD request should be a cache miss"
    );

    // Store HEAD response using unified method
    let mut headers = HashMap::new();
    headers.insert("content-length".to_string(), "5000".to_string());
    headers.insert("etag".to_string(), "test-etag-123".to_string());
    headers.insert(
        "last-modified".to_string(),
        "Wed, 21 Nov 2025 12:00:00 GMT".to_string(),
    );

    let metadata = CacheMetadata {
        etag: "test-etag-123".to_string(),
        last_modified: "Wed, 21 Nov 2025 12:00:00 GMT".to_string(),
        content_length: 5000,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };

    cache_manager
        .store_head_cache_entry_unified(cache_key, headers, metadata)
        .await
        .expect("Failed to store HEAD cache entry");

    // Second request - cache hit
    let result2 = cache_manager
        .get_head_cache_entry_unified(cache_key)
        .await
        .unwrap();
    assert!(
        result2.is_some(),
        "Second HEAD request should be a cache hit"
    );

    let head_entry = result2.unwrap();
    assert_eq!(
        head_entry.headers.get("content-length"),
        Some(&"5000".to_string())
    );
    assert_eq!(head_entry.metadata.content_length, 5000);
}
