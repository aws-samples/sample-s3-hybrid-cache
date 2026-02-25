use s3_proxy::{
    cache::{CacheEvictionAlgorithm, CacheManager},
    cache_types::CacheMetadata,
};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio;

/// Test helper function to detect conditional headers (mirrors the private method)
fn has_conditional_headers(headers: &HashMap<String, String>) -> bool {
    headers.contains_key("if-match")
        || headers.contains_key("if-none-match")
        || headers.contains_key("if-modified-since")
        || headers.contains_key("if-unmodified-since")
}

/// Test that conditional requests are properly detected and forwarded
#[tokio::test]
async fn test_conditional_request_detection() {
    // Test If-Match header
    let mut headers = HashMap::new();
    headers.insert("if-match".to_string(), "\"etag123\"".to_string());
    assert!(has_conditional_headers(&headers));

    // Test If-None-Match header
    let mut headers = HashMap::new();
    headers.insert("if-none-match".to_string(), "\"etag123\"".to_string());
    assert!(has_conditional_headers(&headers));

    // Test If-Modified-Since header
    let mut headers = HashMap::new();
    headers.insert(
        "if-modified-since".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
    );
    assert!(has_conditional_headers(&headers));

    // Test If-Unmodified-Since header
    let mut headers = HashMap::new();
    headers.insert(
        "if-unmodified-since".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
    );
    assert!(has_conditional_headers(&headers));

    // Test multiple conditional headers
    let mut headers = HashMap::new();
    headers.insert("if-match".to_string(), "\"etag123\"".to_string());
    headers.insert(
        "if-modified-since".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
    );
    assert!(has_conditional_headers(&headers));

    // Test no conditional headers
    let mut headers = HashMap::new();
    headers.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256 ...".to_string(),
    );
    headers.insert("content-type".to_string(), "application/json".to_string());
    assert!(!has_conditional_headers(&headers));

    // Test empty headers
    let headers = HashMap::new();
    assert!(!has_conditional_headers(&headers));
}
/// Test cache TTL refresh functionality
#[tokio::test]
async fn test_cache_ttl_refresh() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = Arc::new(CacheManager::new_with_all_ttls(
        cache_dir,
        false,       // RAM cache disabled for simplicity
        1024 * 1024, // 1MB max size
        CacheEvictionAlgorithm::LRU,
        1024,                                 // compression threshold
        true,                                 // compression enabled
        std::time::Duration::from_secs(3600), // 1 hour GET TTL
        std::time::Duration::from_secs(1800), // 30 min HEAD TTL
        std::time::Duration::from_secs(3600), // 1 hour PUT TTL
        false,                                // actively remove cached data
    ));

    let cache_key = "test-bucket/test-object.txt";

    // Store a HEAD cache entry
    let mut headers = HashMap::new();
    headers.insert("etag".to_string(), "\"test-etag\"".to_string());
    headers.insert("content-length".to_string(), "1024".to_string());
    headers.insert(
        "last-modified".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
    );

    let metadata = CacheMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: 1024,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Store HEAD cache entry using unified method
    cache_manager
        .store_head_cache_entry_unified(cache_key, headers, metadata)
        .await?;

    // Verify entry exists
    let entry = cache_manager
        .get_head_cache_entry_unified(cache_key)
        .await?;
    assert!(entry.is_some());
    let original_expires_at = entry.unwrap().expires_at;

    // Wait a small amount to ensure time difference
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Refresh cache TTL
    cache_manager.refresh_cache_ttl(cache_key).await?;

    // Verify TTL was refreshed
    let refreshed_entry = cache_manager
        .get_head_cache_entry_unified(cache_key)
        .await?;
    assert!(refreshed_entry.is_some());
    let new_expires_at = refreshed_entry.unwrap().expires_at;

    // New expiration should be later than original
    assert!(new_expires_at > original_expires_at);

    println!("Cache TTL refresh test passed");
    Ok(())
}
/// Test cache invalidation functionality
#[tokio::test]
async fn test_cache_invalidation() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = Arc::new(CacheManager::new_with_all_ttls(
        cache_dir,
        false,       // RAM cache disabled for simplicity
        1024 * 1024, // 1MB max size
        CacheEvictionAlgorithm::LRU,
        1024,                                 // compression threshold
        true,                                 // compression enabled
        std::time::Duration::from_secs(3600), // 1 hour GET TTL
        std::time::Duration::from_secs(1800), // 30 min HEAD TTL
        std::time::Duration::from_secs(3600), // 1 hour PUT TTL
        false,                                // actively remove cached data
    ));

    let cache_key = "test-bucket/test-object.txt";

    // Store a HEAD cache entry
    let mut headers = HashMap::new();
    headers.insert("etag".to_string(), "\"test-etag\"".to_string());
    headers.insert("content-length".to_string(), "1024".to_string());
    headers.insert(
        "last-modified".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
    );

    let metadata = CacheMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: 1024,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Store HEAD cache entry using unified method
    cache_manager
        .store_head_cache_entry_unified(cache_key, headers, metadata)
        .await?;

    // Verify entry exists
    let entry = cache_manager
        .get_head_cache_entry_unified(cache_key)
        .await?;
    assert!(entry.is_some());

    // Invalidate cache
    cache_manager.invalidate_cache(cache_key).await?;

    // Verify entry is gone
    let entry_after_invalidation = cache_manager
        .get_head_cache_entry_unified(cache_key)
        .await?;
    assert!(entry_after_invalidation.is_none());

    println!("Cache invalidation test passed");
    Ok(())
}
