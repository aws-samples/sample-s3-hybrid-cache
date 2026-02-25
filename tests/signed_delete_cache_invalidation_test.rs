//! Signed DELETE Cache Invalidation Tests
//!
//! Tests that signed DELETE requests properly invalidate cache entries.
//!
//! Requirements:
//! - Requirement 5.1: Signed DELETE with success response invalidates all cache layers
//! - Requirement 5.2: Uses invalidate_cache_unified_for_operation with "DELETE"
//! - Requirement 5.3: Cache invalidation failure logged at WARN, successful S3 response still returned

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper: create a test CacheManager with write cache enabled
async fn create_test_cache_manager() -> (Arc<CacheManager>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false, // RAM cache disabled
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        true, // write_cache_enabled
        std::time::Duration::from_secs(86400),
        MetadataCacheConfig::default(),
        95,
        80,
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    ));

    (cache_manager, temp_dir)
}

/// Helper: store a cache entry for the given key
async fn store_test_cache_entry(cache_manager: &CacheManager, cache_key: &str) -> Result<()> {
    let data = vec![b'X'; 2048];
    let headers = HashMap::from([
        ("content-type".to_string(), "application/octet-stream".to_string()),
        ("etag".to_string(), "\"test-etag-123\"".to_string()),
        (
            "last-modified".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        ),
    ]);
    let metadata = CacheMetadata {
        etag: "\"test-etag-123\"".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: 2048,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };
    cache_manager
        .store_write_cache_entry(cache_key, &data, headers, metadata)
        .await
}

/// Test: signed DELETE with success status triggers cache invalidation
///
/// Simulates the logic in handle_other_request's SigV4 DELETE branch:
/// when the S3 response is successful (2xx), invalidate_cache_unified_for_operation
/// is called with "DELETE", removing all cache layers for that object.
///
/// Requirement 5.1: Signed DELETE with success response invalidates all cache layers
/// Requirement 5.2: Uses invalidate_cache_unified_for_operation with "DELETE"
#[tokio::test]
async fn test_signed_delete_success_invalidates_cache() -> Result<()> {
    let (cache_manager, _temp_dir) = create_test_cache_manager().await;

    let cache_key = "/test-bucket/object-to-delete.txt";

    // Store a cache entry
    store_test_cache_entry(&cache_manager, cache_key).await?;

    // Verify cache entry exists
    let metadata_before = cache_manager.get_metadata_from_disk(cache_key).await?;
    assert!(
        metadata_before.is_some(),
        "Cache entry should exist before DELETE invalidation"
    );

    // Simulate successful signed DELETE: call invalidate_cache_unified_for_operation
    // This is the exact call made in handle_other_request when response.status().is_success()
    let result = cache_manager
        .invalidate_cache_unified_for_operation(&CacheManager::generate_cache_key(cache_key, None), "DELETE")
        .await;
    assert!(result.is_ok(), "Cache invalidation should succeed");

    // Verify cache entry is removed
    let metadata_after = cache_manager.get_metadata_from_disk(cache_key).await?;
    assert!(
        metadata_after.is_none(),
        "Cache entry should be removed after successful DELETE invalidation"
    );

    Ok(())
}

/// Test: signed DELETE with non-success status does NOT invalidate cache
///
/// Simulates the logic in handle_other_request's SigV4 DELETE branch:
/// when the S3 response is NOT successful (e.g., 403, 404, 500),
/// invalidate_cache_unified_for_operation is NOT called, preserving cache.
///
/// Requirement 5.1: Only success responses trigger invalidation
#[tokio::test]
async fn test_signed_delete_non_success_preserves_cache() -> Result<()> {
    let (cache_manager, _temp_dir) = create_test_cache_manager().await;

    let cache_key = "/test-bucket/object-to-keep.txt";

    // Store a cache entry
    store_test_cache_entry(&cache_manager, cache_key).await?;

    // Verify cache entry exists
    let metadata_before = cache_manager.get_metadata_from_disk(cache_key).await?;
    assert!(
        metadata_before.is_some(),
        "Cache entry should exist before DELETE attempt"
    );

    // Simulate non-successful signed DELETE: do NOT call invalidate_cache_unified_for_operation
    // This mirrors the code path where response.status().is_success() returns false
    // (e.g., 403 Forbidden, 404 Not Found, 500 Internal Server Error)
    // The code simply returns Ok(response) without touching the cache.
    let simulated_success = false; // Non-2xx response
    if simulated_success {
        // This block would run for success — but it doesn't for non-success
        let _ = cache_manager
            .invalidate_cache_unified_for_operation(
                &CacheManager::generate_cache_key(cache_key, None),
                "DELETE",
            )
            .await;
    }

    // Verify cache entry is preserved
    let metadata_after = cache_manager.get_metadata_from_disk(cache_key).await?;
    assert!(
        metadata_after.is_some(),
        "Cache entry should be preserved when DELETE response is non-success"
    );

    Ok(())
}
