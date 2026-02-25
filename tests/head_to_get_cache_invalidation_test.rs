//! HEAD to GET Cache Invalidation Test
//!
//! Tests that cached GET data is automatically invalidated when HEAD metadata
//! indicates the object has changed (different ETag or Last-Modified).
//!
//! Requirements: 7.1, 7.2, 7.3, 7.4, 7.5

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{CacheMetadata, ObjectMetadata};
use std::collections::HashMap;
use tempfile::TempDir;

/// Test that GET cache is invalidated when HEAD cache has different ETag
/// Requirements: 7.1, 7.3, 7.4, 7.5
#[tokio::test]
async fn test_get_cache_invalidated_by_head_etag_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

    let cache_key = "test-bucket/test-object";

    // Step 1: Store GET cache with old ETag
    let old_etag = "\"old-etag-123\"";
    let old_last_modified = "Wed, 21 Oct 2015 07:28:00 GMT";

    let mut disk_cache_manager =
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir.clone(), true, 1024, false);
    disk_cache_manager.initialize().await.unwrap();

    // Store a range with old metadata
    let range_data = b"test data for range";
    let object_metadata = ObjectMetadata {
        etag: old_etag.to_string(),
        last_modified: old_last_modified.to_string(),
        content_length: range_data.len() as u64,
        content_type: Some("text/plain".to_string()),
        ..Default::default()
    };

    let ttl = std::time::Duration::from_secs(3600);
    disk_cache_manager
        .store_range(
            cache_key,
            0,
            range_data.len() as u64 - 1,
            range_data,
            object_metadata.clone(),
            ttl, true)
        .await
        .unwrap();

    // Verify GET cache exists
    let metadata = disk_cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(metadata.is_some(), "GET cache should exist");
    let metadata = metadata.unwrap();
    assert_eq!(metadata.object_metadata.etag, old_etag);
    assert_eq!(metadata.ranges.len(), 1);

    // Step 2: Simulate receiving HEAD response with new ETag (from S3)
    // Note: We don't call store_head_cache_entry here because that would update
    // the GET cache metadata. Instead, we directly call validate_cache_against_head
    // to simulate the scenario where we receive fresh HEAD metadata from S3.
    let new_etag = "\"new-etag-456\"";
    let new_last_modified = "Thu, 22 Oct 2015 08:30:00 GMT";

    // Step 3: Validate GET cache against HEAD metadata from S3
    // This should detect the ETag mismatch and invalidate the cache
    let invalidated = disk_cache_manager
        .validate_cache_against_head(cache_key, new_etag, new_last_modified)
        .await
        .unwrap();

    assert!(
        invalidated,
        "GET cache should be invalidated due to ETag mismatch"
    );

    // Step 4: Verify GET cache was deleted
    let metadata_after = disk_cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(
        metadata_after.is_none(),
        "GET cache should be deleted after invalidation"
    );
}

/// Test that GET cache is invalidated when HEAD cache has different Last-Modified
/// Requirements: 7.2, 7.3, 7.4, 7.5
#[tokio::test]
async fn test_get_cache_invalidated_by_head_last_modified_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

    let cache_key = "test-bucket/test-object-2";

    // Step 1: Store GET cache with old Last-Modified
    let etag = "\"same-etag-789\"";
    let old_last_modified = "Wed, 21 Oct 2015 07:28:00 GMT";

    let mut disk_cache_manager =
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir.clone(), true, 1024, false);
    disk_cache_manager.initialize().await.unwrap();

    // Store a range with old metadata
    let range_data = b"test data for range 2";
    let object_metadata = ObjectMetadata {
        etag: etag.to_string(),
        last_modified: old_last_modified.to_string(),
        content_length: range_data.len() as u64,
        content_type: Some("text/plain".to_string()),
        ..Default::default()
    };

    let ttl = std::time::Duration::from_secs(3600);
    disk_cache_manager
        .store_range(
            cache_key,
            0,
            range_data.len() as u64 - 1,
            range_data,
            object_metadata.clone(),
            ttl, true)
        .await
        .unwrap();

    // Verify GET cache exists
    let metadata = disk_cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(metadata.is_some(), "GET cache should exist");
    let metadata = metadata.unwrap();
    assert_eq!(metadata.object_metadata.last_modified, old_last_modified);

    // Step 2: Simulate receiving HEAD response with new Last-Modified (same ETag)
    // Note: We don't call store_head_cache_entry here because that would update
    // the GET cache metadata. Instead, we directly call validate_cache_against_head
    // to simulate the scenario where we receive fresh HEAD metadata from S3.
    let new_last_modified = "Thu, 22 Oct 2015 08:30:00 GMT";

    // Step 3: Validate GET cache against HEAD metadata from S3
    // This should detect the Last-Modified mismatch and invalidate the cache
    let invalidated = disk_cache_manager
        .validate_cache_against_head(cache_key, etag, new_last_modified)
        .await
        .unwrap();

    assert!(
        invalidated,
        "GET cache should be invalidated due to Last-Modified mismatch"
    );

    // Step 4: Verify GET cache was deleted
    let metadata_after = disk_cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(
        metadata_after.is_none(),
        "GET cache should be deleted after invalidation"
    );
}

/// Test that GET cache is NOT invalidated when HEAD metadata matches
/// Requirements: 7.1, 7.2
#[tokio::test]
async fn test_get_cache_not_invalidated_when_head_matches() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

    let cache_key = "test-bucket/test-object-3";

    // Step 1: Store GET cache
    let etag = "\"matching-etag-999\"";
    let last_modified = "Wed, 21 Oct 2015 07:28:00 GMT";

    let mut disk_cache_manager =
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir.clone(), true, 1024, false);
    disk_cache_manager.initialize().await.unwrap();

    // Store a range
    let range_data = b"test data for range 3";
    let object_metadata = ObjectMetadata {
        etag: etag.to_string(),
        last_modified: last_modified.to_string(),
        content_length: range_data.len() as u64,
        content_type: Some("text/plain".to_string()),
        ..Default::default()
    };

    let ttl = std::time::Duration::from_secs(3600);
    disk_cache_manager
        .store_range(
            cache_key,
            0,
            range_data.len() as u64 - 1,
            range_data,
            object_metadata.clone(),
            ttl, true)
        .await
        .unwrap();

    // Step 2: Store HEAD cache with matching metadata using unified method
    let head_headers = HashMap::from([
        ("etag".to_string(), etag.to_string()),
        ("last-modified".to_string(), last_modified.to_string()),
        ("content-length".to_string(), range_data.len().to_string()),
    ]);

    let head_metadata = CacheMetadata {
        etag: etag.to_string(),
        last_modified: last_modified.to_string(),
        content_length: range_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_head_cache_entry_unified(cache_key, head_headers, head_metadata)
        .await
        .unwrap();

    // Step 3: Validate GET cache against HEAD cache
    let invalidated = disk_cache_manager
        .validate_cache_against_head(cache_key, etag, last_modified)
        .await
        .unwrap();

    assert!(
        !invalidated,
        "GET cache should NOT be invalidated when metadata matches"
    );

    // Step 4: Verify GET cache still exists
    let metadata_after = disk_cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(metadata_after.is_some(), "GET cache should still exist");
    assert_eq!(metadata_after.unwrap().object_metadata.etag, etag);
}

/// Test that validation handles missing GET cache gracefully
/// Requirements: 7.1, 7.2
#[tokio::test]
async fn test_validation_with_no_get_cache() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

    let cache_key = "test-bucket/test-object-4";

    let disk_cache_manager =
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir.clone(), true, 1024, false);
    disk_cache_manager.initialize().await.unwrap();

    // No GET cache stored, only HEAD cache using unified method
    let etag = "\"head-only-etag\"";
    let last_modified = "Wed, 21 Oct 2015 07:28:00 GMT";

    let head_headers = HashMap::from([
        ("etag".to_string(), etag.to_string()),
        ("last-modified".to_string(), last_modified.to_string()),
        ("content-length".to_string(), "1024".to_string()),
    ]);

    let head_metadata = CacheMetadata {
        etag: etag.to_string(),
        last_modified: last_modified.to_string(),
        content_length: 1024,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_head_cache_entry_unified(cache_key, head_headers, head_metadata)
        .await
        .unwrap();

    // Validate against non-existent GET cache
    let invalidated = disk_cache_manager
        .validate_cache_against_head(cache_key, etag, last_modified)
        .await
        .unwrap();

    assert!(!invalidated, "Should return false when no GET cache exists");
}

/// Test that validate_cache_against_head can be called directly
/// This simulates what happens when a HEAD request is processed
/// Requirements: 7.1, 7.2, 7.3, 7.4, 7.5
#[tokio::test]
async fn test_direct_validation_call() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

    let cache_key = "test-bucket/test-object-5";

    // Step 1: Store GET cache with old ETag
    let old_etag = "\"old-etag-direct\"";
    let old_last_modified = "Wed, 21 Oct 2015 07:28:00 GMT";

    let mut disk_cache_manager =
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir.clone(), true, 1024, false);
    disk_cache_manager.initialize().await.unwrap();

    // Store a range with old metadata
    let range_data = b"test data for direct validation";
    let object_metadata = ObjectMetadata {
        etag: old_etag.to_string(),
        last_modified: old_last_modified.to_string(),
        content_length: range_data.len() as u64,
        content_type: Some("text/plain".to_string()),
        ..Default::default()
    };

    let ttl = std::time::Duration::from_secs(3600);
    disk_cache_manager
        .store_range(
            cache_key,
            0,
            range_data.len() as u64 - 1,
            range_data,
            object_metadata.clone(),
            ttl, true)
        .await
        .unwrap();

    // Verify GET cache exists
    let metadata_before = disk_cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(
        metadata_before.is_some(),
        "GET cache should exist before validation"
    );

    // Step 2: Simulate HEAD request with new metadata
    let new_etag = "\"new-etag-direct\"";
    let new_last_modified = "Thu, 22 Oct 2015 08:30:00 GMT";

    // This simulates what happens when a HEAD request is processed
    // and the HEAD response metadata differs from cached GET metadata
    let invalidated = disk_cache_manager
        .validate_cache_against_head(cache_key, new_etag, new_last_modified)
        .await
        .unwrap();

    assert!(
        invalidated,
        "GET cache should be invalidated when HEAD metadata differs"
    );

    // Verify GET cache was deleted
    let metadata_after = disk_cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(
        metadata_after.is_none(),
        "GET cache should be deleted after validation"
    );
}
