//! Test metadata update operations with file locking
//! This test validates the new methods added in task 3:
//! - update_range_access()
//! - refresh_object_ttl()
//! - remove_invalidated_range()

use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// Helper to create a test DiskCacheManager
async fn create_test_cache_manager() -> (s3_proxy::disk_cache::DiskCacheManager, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let manager = s3_proxy::disk_cache::DiskCacheManager::new(
        cache_dir, true,  // compression_enabled
        1024,  // compression_threshold
        false, // write_cache_enabled
    );

    // Initialize the cache directory structure
    manager.initialize().await.unwrap();

    (manager, temp_dir)
}

/// Helper to create test metadata with a range
fn create_test_metadata(cache_key: &str) -> NewCacheMetadata {
    let now = SystemTime::now();
    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        10485760,
        Some("application/octet-stream".to_string()),
    );

    let range_spec = RangeSpec::new(
        0,
        8388607,
        format!("{}_0-8388607.bin", cache_key),
        CompressionAlgorithm::Lz4,
        8388608,
        8388608,
    );

    NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata,
        ranges: vec![range_spec],
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_update_range_access() {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create and store initial metadata
    let metadata = create_test_metadata(cache_key);
    let initial_access_count = metadata.ranges[0].access_count;
    let initial_last_accessed = metadata.ranges[0].last_accessed;

    manager.update_metadata(&metadata).await.unwrap();

    // Wait a bit to ensure time difference
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Update range access
    manager
        .update_range_access(cache_key, 0, 8388607)
        .await
        .unwrap();

    // Load metadata and verify access was recorded
    let updated_metadata = manager.get_metadata(cache_key).await.unwrap().unwrap();
    let updated_range = &updated_metadata.ranges[0];

    assert_eq!(updated_range.access_count, initial_access_count + 1);
    assert!(updated_range.last_accessed > initial_last_accessed);
}

#[tokio::test]
async fn test_update_range_access_nonexistent_range() {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create and store initial metadata
    let metadata = create_test_metadata(cache_key);
    manager.update_metadata(&metadata).await.unwrap();

    // Try to update a non-existent range (should not error)
    let result = manager
        .update_range_access(cache_key, 8388608, 16777215)
        .await;
    assert!(result.is_ok());

    // Verify original range is unchanged
    let updated_metadata = manager.get_metadata(cache_key).await.unwrap().unwrap();
    assert_eq!(updated_metadata.ranges.len(), 1);
    assert_eq!(updated_metadata.ranges[0].start, 0);
    assert_eq!(updated_metadata.ranges[0].end, 8388607);
}

#[tokio::test]
async fn test_update_range_access_nonexistent_metadata() {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "nonexistent-bucket/nonexistent-object";

    // Try to update access for non-existent metadata (should not error)
    let result = manager.update_range_access(cache_key, 0, 8388607).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_refresh_object_ttl() {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create and store initial metadata
    let metadata = create_test_metadata(cache_key);
    let initial_expires_at = metadata.expires_at;

    manager.update_metadata(&metadata).await.unwrap();

    // Wait a bit to ensure time difference
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Refresh object TTL with a longer duration
    let new_ttl = Duration::from_secs(7200);
    manager
        .refresh_object_ttl(cache_key, new_ttl)
        .await
        .unwrap();

    // Load metadata and verify TTL was refreshed
    let updated_metadata = manager.get_metadata(cache_key).await.unwrap().unwrap();

    assert!(updated_metadata.expires_at > initial_expires_at);
    // access_count should remain unchanged
    assert_eq!(updated_metadata.ranges[0].access_count, 1);
}

#[tokio::test]
async fn test_refresh_object_ttl_nonexistent_key() {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/nonexistent-object";

    // Try to refresh TTL for non-existent key (should not error - metadata file doesn't exist)
    let result = manager
        .refresh_object_ttl(cache_key, Duration::from_secs(7200))
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_remove_invalidated_range() {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create metadata with an expired range
    let now = SystemTime::now();
    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        10485760,
        Some("application/octet-stream".to_string()),
    );

    // Create an expired range
    let expired_range = RangeSpec {
        start: 0,
        end: 8388607,
        file_path: format!("{}_0-8388607.bin", cache_key),
        compression_algorithm: CompressionAlgorithm::Lz4,
        compressed_size: 8388608,
        uncompressed_size: 8388608,
        created_at: now - Duration::from_secs(7200),
        last_accessed: now - Duration::from_secs(3600),
        access_count: 5,
        frequency_score: 0,
    };

    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata,
        ranges: vec![expired_range],
        created_at: now,
        expires_at: now - Duration::from_secs(1), // Expired object
        compression_info: CompressionInfo::default(),
        ..Default::default()
    };

    manager.update_metadata(&metadata).await.unwrap();

    // Remove the invalidated range
    manager
        .remove_invalidated_range(cache_key, 0, 8388607)
        .await
        .unwrap();

    // Verify metadata file was deleted (no ranges remaining)
    let result = manager.get_metadata(cache_key).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_remove_invalidated_range_with_remaining_ranges() {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create metadata with two ranges, one expired
    let now = SystemTime::now();
    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        16777216,
        Some("application/octet-stream".to_string()),
    );

    // Expired range
    let expired_range = RangeSpec {
        start: 0,
        end: 8388607,
        file_path: format!("{}_0-8388607.bin", cache_key),
        compression_algorithm: CompressionAlgorithm::Lz4,
        compressed_size: 8388608,
        uncompressed_size: 8388608,
        created_at: now - Duration::from_secs(7200),
        last_accessed: now - Duration::from_secs(3600),
        access_count: 5,
        frequency_score: 0,
    };

    // Valid range
    let valid_range = RangeSpec::new(
        8388608,
        16777215,
        format!("{}_8388608-16777215.bin", cache_key),
        CompressionAlgorithm::Lz4,
        8388608,
        8388608,
    );

    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata,
        ranges: vec![expired_range, valid_range],
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        ..Default::default()
    };

    manager.update_metadata(&metadata).await.unwrap();

    // Remove the invalidated range
    manager
        .remove_invalidated_range(cache_key, 0, 8388607)
        .await
        .unwrap();

    // Verify only the valid range remains
    let updated_metadata = manager.get_metadata(cache_key).await.unwrap().unwrap();
    assert_eq!(updated_metadata.ranges.len(), 1);
    assert_eq!(updated_metadata.ranges[0].start, 8388608);
    assert_eq!(updated_metadata.ranges[0].end, 16777215);
}

#[tokio::test]
async fn test_remove_invalidated_range_nonexpired() {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create metadata with a non-expired range
    let metadata = create_test_metadata(cache_key);
    manager.update_metadata(&metadata).await.unwrap();

    // Remove a non-expired range (removes regardless of expiration status)
    manager
        .remove_invalidated_range(cache_key, 0, 8388607)
        .await
        .unwrap();

    // Verify range was removed (metadata file deleted when no ranges remain)
    let updated_metadata = manager.get_metadata(cache_key).await.unwrap();
    assert!(updated_metadata.is_none(), "remove_invalidated_range removes the specified range regardless of expiration; metadata deleted when no ranges remain");
}

#[tokio::test]
async fn test_concurrent_metadata_updates() {
    let (mut manager1, temp_dir) = create_test_cache_manager().await;
    let cache_dir = temp_dir.path().to_path_buf();

    // Create a second manager pointing to the same cache directory
    let mut manager2 =
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir.clone(), true, 1024, false);

    let cache_key = "test-bucket/test-object";

    // Create and store initial metadata
    let metadata = create_test_metadata(cache_key);
    manager1.update_metadata(&metadata).await.unwrap();

    // Spawn concurrent updates from both managers
    let cache_key1 = cache_key.to_string();
    let cache_key2 = cache_key.to_string();

    let handle1 = tokio::spawn(async move {
        for _ in 0..5 {
            manager1
                .update_range_access(&cache_key1, 0, 8388607)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    let handle2 = tokio::spawn(async move {
        for _ in 0..5 {
            manager2
                .update_range_access(&cache_key2, 0, 8388607)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Wait for both to complete
    handle1.await.unwrap();
    handle2.await.unwrap();

    // Create a third manager to read the final state
    let manager3 = s3_proxy::disk_cache::DiskCacheManager::new(cache_dir, true, 1024, false);

    // Verify access count was incremented correctly
    // Should be 1 (initial) + 5 + 5 = 11
    let final_metadata = manager3.get_metadata(cache_key).await.unwrap().unwrap();
    assert_eq!(final_metadata.ranges[0].access_count, 11);
}

#[tokio::test]
async fn test_update_range_access_performance() {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create and store initial metadata
    let metadata = create_test_metadata(cache_key);
    manager.update_metadata(&metadata).await.unwrap();

    // Measure performance of update_range_access
    let start = std::time::Instant::now();
    manager
        .update_range_access(cache_key, 0, 8388607)
        .await
        .unwrap();
    let duration = start.elapsed();

    // Should complete in reasonable time (Requirement 7.1)
    // Using 50ms threshold to account for system load variations
    println!(
        "update_range_access took: {:.2}ms",
        duration.as_secs_f64() * 1000.0
    );
    assert!(
        duration.as_millis() < 50,
        "update_range_access took too long: {:.2}ms",
        duration.as_secs_f64() * 1000.0
    );
}
