//! Error Handling and Recovery Tests
//!
//! Tests for Requirements 8.1, 8.2, 8.3, 8.4, 8.5
//! Validates error handling for corrupted metadata, missing range files,
//! disk space exhaustion, and ETag-mismatch invalidation.

use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::DiskCacheManager;
use std::time::SystemTime;
use tempfile::TempDir;

/// Test Requirement 8.1: Handle corrupted metadata files
/// When metadata file is corrupted, system should:
/// - Log error
/// - Treat as cache miss (return None)
/// - Preserve file (may be transient on shared storage)
#[tokio::test]
async fn test_corrupted_metadata_handling() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager =
        DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/test-object";

    // Write corrupted JSON to metadata file
    let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
    std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
    std::fs::write(&metadata_file_path, "{ this is not valid json }").unwrap();

    // Verify file exists before test
    assert!(
        metadata_file_path.exists(),
        "Corrupted metadata file should exist"
    );

    // Try to get metadata - should return None (cache miss)
    let result = cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(
        result.is_none(),
        "Should return None for corrupted metadata"
    );
}

/// Test Requirement 8.1: Handle empty metadata files
/// Empty metadata files are treated as cache miss but preserved
/// (may be mid-write on shared storage)
#[tokio::test]
async fn test_empty_metadata_handling() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager =
        DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/test-object";

    // Write empty file
    let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
    std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
    std::fs::write(&metadata_file_path, "").unwrap();

    // Try to get metadata - should return None (cache miss)
    let result = cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(
        result.is_none(),
        "Should return None for empty metadata file"
    );
}

/// Test Requirement 8.2: Handle missing range binary files
/// When range binary file is missing, system should:
/// - Return error (cache miss)
/// - Caller should fetch from S3 and recache
#[tokio::test]
async fn test_missing_range_file_handling() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager =
        DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/test-object";

    // Create metadata with a range spec, but don't create the actual range file
    let now = SystemTime::now();
    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata::new(
            "test-etag".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            1024,
            Some("application/octet-stream".to_string()),
        ),
        ranges: vec![RangeSpec {
            start: 0,
            end: 1023,
            file_path: "missing_file_0-1023.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 1024,
            uncompressed_size: 1024,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: None,
        }],
        created_at: now,
        expires_at: now + std::time::Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        ..Default::default()
    };

    // Write metadata
    cache_manager.update_metadata(&metadata).await.unwrap();

    // Try to load range data - should return error
    let range_spec = &metadata.ranges[0];
    let result = cache_manager.load_range_data(range_spec).await;

    assert!(
        result.is_err(),
        "Should return error for missing range file"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Range file not found"),
        "Error should indicate missing range file"
    );
}

/// Test Requirement 8.3: Handle disk space exhaustion
/// This test simulates disk space exhaustion by trying to write to a read-only directory
#[tokio::test]
async fn test_disk_space_exhaustion_handling() {
    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager =
        DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/test-object";
    let data = vec![42u8; 1024];

    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        10240,
        Some("application/octet-stream".to_string()),
    );

    // Make ranges directory read-only to simulate disk space issues
    let ranges_dir = temp_dir.path().join("ranges");
    let mut perms = std::fs::metadata(&ranges_dir).unwrap().permissions();
    perms.set_readonly(true);
    std::fs::set_permissions(&ranges_dir, perms).unwrap();

    // Try to store range - should fail gracefully
    let result = cache_manager
        .store_range(
            cache_key,
            0,
            1023,
            &data,
            object_metadata,
            std::time::Duration::from_secs(315360000),
            true,
        )
        .await;

    // Restore permissions for cleanup
    #[allow(clippy::permissions_set_readonly_false)]
    {
        let mut perms = std::fs::metadata(&ranges_dir).unwrap().permissions();
        perms.set_readonly(false);
        std::fs::set_permissions(&ranges_dir, perms).unwrap();
    }

    assert!(result.is_err(), "Should return error when disk write fails");
}

/// Test error recovery: corrupted metadata doesn't crash the system
#[tokio::test]
async fn test_error_recovery_no_crash() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager =
        DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
    cache_manager.initialize().await.unwrap();

    // Create multiple corrupted metadata files
    for i in 0..10 {
        let cache_key = format!("test-bucket/object-{}", i);
        let metadata_file_path = cache_manager.get_new_metadata_file_path(&cache_key);
        std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_file_path, format!("corrupted data {}", i)).unwrap();
    }

    // Try to get metadata for all keys - should handle gracefully
    for i in 0..10 {
        let cache_key = format!("test-bucket/object-{}", i);
        let result = cache_manager.get_metadata(&cache_key).await;

        // Should not crash, should return Ok(None)
        assert!(
            result.is_ok(),
            "Should handle corrupted metadata gracefully"
        );
        assert!(
            result.unwrap().is_none(),
            "Should return None for corrupted metadata"
        );
    }
}

/// Test that ETag mismatch invalidates existing ranges and stores new data
/// Requirements 14.1, 14.2, 14.3: ETag mismatch triggers invalidation
#[tokio::test]
async fn test_partial_write_cleanup() {
    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager =
        DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/test-object";

    // Store first range successfully
    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        2048,
        Some("application/octet-stream".to_string()),
    );

    let data = vec![42u8; 1024];
    cache_manager
        .store_range(
            cache_key,
            0,
            1023,
            &data,
            object_metadata.clone(),
            std::time::Duration::from_secs(315360000),
            true,
        )
        .await
        .unwrap();

    // Verify first range exists
    let metadata = cache_manager
        .get_metadata(cache_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata.ranges.len(), 1, "Should have 1 range");
    assert_eq!(metadata.object_metadata.etag, "test-etag");

    // Store with different ETag - should invalidate old ranges and succeed
    let different_metadata = ObjectMetadata::new(
        "different-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        2048,
        Some("application/octet-stream".to_string()),
    );

    let data2 = vec![99u8; 1024];
    let result = cache_manager
        .store_range(
            cache_key,
            1024,
            2047,
            &data2,
            different_metadata,
            std::time::Duration::from_secs(315360000),
            true,
        )
        .await;
    assert!(
        result.is_ok(),
        "ETag mismatch should invalidate old ranges and store new data"
    );

    // Verify old range was invalidated and new range stored
    let metadata = cache_manager
        .get_metadata(cache_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        metadata.object_metadata.etag, "different-etag",
        "ETag should be updated"
    );
    assert_eq!(metadata.ranges.len(), 1, "Should have only the new range");
    assert_eq!(
        metadata.ranges[0].start, 1024,
        "New range should start at 1024"
    );
    assert_eq!(metadata.ranges[0].end, 2047, "New range should end at 2047");

    // Verify old range file was cleaned up
    let old_range_path = cache_manager.get_new_range_file_path(cache_key, 0, 1023);
    assert!(
        !old_range_path.exists(),
        "Old range file should be deleted after ETag mismatch invalidation"
    );
}
