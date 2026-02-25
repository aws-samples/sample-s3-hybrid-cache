//! Error Handling and Recovery Tests
//!
//! Tests for Requirements 8.1, 8.2, 8.3, 8.4, 8.5
//! Validates error handling for corrupted metadata, missing range files,
//! disk space exhaustion, inconsistent metadata, and error metrics.

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
    let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
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
    let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
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
    let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
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
            frequency_score: 0,
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
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
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
            std::time::Duration::from_secs(315360000), true)
        .await;

    // Restore permissions for cleanup
    let mut perms = std::fs::metadata(&ranges_dir).unwrap().permissions();
    perms.set_readonly(false);
    std::fs::set_permissions(&ranges_dir, perms).unwrap();

    assert!(result.is_err(), "Should return error when disk write fails");
}

/// Test Requirement 8.4: Handle inconsistent metadata
/// When metadata references range files that don't exist, system should:
/// - Detect inconsistency
/// - Remove missing range specs from metadata
/// - Continue serving other ranges
#[tokio::test]
async fn test_inconsistent_metadata_handling() {
    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/test-object";

    // Store two ranges
    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        2048,
        Some("application/octet-stream".to_string()),
    );

    let data1 = vec![1u8; 1024];
    let data2 = vec![2u8; 1024];

    cache_manager
        .store_range(
            cache_key,
            0,
            1023,
            &data1,
            object_metadata.clone(),
            std::time::Duration::from_secs(315360000), true)
        .await
        .unwrap();
    cache_manager
        .store_range(
            cache_key,
            1024,
            2047,
            &data2,
            object_metadata.clone(),
            std::time::Duration::from_secs(315360000), true)
        .await
        .unwrap();

    // Verify both ranges exist
    let metadata = cache_manager
        .get_metadata(cache_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata.ranges.len(), 2, "Should have 2 ranges");

    // Delete one range file manually (simulate corruption/inconsistency)
    let range_file_path = cache_manager.get_new_range_file_path(cache_key, 1024, 2047);
    std::fs::remove_file(&range_file_path).unwrap();

    // Verify and fix metadata
    let fixed_count = cache_manager
        .verify_and_fix_metadata(cache_key)
        .await
        .unwrap();
    assert_eq!(fixed_count, 1, "Should fix 1 inconsistency");

    // Verify metadata was updated
    let metadata = cache_manager
        .get_metadata(cache_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata.ranges.len(), 1, "Should have 1 range remaining");
    assert_eq!(
        metadata.ranges[0].start, 0,
        "Remaining range should be 0-1023"
    );
    assert_eq!(
        metadata.ranges[0].end, 1023,
        "Remaining range should be 0-1023"
    );
}

/// Test Requirement 8.4: Handle all ranges missing
/// When all range files are missing, should delete entire cache entry
#[tokio::test]
async fn test_all_ranges_missing() {
    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/test-object";

    // Store a range
    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        1024,
        Some("application/octet-stream".to_string()),
    );

    let data = vec![42u8; 1024];
    cache_manager
        .store_range(
            cache_key,
            0,
            1023,
            &data,
            object_metadata,
            std::time::Duration::from_secs(315360000), true)
        .await
        .unwrap();

    // Verify range exists
    let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
    assert!(metadata_file_path.exists(), "Metadata file should exist");

    // Delete the range file manually
    let range_file_path = cache_manager.get_new_range_file_path(cache_key, 0, 1023);
    std::fs::remove_file(&range_file_path).unwrap();

    // Verify and fix metadata - should delete entire entry
    let fixed_count = cache_manager
        .verify_and_fix_metadata(cache_key)
        .await
        .unwrap();
    assert_eq!(fixed_count, 1, "Should fix 1 inconsistency");

    // Verify entire entry was deleted
    assert!(
        !metadata_file_path.exists(),
        "Metadata file should be deleted when all ranges are missing"
    );
}

/// Test Requirement 8.4: Cleanup temporary files
#[tokio::test]
async fn test_cleanup_temp_files() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/test-object";

    // Create some temporary files using sharded paths
    let temp_range_file = cache_manager
        .get_new_range_file_path(cache_key, 0, 1023)
        .with_extension("bin.tmp");
    let temp_metadata_file = cache_manager
        .get_new_metadata_file_path(cache_key)
        .with_extension("meta.tmp");

    // Create parent directories
    std::fs::create_dir_all(temp_range_file.parent().unwrap()).unwrap();
    std::fs::create_dir_all(temp_metadata_file.parent().unwrap()).unwrap();

    std::fs::write(&temp_range_file, b"temp data").unwrap();
    std::fs::write(&temp_metadata_file, b"temp metadata").unwrap();

    // Verify temp files exist
    assert!(temp_range_file.exists(), "Temp range file should exist");
    assert!(
        temp_metadata_file.exists(),
        "Temp metadata file should exist"
    );

    // Clean up temp files
    cache_manager.cleanup_temp_files(cache_key).await.unwrap();

    // Verify temp files were deleted
    assert!(
        !temp_range_file.exists(),
        "Temp range file should be deleted"
    );
    assert!(
        !temp_metadata_file.exists(),
        "Temp metadata file should be deleted"
    );
}

/// Test comprehensive cache cleanup
#[tokio::test]
async fn test_comprehensive_cache_cleanup() {
    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/test-object";

    // Create a valid cache entry
    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        1024,
        Some("application/octet-stream".to_string()),
    );

    let data = vec![42u8; 1024];
    cache_manager
        .store_range(
            cache_key,
            0,
            1023,
            &data,
            object_metadata,
            std::time::Duration::from_secs(315360000), true)
        .await
        .unwrap();

    // Create some orphaned files in sharded structure
    let orphaned_key = "test-bucket/orphaned-object";
    let orphaned_range = cache_manager.get_new_range_file_path(orphaned_key, 0, 1023);
    std::fs::create_dir_all(orphaned_range.parent().unwrap()).unwrap();
    std::fs::write(&orphaned_range, b"orphaned data").unwrap();

    // Create some temp files in sharded structure
    let temp_range_file = cache_manager
        .get_new_range_file_path(cache_key, 1024, 2047)
        .with_extension("bin.tmp");
    std::fs::create_dir_all(temp_range_file.parent().unwrap()).unwrap();
    std::fs::write(&temp_range_file, b"temp data").unwrap();

    // Delete one range file to create inconsistency
    let range_file_path = cache_manager.get_new_range_file_path(cache_key, 0, 1023);
    std::fs::remove_file(&range_file_path).unwrap();

    // Perform comprehensive cleanup
    let stats = cache_manager.perform_cache_cleanup().await.unwrap();

    // Verify cleanup stats
    assert!(
        stats.temp_files_cleaned >= 1,
        "Should clean up at least 1 temp file"
    );
    assert!(
        stats.orphaned_files_cleaned >= 1,
        "Should clean up at least 1 orphaned file"
    );
    assert_eq!(stats.inconsistencies_fixed, 1, "Should fix 1 inconsistency");

    // Verify orphaned file was deleted
    assert!(
        !orphaned_range.exists(),
        "Orphaned range file should be deleted"
    );

    // Verify temp file was deleted
    assert!(
        !temp_range_file.exists(),
        "Temp range file should be deleted"
    );
}

/// Test that cleanup doesn't affect valid cache entries
#[tokio::test]
async fn test_cleanup_preserves_valid_entries() {
    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/test-object";

    // Create a valid cache entry
    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        1024,
        Some("application/octet-stream".to_string()),
    );

    let data = vec![42u8; 1024];
    cache_manager
        .store_range(
            cache_key,
            0,
            1023,
            &data,
            object_metadata,
            std::time::Duration::from_secs(315360000), true)
        .await
        .unwrap();

    // Perform cleanup
    let stats = cache_manager.perform_cache_cleanup().await.unwrap();

    // Verify valid entry still exists
    let metadata = cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(metadata.is_some(), "Valid cache entry should still exist");

    let metadata = metadata.unwrap();
    assert_eq!(metadata.ranges.len(), 1, "Valid range should still exist");

    // Verify we can still load the range data
    let range_data = cache_manager
        .load_range_data(&metadata.ranges[0])
        .await
        .unwrap();
    assert_eq!(
        range_data.len(),
        1024,
        "Should be able to load valid range data"
    );
    assert_eq!(range_data, data, "Range data should match original data");
}

/// Test error recovery: corrupted metadata doesn't crash the system
#[tokio::test]
async fn test_error_recovery_no_crash() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
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
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
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
            std::time::Duration::from_secs(315360000), true)
        .await
        .unwrap();

    // Verify first range exists
    let metadata = cache_manager.get_metadata(cache_key).await.unwrap().unwrap();
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
            std::time::Duration::from_secs(315360000), true)
        .await;
    assert!(result.is_ok(), "ETag mismatch should invalidate old ranges and store new data");

    // Verify old range was invalidated and new range stored
    let metadata = cache_manager.get_metadata(cache_key).await.unwrap().unwrap();
    assert_eq!(metadata.object_metadata.etag, "different-etag", "ETag should be updated");
    assert_eq!(metadata.ranges.len(), 1, "Should have only the new range");
    assert_eq!(metadata.ranges[0].start, 1024, "New range should start at 1024");
    assert_eq!(metadata.ranges[0].end, 2047, "New range should end at 2047");

    // Verify old range file was cleaned up
    let old_range_path = cache_manager.get_new_range_file_path(cache_key, 0, 1023);
    assert!(
        !old_range_path.exists(),
        "Old range file should be deleted after ETag mismatch invalidation"
    );
}
