//! Stale Data Removal Operations Test
//!
//! Tests the delete_stale_range() and delete_all_ranges() methods for removing
//! known invalid data from the cache.
//!
//! Requirements:
//! - Requirement 10.1: ETag mismatch detection and immediate deletion
//! - Requirement 10.2: PUT conflict invalidation
//! - Requirement 10.3: Corrupted cache file deletion
//! - Requirement 10.4: Conditional validation returning 200 OK
//! - Requirement 10.5: Logging of all stale data removal operations

use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::DiskCacheManager;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Helper function to create a test cache manager
async fn create_test_cache_manager() -> (DiskCacheManager, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
    manager.initialize().await.unwrap();
    (manager, temp_dir)
}

/// Helper function to create test metadata with ranges
fn create_test_metadata(cache_key: &str, range_count: usize) -> NewCacheMetadata {
    let now = SystemTime::now();
    let mut ranges = Vec::new();

    for i in 0..range_count {
        let start = i as u64 * 8388608;
        let end = start + 8388607;
        ranges.push(RangeSpec {
            start,
            end,
            file_path: format!("test_range_{}_{}.bin", start, end),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 8388608,
            uncompressed_size: 8388608,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            frequency_score: 0,
        });
    }

    NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata::new(
            "test-etag".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            (range_count as u64) * 8388608,
            Some("application/octet-stream".to_string()),
        ),
        ranges,
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_delete_stale_range_single_range() -> Result<()> {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create metadata with one range
    let metadata = create_test_metadata(cache_key, 1);
    manager.update_metadata(&metadata).await?;

    // Verify metadata exists
    let loaded = manager.get_metadata(cache_key).await?.unwrap();
    assert_eq!(loaded.ranges.len(), 1);

    // Delete the stale range with reason
    manager
        .delete_stale_range(cache_key, 0, 8388607, "ETag mismatch")
        .await?;

    // Verify metadata file was deleted (no ranges remaining)
    let result = manager.get_metadata(cache_key).await?;
    assert!(
        result.is_none(),
        "Metadata should be deleted when last range is removed"
    );

    Ok(())
}

#[tokio::test]
async fn test_delete_stale_range_with_remaining_ranges() -> Result<()> {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create metadata with three ranges
    let metadata = create_test_metadata(cache_key, 3);
    manager.update_metadata(&metadata).await?;

    // Verify metadata exists with 3 ranges
    let loaded = manager.get_metadata(cache_key).await?.unwrap();
    assert_eq!(loaded.ranges.len(), 3);

    // Delete the first stale range
    manager
        .delete_stale_range(cache_key, 0, 8388607, "Validation returned 200 OK")
        .await?;

    // Verify only 2 ranges remain
    let loaded = manager.get_metadata(cache_key).await?.unwrap();
    assert_eq!(loaded.ranges.len(), 2);
    assert_eq!(loaded.ranges[0].start, 8388608);
    assert_eq!(loaded.ranges[1].start, 16777216);

    Ok(())
}

#[tokio::test]
async fn test_delete_stale_range_nonexistent() -> Result<()> {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Try to delete a range when no metadata exists (should not error)
    manager
        .delete_stale_range(cache_key, 0, 8388607, "Corrupted cache file")
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_delete_stale_range_not_found() -> Result<()> {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create metadata with one range
    let metadata = create_test_metadata(cache_key, 1);
    manager.update_metadata(&metadata).await?;

    // Try to delete a different range (should not error, just skip)
    manager
        .delete_stale_range(cache_key, 8388608, 16777215, "ETag mismatch")
        .await?;

    // Verify original range still exists
    let loaded = manager.get_metadata(cache_key).await?.unwrap();
    assert_eq!(loaded.ranges.len(), 1);
    assert_eq!(loaded.ranges[0].start, 0);

    Ok(())
}

#[tokio::test]
async fn test_delete_all_ranges() -> Result<()> {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create metadata with multiple ranges
    let metadata = create_test_metadata(cache_key, 5);
    manager.update_metadata(&metadata).await?;

    // Verify metadata exists with 5 ranges
    let loaded = manager.get_metadata(cache_key).await?.unwrap();
    assert_eq!(loaded.ranges.len(), 5);

    // Delete all ranges
    manager.delete_all_ranges(cache_key, "PUT conflict").await?;

    // Verify metadata file was deleted
    let result = manager.get_metadata(cache_key).await?;
    assert!(
        result.is_none(),
        "Metadata should be deleted after delete_all_ranges"
    );

    Ok(())
}

#[tokio::test]
async fn test_delete_all_ranges_nonexistent() -> Result<()> {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Try to delete all ranges when no metadata exists (should not error)
    manager.delete_all_ranges(cache_key, "PUT conflict").await?;

    Ok(())
}

#[tokio::test]
async fn test_delete_all_ranges_with_binary_files() -> Result<()> {
    let (mut manager, temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create metadata with ranges
    let metadata = create_test_metadata(cache_key, 3);

    // Create actual binary files for the ranges
    let ranges_dir = temp_dir.path().join("ranges");
    for range in &metadata.ranges {
        let range_path = ranges_dir.join(&range.file_path);
        std::fs::write(&range_path, b"test data")?;
    }

    manager.update_metadata(&metadata).await?;

    // Verify binary files exist
    for range in &metadata.ranges {
        let range_path = ranges_dir.join(&range.file_path);
        assert!(
            range_path.exists(),
            "Binary file should exist before deletion"
        );
    }

    // Delete all ranges
    manager.delete_all_ranges(cache_key, "PUT conflict").await?;

    // Verify binary files were deleted
    for range in &metadata.ranges {
        let range_path = ranges_dir.join(&range.file_path);
        assert!(!range_path.exists(), "Binary file should be deleted");
    }

    // Verify metadata file was deleted
    let result = manager.get_metadata(cache_key).await?;
    assert!(
        result.is_none(),
        "Metadata should be deleted after delete_all_ranges"
    );

    Ok(())
}

#[tokio::test]
async fn test_delete_stale_range_logging() -> Result<()> {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create metadata with one range
    let metadata = create_test_metadata(cache_key, 1);
    manager.update_metadata(&metadata).await?;

    // Delete the stale range with various reasons
    let reasons = vec![
        "ETag mismatch",
        "Validation returned 200 OK",
        "Corrupted cache file",
    ];

    for reason in reasons {
        // Re-create metadata
        let metadata = create_test_metadata(cache_key, 1);
        manager.update_metadata(&metadata).await?;

        // Delete with reason (should log the reason)
        manager
            .delete_stale_range(cache_key, 0, 8388607, reason)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn test_delete_all_ranges_logging() -> Result<()> {
    let (mut manager, _temp_dir) = create_test_cache_manager().await;
    let cache_key = "test-bucket/test-object";

    // Create metadata with multiple ranges
    let metadata = create_test_metadata(cache_key, 3);
    manager.update_metadata(&metadata).await?;

    // Delete all ranges with reason (should log the reason)
    manager.delete_all_ranges(cache_key, "PUT conflict").await?;

    Ok(())
}
