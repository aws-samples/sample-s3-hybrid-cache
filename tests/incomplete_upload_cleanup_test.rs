//! Incomplete Upload Cleanup Tests
//!
//! Tests cleanup of incomplete multipart uploads that have exceeded the timeout.
//! Validates Requirements 7a.1, 7a.2, 7a.3, 7a.4, 7a.5 from unified-range-write-cache spec.

use s3_proxy::{cache::CacheManager, config::SharedStorageConfig, Result};
use std::collections::HashMap;
use std::time::SystemTime;
use tempfile::TempDir;

#[tokio::test]
async fn test_cleanup_incomplete_uploads_basic() -> Result<()> {
    use s3_proxy::cache::CacheEvictionAlgorithm;
    use std::time::Duration;

    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        false, // write_cache_enabled: false - this test doesn't use write cache
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let path = "/test-bucket/test-object";

    // Requirement 4.1: Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await?;

    // Verify upload is in InProgress state
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(metadata.is_some());
    let metadata = metadata.unwrap();
    assert_eq!(
        metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::InProgress
    );

    // Manually modify the created_at timestamp to simulate old upload
    let mut modified_metadata = metadata.clone();
    modified_metadata.created_at = SystemTime::now() - Duration::from_secs(7200); // 2 hours ago

    // Write modified metadata back to disk
    let metadata_file_path = temp_dir
        .path()
        .join("metadata")
        .join(format!("{}.meta", cache_key.replace("/", "%2F")));
    let metadata_json = serde_json::to_string_pretty(&modified_metadata).unwrap();
    std::fs::write(&metadata_file_path, metadata_json).unwrap();

    // Requirement 7a.1, 7a.2: Cleanup should remove old incomplete uploads
    let cleaned = cache_manager.cleanup_incomplete_uploads().await?;
    assert_eq!(cleaned, 1, "Should have cleaned up 1 incomplete upload");

    // Verify metadata was deleted
    let metadata_after = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(
        metadata_after.is_none(),
        "Metadata should be deleted after cleanup"
    );

    Ok(())
}

#[tokio::test]
async fn test_cleanup_incomplete_uploads_preserves_recent() -> Result<()> {
    use s3_proxy::cache::CacheEvictionAlgorithm;
    use std::time::Duration;

    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        false, // write_cache_enabled: false - this test doesn't use write cache
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let path = "/test-bucket/recent-upload";

    // Initiate a recent multipart upload
    cache_manager.initiate_multipart_upload(path).await?;

    let cache_key = CacheManager::generate_cache_key(path, None);

    // Verify upload exists
    let metadata_before = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(metadata_before.is_some());

    // Cleanup should NOT remove recent uploads (< 1 hour old)
    let cleaned = cache_manager.cleanup_incomplete_uploads().await?;
    assert_eq!(cleaned, 0, "Should not clean up recent uploads");

    // Verify metadata still exists
    let metadata_after = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(
        metadata_after.is_some(),
        "Recent upload should be preserved"
    );

    Ok(())
}

#[tokio::test]
async fn test_cleanup_incomplete_uploads_with_parts() -> Result<()> {
    use s3_proxy::cache::CacheEvictionAlgorithm;
    use std::time::Duration;

    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        false, // write_cache_enabled: false - this test doesn't use write cache
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let path = "/test-bucket/upload-with-parts";

    // Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await?;

    // Store some parts
    let part_data = vec![0u8; 1024];
    let etag = "part-etag-1".to_string();

    cache_manager
        .store_multipart_part(path, 1, &part_data, etag)
        .await?;

    let cache_key = CacheManager::generate_cache_key(path, None);

    // Verify parts are stored
    let metadata_before = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(metadata_before.is_some());
    let metadata_before = metadata_before.unwrap();
    assert_eq!(metadata_before.object_metadata.parts.len(), 1);

    // Manually modify the created_at timestamp to simulate old upload
    let mut modified_metadata = metadata_before.clone();
    modified_metadata.created_at = SystemTime::now() - Duration::from_secs(7200); // 2 hours ago

    // Write modified metadata back to disk
    let metadata_file_path = temp_dir
        .path()
        .join("metadata")
        .join(format!("{}.meta", cache_key.replace("/", "%2F")));
    let metadata_json = serde_json::to_string_pretty(&modified_metadata).unwrap();
    std::fs::write(&metadata_file_path, metadata_json).unwrap();

    // Requirement 7a.2: Cleanup should remove metadata and cached parts
    let cleaned = cache_manager.cleanup_incomplete_uploads().await?;
    assert_eq!(
        cleaned, 1,
        "Should have cleaned up 1 incomplete upload with parts"
    );

    // Verify metadata and parts were deleted
    let metadata_after = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(
        metadata_after.is_none(),
        "Metadata with parts should be deleted after cleanup"
    );

    Ok(())
}

#[tokio::test]
async fn test_cleanup_incomplete_uploads_preserves_completed() -> Result<()> {
    use s3_proxy::cache::CacheEvictionAlgorithm;
    use std::time::Duration;

    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        true, // write_cache_enabled: true - this test uses store_write_cache_entry
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let path = "/test-bucket/completed-upload";

    // Store a completed PUT (not multipart)
    let test_data = b"Completed upload data";
    let headers = HashMap::new();
    let metadata = s3_proxy::cache_types::CacheMetadata {
        etag: "completed-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(path, test_data, headers, metadata)
        .await?;

    let cache_key = CacheManager::generate_cache_key(path, None);

    // Verify it's stored with Complete state
    let metadata_before = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(metadata_before.is_some());
    let metadata_before = metadata_before.unwrap();
    assert_eq!(
        metadata_before.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::Complete
    );

    // Cleanup should NOT remove completed uploads
    let cleaned = cache_manager.cleanup_incomplete_uploads().await?;
    assert_eq!(cleaned, 0, "Should not clean up completed uploads");

    // Verify metadata still exists
    let metadata_after = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(
        metadata_after.is_some(),
        "Completed upload should be preserved"
    );

    Ok(())
}

#[tokio::test]
async fn test_cleanup_incomplete_uploads_multiple() -> Result<()> {
    use s3_proxy::cache::CacheEvictionAlgorithm;
    use std::time::Duration;

    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        false, // write_cache_enabled: false - this test doesn't use write cache
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    // Create multiple incomplete uploads
    let paths = vec![
        "/test-bucket/upload-1",
        "/test-bucket/upload-2",
        "/test-bucket/upload-3",
    ];

    for path in &paths {
        cache_manager.initiate_multipart_upload(path).await?;

        // Manually modify timestamps to simulate old uploads
        let cache_key = CacheManager::generate_cache_key(path, None);
        let metadata = cache_manager
            .get_metadata_from_disk(&cache_key)
            .await?
            .unwrap();

        let mut modified_metadata = metadata.clone();
        modified_metadata.created_at = SystemTime::now() - Duration::from_secs(7200); // 2 hours ago

        let metadata_file_path = temp_dir
            .path()
            .join("metadata")
            .join(format!("{}.meta", cache_key.replace("/", "%2F")));
        let metadata_json = serde_json::to_string_pretty(&modified_metadata).unwrap();
        std::fs::write(&metadata_file_path, metadata_json).unwrap();
    }

    // Cleanup should remove all old incomplete uploads
    let cleaned = cache_manager.cleanup_incomplete_uploads().await?;
    assert_eq!(cleaned, 3, "Should have cleaned up 3 incomplete uploads");

    // Verify all metadata was deleted
    for path in &paths {
        let cache_key = CacheManager::generate_cache_key(path, None);
        let metadata_after = cache_manager.get_metadata_from_disk(&cache_key).await?;
        assert!(
            metadata_after.is_none(),
            "All incomplete uploads should be deleted"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_cleanup_incomplete_uploads_integrated_with_maintenance() -> Result<()> {
    use s3_proxy::cache::CacheEvictionAlgorithm;
    use std::time::Duration;

    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        false, // write_cache_enabled: false - this test doesn't use write cache
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let path = "/test-bucket/maintenance-test";

    // Initiate incomplete upload
    cache_manager.initiate_multipart_upload(path).await?;

    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await?
        .unwrap();

    // Manually modify timestamp
    let mut modified_metadata = metadata.clone();
    modified_metadata.created_at = SystemTime::now() - Duration::from_secs(7200); // 2 hours ago

    let metadata_file_path = temp_dir
        .path()
        .join("metadata")
        .join(format!("{}.meta", cache_key.replace("/", "%2F")));
    let metadata_json = serde_json::to_string_pretty(&modified_metadata).unwrap();
    std::fs::write(&metadata_file_path, metadata_json).unwrap();

    // Requirement 7a.3: Cleanup should be called from periodic cache maintenance
    let result = cache_manager
        .cleanup_expired_entries_comprehensive()
        .await?;

    // Verify cleanup was performed
    assert!(
        result.disk_cleaned >= 1,
        "Maintenance should have cleaned up incomplete upload"
    );

    // Verify metadata was deleted
    let metadata_after = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(
        metadata_after.is_none(),
        "Incomplete upload should be deleted by maintenance"
    );

    Ok(())
}

#[tokio::test]
async fn test_cleanup_incomplete_uploads_bypassed_state() -> Result<()> {
    use s3_proxy::cache::CacheEvictionAlgorithm;
    use std::time::Duration;

    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        false, // write_cache_enabled: false - this test doesn't use write cache
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let path = "/test-bucket/bypassed-upload";

    // Initiate upload
    cache_manager.initiate_multipart_upload(path).await?;

    let cache_key = CacheManager::generate_cache_key(path, None);
    let mut metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await?
        .unwrap();

    // Manually set to Bypassed state and old timestamp
    metadata.object_metadata.upload_state = s3_proxy::cache_types::UploadState::Bypassed;
    metadata.created_at = SystemTime::now() - Duration::from_secs(7200); // 2 hours ago

    let metadata_file_path = temp_dir
        .path()
        .join("metadata")
        .join(format!("{}.meta", cache_key.replace("/", "%2F")));
    let metadata_json = serde_json::to_string_pretty(&metadata).unwrap();
    std::fs::write(&metadata_file_path, metadata_json).unwrap();

    // Cleanup should NOT remove Bypassed uploads (only InProgress)
    let cleaned = cache_manager.cleanup_incomplete_uploads().await?;
    assert_eq!(cleaned, 0, "Should not clean up Bypassed uploads");

    // Verify metadata still exists
    let metadata_after = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(
        metadata_after.is_some(),
        "Bypassed upload should be preserved"
    );

    Ok(())
}
