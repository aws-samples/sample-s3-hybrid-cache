//! Tests for multipart upload initiation functionality

use s3_proxy::{cache::CacheManager, Result};
use std::time::SystemTime;
use tempfile::TempDir;

#[tokio::test]
async fn test_initiate_multipart_upload_creates_metadata() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";

    // Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await?;

    // Verify metadata was created by reading it from cache
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;

    assert!(metadata.is_some(), "Metadata should exist after initiation");
    let metadata = metadata.unwrap();

    // Verify upload state is InProgress
    assert_eq!(
        metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::InProgress
    );

    // Verify cumulative_size is 0
    assert_eq!(metadata.object_metadata.cumulative_size, 0);

    // Verify parts list is empty
    assert_eq!(metadata.object_metadata.parts.len(), 0);

    // Verify expiration is set to 1 hour
    let now = SystemTime::now();
    let expiration_diff = metadata.expires_at.duration_since(now).unwrap_or_default();

    // Allow some tolerance for test execution time (within 10 seconds)
    assert!(
        expiration_diff.as_secs() >= 3590 && expiration_diff.as_secs() <= 3610,
        "Expiration should be approximately 1 hour from now"
    );

    Ok(())
}

#[tokio::test]
async fn test_initiate_multipart_upload_invalidates_existing_cache() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // First, initiate a multipart upload to create initial metadata
    cache_manager.initiate_multipart_upload(path).await?;

    // Verify initial metadata exists
    let initial_metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(
        initial_metadata.is_some(),
        "Initial metadata should be present"
    );
    let initial_created_at = initial_metadata.unwrap().created_at;

    // Wait a moment to ensure timestamp difference
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Now initiate multipart upload again - should invalidate and recreate
    cache_manager.initiate_multipart_upload(path).await?;

    // Verify metadata was replaced with new in-progress upload
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(
        metadata.is_some(),
        "Metadata should exist after re-initiation"
    );
    let metadata = metadata.unwrap();

    // Verify it's a new in-progress upload
    assert_eq!(
        metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::InProgress
    );
    assert_eq!(metadata.object_metadata.cumulative_size, 0);
    // Verify timestamp changed (indicating invalidation and recreation)
    assert!(
        metadata.created_at > initial_created_at,
        "Metadata should have been recreated"
    );

    Ok(())
}

#[tokio::test]
async fn test_initiate_multipart_upload_non_versioned() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";

    // Initiate multipart upload (version ID no longer supported)
    cache_manager.initiate_multipart_upload(path).await?;

    // Verify metadata was created with non-versioned cache key
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;

    assert!(
        metadata.is_some(),
        "Metadata should exist for non-versioned object"
    );
    let metadata = metadata.unwrap();

    // Verify upload state is InProgress
    assert_eq!(
        metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::InProgress
    );

    Ok(())
}
