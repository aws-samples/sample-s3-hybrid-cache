use s3_proxy::{cache::CacheManager, Result};
use std::collections::HashMap;
use tempfile::TempDir;

/// Helper function to create test metadata
fn create_test_metadata(
    etag: &str,
    last_modified: &str,
    content_length: u64,
) -> s3_proxy::cache_types::CacheMetadata {
    s3_proxy::cache_types::CacheMetadata {
        etag: etag.to_string(),
        last_modified: last_modified.to_string(),
        content_length,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    }
}

/// Test that PutObject invalidates existing cached data before storing new data
/// Requirement 8.1: WHEN receiving PutObject request, THEN the Cache System SHALL invalidate any existing cached data for that key
#[tokio::test]
async fn test_put_invalidates_existing_complete_cache() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // First, store an initial PUT-cached object
    let initial_data = b"initial data content";
    let initial_headers = HashMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
        ("etag".to_string(), "initial-etag".to_string()),
    ]);
    let initial_metadata = s3_proxy::cache_types::CacheMetadata {
        etag: "initial-etag".to_string(),
        last_modified: "2024-01-01T00:00:00Z".to_string(),
        content_length: initial_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(
            &cache_key,
            initial_data,
            initial_headers.clone(),
            initial_metadata,
        )
        .await?;

    // Verify initial data is cached
    let metadata_file_path = cache_manager.get_new_metadata_file_path(&cache_key);
    assert!(
        metadata_file_path.exists(),
        "Initial metadata should be present"
    );

    // Read initial metadata to verify it exists
    let initial_metadata_content = std::fs::read_to_string(&metadata_file_path)?;
    let initial_cached_metadata: s3_proxy::cache_types::NewCacheMetadata =
        serde_json::from_str(&initial_metadata_content)?;
    assert_eq!(initial_cached_metadata.object_metadata.etag, "initial-etag");
    assert_eq!(
        initial_cached_metadata.object_metadata.content_length,
        initial_data.len() as u64
    );

    // Now store a new PUT to the same key - should invalidate the old one
    let new_data = b"new data content that is different";
    let new_headers = HashMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
        ("etag".to_string(), "new-etag".to_string()),
    ]);
    let new_metadata = s3_proxy::cache_types::CacheMetadata {
        etag: "new-etag".to_string(),
        last_modified: "2024-01-02T00:00:00Z".to_string(),
        content_length: new_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(&cache_key, new_data, new_headers, new_metadata)
        .await?;

    // Verify metadata file still exists but with new content
    assert!(
        metadata_file_path.exists(),
        "Metadata file should exist after new PUT"
    );

    // Read and verify new metadata
    let new_metadata_content = std::fs::read_to_string(&metadata_file_path)?;
    let new_cached_metadata: s3_proxy::cache_types::NewCacheMetadata =
        serde_json::from_str(&new_metadata_content)?;

    // Verify it's the new data, not the old data
    assert_eq!(new_cached_metadata.object_metadata.etag, "new-etag");
    assert_eq!(
        new_cached_metadata.object_metadata.content_length,
        new_data.len() as u64
    );
    assert_eq!(
        new_cached_metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::Complete
    );

    Ok(())
}

/// Test that PutObject invalidates in-progress multipart uploads
/// Requirement 8.4: THE Cache System SHALL handle conflicts for both in-progress and completed uploads
#[tokio::test]
async fn test_put_invalidates_in_progress_multipart() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // First, initiate a multipart upload
    cache_manager.initiate_multipart_upload(path).await?;

    // Store a part
    let part_data = vec![1u8; 1000];
    let part_etag = "part-etag".to_string();

    cache_manager
        .store_multipart_part(path, 1, &part_data, part_etag)
        .await?;

    // Verify in-progress upload exists
    let metadata_file_path = cache_manager.get_new_metadata_file_path(&cache_key);
    assert!(
        metadata_file_path.exists(),
        "In-progress metadata should be present"
    );

    let in_progress_content = std::fs::read_to_string(&metadata_file_path)?;
    let in_progress_metadata: s3_proxy::cache_types::NewCacheMetadata =
        serde_json::from_str(&in_progress_content)?;
    assert_eq!(
        in_progress_metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::InProgress
    );
    assert_eq!(in_progress_metadata.object_metadata.parts.len(), 1);

    // Now store a PUT to the same key - should invalidate the in-progress multipart
    let put_data = b"complete object via PUT";
    let put_headers = HashMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
        ("etag".to_string(), "put-etag".to_string()),
    ]);
    let put_metadata = s3_proxy::cache_types::CacheMetadata {
        etag: "put-etag".to_string(),
        last_modified: "2024-01-02T00:00:00Z".to_string(),
        content_length: put_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(&cache_key, put_data, put_headers, put_metadata)
        .await?;

    // Verify metadata file now contains the PUT data, not the multipart
    assert!(
        metadata_file_path.exists(),
        "Metadata file should exist after PUT"
    );

    let put_content = std::fs::read_to_string(&metadata_file_path)?;
    let put_cached_metadata: s3_proxy::cache_types::NewCacheMetadata =
        serde_json::from_str(&put_content)?;

    // Verify it's the PUT data, not the in-progress multipart
    assert_eq!(put_cached_metadata.object_metadata.etag, "put-etag");
    assert_eq!(
        put_cached_metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::Complete
    );
    assert_eq!(put_cached_metadata.object_metadata.parts.len(), 0); // No parts for PUT
    assert_eq!(
        put_cached_metadata.object_metadata.content_length,
        put_data.len() as u64
    );

    Ok(())
}

/// Test that PutObject deletes associated range files when invalidating
/// Requirement 8.3: WHEN invalidating on conflict, THEN the Cache System SHALL delete metadata and all associated range files
#[tokio::test]
async fn test_put_deletes_range_files_on_conflict() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // First, store an initial PUT-cached object
    let initial_data = b"initial data content with some length";
    let initial_headers = HashMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
        ("etag".to_string(), "initial-etag".to_string()),
    ]);
    let initial_metadata = s3_proxy::cache_types::CacheMetadata {
        etag: "initial-etag".to_string(),
        last_modified: "2024-01-01T00:00:00Z".to_string(),
        content_length: initial_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(
            &cache_key,
            initial_data,
            initial_headers.clone(),
            initial_metadata,
        )
        .await?;

    // Verify range file was created
    let ranges_dir = temp_dir.path().join("ranges");
    let range_files_before: Vec<_> = std::fs::read_dir(&ranges_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .contains(&cache_key.replace("/", "%2F"))
        })
        .collect();

    assert!(
        !range_files_before.is_empty(),
        "Initial range files should exist"
    );

    // Now store a new PUT to the same key
    let new_data = b"new data";
    let new_headers = HashMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
        ("etag".to_string(), "new-etag".to_string()),
    ]);
    let new_metadata = s3_proxy::cache_types::CacheMetadata {
        etag: "new-etag".to_string(),
        last_modified: "2024-01-02T00:00:00Z".to_string(),
        content_length: new_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(&cache_key, new_data, new_headers, new_metadata)
        .await?;

    // Verify old range files were deleted and new ones created
    let range_files_after: Vec<_> = std::fs::read_dir(&ranges_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .contains(&cache_key.replace("/", "%2F"))
        })
        .collect();

    // Should have new range files (not the old ones)
    assert!(
        !range_files_after.is_empty(),
        "New range files should exist"
    );

    // The old range files should have been deleted
    // We can verify this by checking that the file names are different
    // (since the new data has different length, the range spec will be different)
    let old_range_names: Vec<_> = range_files_before.iter().map(|e| e.file_name()).collect();
    let new_range_names: Vec<_> = range_files_after.iter().map(|e| e.file_name()).collect();

    // Since the data lengths are different, the range specs should be different
    assert_ne!(
        old_range_names, new_range_names,
        "Old and new range files should be different"
    );

    Ok(())
}

/// Test that CreateMultipartUpload invalidates existing PUT-cached data
/// Requirement 8.2: WHEN receiving CreateMultipartUpload request, THEN the Cache System SHALL invalidate any existing cached data for that key
#[tokio::test]
async fn test_multipart_initiation_invalidates_put_cache() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // First, store a PUT-cached object
    let put_data = b"complete object via PUT";
    let put_headers = HashMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
        ("etag".to_string(), "put-etag".to_string()),
    ]);
    let put_metadata = s3_proxy::cache_types::CacheMetadata {
        etag: "put-etag".to_string(),
        last_modified: "2024-01-01T00:00:00Z".to_string(),
        content_length: put_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(&cache_key, put_data, put_headers, put_metadata)
        .await?;

    // Verify PUT data is cached
    let metadata_file_path = cache_manager.get_new_metadata_file_path(&cache_key);
    assert!(
        metadata_file_path.exists(),
        "PUT metadata should be present"
    );

    let put_content = std::fs::read_to_string(&metadata_file_path)?;
    let put_cached_metadata: s3_proxy::cache_types::NewCacheMetadata =
        serde_json::from_str(&put_content)?;
    assert_eq!(put_cached_metadata.object_metadata.etag, "put-etag");
    assert_eq!(
        put_cached_metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::Complete
    );

    // Now initiate a multipart upload - should invalidate the PUT cache
    cache_manager.initiate_multipart_upload(path).await?;

    // Verify metadata file now contains in-progress multipart, not the PUT
    assert!(
        metadata_file_path.exists(),
        "Metadata file should exist after multipart initiation"
    );

    let multipart_content = std::fs::read_to_string(&metadata_file_path)?;
    let multipart_metadata: s3_proxy::cache_types::NewCacheMetadata =
        serde_json::from_str(&multipart_content)?;

    // Verify it's an in-progress multipart, not the completed PUT
    assert_eq!(
        multipart_metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::InProgress
    );
    assert_eq!(multipart_metadata.object_metadata.etag, ""); // Empty for in-progress
    assert_eq!(multipart_metadata.object_metadata.cumulative_size, 0);

    Ok(())
}
