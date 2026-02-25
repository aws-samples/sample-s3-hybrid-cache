//! Tests for multipart part storage functionality
//! Implements Requirements 5.1, 5.2, 5.3, 5.4, 5.5, 6.1, 6.2, 6.3

use s3_proxy::{cache::CacheManager, Result};
use tempfile::TempDir;

#[tokio::test]
async fn test_store_multipart_part_basic() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";
    let part_number = 1u32;
    let part_data = vec![42u8; 5 * 1024 * 1024]; // 5MB
    let etag = "part-etag-1".to_string();

    // First, initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await?;

    // Store the part
    cache_manager
        .store_multipart_part(path, part_number, &part_data, etag.clone())
        .await?;

    // Verify metadata was updated
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;

    assert!(metadata.is_some(), "Metadata should exist");
    let metadata = metadata.unwrap();

    // Requirement 5.1: Part info should be stored
    assert_eq!(
        metadata.object_metadata.parts.len(),
        1,
        "Should have 1 part"
    );
    assert_eq!(metadata.object_metadata.parts[0].part_number, part_number);
    assert_eq!(
        metadata.object_metadata.parts[0].size,
        part_data.len() as u64
    );
    assert_eq!(metadata.object_metadata.parts[0].etag, etag);
    assert_eq!(
        metadata.object_metadata.parts[0].data.len(),
        part_data.len()
    );

    // Requirement 5.2: Cumulative size should be incremented
    assert_eq!(
        metadata.object_metadata.cumulative_size,
        part_data.len() as u64
    );

    // Upload state should still be InProgress
    assert_eq!(
        metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::InProgress
    );

    Ok(())
}

#[tokio::test]
async fn test_store_multipart_part_out_of_order() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";

    // Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await?;

    // Requirement 5.3: Store parts out of order (3, 1, 2)
    let part3_data = vec![3u8; 5 * 1024 * 1024]; // 5MB
    let part1_data = vec![1u8; 5 * 1024 * 1024]; // 5MB
    let part2_data = vec![2u8; 5 * 1024 * 1024]; // 5MB

    cache_manager
        .store_multipart_part(path, 3, &part3_data, "etag-3".to_string())
        .await?;
    cache_manager
        .store_multipart_part(path, 1, &part1_data, "etag-1".to_string())
        .await?;
    cache_manager
        .store_multipart_part(path, 2, &part2_data, "etag-2".to_string())
        .await?;

    // Verify all parts are stored
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;

    assert!(metadata.is_some());
    let metadata = metadata.unwrap();

    assert_eq!(
        metadata.object_metadata.parts.len(),
        3,
        "Should have 3 parts"
    );

    // Verify cumulative size is correct
    let expected_size = (part1_data.len() + part2_data.len() + part3_data.len()) as u64;
    assert_eq!(metadata.object_metadata.cumulative_size, expected_size);

    // Verify all part numbers are present (order doesn't matter in storage)
    let part_numbers: Vec<u32> = metadata
        .object_metadata
        .parts
        .iter()
        .map(|p| p.part_number)
        .collect();
    assert!(part_numbers.contains(&1));
    assert!(part_numbers.contains(&2));
    assert!(part_numbers.contains(&3));

    Ok(())
}

#[tokio::test]
async fn test_store_multipart_part_replace_existing() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";
    let part_number = 1u32;

    // Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await?;

    // Store part 1 first time
    let part1_data_v1 = vec![1u8; 5 * 1024 * 1024]; // 5MB
    cache_manager
        .store_multipart_part(path, part_number, &part1_data_v1, "etag-1-v1".to_string())
        .await?;

    // Store part 1 again (replacement)
    let part1_data_v2 = vec![2u8; 6 * 1024 * 1024]; // 6MB (different size)
    cache_manager
        .store_multipart_part(path, part_number, &part1_data_v2, "etag-1-v2".to_string())
        .await?;

    // Verify only one part exists with updated data
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;

    assert!(metadata.is_some());
    let metadata = metadata.unwrap();

    assert_eq!(
        metadata.object_metadata.parts.len(),
        1,
        "Should still have only 1 part"
    );
    assert_eq!(metadata.object_metadata.parts[0].part_number, part_number);
    assert_eq!(
        metadata.object_metadata.parts[0].size,
        part1_data_v2.len() as u64
    );
    assert_eq!(metadata.object_metadata.parts[0].etag, "etag-1-v2");

    // Cumulative size should reflect the new size
    assert_eq!(
        metadata.object_metadata.cumulative_size,
        part1_data_v2.len() as u64
    );

    Ok(())
}

#[tokio::test]
async fn test_store_multipart_part_capacity_bypass() -> Result<()> {
    let temp_dir = TempDir::new()?;

    // Create cache manager with small capacity
    // 10MB max_cache_size with 10% write cache = 1MB write cache capacity
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false, // RAM cache disabled
        0,
        10 * 1024 * 1024, // 10MB max cache size
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        1024,
        true,
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        false,
        s3_proxy::config::SharedStorageConfig::default(),
        10.0,                                          // 10% write cache = 1MB
        true,                                          // write_cache_enabled
        std::time::Duration::from_secs(86400),         // incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                            // eviction_trigger_percent
        80,                                            // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();
    cache_manager.initialize().await?;

    let path = "/test-bucket/large-object";

    // Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await?;

    // Store first part (500KB - should succeed)
    let part1_data = vec![1u8; 500 * 1024]; // 500KB
    cache_manager
        .store_multipart_part(path, 1, &part1_data, "etag-1".to_string())
        .await?;

    // Verify part 1 was stored
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(metadata.is_some());
    let metadata = metadata.unwrap();
    assert_eq!(metadata.object_metadata.parts.len(), 1);
    assert_eq!(
        metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::InProgress
    );

    // Requirement 6.1: Store second part (600KB - total 1.1MB, should exceed 1MB capacity and trigger bypass)
    let part2_data = vec![2u8; 600 * 1024]; // 600KB
    cache_manager
        .store_multipart_part(path, 2, &part2_data, "etag-2".to_string())
        .await?;

    // Requirement 6.2: Verify upload was marked as Bypassed and parts were cleared
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(metadata.is_some());
    let metadata = metadata.unwrap();
    assert_eq!(
        metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::Bypassed
    );
    assert_eq!(
        metadata.object_metadata.parts.len(),
        0,
        "Parts should be cleared when bypassed"
    );
    assert_eq!(
        metadata.object_metadata.cumulative_size, 0,
        "Cumulative size should be reset"
    );

    // Requirement 6.3: Store third part (should be skipped since upload is bypassed)
    let part3_data = vec![3u8; 100 * 1024]; // 100KB
    cache_manager
        .store_multipart_part(path, 3, &part3_data, "etag-3".to_string())
        .await?;

    // Verify part 3 was not stored
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(metadata.is_some());
    let metadata = metadata.unwrap();
    assert_eq!(
        metadata.object_metadata.parts.len(),
        0,
        "No parts should be stored after bypass"
    );
    assert_eq!(
        metadata.object_metadata.upload_state,
        s3_proxy::cache_types::UploadState::Bypassed
    );

    Ok(())
}

#[tokio::test]
async fn test_store_multipart_part_no_initiation() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";
    let part_data = vec![42u8; 1024];

    // Try to store part without initiating multipart upload
    // Should succeed but do nothing (no error)
    cache_manager
        .store_multipart_part(path, 1, &part_data, "etag-1".to_string())
        .await?;

    // Verify no metadata was created
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;
    assert!(
        metadata.is_none(),
        "No metadata should exist without initiation"
    );

    Ok(())
}

#[tokio::test]
async fn test_store_multipart_part_single_part_verification() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";
    let part_data = vec![42u8; 5 * 1024 * 1024]; // 5MB

    // Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await?;

    // Store part
    cache_manager
        .store_multipart_part(path, 1, &part_data, "etag-1".to_string())
        .await?;

    // Verify metadata was created with correct cache key
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;

    assert!(metadata.is_some());
    let metadata = metadata.unwrap();
    assert_eq!(metadata.object_metadata.parts.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_store_multipart_part_multiple_parts() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let path = "/test-bucket/test-object";

    // Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await?;

    // Store 10 parts
    let mut expected_cumulative_size = 0u64;
    for i in 1..=10 {
        let part_data = vec![i as u8; 5 * 1024 * 1024]; // 5MB each
        expected_cumulative_size += part_data.len() as u64;
        cache_manager
            .store_multipart_part(path, i, &part_data, format!("etag-{}", i))
            .await?;
    }

    // Verify all parts are stored
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager.get_metadata_from_disk(&cache_key).await?;

    assert!(metadata.is_some());
    let metadata = metadata.unwrap();

    assert_eq!(
        metadata.object_metadata.parts.len(),
        10,
        "Should have 10 parts"
    );
    assert_eq!(
        metadata.object_metadata.cumulative_size,
        expected_cumulative_size
    );

    // Verify all part numbers are present
    for i in 1..=10 {
        assert!(
            metadata
                .object_metadata
                .parts
                .iter()
                .any(|p| p.part_number == i),
            "Part {} should be present",
            i
        );
    }

    Ok(())
}
