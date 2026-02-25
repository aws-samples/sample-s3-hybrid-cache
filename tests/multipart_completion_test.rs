//! Tests for multipart upload completion functionality
//!
//! Tests Requirements 7.1, 7.2, 7.3, 7.4, 7.5:
//! - Sorting parts by part number
//! - Calculating byte positions from part sizes
//! - Storing each part as a range at calculated position
//! - Updating upload_state to Complete
//! - Clearing temporary part data from metadata
//! - Setting expires_at using PUT_TTL

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::UploadState;
use std::time::SystemTime;
use tempfile::TempDir;

/// Helper function to create a test cache manager
fn create_test_cache_manager() -> (CacheManager, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_all_ttls(
        temp_dir.path().to_path_buf(),
        false, // RAM cache disabled for these tests
        0,
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        1024,                                 // compression threshold
        true,                                 // compression enabled
        std::time::Duration::from_secs(3600), // GET_TTL
        std::time::Duration::from_secs(1800), // HEAD_TTL
        std::time::Duration::from_secs(900),  // PUT_TTL
        false,                                // actively_remove_cached_data
    );
    (cache_manager, temp_dir)
}

#[tokio::test]
async fn test_complete_multipart_upload_basic() {
    let (cache_manager, _temp_dir) = create_test_cache_manager();

    let path = "/test-bucket/test-object";

    // Step 1: Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await.unwrap();

    // Step 2: Store parts (in order)
    let part1_data = vec![1u8; 1024]; // 1KB
    let part2_data = vec![2u8; 2048]; // 2KB
    let part3_data = vec![3u8; 512]; // 512B

    cache_manager
        .store_multipart_part(path, 1, &part1_data, "etag1".to_string())
        .await
        .unwrap();
    cache_manager
        .store_multipart_part(path, 2, &part2_data, "etag2".to_string())
        .await
        .unwrap();
    cache_manager
        .store_multipart_part(path, 3, &part3_data, "etag3".to_string())
        .await
        .unwrap();

    // Step 3: Complete multipart upload
    cache_manager.complete_multipart_upload(path).await.unwrap();

    // Step 4: Verify the upload is complete
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap()
        .unwrap();

    // Requirement 7.4: Verify upload_state is Complete
    assert_eq!(metadata.object_metadata.upload_state, UploadState::Complete);

    // Requirement 7.5: Verify parts are cleared
    assert_eq!(metadata.object_metadata.parts.len(), 0);

    // Verify content_length is correct (sum of all parts)
    let expected_length = 1024 + 2048 + 512;
    assert_eq!(metadata.object_metadata.content_length, expected_length);

    // Requirement 7.3: Verify ranges are created
    assert_eq!(metadata.ranges.len(), 3);

    // Requirement 7.2: Verify byte positions are correct
    assert_eq!(metadata.ranges[0].start, 0);
    assert_eq!(metadata.ranges[0].end, 1023);
    assert_eq!(metadata.ranges[1].start, 1024);
    assert_eq!(metadata.ranges[1].end, 3071);
    assert_eq!(metadata.ranges[2].start, 3072);
    assert_eq!(metadata.ranges[2].end, 3583);

    // Requirement 7.5: Verify PUT_TTL is used
    let now = SystemTime::now();
    let time_until_expiry = metadata.expires_at.duration_since(now).unwrap();
    assert!(time_until_expiry.as_secs() > 800 && time_until_expiry.as_secs() <= 900);
}

#[tokio::test]
async fn test_complete_multipart_upload_out_of_order() {
    let (cache_manager, _temp_dir) = create_test_cache_manager();

    let path = "/test-bucket/test-object-ooo";

    // Step 1: Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await.unwrap();

    // Step 2: Store parts OUT OF ORDER
    let part1_data = vec![1u8; 1000];
    let part2_data = vec![2u8; 2000];
    let part3_data = vec![3u8; 3000];

    // Store in order: 3, 1, 2
    cache_manager
        .store_multipart_part(path, 3, &part3_data, "etag3".to_string())
        .await
        .unwrap();
    cache_manager
        .store_multipart_part(path, 1, &part1_data, "etag1".to_string())
        .await
        .unwrap();
    cache_manager
        .store_multipart_part(path, 2, &part2_data, "etag2".to_string())
        .await
        .unwrap();

    // Step 3: Complete multipart upload
    cache_manager.complete_multipart_upload(path).await.unwrap();

    // Step 4: Verify parts are sorted correctly
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap()
        .unwrap();

    // Requirement 7.1: Verify parts were sorted by part number
    assert_eq!(metadata.ranges.len(), 3);

    // Requirement 7.2: Verify byte positions reflect sorted order (1, 2, 3)
    assert_eq!(metadata.ranges[0].start, 0);
    assert_eq!(metadata.ranges[0].end, 999); // Part 1: 1000 bytes
    assert_eq!(metadata.ranges[1].start, 1000);
    assert_eq!(metadata.ranges[1].end, 2999); // Part 2: 2000 bytes
    assert_eq!(metadata.ranges[2].start, 3000);
    assert_eq!(metadata.ranges[2].end, 5999); // Part 3: 3000 bytes

    assert_eq!(metadata.object_metadata.content_length, 6000);
}

#[tokio::test]
async fn test_complete_multipart_upload_bypassed() {
    // Create a cache manager with very small write cache capacity
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false, // RAM cache disabled
        0,
        1024 * 1024, // 1MB max cache size - with 10% write cache = ~100KB capacity
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        1024,                                 // compression threshold
        true,                                 // compression enabled
        std::time::Duration::from_secs(3600), // GET_TTL
        std::time::Duration::from_secs(1800), // HEAD_TTL
        std::time::Duration::from_secs(900),  // PUT_TTL
        false,                                // actively_remove_cached_data
        s3_proxy::config::SharedStorageConfig::default(),
        10.0,                                          // 10% write cache
        true,                                          // write_cache_enabled
        std::time::Duration::from_secs(86400),         // incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                            // eviction_trigger_percent
        80,                                            // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();
    cache_manager.initialize().await.unwrap();
    // max_cache_size=1MB with 10% write cache = ~100KB write cache capacity

    let path = "/test-bucket/test-object-bypassed";

    // Step 1: Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await.unwrap();

    // Step 2: Store a part larger than write cache capacity
    let large_part_data = vec![1u8; 200 * 1024]; // 200KB - exceeds 100KB write cache
    cache_manager
        .store_multipart_part(path, 1, &large_part_data, "etag1".to_string())
        .await
        .unwrap();

    // Step 3: Try to complete - should be a no-op since it's bypassed
    cache_manager.complete_multipart_upload(path).await.unwrap();

    // Step 4: Verify state is Bypassed
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata.object_metadata.upload_state, UploadState::Bypassed);
}

#[tokio::test]
async fn test_complete_multipart_upload_no_parts() {
    let (cache_manager, _temp_dir) = create_test_cache_manager();

    let path = "/test-bucket/test-object-empty";

    // Step 1: Initiate multipart upload
    cache_manager.initiate_multipart_upload(path).await.unwrap();

    // Step 2: Complete without storing any parts
    cache_manager.complete_multipart_upload(path).await.unwrap();

    // Step 3: Verify upload is still in progress (no parts to complete)
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        metadata.object_metadata.upload_state,
        UploadState::InProgress
    );
}

#[tokio::test]
async fn test_complete_multipart_upload_basic_non_versioned() {
    let (cache_manager, _temp_dir) = create_test_cache_manager();

    let path = "/test-bucket/test-object-non-versioned";

    // Step 1: Initiate multipart upload (version ID no longer supported)
    cache_manager.initiate_multipart_upload(path).await.unwrap();

    // Step 2: Store parts
    let part1_data = vec![1u8; 500];
    let part2_data = vec![2u8; 500];

    cache_manager
        .store_multipart_part(path, 1, &part1_data, "etag1".to_string())
        .await
        .unwrap();
    cache_manager
        .store_multipart_part(path, 2, &part2_data, "etag2".to_string())
        .await
        .unwrap();

    // Step 3: Complete multipart upload
    cache_manager.complete_multipart_upload(path).await.unwrap();

    // Step 4: Verify completion
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(metadata.object_metadata.upload_state, UploadState::Complete);
    assert_eq!(metadata.object_metadata.parts.len(), 0);
    assert_eq!(metadata.ranges.len(), 2);
    assert_eq!(metadata.object_metadata.content_length, 1000);
}

#[tokio::test]
async fn test_complete_multipart_upload_range_files_exist() {
    let (cache_manager, temp_dir) = create_test_cache_manager();

    let path = "/test-bucket/test-object-files";

    // Step 1: Initiate and store parts
    cache_manager.initiate_multipart_upload(path).await.unwrap();

    let part1_data = vec![1u8; 100];
    let part2_data = vec![2u8; 200];

    cache_manager
        .store_multipart_part(path, 1, &part1_data, "etag1".to_string())
        .await
        .unwrap();
    cache_manager
        .store_multipart_part(path, 2, &part2_data, "etag2".to_string())
        .await
        .unwrap();

    // Step 2: Complete multipart upload
    cache_manager.complete_multipart_upload(path).await.unwrap();

    // Step 3: Verify range files exist on disk
    let ranges_dir = temp_dir.path().join("ranges");
    assert!(ranges_dir.exists());

    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap()
        .unwrap();

    for range_spec in &metadata.ranges {
        let range_file = ranges_dir.join(&range_spec.file_path);
        assert!(
            range_file.exists(),
            "Range file should exist: {:?}",
            range_file
        );

        // Verify file is not empty
        let file_size = std::fs::metadata(&range_file).unwrap().len();
        assert!(file_size > 0, "Range file should not be empty");
    }
}
