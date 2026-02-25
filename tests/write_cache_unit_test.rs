//! Unit tests for write cache functionality
//!
//! Tests for Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 1.6:
//! - PUT storage creates correct metadata and range files
//! - upload_state is set to Complete
//! - PUT_TTL is used for expiration
//! - PUT-cached objects are not in RAM cache

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{CacheMetadata, UploadState};
use s3_proxy::config::SharedStorageConfig;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

#[tokio::test]
async fn test_put_storage_creates_correct_metadata_and_range_files() {
    // Requirement 1.1, 1.2, 1.3: Test that PUT storage creates correct metadata and range files

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager with PUT_TTL
    let put_ttl = Duration::from_secs(300); // 5 minutes
    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false, // RAM cache disabled
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600), // GET_TTL
        Duration::from_secs(300),  // HEAD_TTL
        put_ttl,
        false,
        SharedStorageConfig::default(),
        10.0,
        true,                       // write_cache_enabled: true - required for write cache tests
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95, // eviction_trigger_percent
        80, // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    // Test data
    let cache_key = "/test-bucket/test-object.txt";
    let test_data = b"Hello, World! This is test data for PUT caching.";
    let content_length = test_data.len() as u64;

    let metadata = CacheMetadata {
        etag: "test-etag-123".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };

    // Store as write cache entry
    cache_manager
        .store_write_cache_entry(cache_key, test_data, HashMap::new(), metadata.clone())
        .await
        .unwrap();

    // Verify metadata using CacheManager API
    let stored_metadata = cache_manager
        .get_metadata_from_disk(cache_key)
        .await
        .unwrap()
        .unwrap();

    // Verify range file was created (uses sharded directory structure)
    let range_file_path = &stored_metadata.ranges[0].file_path;
    let range_file = cache_dir.join("ranges").join(range_file_path);
    assert!(
        range_file.exists(),
        "Range file should exist in ranges/ directory"
    );

    // Verify metadata has correct content_length
    assert_eq!(
        stored_metadata.object_metadata.content_length,
        content_length
    );
    assert_eq!(stored_metadata.object_metadata.etag, metadata.etag);

    // Verify range specification
    assert_eq!(
        stored_metadata.ranges.len(),
        1,
        "Should have exactly one range"
    );
    let range_spec = &stored_metadata.ranges[0];
    assert_eq!(range_spec.start, 0, "Range should start at 0");
    assert_eq!(
        range_spec.end,
        content_length - 1,
        "Range should end at content_length-1"
    );

    println!("✓ PUT storage creates correct metadata and range files");
}

#[tokio::test]
async fn test_upload_state_is_complete() {
    // Requirement 1.5: Test that upload_state is set to Complete for PUT-cached objects

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(300),
        Duration::from_secs(300),
        false,
        SharedStorageConfig::default(),
        10.0,
        true,                       // write_cache_enabled: true - required for write cache tests
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95, // eviction_trigger_percent
        80, // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let cache_key = "/test-bucket/complete-state.txt";
    let test_data = b"Test data for upload state verification";

    let metadata = CacheMetadata {
        etag: "complete-etag".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };

    // Store as write cache entry
    cache_manager
        .store_write_cache_entry(cache_key, test_data, HashMap::new(), metadata)
        .await
        .unwrap();

    // Verify upload_state is Complete
    let stored_metadata = cache_manager
        .get_metadata_from_disk(cache_key)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        stored_metadata.object_metadata.upload_state,
        UploadState::Complete,
        "upload_state should be Complete for PUT-cached objects"
    );

    println!("✓ upload_state is set to Complete");
}

#[tokio::test]
async fn test_put_ttl_is_used_for_expiration() {
    // Requirement 1.4: Test that PUT_TTL is used for expiration

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let put_ttl = Duration::from_secs(180); // 3 minutes
    let get_ttl = Duration::from_secs(3600); // 1 hour (much longer)

    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        get_ttl,
        Duration::from_secs(300),
        put_ttl,
        false,
        SharedStorageConfig::default(),
        10.0,
        true,                       // write_cache_enabled: true - required for write cache tests
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95, // eviction_trigger_percent
        80, // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let cache_key = "/test-bucket/ttl-test.txt";
    let test_data = b"Test data for TTL verification";

    let metadata = CacheMetadata {
        etag: "ttl-etag".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };

    let before_store = SystemTime::now();

    // Store as write cache entry
    cache_manager
        .store_write_cache_entry(cache_key, test_data, HashMap::new(), metadata)
        .await
        .unwrap();

    let after_store = SystemTime::now();

    // Verify expiration time is set to PUT_TTL
    let stored_metadata = cache_manager
        .get_metadata_from_disk(cache_key)
        .await
        .unwrap()
        .unwrap();

    let expires_at = stored_metadata.expires_at;
    let now = SystemTime::now();

    // Calculate time until expiry
    let time_until_expiry = expires_at.duration_since(now).unwrap();

    // Should expire within PUT_TTL window (with some tolerance for test execution time)
    let tolerance = Duration::from_secs(10);
    assert!(
        time_until_expiry <= put_ttl + tolerance,
        "Expiration should be within PUT_TTL + tolerance. Expected: ~{:?}, Got: {:?}",
        put_ttl,
        time_until_expiry
    );
    assert!(
        time_until_expiry >= put_ttl - tolerance,
        "Expiration should be at least PUT_TTL - tolerance. Expected: ~{:?}, Got: {:?}",
        put_ttl,
        time_until_expiry
    );

    // Verify it's NOT using GET_TTL (which is much longer)
    assert!(
        time_until_expiry < get_ttl - Duration::from_secs(60),
        "Should be using PUT_TTL, not GET_TTL. PUT_TTL: {:?}, GET_TTL: {:?}, Actual: {:?}",
        put_ttl,
        get_ttl,
        time_until_expiry
    );

    println!(
        "✓ PUT_TTL is used for expiration (expires in ~{:?})",
        time_until_expiry
    );
}

#[tokio::test]
async fn test_put_cached_objects_not_in_ram_cache() {
    // Requirement 1.6: Test that PUT-cached objects are NOT stored in RAM cache

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager with RAM cache ENABLED
    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        true,               // RAM cache ENABLED
        10 * 1024 * 1024,   // 10MB RAM cache
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(300),
        Duration::from_secs(300),
        false,
        SharedStorageConfig::default(),
        10.0,
        true,                       // write_cache_enabled: true - required for write cache tests
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95, // eviction_trigger_percent
        80, // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let cache_key = "/test-bucket/no-ram-cache.txt";
    let test_data = b"Test data that should NOT be in RAM cache";

    let metadata = CacheMetadata {
        etag: "no-ram-etag".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };

    // Store as write cache entry
    cache_manager
        .store_write_cache_entry(cache_key, test_data, HashMap::new(), metadata)
        .await
        .unwrap();

    // Verify object is in disk cache
    let disk_metadata = cache_manager
        .get_metadata_from_disk(cache_key)
        .await
        .unwrap();
    assert!(disk_metadata.is_some(), "Object should be in disk cache");

    // Verify object is NOT in RAM cache by checking entry count
    // RAM cache should be empty (0 entries) since PUT-cached objects should not be stored there
    let ram_stats = cache_manager.get_ram_cache_stats();
    assert!(
        ram_stats.is_some(),
        "RAM cache should be enabled for this test"
    );
    let stats = ram_stats.unwrap();
    assert_eq!(
        stats.entries_count, 0,
        "PUT-cached object should NOT be in RAM cache (entries_count should be 0)"
    );

    println!("✓ PUT-cached objects are not in RAM cache (disk only)");
}

#[tokio::test]
async fn test_put_storage_with_headers() {
    // Additional test: Verify headers are preserved in metadata

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(300),
        Duration::from_secs(300),
        false,
        SharedStorageConfig::default(),
        10.0,
        true,                       // write_cache_enabled: true - required for write cache tests
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95, // eviction_trigger_percent
        80, // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let cache_key = "/test-bucket/with-headers.txt";
    let test_data = b"Test data with headers";

    let mut headers = HashMap::new();
    headers.insert("content-type".to_string(), "text/plain".to_string());
    headers.insert("x-custom-header".to_string(), "custom-value".to_string());

    let metadata = CacheMetadata {
        etag: "headers-etag".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };

    // Store with headers
    cache_manager
        .store_write_cache_entry(cache_key, test_data, headers.clone(), metadata.clone())
        .await
        .unwrap();

    // Verify metadata includes content-type
    let stored_metadata = cache_manager
        .get_metadata_from_disk(cache_key)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        stored_metadata.object_metadata.content_type,
        Some("text/plain".to_string()),
        "Content-Type should be preserved in metadata"
    );

    println!("✓ Headers are preserved in metadata");
}

#[tokio::test]
async fn test_multiple_put_operations_same_key() {
    // Test that multiple PUTs to the same key work correctly (conflict invalidation)

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(300),
        Duration::from_secs(300),
        false,
        SharedStorageConfig::default(),
        10.0,
        true,                       // write_cache_enabled: true - required for write cache tests
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95, // eviction_trigger_percent
        80, // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let cache_key = "/test-bucket/overwrite-test.txt";

    // First PUT
    let test_data_1 = b"First version of data";
    let metadata_1 = CacheMetadata {
        etag: "etag-v1".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length: test_data_1.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(cache_key, test_data_1, HashMap::new(), metadata_1)
        .await
        .unwrap();

    // Verify first version is stored
    let stored_metadata_1 = cache_manager
        .get_metadata_from_disk(cache_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored_metadata_1.object_metadata.etag, "etag-v1");

    // Second PUT (should overwrite)
    let test_data_2 = b"Second version of data - longer content";
    let metadata_2 = CacheMetadata {
        etag: "etag-v2".to_string(),
        last_modified: "Tue, 02 Jan 2024 00:00:00 GMT".to_string(),
        content_length: test_data_2.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(cache_key, test_data_2, HashMap::new(), metadata_2)
        .await
        .unwrap();

    // Verify second version replaced the first
    let stored_metadata_2 = cache_manager
        .get_metadata_from_disk(cache_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored_metadata_2.object_metadata.etag, "etag-v2");
    assert_eq!(
        stored_metadata_2.object_metadata.content_length,
        test_data_2.len() as u64
    );

    // Verify only one range exists (old one was cleaned up)
    assert_eq!(
        stored_metadata_2.ranges.len(),
        1,
        "Should have exactly one range after overwrite"
    );

    println!("✓ Multiple PUTs to same key work correctly");
}
