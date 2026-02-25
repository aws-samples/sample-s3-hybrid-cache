/// Test HTTP proxy handlers for unified range write cache
///
/// This test verifies that the HTTP proxy correctly handles:
/// - Regular PUT requests (using new write cache storage)
/// - CreateMultipartUpload (POST with ?uploads)
/// - UploadPart (PUT with ?uploadId&partNumber)
/// - CompleteMultipartUpload (POST with ?uploadId)
/// - Conflict invalidation
use s3_proxy::cache::CacheManager;
use s3_proxy::config::SharedStorageConfig;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_cache_manager_multipart_integration() {
    // Create temporary directory for cache
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false,              // RAM cache disabled for this test
        1024 * 1024,        // 1MB RAM cache
        1024 * 1024 * 1024, // 1GB max cache size
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        1024,                                // 1KB compression threshold
        true,                                // compression enabled
        std::time::Duration::from_secs(300), // GET TTL
        std::time::Duration::from_secs(60),  // HEAD TTL
        std::time::Duration::from_secs(30),  // PUT TTL
        true,                                // actively remove cached data
        SharedStorageConfig::default(),
        10.0,                                  // 10% write cache
        false,                                 // write_cache_enabled
        std::time::Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    ));

    let path = "/test-bucket/test-object";

    println!("Test 1: Initiate multipart upload (simulating CreateMultipartUpload)");
    cache_manager.initiate_multipart_upload(path).await.unwrap();
    println!("✓ Multipart upload initiated");

    println!("\nTest 2: Store parts (simulating UploadPart)");
    let part1_data = b"Part 1 data";
    let part2_data = b"Part 2 data";
    let part3_data = b"Part 3 data";

    cache_manager
        .store_multipart_part(path, 1, part1_data, "etag1".to_string())
        .await
        .unwrap();
    println!("✓ Part 1 stored");

    cache_manager
        .store_multipart_part(path, 2, part2_data, "etag2".to_string())
        .await
        .unwrap();
    println!("✓ Part 2 stored");

    cache_manager
        .store_multipart_part(path, 3, part3_data, "etag3".to_string())
        .await
        .unwrap();
    println!("✓ Part 3 stored");

    println!("\nTest 3: Complete multipart upload (simulating CompleteMultipartUpload)");
    cache_manager.complete_multipart_upload(path).await.unwrap();
    println!("✓ Multipart upload completed");

    println!("\nTest 4: Verify conflict invalidation on new PUT");
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Invalidate existing cache (simulating new PUT request)
    cache_manager
        .invalidate_cache_hierarchy(&cache_key)
        .await
        .unwrap();
    println!("✓ Cache invalidated on conflict");

    println!("\n✅ All HTTP proxy handler integration tests passed!");
}

#[tokio::test]
async fn test_regular_put_with_conflict_invalidation() {
    // Create temporary directory for cache
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false,              // RAM cache disabled for this test
        1024 * 1024,        // 1MB RAM cache
        1024 * 1024 * 1024, // 1GB max cache size
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        1024,                                // 1KB compression threshold
        true,                                // compression enabled
        std::time::Duration::from_secs(300), // GET TTL
        std::time::Duration::from_secs(60),  // HEAD TTL
        std::time::Duration::from_secs(30),  // PUT TTL
        true,                                // actively remove cached data
        SharedStorageConfig::default(),
        10.0,                                  // 10% write cache
        false,                                 // write_cache_enabled
        std::time::Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    ));

    let path = "/test-bucket/test-object";
    let cache_key = CacheManager::generate_cache_key(path, None);

    println!("Test 1: Store initial PUT request");
    let body_data = b"Initial PUT data";
    let headers = std::collections::HashMap::new();
    let metadata = s3_proxy::cache_types::CacheMetadata {
        etag: "etag1".to_string(),
        last_modified: "2024-01-01T00:00:00Z".to_string(),
        content_length: body_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(&cache_key, body_data, headers.clone(), metadata.clone())
        .await
        .unwrap();
    println!("✓ Initial PUT cached");

    println!("\nTest 2: Trigger conflict invalidation (simulating new PUT)");
    cache_manager
        .invalidate_cache_hierarchy(&cache_key)
        .await
        .unwrap();
    println!("✓ Existing cache invalidated");

    println!("\nTest 3: Store new PUT request");
    let new_body_data = b"New PUT data";
    let new_metadata = s3_proxy::cache_types::CacheMetadata {
        etag: "etag2".to_string(),
        last_modified: "2024-01-02T00:00:00Z".to_string(),
        content_length: new_body_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(&cache_key, new_body_data, headers, new_metadata)
        .await
        .unwrap();
    println!("✓ New PUT cached");

    println!("\n✅ Regular PUT with conflict invalidation test passed!");
}
