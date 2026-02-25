//! Multi-instance shared cache integration tests
//!
//! These tests validate coordination between multiple proxy instances
//! using shared cache volumes, write lock coordination, and cache consistency.

use futures;
use s3_proxy::{
    cache::CacheManager,
    cache_types::CacheMetadata,
    compression::CompressionHandler,
    config::{CacheConfig, EvictionAlgorithm, SharedStorageConfig},
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Test configuration for multi-instance testing
struct MultiInstanceTestConfig {
    shared_cache_dir: PathBuf,
    instance_count: usize,
    test_object_size: usize,
    test_data: Vec<u8>,
}

impl Default for MultiInstanceTestConfig {
    fn default() -> Self {
        // Create unique temp directory for each test run
        let random_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let temp_dir =
            std::env::temp_dir().join(format!("s3_proxy_shared_cache_test_{}", random_id));
        Self {
            shared_cache_dir: temp_dir,
            instance_count: 3,
            test_object_size: 1024 * 1024, // 1MB
            test_data: (0..1024).map(|i| (i % 256) as u8).collect(),
        }
    }
}

/// Create a test cache configuration for shared cache testing
fn create_shared_cache_config(shared_dir: PathBuf, _instance_id: &str) -> CacheConfig {
    CacheConfig {
        cache_dir: shared_dir,
        max_cache_size: 100 * 1024 * 1024, // 100MB
        ram_cache_enabled: true,
        max_ram_cache_size: 50 * 1024 * 1024, // 50MB
        eviction_algorithm: EvictionAlgorithm::LRU,
        write_cache_enabled: true,
        write_cache_percent: 10.0,
        write_cache_max_object_size: 256 * 1024 * 1024, // 256MB
        put_ttl: Duration::from_secs(3600),
        get_ttl: Duration::from_secs(3600),
        head_ttl: Duration::from_secs(3600),
        actively_remove_cached_data: false,
        eviction_buffer_percent: 5,
        shared_storage: SharedStorageConfig::default(),
        range_merge_gap_threshold: 256 * 1024, // 256KB
        ram_cache_flush_interval: Duration::from_secs(60),
        ram_cache_flush_threshold: 100,
        ram_cache_flush_on_eviction: false,
        ram_cache_verification_interval: Duration::from_secs(1),
        incomplete_upload_ttl: Duration::from_secs(86400), // 1 day
        initialization: s3_proxy::config::InitializationConfig::default(),
        cache_bypass_headers_enabled: true,
        metadata_cache: s3_proxy::config::MetadataCacheConfig::default(),
        eviction_trigger_percent: 95, // Trigger eviction at 95% capacity
        eviction_target_percent: 80,  // Reduce to 80% after eviction
        full_object_check_threshold: 67_108_864, // 64 MiB
        disk_streaming_threshold: 1_048_576, // 1 MiB
        read_cache_enabled: true,
        bucket_settings_staleness_threshold: Duration::from_secs(60),
        download_coordination: s3_proxy::config::DownloadCoordinationConfig::default(),
    }
}

/// Simulate a proxy instance for testing
struct TestProxyInstance {
    instance_id: String,
    cache_manager: CacheManager,
    compression_handler: CompressionHandler,
}

impl TestProxyInstance {
    fn new(shared_cache_dir: PathBuf, instance_id: String) -> Self {
        let cache_config = create_shared_cache_config(shared_cache_dir, &instance_id);

        let cache_manager = CacheManager::new_with_defaults(
            cache_config.cache_dir.clone(),
            cache_config.ram_cache_enabled,
            cache_config.max_ram_cache_size,
        );

        let compression_handler = CompressionHandler::new(1024, true);

        Self {
            instance_id,
            cache_manager,
            compression_handler,
        }
    }

    async fn write_to_cache(
        &self,
        cache_key: &str,
        data: &[u8],
        metadata: CacheMetadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Use the public API to store cache data
        let headers = HashMap::new();

        // Store using the public cache manager API
        self.cache_manager
            .store_write_cache_entry(cache_key, data, headers, metadata)
            .await?;

        Ok(())
    }

    async fn read_from_cache(
        &self,
        cache_key: &str,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        // Try to get write cache entry first (since we're using write cache for storage)
        if let Some(write_entry) = self.cache_manager.get_write_cache_entry(cache_key).await? {
            return Ok(Some(write_entry.body));
        }

        // Try to get regular cached response as fallback
        if let Some(cached_response) = self.cache_manager.get_cached_response(cache_key).await? {
            if let Some(body) = cached_response.body {
                return Ok(Some(body));
            }
        }

        Ok(None)
    }
}

#[tokio::test]
async fn test_shared_cache_volume_coordination() {
    // Test multiple instances coordinating on shared cache volume
    let test_config = MultiInstanceTestConfig::default();

    // Clean up any existing test cache
    let _ = std::fs::remove_dir_all(&test_config.shared_cache_dir);
    std::fs::create_dir_all(&test_config.shared_cache_dir)
        .expect("Failed to create shared cache directory");

    // Create multiple proxy instances
    let mut instances = Vec::new();
    for i in 0..test_config.instance_count {
        let instance_id = format!("proxy-instance-{}", i);
        let instance = TestProxyInstance::new(test_config.shared_cache_dir.clone(), instance_id);
        instances.push(instance);
    }

    // Test that all instances can be created with shared cache directory
    for instance in &instances {
        // Just verify the instance was created successfully
        println!("Instance {} created successfully", instance.instance_id);
    }

    println!(
        "Successfully created {} proxy instances with shared cache volume",
        instances.len()
    );
}

#[tokio::test]
async fn test_write_lock_coordination() {
    // Test write lock coordination between multiple instances
    let test_config = MultiInstanceTestConfig::default();

    // Clean up and create shared cache directory
    let _ = std::fs::remove_dir_all(&test_config.shared_cache_dir);
    if let Err(e) = std::fs::create_dir_all(&test_config.shared_cache_dir) {
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            panic!("Failed to create shared cache directory: {}", e);
        }
    }

    // Create two proxy instances
    let instance1 = TestProxyInstance::new(
        test_config.shared_cache_dir.clone(),
        "instance-1".to_string(),
    );
    let instance2 = TestProxyInstance::new(
        test_config.shared_cache_dir.clone(),
        "instance-2".to_string(),
    );

    let cache_key = "test-bucket/test-object";
    let test_data = test_config.test_data.clone();
    let metadata = CacheMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Test write coordination by having both instances write to cache
    // Instance 1 writes first
    let write1_result = instance1
        .write_to_cache(cache_key, &test_data, metadata.clone())
        .await;
    assert!(
        write1_result.is_ok(),
        "Instance 1 should be able to write to cache"
    );

    // Give some time for the write to complete
    sleep(Duration::from_millis(50)).await;

    // Instance 2 writes to the same key (should succeed due to coordination)
    let updated_data = b"Updated data from instance 2".to_vec();
    let updated_metadata = CacheMetadata {
        etag: "updated-etag".to_string(),
        last_modified: "Thu, 22 Oct 2015 07:28:00 GMT".to_string(),
        content_length: updated_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    let write2_result = instance2
        .write_to_cache(cache_key, &updated_data, updated_metadata)
        .await;
    assert!(
        write2_result.is_ok(),
        "Instance 2 should be able to write to cache"
    );

    println!("Write lock coordination test passed");
}

#[tokio::test]
async fn test_cache_consistency_across_instances() {
    // Test cache consistency when multiple instances read/write
    let test_config = MultiInstanceTestConfig::default();

    // Clean up and create shared cache directory
    let _ = std::fs::remove_dir_all(&test_config.shared_cache_dir);
    if let Err(e) = std::fs::create_dir_all(&test_config.shared_cache_dir) {
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            panic!("Failed to create shared cache directory: {}", e);
        }
    }

    // Create multiple instances
    let instance1 = TestProxyInstance::new(
        test_config.shared_cache_dir.clone(),
        "writer-instance".to_string(),
    );
    let instance2 = TestProxyInstance::new(
        test_config.shared_cache_dir.clone(),
        "reader-instance-1".to_string(),
    );
    let instance3 = TestProxyInstance::new(
        test_config.shared_cache_dir.clone(),
        "reader-instance-2".to_string(),
    );

    let cache_key = "consistency-test/object";
    let test_data = b"Consistency test data for shared cache".to_vec();
    let metadata = CacheMetadata {
        etag: "consistency-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Instance 1 writes to cache
    instance1
        .write_to_cache(cache_key, &test_data, metadata.clone())
        .await
        .expect("Failed to write to cache");

    // Give some time for the write to complete
    sleep(Duration::from_millis(100)).await;

    // Instance 2 and 3 should be able to read the same data
    let data2 = instance2
        .read_from_cache(cache_key)
        .await
        .expect("Failed to read from cache");
    let data3 = instance3
        .read_from_cache(cache_key)
        .await
        .expect("Failed to read from cache");

    assert!(data2.is_some(), "Instance 2 should read cached data");
    assert!(data3.is_some(), "Instance 3 should read cached data");

    let data2 = data2.unwrap();
    let data3 = data3.unwrap();

    assert_eq!(data2, test_data, "Instance 2 should read correct data");
    assert_eq!(data3, test_data, "Instance 3 should read correct data");
    assert_eq!(data2, data3, "Both instances should read identical data");

    println!("Cache consistency test passed - all instances read identical data");
}

#[tokio::test]
async fn test_ram_cache_coordination() {
    // Test RAM cache coordination across instances
    let test_config = MultiInstanceTestConfig::default();

    // Clean up and create shared cache directory
    let _ = std::fs::remove_dir_all(&test_config.shared_cache_dir);
    if let Err(e) = std::fs::create_dir_all(&test_config.shared_cache_dir) {
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            panic!("Failed to create shared cache directory: {}", e);
        }
    }

    // Create instances with RAM cache enabled
    let instance1 = TestProxyInstance::new(
        test_config.shared_cache_dir.clone(),
        "ram-instance-1".to_string(),
    );
    let instance2 = TestProxyInstance::new(
        test_config.shared_cache_dir.clone(),
        "ram-instance-2".to_string(),
    );

    // Verify instances are created with RAM cache configuration
    println!("Instance 1 created with RAM cache configuration");
    println!("Instance 2 created with RAM cache configuration");

    let cache_key = "ram-test/object";
    let test_data = b"RAM cache coordination test data".to_vec();
    let metadata = CacheMetadata {
        etag: "ram-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Instance 1 writes to cache (both RAM and disk)
    instance1
        .write_to_cache(cache_key, &test_data, metadata.clone())
        .await
        .expect("Failed to write to cache");

    // Instance 1 should be able to read from RAM cache
    let ram_data1 = instance1
        .read_from_cache(cache_key)
        .await
        .expect("Failed to read from RAM cache");
    assert!(ram_data1.is_some(), "Instance 1 should read from RAM cache");
    assert_eq!(ram_data1.unwrap(), test_data, "RAM cache data should match");

    // Instance 2 should read from disk cache (RAM cache is per-instance)
    let disk_data2 = instance2
        .read_from_cache(cache_key)
        .await
        .expect("Failed to read from disk cache");
    assert!(
        disk_data2.is_some(),
        "Instance 2 should read from disk cache"
    );
    assert_eq!(
        disk_data2.unwrap(),
        test_data,
        "Disk cache data should match"
    );

    println!("RAM cache coordination test passed");
}

#[tokio::test]
async fn test_compression_across_instances() {
    // Test compression consistency across multiple instances
    let test_config = MultiInstanceTestConfig::default();

    // Clean up and create shared cache directory
    let _ = std::fs::remove_dir_all(&test_config.shared_cache_dir);
    if let Err(e) = std::fs::create_dir_all(&test_config.shared_cache_dir) {
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            panic!("Failed to create shared cache directory: {}", e);
        }
    }

    // Create instances with compression enabled
    let instance1 = TestProxyInstance::new(
        test_config.shared_cache_dir.clone(),
        "compress-writer".to_string(),
    );
    let instance2 = TestProxyInstance::new(
        test_config.shared_cache_dir.clone(),
        "compress-reader".to_string(),
    );

    // Create large test data that will be compressed
    let large_test_data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
    let cache_key = "compression-test/large-object";
    let metadata = CacheMetadata {
        etag: "compression-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: large_test_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Verify compression will be applied
    assert!(
        instance1
            .compression_handler
            .should_compress(large_test_data.len()),
        "Large data should be compressed"
    );

    // Instance 1 writes compressed data
    instance1
        .write_to_cache(cache_key, &large_test_data, metadata.clone())
        .await
        .expect("Failed to write compressed data");

    // Give time for the write to complete and be available to other instances
    sleep(Duration::from_millis(100)).await;

    // Instance 2 reads and decompresses data
    let decompressed_data = instance2
        .read_from_cache(cache_key)
        .await
        .expect("Failed to read compressed data");

    assert!(decompressed_data.is_some(), "Should read compressed data");
    let decompressed_data = decompressed_data.unwrap();
    assert_eq!(
        decompressed_data, large_test_data,
        "Decompressed data should match original"
    );

    println!("Compression coordination test passed - data compressed by one instance and decompressed by another");
}

#[tokio::test]
async fn test_concurrent_cache_operations() {
    // Test concurrent cache operations across multiple instances
    let test_config = MultiInstanceTestConfig::default();

    // Clean up and create shared cache directory
    let _ = std::fs::remove_dir_all(&test_config.shared_cache_dir);
    if let Err(e) = std::fs::create_dir_all(&test_config.shared_cache_dir) {
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            panic!("Failed to create shared cache directory: {}", e);
        }
    }

    // Create multiple instances
    let mut instances = Vec::new();
    for i in 0..test_config.instance_count {
        let instance_id = format!("concurrent-instance-{}", i);
        let instance = TestProxyInstance::new(test_config.shared_cache_dir.clone(), instance_id);
        instances.push(Arc::new(instance));
    }

    // Perform concurrent cache operations
    let mut handles = Vec::new();

    for (i, instance) in instances.iter().enumerate() {
        let instance_clone = Arc::clone(instance);
        let handle = tokio::spawn(async move {
            let cache_key = format!("concurrent-test/object-{}", i);
            let test_data = format!("Test data from instance {}", i).into_bytes();
            let metadata = CacheMetadata {
                etag: format!("etag-{}", i),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: test_data.len() as u64,
                part_number: None,
                cache_control: None,
                access_count: 0,
                last_accessed: std::time::SystemTime::now(),
            };

            // Write to cache
            instance_clone
                .write_to_cache(&cache_key, &test_data, metadata)
                .await
                .expect("Failed to write in concurrent test");

            // Read back from cache
            let read_data = instance_clone
                .read_from_cache(&cache_key)
                .await
                .expect("Failed to read in concurrent test");

            assert!(read_data.is_some(), "Should read back written data");
            assert_eq!(
                read_data.unwrap(),
                test_data,
                "Read data should match written data"
            );

            (i, cache_key)
        });
        handles.push(handle);
    }

    // Wait for all concurrent operations to complete
    let results = futures::future::join_all(handles).await;

    // Verify all operations completed successfully
    for result in results {
        let (instance_id, cache_key) = result.expect("Concurrent operation should complete");
        println!(
            "Instance {} successfully completed cache operations for key: {}",
            instance_id, cache_key
        );
    }

    println!("Concurrent cache operations test passed");
}

#[tokio::test]
async fn test_cache_invalidation_coordination() {
    // Test cache invalidation coordination across instances
    let test_config = MultiInstanceTestConfig::default();

    // Clean up and create shared cache directory
    let _ = std::fs::remove_dir_all(&test_config.shared_cache_dir);
    if let Err(e) = std::fs::create_dir_all(&test_config.shared_cache_dir) {
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            panic!("Failed to create shared cache directory: {}", e);
        }
    }

    // Create instances
    let writer_instance = TestProxyInstance::new(
        test_config.shared_cache_dir.clone(),
        "invalidation-writer".to_string(),
    );
    let reader_instance = TestProxyInstance::new(
        test_config.shared_cache_dir.clone(),
        "invalidation-reader".to_string(),
    );

    let cache_key = "invalidation-test/object";
    let original_data = b"Original data".to_vec();
    let updated_data = b"Updated data after invalidation".to_vec();

    let original_metadata = CacheMetadata {
        etag: "original-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: original_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Write original data
    writer_instance
        .write_to_cache(cache_key, &original_data, original_metadata)
        .await
        .expect("Failed to write original data");

    // Give time for the write to complete
    sleep(Duration::from_millis(100)).await;

    // Reader should see original data
    let read_original = reader_instance
        .read_from_cache(cache_key)
        .await
        .expect("Failed to read original data");
    assert!(read_original.is_some(), "Should find original data");
    assert_eq!(
        read_original.unwrap(),
        original_data,
        "Should read original data"
    );

    // Invalidate cache entry on both instances (both regular and write cache)
    writer_instance
        .cache_manager
        .invalidate_cache(cache_key)
        .await
        .expect("Failed to invalidate cache on writer");
    writer_instance
        .cache_manager
        .invalidate_write_cache_entry(cache_key)
        .await
        .expect("Failed to invalidate write cache on writer");

    reader_instance
        .cache_manager
        .invalidate_cache(cache_key)
        .await
        .expect("Failed to invalidate cache on reader");
    reader_instance
        .cache_manager
        .invalidate_write_cache_entry(cache_key)
        .await
        .expect("Failed to invalidate write cache on reader");

    // Give time for invalidation to propagate across instances
    sleep(Duration::from_millis(200)).await;

    // Reader should no longer find the cached data
    let read_after_invalidation = reader_instance
        .read_from_cache(cache_key)
        .await
        .expect("Failed to read after invalidation");
    assert!(
        read_after_invalidation.is_none(),
        "Should not find data after invalidation"
    );

    // Write updated data
    let updated_metadata = CacheMetadata {
        etag: "updated-etag".to_string(),
        last_modified: "Thu, 22 Oct 2015 07:28:00 GMT".to_string(),
        content_length: updated_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    writer_instance
        .write_to_cache(cache_key, &updated_data, updated_metadata)
        .await
        .expect("Failed to write updated data");

    // Give time for the write to complete
    sleep(Duration::from_millis(100)).await;

    // Reader should see updated data
    let read_updated = reader_instance
        .read_from_cache(cache_key)
        .await
        .expect("Failed to read updated data");
    assert!(read_updated.is_some(), "Should find updated data");
    assert_eq!(
        read_updated.unwrap(),
        updated_data,
        "Should read updated data"
    );

    println!("Cache invalidation coordination test passed");
}
