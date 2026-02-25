//! Test for range request support from write cache
//!
//! This test verifies that PUT-cached objects can serve range requests
//! and that TTL transitions happen correctly.

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{CacheMetadata, UploadState};
use s3_proxy::config::SharedStorageConfig;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

#[tokio::test]
async fn test_range_request_from_write_cache() {
    // Create temporary cache directory
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager with short PUT_TTL and long GET_TTL
    let put_ttl = Duration::from_secs(60); // 1 minute
    let get_ttl = Duration::from_secs(3600); // 1 hour
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false, // RAM cache disabled for simplicity
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        get_ttl,
        Duration::from_secs(300), // HEAD_TTL
        put_ttl,
        false,
        SharedStorageConfig::default(),
        10.0,
        true,                       // write_cache_enabled: true - required for write cache tests
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    ));

    // Create disk cache manager for range handler
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(DiskCacheManager::new(
        cache_dir.clone(),
        true,
        1024,
        false,
    )));
    disk_cache_manager.write().await.initialize().await.unwrap();

    // Create range handler
    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Test data
    let cache_key = "/test-bucket/test-object.txt";
    let test_data = b"Hello, World! This is test data for range requests.";
    let content_length = test_data.len() as u64;

    // Create metadata
    let metadata = CacheMetadata {
        etag: "test-etag-123".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Store as write cache entry (simulating PUT request)
    println!("Storing PUT-cached object...");
    cache_manager
        .store_write_cache_entry(cache_key, test_data, HashMap::new(), metadata.clone())
        .await
        .unwrap();

    // Verify metadata was created with PUT_TTL
    let disk_cache = disk_cache_manager.read().await;
    let stored_metadata = disk_cache.get_metadata(cache_key).await.unwrap().unwrap();
    let now = SystemTime::now();
    let time_until_expiry = stored_metadata.expires_at.duration_since(now).unwrap();

    // Should expire within PUT_TTL window (with some tolerance)
    assert!(time_until_expiry <= put_ttl + Duration::from_secs(5));
    assert!(time_until_expiry >= put_ttl - Duration::from_secs(5));

    // Verify upload_state is Complete
    assert_eq!(
        stored_metadata.object_metadata.upload_state,
        UploadState::Complete
    );

    println!("✓ Object stored with PUT_TTL and upload_state=Complete");
    drop(disk_cache);

    // Test 1: Serve range request from write cache
    println!("\nTest 1: Serving range request from write cache...");
    let range_spec = RangeSpec { start: 7, end: 18 };

    // Find cached ranges
    let overlap = range_handler
        .find_cached_ranges(cache_key, &range_spec, None, None)
        .await
        .unwrap();

    // Should be able to serve from cache
    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve range from write cache"
    );
    assert_eq!(
        overlap.missing_ranges.len(),
        0,
        "Should have no missing ranges"
    );
    assert_eq!(
        overlap.cached_ranges.len(),
        1,
        "Should have one cached range"
    );

    println!("✓ Range found in write cache");

    // Load the cached range data (this loads the full cached range 0-51)
    let cached_range = &overlap.cached_ranges[0];
    let (cached_data, _is_ram_hit) = cache_manager
        .load_range_data_with_cache(cache_key, cached_range, &range_handler)
        .await
        .unwrap();

    // Extract the requested sub-range from the cached data
    // This is what serve_range_from_cache does
    let offset_in_range = (range_spec.start - cached_range.start) as usize;
    let length = (range_spec.end - range_spec.start + 1) as usize;
    let range_data = &cached_data[offset_in_range..offset_in_range + length];

    // Verify the data is correct
    let expected_data = &test_data[7..=18];
    assert_eq!(range_data, expected_data, "Range data should match");
    println!(
        "✓ Range data correct: {:?}",
        String::from_utf8_lossy(range_data)
    );

    // Test 2: TTL transition on first GET
    println!("\nTest 2: Testing TTL transition...");

    // Trigger TTL transition
    cache_manager
        .transition_to_get_ttl(cache_key)
        .await
        .unwrap();

    // Verify metadata was updated with GET_TTL
    let disk_cache = disk_cache_manager.read().await;
    let updated_metadata = disk_cache.get_metadata(cache_key).await.unwrap().unwrap();
    let now = SystemTime::now();
    let new_time_until_expiry = updated_metadata.expires_at.duration_since(now).unwrap();

    // Should now expire within GET_TTL window (with some tolerance)
    assert!(
        new_time_until_expiry > put_ttl,
        "Should have transitioned to GET_TTL"
    );
    assert!(new_time_until_expiry <= get_ttl + Duration::from_secs(5));
    assert!(new_time_until_expiry >= get_ttl - Duration::from_secs(5));

    println!("✓ TTL transitioned from PUT_TTL to GET_TTL");
    drop(disk_cache);

    // Test 3: Verify range data still accessible after TTL transition
    println!("\nTest 3: Verifying range access after TTL transition...");

    let overlap2 = range_handler
        .find_cached_ranges(cache_key, &range_spec, None, None)
        .await
        .unwrap();
    assert!(
        overlap2.can_serve_from_cache,
        "Should still be able to serve range after TTL transition"
    );

    let cached_range2 = &overlap2.cached_ranges[0];
    let (cached_data2, _is_ram_hit) = cache_manager
        .load_range_data_with_cache(cache_key, cached_range2, &range_handler)
        .await
        .unwrap();

    let offset_in_range2 = (range_spec.start - cached_range2.start) as usize;
    let length2 = (range_spec.end - range_spec.start + 1) as usize;
    let range_data2 = &cached_data2[offset_in_range2..offset_in_range2 + length2];

    assert_eq!(
        range_data2, expected_data,
        "Range data should still be correct after TTL transition"
    );
    println!("✓ Range data still accessible and correct after TTL transition");

    // Test 4: Test different range
    println!("\nTest 4: Testing different range...");
    let range_spec2 = RangeSpec { start: 0, end: 4 };

    let overlap3 = range_handler
        .find_cached_ranges(cache_key, &range_spec2, None, None)
        .await
        .unwrap();
    assert!(
        overlap3.can_serve_from_cache,
        "Should be able to serve different range"
    );

    let cached_range3 = &overlap3.cached_ranges[0];
    let (cached_data3, _is_ram_hit) = cache_manager
        .load_range_data_with_cache(cache_key, cached_range3, &range_handler)
        .await
        .unwrap();

    let offset_in_range3 = (range_spec2.start - cached_range3.start) as usize;
    let length3 = (range_spec2.end - range_spec2.start + 1) as usize;
    let range_data3 = &cached_data3[offset_in_range3..offset_in_range3 + length3];

    let expected_data2 = &test_data[0..=4];
    assert_eq!(
        range_data3, expected_data2,
        "Different range data should be correct"
    );
    println!(
        "✓ Different range served correctly: {:?}",
        String::from_utf8_lossy(range_data3)
    );

    println!("\n✅ All tests passed!");
}

#[tokio::test]
async fn test_full_object_range_from_write_cache() {
    // Create temporary cache directory
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(300),
        Duration::from_secs(60),
        false,
        SharedStorageConfig::default(),
        10.0,
        true,                       // write_cache_enabled: true - required for write cache tests
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    ));

    // Create disk cache manager
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(DiskCacheManager::new(
        cache_dir.clone(),
        true,
        1024,
        false,
    )));
    disk_cache_manager.write().await.initialize().await.unwrap();

    // Create range handler
    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Test data
    let cache_key = "/test-bucket/full-object.bin";
    let test_data = b"Full object data for testing complete range requests.";
    let content_length = test_data.len() as u64;

    // Create metadata
    let metadata = CacheMetadata {
        etag: "full-object-etag".to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Store as write cache entry
    cache_manager
        .store_write_cache_entry(cache_key, test_data, HashMap::new(), metadata.clone())
        .await
        .unwrap();

    // Request full object as range (0 to content_length-1)
    let range_spec = RangeSpec {
        start: 0,
        end: content_length - 1,
    };

    let overlap = range_handler
        .find_cached_ranges(cache_key, &range_spec, None, None)
        .await
        .unwrap();

    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve full object from write cache"
    );
    assert_eq!(overlap.missing_ranges.len(), 0);

    let cached_range = &overlap.cached_ranges[0];
    let (range_data, _is_ram_hit) = cache_manager
        .load_range_data_with_cache(cache_key, cached_range, &range_handler)
        .await
        .unwrap();

    assert_eq!(range_data, test_data, "Full object data should match");

    println!("✅ Full object range request test passed!");
}
