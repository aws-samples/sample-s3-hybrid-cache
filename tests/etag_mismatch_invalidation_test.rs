//! ETag Mismatch Detection and Invalidation Test
//!
//! Tests that cached ranges with mismatched ETags are properly detected and invalidated.
//!
//! Requirements:
//! - Requirement 3.3: ETag mismatch detection and invalidation
//! - Task 6.1: ETag comparison in find_cached_ranges
//! - Task 6.2: Method to invalidate stale ranges

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::config::SharedStorageConfig;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use s3_proxy::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_etag_mismatch_detection_in_find_cached_ranges() -> Result<()> {
    // Create temporary cache directory
    let temp_dir = TempDir::new()?;
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false, // RAM cache disabled
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        true, // write_cache_enabled: true - required for write cache tests
        std::time::Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    ));

    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(DiskCacheManager::new(
        cache_dir.clone(),
        true,
        1024,
        false,
    )));

    let cache_key = "/test-bucket/test-object.txt";
    let old_etag = "\"old-etag-123\"";
    let new_etag = "\"new-etag-456\"";

    // Step 1: Store a range with old ETag using write cache
    let data = vec![b'A'; 1024];
    let headers = HashMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
        ("etag".to_string(), old_etag.to_string()),
        (
            "last-modified".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        ),
    ]);

    let metadata = CacheMetadata {
        etag: old_etag.to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: 1024,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(cache_key, &data, headers, metadata)
        .await?;

    // Step 2: Create range handler
    let range_handler = RangeHandler::new(cache_manager.clone(), disk_cache_manager.clone());

    // Step 3: Query with old ETag (should find the range)
    let requested_range = RangeSpec {
        start: 0,
        end: 1023,
    };

    let overlap_with_old_etag = range_handler
        .find_cached_ranges(cache_key, &requested_range, Some(old_etag), None)
        .await?;

    assert_eq!(
        overlap_with_old_etag.cached_ranges.len(),
        1,
        "Should find cached range when ETag matches"
    );
    assert!(
        overlap_with_old_etag.can_serve_from_cache,
        "Should be able to serve from cache when ETag matches"
    );

    // Step 4: Query with new ETag (should NOT find the range due to mismatch)
    let overlap_with_new_etag = range_handler
        .find_cached_ranges(cache_key, &requested_range, Some(new_etag), None)
        .await?;

    assert_eq!(
        overlap_with_new_etag.cached_ranges.len(),
        0,
        "Should NOT find cached range when ETag mismatches"
    );
    assert_eq!(
        overlap_with_new_etag.missing_ranges.len(),
        1,
        "Should report range as missing when ETag mismatches"
    );
    assert!(
        !overlap_with_new_etag.can_serve_from_cache,
        "Should NOT be able to serve from cache when ETag mismatches"
    );

    Ok(())
}

#[tokio::test]
async fn test_invalidate_stale_ranges() -> Result<()> {
    // Create temporary cache directory
    let temp_dir = TempDir::new()?;
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false, // RAM cache disabled
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        true, // write_cache_enabled: true - required for write cache tests
        std::time::Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let cache_key = "/test-bucket/test-object.txt";
    let old_etag = "\"old-etag-123\"";
    let new_etag = "\"new-etag-456\"";

    // Step 1: Store data with old ETag using write cache
    let data = vec![b'A'; 3072]; // 3KB of data
    let headers = HashMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
        ("etag".to_string(), old_etag.to_string()),
        (
            "last-modified".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        ),
    ]);

    let metadata = CacheMetadata {
        etag: old_etag.to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: 3072,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(cache_key, &data, headers, metadata)
        .await?;

    // Step 2: Verify data is stored
    let metadata_before = cache_manager.get_metadata_from_disk(cache_key).await?;
    assert!(metadata_before.is_some(), "Metadata should exist");
    let metadata_before = metadata_before.unwrap();
    assert!(
        !metadata_before.ranges.is_empty(),
        "Should have at least one range"
    );
    assert_eq!(metadata_before.object_metadata.etag, old_etag);

    // Step 3: Invalidate stale ranges with new ETag
    let invalidated = cache_manager
        .invalidate_stale_ranges(cache_key, new_etag)
        .await?;

    assert!(invalidated, "Should report that ranges were invalidated");

    // Step 4: Verify metadata is removed
    let metadata_after = cache_manager.get_metadata_from_disk(cache_key).await?;
    assert!(
        metadata_after.is_none(),
        "Metadata should be removed after invalidation"
    );

    // Step 5: Verify range files are removed
    // We can't easily check this without accessing private methods,
    // but the metadata removal is sufficient to verify invalidation worked

    Ok(())
}

#[tokio::test]
async fn test_invalidate_stale_ranges_no_mismatch() -> Result<()> {
    // Create temporary cache directory
    let temp_dir = TempDir::new()?;
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false, // RAM cache disabled
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        true, // write_cache_enabled: true - required for write cache tests
        std::time::Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let cache_key = "/test-bucket/test-object.txt";
    let etag = "\"same-etag-123\"";

    // Step 1: Store data using write cache
    let data = vec![b'A'; 1024];
    let headers = HashMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
        ("etag".to_string(), etag.to_string()),
        (
            "last-modified".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        ),
    ]);

    let metadata = CacheMetadata {
        etag: etag.to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: 1024,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_write_cache_entry(cache_key, &data, headers, metadata)
        .await?;

    // Step 2: Try to invalidate with same ETag (should not invalidate)
    let invalidated = cache_manager
        .invalidate_stale_ranges(cache_key, etag)
        .await?;

    assert!(!invalidated, "Should NOT invalidate when ETag matches");

    // Step 3: Verify metadata still exists
    let metadata_after = cache_manager.get_metadata_from_disk(cache_key).await?;
    assert!(
        metadata_after.is_some(),
        "Metadata should still exist when ETag matches"
    );
    assert!(
        !metadata_after.unwrap().ranges.is_empty(),
        "Ranges should still exist when ETag matches"
    );

    Ok(())
}

#[tokio::test]
async fn test_invalidate_stale_ranges_no_metadata() -> Result<()> {
    // Create temporary cache directory
    let temp_dir = TempDir::new()?;
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false, // RAM cache disabled
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        true, // write_cache_enabled: true - required for write cache tests
        std::time::Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    let cache_key = "/test-bucket/nonexistent-object.txt";
    let etag = "\"some-etag\"";

    // Try to invalidate non-existent metadata (should not error)
    let invalidated = cache_manager
        .invalidate_stale_ranges(cache_key, etag)
        .await?;

    assert!(!invalidated, "Should return false when no metadata exists");

    Ok(())
}
