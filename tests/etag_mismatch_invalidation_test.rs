//! ETag Mismatch Detection and Invalidation Test
//!
//! Tests that cached ranges with mismatched ETags are properly detected and invalidated.
//!
//! Requirements:
//! - Requirement 3.3: ETag mismatch detection and invalidation
//! - Task 6.1: ETag comparison in find_cached_ranges
//! - Task 6.2: Method to invalidate stale ranges

mod common;

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
        CacheEvictionAlgorithm::TinyLFU,
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
        95,                                 // eviction_trigger_percent
        80,                                 // eviction_target_percent
        true,                               // read_cache_enabled
        std::time::Duration::from_secs(60), // bucket_settings_staleness_threshold
        1_048_576,                          // compression_batch_size
        false,                              // evaluate_conditions_from_cache,
        std::time::Duration::from_secs(10), // ram_cache_flush_interval (Req 19)
        64,                                 // ram_cache_shard_count
        std::time::Duration::from_secs(5),  // upstream_first_byte_timeout
    ));

    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(DiskCacheManager::new(
        cache_dir.clone(),
        true,
        1024,
        false,
        1_048_576,
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

    common::put_through_write_cache(
        &cache_manager,
        cache_key,
        &data,
        headers,
        metadata,
        HashMap::new(),
    )
    .await?;

    // Step 2: Create range handler
    let range_handler = RangeHandler::new(cache_manager.clone(), disk_cache_manager.clone());

    // Step 3: Query with old ETag (should find the range)
    let requested_range = RangeSpec {
        start: 0,
        end: 1023,
    };

    let overlap_with_old_etag = range_handler
        .find_cached_ranges(
            cache_key,
            &requested_range,
            Some(old_etag),
            None,
            s3_proxy::cache_types::RangeLookupPurpose::FreshServe,
        )
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
        .find_cached_ranges(
            cache_key,
            &requested_range,
            Some(new_etag),
            None,
            s3_proxy::cache_types::RangeLookupPurpose::FreshServe,
        )
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
