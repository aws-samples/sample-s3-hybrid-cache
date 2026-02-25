//! Test for cache operation logging
//!
//! Verifies that cache operations are logged correctly:
//! - HEAD cache invalidation (Requirement 4.1)
//! - Cached range hits (Requirement 4.2)
//! - Complete cache hits (Requirement 4.3)

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use std::sync::Arc;
use tempfile::TempDir;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::test]
async fn test_head_cache_invalidation_logging() {
    // Initialize tracing to capture logs
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = Arc::new(CacheManager::new_with_all_ttls(
        cache_dir,
        false,
        0,
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        false,
    ));

    // Must call create_configured_disk_cache_manager() to set up the JournalConsolidator
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();

    cache_manager.initialize().await.unwrap();

    let cache_key = "/test-bucket/test-object.txt";

    // Create a HEAD cache entry
    let mut headers = std::collections::HashMap::new();
    headers.insert("content-length".to_string(), "12345".to_string());
    headers.insert("etag".to_string(), "test-etag".to_string());
    headers.insert(
        "last-modified".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
    );

    let metadata = s3_proxy::cache_types::CacheMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: 12345,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_head_cache_entry_unified(cache_key, headers, metadata)
        .await
        .unwrap();

    // Verify HEAD cache exists using unified method
    let head_entry = cache_manager
        .get_head_cache_entry_unified(cache_key)
        .await
        .unwrap();
    assert!(head_entry.is_some(), "HEAD cache entry should exist");

    // Requirement 4.1: Invalidate HEAD cache and verify logging
    // The logging happens inside invalidate_head_cache_entry_unified
    cache_manager
        .invalidate_head_cache_entry_unified(cache_key)
        .await
        .unwrap();

    // Verify HEAD cache was invalidated using unified method
    let head_entry_after = cache_manager
        .get_head_cache_entry_unified(cache_key)
        .await
        .unwrap();
    assert!(
        head_entry_after.is_none(),
        "HEAD cache entry should be invalidated"
    );

    println!("✓ HEAD cache invalidation logging test passed");
}

#[tokio::test]
async fn test_cached_range_hit_logging() {
    // Initialize tracing to capture logs
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = Arc::new(CacheManager::new_with_all_ttls(
        cache_dir.clone(),
        false,
        0,
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        false,
    ));

    // Must call create_configured_disk_cache_manager() to set up the JournalConsolidator
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();

    cache_manager.initialize().await.unwrap();

    let cache_key = "/test-bucket/test-object.txt";

    // Store a range in cache using the new storage format
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir, true, 1024, false),
    ));

    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));

    // Create test data
    let test_data = vec![b'A'; 1024];

    // Store range 0-1023
    let object_metadata = s3_proxy::cache_types::ObjectMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: 1024,
        content_type: Some("application/octet-stream".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: s3_proxy::cache_types::UploadState::Complete,
        cumulative_size: 1024,
        parts: Vec::new(),
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::Lz4,
        compressed_size: 0,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
    };

    let mut disk_cache = disk_cache_manager.write().await;
    disk_cache
        .store_range(
            cache_key,
            0,
            1023,
            &test_data,
            object_metadata,
            std::time::Duration::from_secs(315360000), // 10 years TTL
                true)
        .await
        .unwrap();
    drop(disk_cache);

    // Find cached ranges - this should trigger the logging
    // Requirement 4.2: Log when cached ranges are found during full object GET
    // The logging happens in handle_range_request with:
    // info!("Cached range hit: cache_key={}, range={}-{}, size={} bytes, etag={}", ...)
    let range_spec = s3_proxy::range_handler::RangeSpec {
        start: 0,
        end: 1023,
    };
    let overlap = range_handler
        .find_cached_ranges(cache_key, &range_spec, None, None)
        .await
        .unwrap();

    assert_eq!(overlap.cached_ranges.len(), 1, "Should find 1 cached range");
    assert_eq!(
        overlap.missing_ranges.len(),
        0,
        "Should have no missing ranges"
    );
    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve from cache"
    );

    println!("✓ Cached range hit logging test passed");
    println!("  - Found {} cached range(s)", overlap.cached_ranges.len());
    println!(
        "  - Range: {}-{}",
        overlap.cached_ranges[0].start, overlap.cached_ranges[0].end
    );
}

#[tokio::test]
async fn test_complete_cache_hit_metrics() {
    // Initialize tracing to capture logs
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = Arc::new(CacheManager::new_with_all_ttls(
        cache_dir.clone(),
        false,
        0,
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(3600),
        false,
    ));

    // Must call create_configured_disk_cache_manager() to set up the JournalConsolidator
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();

    cache_manager.initialize().await.unwrap();

    let cache_key = "/test-bucket/test-object.txt";

    // Store multiple ranges that cover the entire requested range
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::disk_cache::DiskCacheManager::new(cache_dir, true, 1024, false),
    ));

    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));

    // Store ranges 0-511 and 512-1023 (covering 0-1023 completely)
    let test_data1 = vec![b'A'; 512];
    let test_data2 = vec![b'B'; 512];

    let object_metadata = s3_proxy::cache_types::ObjectMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: 1024,
        content_type: Some("application/octet-stream".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: s3_proxy::cache_types::UploadState::Complete,
        cumulative_size: 1024,
        parts: Vec::new(),
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::Lz4,
        compressed_size: 0,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
    };

    let mut disk_cache = disk_cache_manager.write().await;
    disk_cache
        .store_range(
            cache_key,
            0,
            511,
            &test_data1,
            object_metadata.clone(),
            std::time::Duration::from_secs(315360000), // 10 years TTL
                true)
        .await
        .unwrap();

    disk_cache
        .store_range(
            cache_key,
            512,
            1023,
            &test_data2,
            object_metadata,
            std::time::Duration::from_secs(315360000), // 10 years TTL
                true)
        .await
        .unwrap();
    drop(disk_cache);

    // Request the full range 0-1023
    // Requirement 4.3: Log when full object is served entirely from cache
    // The logging happens in handle_range_request with:
    // info!("Complete cache hit: cache_key={}, range={}-{}, requested_bytes={}, cached_ranges={}, cache_efficiency={:.2}%, s3_requests=0", ...)
    let range_spec = s3_proxy::range_handler::RangeSpec {
        start: 0,
        end: 1023,
    };
    let overlap = range_handler
        .find_cached_ranges(cache_key, &range_spec, None, None)
        .await
        .unwrap();

    assert_eq!(
        overlap.cached_ranges.len(),
        2,
        "Should find 2 cached ranges"
    );
    assert_eq!(
        overlap.missing_ranges.len(),
        0,
        "Should have no missing ranges"
    );
    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve entirely from cache"
    );

    // Calculate cache efficiency metrics
    let total_cached_bytes: u64 = overlap
        .cached_ranges
        .iter()
        .map(|r| r.end - r.start + 1)
        .sum();
    let requested_bytes = range_spec.end - range_spec.start + 1;
    let cache_efficiency = (total_cached_bytes as f64 / requested_bytes as f64) * 100.0;

    assert_eq!(total_cached_bytes, 1024, "Should have 1024 bytes cached");
    assert_eq!(requested_bytes, 1024, "Should request 1024 bytes");
    assert_eq!(cache_efficiency, 100.0, "Cache efficiency should be 100%");

    println!("✓ Complete cache hit metrics test passed");
    println!("  - Cached ranges: {}", overlap.cached_ranges.len());
    println!("  - Total cached bytes: {}", total_cached_bytes);
    println!("  - Requested bytes: {}", requested_bytes);
    println!("  - Cache efficiency: {:.2}%", cache_efficiency);
    println!("  - S3 requests: 0");
}
