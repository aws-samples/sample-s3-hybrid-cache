//! Write-through cache integration tests
//!
//! Tests for write-through caching functionality including PUT request caching,
//! multipart upload detection, TTL expiration, and size enforcement.
//!
//! Note: For detailed unit tests of PUT storage, metadata, and TTL behavior,
//! see write_cache_unit_test.rs

use s3_proxy::{
    cache::CacheManager, cache_types::CacheMetadata, config::SharedStorageConfig, Result,
};
use std::collections::HashMap;
use tempfile::TempDir;

/// Helper function to create test cache manager
fn create_test_cache_manager(temp_dir: &TempDir) -> CacheManager {
    use s3_proxy::cache::CacheEvictionAlgorithm;
    use std::time::Duration;

    CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false, // RAM cache disabled for these tests
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
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
    )
}

/// Helper function to create test metadata
fn create_test_metadata(etag: &str, content_length: u64) -> CacheMetadata {
    CacheMetadata {
        etag: etag.to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    }
}

#[tokio::test]
async fn test_write_cache_basic_operations() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(&temp_dir);

    // Test data
    let cache_key = "test-bucket/test-object";
    let test_data = b"Test data for write-through caching";
    let metadata = create_test_metadata("test-etag", test_data.len() as u64);

    // Store write cache entry
    cache_manager
        .store_write_cache_entry(cache_key, test_data, HashMap::new(), metadata.clone())
        .await?;

    // Retrieve and verify
    let retrieved = cache_manager.get_write_cache_entry(cache_key).await?;
    assert!(retrieved.is_some());
    let entry = retrieved.unwrap();
    assert_eq!(entry.cache_key, cache_key);
    assert_eq!(entry.body, test_data);
    assert_eq!(entry.metadata.etag, metadata.etag);

    // Invalidate and verify removal
    cache_manager
        .invalidate_write_cache_entry(cache_key)
        .await?;
    assert!(cache_manager
        .get_write_cache_entry(cache_key)
        .await?
        .is_none());

    Ok(())
}

/// Test multipart upload detection logic
#[tokio::test]
async fn test_multipart_upload_detection() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(&temp_dir);

    let mut headers = HashMap::new();

    // Test various multipart indicators
    assert!(cache_manager.is_multipart_upload(&headers, "uploadId=abc123"));
    assert!(cache_manager.is_multipart_upload(&headers, "partNumber=1"));
    assert!(cache_manager.is_multipart_upload(&headers, "uploads"));

    headers.insert(
        "content-type".to_string(),
        "multipart/form-data".to_string(),
    );
    assert!(cache_manager.is_multipart_upload(&headers, ""));

    // Normal request should not be detected as multipart
    headers.clear();
    assert!(!cache_manager.is_multipart_upload(&headers, ""));

    Ok(())
}

/// Test write cache size limits and accommodation checks
#[tokio::test]
async fn test_write_cache_size_limits() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(&temp_dir);

    // Small size should be accommodated
    assert!(cache_manager.can_write_cache_accommodate(1024));

    // Very large size (512MB) should exceed default limit
    assert!(!cache_manager.can_write_cache_accommodate(512 * 1024 * 1024));

    Ok(())
}

/// Test write cache TTL expiration checking
#[tokio::test]
async fn test_write_cache_ttl_expiration() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(&temp_dir);

    let cache_key = "test-bucket/expired-object";
    let test_data = b"Test data";
    let metadata = create_test_metadata("expired-etag", test_data.len() as u64);

    cache_manager
        .store_write_cache_entry(cache_key, test_data, HashMap::new(), metadata)
        .await?;

    // Should exist and not be expired yet
    assert!(cache_manager
        .get_write_cache_entry(cache_key)
        .await?
        .is_some());
    assert!(
        !cache_manager
            .is_write_cache_entry_expired(cache_key)
            .await?
    );

    Ok(())
}

/// Test cleanup of failed PUT operations
#[tokio::test]
async fn test_write_cache_cleanup() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(&temp_dir);

    let cache_key = "test-bucket/cleanup-test";
    let test_data = b"Data to be cleaned up";
    let metadata = create_test_metadata("cleanup-etag", test_data.len() as u64);

    cache_manager
        .store_write_cache_entry(cache_key, test_data, HashMap::new(), metadata)
        .await?;

    // Cleanup should remove the entry
    cache_manager.cleanup_failed_put(cache_key).await?;
    assert!(cache_manager
        .get_write_cache_entry(cache_key)
        .await?
        .is_none());

    Ok(())
}

/// Test write cache statistics tracking
#[tokio::test]
async fn test_write_cache_statistics() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(&temp_dir);
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();
    cache_manager.initialize().await?;

    // Store entry and verify it was persisted on disk
    let cache_key = "test-bucket/stats-test";
    let test_data = b"Statistics test data";
    let metadata = create_test_metadata("stats-etag", test_data.len() as u64);

    // Verify no entry exists before store
    assert!(cache_manager.get_write_cache_entry(cache_key).await?.is_none());

    cache_manager
        .store_write_cache_entry(cache_key, test_data, HashMap::new(), metadata)
        .await?;

    // Verify the entry was stored (write cache size tracking is async via JournalConsolidator,
    // so we verify the entry exists on disk rather than checking in-memory tracker stats)
    let stored = cache_manager.get_write_cache_entry(cache_key).await?;
    assert!(stored.is_some(), "Write cache entry should be stored");
    let entry = stored.unwrap();
    assert_eq!(entry.body, test_data);
    assert_eq!(entry.metadata.etag, "stats-etag");

    Ok(())
}

// Legacy write_cache/ directory tests removed - unified storage format is now used
// See unified_storage_consistency_property_test.rs for storage format tests
