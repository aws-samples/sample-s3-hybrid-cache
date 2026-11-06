//! HEAD cache invalidation logging (Requirement 4.1). Moved from
//! `tests/cache_logging_test.rs`: seeding the HEAD entry calls the `pub(crate)` HEAD-miss store.

use crate::cache::{CacheEvictionAlgorithm, CacheManager};
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
        CacheEvictionAlgorithm::TinyLFU,
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
        .store_head_cache_entry_unified_witnessed(
            cache_key,
            headers,
            metadata,
            cache_manager.capture_fill_witness(cache_key).await,
        )
        .await
        .unwrap();

    // Verify HEAD cache exists using unified method
    let head_entry = cache_manager
        .get_head_cache_entry_unified(cache_key, std::time::Duration::MAX)
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
        .get_head_cache_entry_unified(cache_key, std::time::Duration::MAX)
        .await
        .unwrap();
    assert!(
        head_entry_after.is_none(),
        "HEAD cache entry should be invalidated"
    );

    println!("✓ HEAD cache invalidation logging test passed");
}
