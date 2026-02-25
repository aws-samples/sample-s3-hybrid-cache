//! Cache Cleanup Tests for New Format
//!
//! Tests cache cleanup operations with the new cache key format (without hostname).
//! Validates Requirements 8.1, 8.2, 8.3, 8.4 from cache-key-simplification spec.

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::Result;
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::TempDir;

/// Helper to create test metadata
fn create_test_metadata(etag: &str, content_length: u64) -> CacheMetadata {
    CacheMetadata {
        etag: etag.to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    }
}

/// Helper to create and initialize a test cache manager with JournalConsolidator
async fn create_test_cache_manager(cache_dir: std::path::PathBuf) -> Arc<CacheManager> {
    let cache_manager = Arc::new(CacheManager::new_with_defaults(cache_dir, false, 0));

    // Must call create_configured_disk_cache_manager() to set up the JournalConsolidator
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();

    // Now initialize the cache manager (which requires the consolidator to exist)
    cache_manager.initialize().await.unwrap();

    cache_manager
}

/// Test cache entry identification with new format (path-only, no hostname)
/// Requirement 8.1: Cache cleanup correctly identifies cache entries using the new format
#[tokio::test]
async fn test_cache_entry_identification_new_format() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    // Test various cache key formats
    let test_cases = vec![
        ("bucket/object.jpg", b"Full object" as &[u8]),
        ("bucket/object.jpg:version:abc123", b"Versioned" as &[u8]),
        ("bucket/object.jpg:range:0-8388607", b"Range" as &[u8]),
        (
            "bucket/object.jpg:version:abc123:range:0-8388607",
            b"Versioned range" as &[u8],
        ),
    ];

    for (cache_key, data) in &test_cases {
        cache_manager
            .store_response(
                cache_key,
                data,
                create_test_metadata(&format!("etag-{}", cache_key), data.len() as u64),
            )
            .await?;
    }

    // Verify all entries can be retrieved
    for (cache_key, _) in &test_cases {
        assert!(
            cache_manager
                .get_cached_response(cache_key)
                .await?
                .is_some(),
            "Cache entry should be identified: {}",
            cache_key
        );
    }

    // Verify eviction can identify all entries
    let entries = cache_manager.collect_cache_entries_for_eviction().await?;
    assert!(
        entries.len() >= test_cases.len(),
        "Should identify at least {} cache entries, found {}",
        test_cases.len(),
        entries.len()
    );

    Ok(())
}

/// Test cache eviction with new format
/// Requirement 8.2: Cache eviction properly deletes cache entries with new format
#[tokio::test]
async fn test_cache_eviction_new_format() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    let cache_keys = vec!["bucket/file1.txt", "bucket/file2.txt", "bucket/file3.txt"];

    // Store entries
    for cache_key in &cache_keys {
        let data = format!("Data for {}", cache_key).into_bytes();
        cache_manager
            .store_response(
                cache_key,
                &data,
                create_test_metadata(&format!("etag-{}", cache_key), data.len() as u64),
            )
            .await?;
    }

    // Verify all exist
    for key in &cache_keys {
        assert!(cache_manager.get_cached_response(key).await?.is_some());
    }

    // Invalidate first entry
    cache_manager.invalidate_cache(&cache_keys[0]).await?;

    // Verify first is gone, others remain
    assert!(cache_manager
        .get_cached_response(&cache_keys[0])
        .await?
        .is_none());
    for key in &cache_keys[1..] {
        assert!(cache_manager.get_cached_response(key).await?.is_some());
    }

    Ok(())
}

/// Test that all associated files are deleted correctly
/// Requirement 8.3: Cache cleanup properly deletes all associated files
#[tokio::test]
async fn test_all_associated_files_deleted() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    let cache_key = "bucket/test-object.bin";
    let test_data = b"Test data for file deletion";

    cache_manager
        .store_response(
            cache_key,
            test_data,
            create_test_metadata("test-etag", test_data.len() as u64),
        )
        .await?;

    assert!(cache_manager
        .get_cached_response(cache_key)
        .await?
        .is_some());

    // Verify files exist before deletion
    let cache_file_path = temp_dir.path().join("metadata");
    let files_before: Vec<_> = std::fs::read_dir(&cache_file_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert!(!files_before.is_empty());

    // Invalidate and verify cleanup
    cache_manager.invalidate_cache(cache_key).await?;
    assert!(cache_manager
        .get_cached_response(cache_key)
        .await?
        .is_none());

    // Verify associated files are deleted
    let files_after: Vec<_> = std::fs::read_dir(&cache_file_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let path = e.path().to_string_lossy().to_string();
            path.contains(&cache_key.replace("/", "%2F"))
        })
        .collect();

    assert!(
        files_after.is_empty(),
        "All associated files should be deleted"
    );

    Ok(())
}

/// Test cache size calculation with new format
/// Requirement 8.4: Cache size calculation works correctly with new format
/// Note: With accumulator-based size tracking, size updates require a consolidation cycle
#[tokio::test]
async fn test_cache_size_calculation_new_format() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    let initial_size = cache_manager.get_cache_size_stats().await?.read_cache_size;

    // Store two entries
    let test_data_1 = vec![0u8; 1024]; // 1 KB
    let test_data_2 = vec![0u8; 2048]; // 2 KB

    cache_manager
        .store_response(
            "bucket/file1.bin",
            &test_data_1,
            create_test_metadata("etag1", test_data_1.len() as u64),
        )
        .await?;
    cache_manager
        .store_response(
            "bucket/file2.bin",
            &test_data_2,
            create_test_metadata("etag2", test_data_2.len() as u64),
        )
        .await?;

    // Run consolidation cycle to flush accumulator and update size_state.json
    // This is required because accumulator-based size tracking defers updates to consolidation
    if let Some(consolidator) = cache_manager.get_journal_consolidator().await {
        let _ = consolidator.run_consolidation_cycle().await;
    }

    let size_after = cache_manager.get_cache_size_stats().await?.read_cache_size;
    assert!(
        size_after > initial_size,
        "Cache size should increase after storing entries (initial={}, after={})",
        initial_size, size_after
    );

    // Delete one entry
    cache_manager.invalidate_cache("bucket/file1.bin").await?;

    // Run consolidation cycle again to update size_state.json after invalidation
    if let Some(consolidator) = cache_manager.get_journal_consolidator().await {
        let _ = consolidator.run_consolidation_cycle().await;
    }

    let size_after_delete = cache_manager.get_cache_size_stats().await?.read_cache_size;
    assert!(
        size_after_delete < size_after,
        "Cache size should decrease after deletion (after_store={}, after_delete={})",
        size_after, size_after_delete
    );

    Ok(())
}

/// Test cache cleanup with versioned objects in new format
#[tokio::test]
async fn test_cache_cleanup_versioned_objects_new_format() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    let cache_key_v1 = "bucket/versioned-object.txt:version:version1";
    let cache_key_v2 = "bucket/versioned-object.txt:version:version2";

    cache_manager
        .store_response(
            cache_key_v1,
            b"Version 1 data",
            create_test_metadata("etag-v1", 14),
        )
        .await?;
    cache_manager
        .store_response(
            cache_key_v2,
            b"Version 2 data",
            create_test_metadata("etag-v2", 14),
        )
        .await?;

    // Verify both versions exist
    assert!(cache_manager
        .get_cached_response(cache_key_v1)
        .await?
        .is_some());
    assert!(cache_manager
        .get_cached_response(cache_key_v2)
        .await?
        .is_some());

    // Invalidate version 1, verify version 2 remains
    cache_manager.invalidate_cache(cache_key_v1).await?;
    assert!(cache_manager
        .get_cached_response(cache_key_v1)
        .await?
        .is_none());
    assert!(cache_manager
        .get_cached_response(cache_key_v2)
        .await?
        .is_some());

    Ok(())
}

/// Test cache cleanup with range requests in new format
#[tokio::test]
async fn test_cache_cleanup_range_requests_new_format() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    let cache_key_range1 = "bucket/large-file.bin:range:0-1023";
    let cache_key_range2 = "bucket/large-file.bin:range:1024-2047";

    let data_range1 = vec![0u8; 512]; // Reduced size for faster test
    let data_range2 = vec![1u8; 512];

    cache_manager
        .store_response(
            cache_key_range1,
            &data_range1,
            create_test_metadata("etag-range", data_range1.len() as u64),
        )
        .await?;
    cache_manager
        .store_response(
            cache_key_range2,
            &data_range2,
            create_test_metadata("etag-range", data_range2.len() as u64),
        )
        .await?;

    // Verify both ranges exist
    assert!(cache_manager
        .get_cached_response(cache_key_range1)
        .await?
        .is_some());
    assert!(cache_manager
        .get_cached_response(cache_key_range2)
        .await?
        .is_some());

    // Invalidate range 1, verify range 2 remains
    cache_manager.invalidate_cache(cache_key_range1).await?;
    assert!(cache_manager
        .get_cached_response(cache_key_range1)
        .await?
        .is_none());
    assert!(cache_manager
        .get_cached_response(cache_key_range2)
        .await?
        .is_some());

    Ok(())
}

/// Test comprehensive cache cleanup across all cache types
#[tokio::test]
async fn test_comprehensive_cache_cleanup_new_format() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    // Create various types of cache entries
    let entries = vec![
        ("bucket/full-object.txt", b"Full object" as &[u8]),
        ("bucket/versioned.txt:version:v1", b"Versioned" as &[u8]),
        ("bucket/range.bin:range:0-1023", b"Range" as &[u8]),
        (
            "bucket/versioned-range.bin:version:v2:range:0-1023",
            b"Versioned range" as &[u8],
        ),
    ];

    for (cache_key, data) in &entries {
        cache_manager
            .store_response(
                cache_key,
                data,
                create_test_metadata(&format!("etag-{}", cache_key), data.len() as u64),
            )
            .await?;
    }

    // Verify all entries exist
    for (cache_key, _) in &entries {
        assert!(cache_manager
            .get_cached_response(cache_key)
            .await?
            .is_some());
    }

    // Cleanup should not remove non-expired entries
    cache_manager
        .cleanup_expired_entries_comprehensive()
        .await?;
    for (cache_key, _) in &entries {
        assert!(
            cache_manager
                .get_cached_response(cache_key)
                .await?
                .is_some(),
            "Non-expired entry should remain: {}",
            cache_key
        );
    }

    // Invalidate all and verify removal
    for (cache_key, _) in &entries {
        cache_manager.invalidate_cache(cache_key).await?;
        assert!(cache_manager
            .get_cached_response(cache_key)
            .await?
            .is_none());
    }

    Ok(())
}
