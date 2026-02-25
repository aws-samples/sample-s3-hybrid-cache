//! Tests for shared cache coordination functionality

use s3_proxy::{cache::CacheManager, cache_types::CacheMetadata, Result};

use tempfile::TempDir;
use tokio;

#[tokio::test]
async fn test_write_lock_coordination() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let cache_key = "test-bucket/test-object";

    // Test acquiring write lock
    let lock_acquired = cache_manager
        .acquire_write_lock_with_timeout(cache_key, std::time::Duration::from_secs(1))
        .await?;

    assert!(lock_acquired, "Should be able to acquire write lock");

    // Test that second lock acquisition fails (timeout)
    let second_lock = cache_manager
        .acquire_write_lock_with_timeout(cache_key, std::time::Duration::from_millis(100))
        .await?;

    assert!(!second_lock, "Should not be able to acquire second lock");

    // Release the lock
    cache_manager.release_write_lock(cache_key).await?;

    // Now should be able to acquire lock again
    let third_lock = cache_manager
        .acquire_write_lock_with_timeout(cache_key, std::time::Duration::from_secs(1))
        .await?;

    assert!(third_lock, "Should be able to acquire lock after release");

    // Clean up
    cache_manager.release_write_lock(cache_key).await?;

    Ok(())
}

#[tokio::test]
async fn test_cache_entry_active_detection() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let cache_key = "test-bucket/active-test";

    // Initially, entry should not be active
    let is_active = cache_manager.is_cache_entry_active(cache_key).await?;
    assert!(!is_active, "Entry should not be active initially");

    // Acquire lock to make entry active
    let lock_acquired = cache_manager
        .acquire_write_lock_with_timeout(cache_key, std::time::Duration::from_secs(1))
        .await?;
    assert!(lock_acquired, "Should acquire lock");

    // Now entry should be active
    let is_active_after_lock = cache_manager.is_cache_entry_active(cache_key).await?;
    assert!(
        is_active_after_lock,
        "Entry should be active after acquiring lock"
    );

    // Release lock
    cache_manager.release_write_lock(cache_key).await?;

    // Entry should no longer be active
    let is_active_after_release = cache_manager.is_cache_entry_active(cache_key).await?;
    assert!(
        !is_active_after_release,
        "Entry should not be active after releasing lock"
    );

    Ok(())
}

#[tokio::test]
async fn test_coordinated_cache_cleanup() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let cache_key = "test-bucket/cleanup-test";
    let test_data = b"Test data for cleanup";
    let metadata = CacheMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Store a cache entry
    cache_manager
        .store_response(cache_key, test_data, metadata)
        .await?;

    // Verify entry exists
    let cached_entry = cache_manager.get_cached_response(cache_key).await?;
    assert!(cached_entry.is_some(), "Cache entry should exist");

    // Run coordinated cleanup (should not remove non-expired entries)
    let cleaned_count = cache_manager.coordinate_cleanup().await?;

    // Entry should still exist (not expired)
    let cached_entry_after_cleanup = cache_manager.get_cached_response(cache_key).await?;
    assert!(
        cached_entry_after_cleanup.is_some(),
        "Non-expired entry should still exist after cleanup"
    );

    // Force invalidate the entry
    cache_manager.force_invalidate_cache(cache_key).await?;

    // Entry should be gone
    let cached_entry_after_invalidation = cache_manager.get_cached_response(cache_key).await?;
    assert!(
        cached_entry_after_invalidation.is_none(),
        "Entry should be gone after force invalidation"
    );

    Ok(())
}

#[tokio::test]
async fn test_cache_consistency_check() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let cache_key = "test-bucket/consistency-test";
    let test_data = b"Test data for consistency check";
    let metadata = CacheMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Store a cache entry
    cache_manager
        .store_response(cache_key, test_data, metadata)
        .await?;

    // Check consistency immediately after storage
    let is_consistent = cache_manager.check_cache_consistency(cache_key).await?;
    assert!(
        is_consistent,
        "Newly created cache entry should be consistent"
    );

    // Double-check by retrieving the entry
    let cached_response = cache_manager.get_cached_response(cache_key).await?;
    assert!(
        cached_response.is_some(),
        "Cache entry should be retrievable after storage"
    );

    // Check consistency for non-existent entry
    let non_existent_consistent = cache_manager
        .check_cache_consistency("non-existent-key")
        .await?;
    assert!(
        !non_existent_consistent,
        "Non-existent entry should not be consistent"
    );

    Ok(())
}

#[tokio::test]
async fn test_coordination_statistics() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    // Get initial coordination stats
    let initial_stats = cache_manager.get_coordination_stats();
    // active_locks is only present when the locks directory exists
    assert!(initial_stats.contains_key("cache_hits"));
    assert!(initial_stats.contains_key("cache_misses"));
    assert!(initial_stats.contains_key("expired_entries"));

    let initial_hits = *initial_stats.get("cache_hits").unwrap();

    // Acquire a lock (this creates the locks directory)
    let cache_key = "test-bucket/stats-test";
    let _lock_acquired = cache_manager
        .acquire_write_lock_with_timeout(cache_key, std::time::Duration::from_secs(1))
        .await?;

    // Check stats again - locks directory now exists, so active_locks should be present
    let stats_with_lock = cache_manager.get_coordination_stats();
    assert!(stats_with_lock.contains_key("cache_hits"));
    assert!(stats_with_lock.contains_key("cache_misses"));
    assert!(stats_with_lock.contains_key("expired_entries"));

    // Release lock
    cache_manager.release_write_lock(cache_key).await?;

    Ok(())
}
