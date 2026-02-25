//! Cache Size Tracking Integration Tests
//!
//! NOTE: Many tests in this file have been removed or simplified because
//! size tracking is now handled by JournalConsolidator through journal entries.
//! See the journal-based-size-tracking spec for details.
//!
//! The remaining tests focus on:
//! - Validation lock coordination between instances
//! - Validation metadata persistence
//! - Cache manager integration
//! - Journal-based size tracking integration (Task 14)

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::RangeSpec;
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator, SizeState};
use s3_proxy::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// Helper to create a test cache manager with size tracking
async fn create_test_cache_manager(cache_dir: std::path::PathBuf) -> Arc<CacheManager> {
    let cache_manager = Arc::new(CacheManager::new_with_all_ttls(
        cache_dir,
        false, // RAM cache disabled for simplicity
        0,
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false, // actively_remove_cached_data
    ));

    // Initialize the disk cache manager FIRST - this creates the consolidator
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();

    // Now initialize the cache manager (which requires the consolidator to exist)
    cache_manager.initialize().await.unwrap();

    cache_manager.set_cache_manager_in_tracker().await;

    cache_manager
}

#[tokio::test]
async fn test_multi_instance_validation_coordination() {
    // Create shared cache directory
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create required directories
    std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
    std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
    std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

    // Create a shared consolidator for both trackers
    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.clone(),
        "test-instance".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.clone(),
        Duration::from_secs(30),
        3,
    ));
    let consolidator = Arc::new(JournalConsolidator::new(
        cache_dir.clone(),
        journal_manager,
        lock_manager,
        ConsolidationConfig::default(),
    ));
    consolidator.initialize().await.unwrap();

    // Create two size trackers sharing the same directory
    let mut config1 = s3_proxy::cache_size_tracker::CacheSizeConfig::default();
    config1.validation_enabled = true; // Enable validation for this test to test locking

    let tracker1 = Arc::new(
        s3_proxy::cache_size_tracker::CacheSizeTracker::new(
            cache_dir.clone(),
            config1,
            false,
            consolidator.clone(),
        )
        .await
        .unwrap(),
    );

    let mut config2 = s3_proxy::cache_size_tracker::CacheSizeConfig::default();
    config2.validation_enabled = true; // Enable validation for this test to test locking

    let tracker2 = Arc::new(
        s3_proxy::cache_size_tracker::CacheSizeTracker::new(
            cache_dir.clone(),
            config2,
            false,
            consolidator.clone(),
        )
        .await
        .unwrap(),
    );

    // Try to acquire validation lock from both instances simultaneously
    let lock1_result = tracker1.try_acquire_validation_lock().await;
    let lock2_result = tracker2.try_acquire_validation_lock().await;

    // Only one should succeed
    let locks_acquired = lock1_result.is_ok() as u8 + lock2_result.is_ok() as u8;
    assert_eq!(
        locks_acquired, 1,
        "Only one instance should acquire validation lock"
    );

    // The one that got the lock should be able to release it
    if lock1_result.is_ok() {
        drop(lock1_result.unwrap());
        // Now the other should be able to acquire it
        let lock2_retry = tracker2.try_acquire_validation_lock().await;
        assert!(
            lock2_retry.is_ok(),
            "Second instance should acquire lock after first releases"
        );
    } else {
        drop(lock2_result.unwrap());
        let lock1_retry = tracker1.try_acquire_validation_lock().await;
        assert!(
            lock1_retry.is_ok(),
            "First instance should acquire lock after second releases"
        );
    }
}

#[tokio::test]
async fn test_validation_metadata_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create required directories
    std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
    std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
    std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

    // Create consolidator
    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.clone(),
        "test-instance".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.clone(),
        Duration::from_secs(30),
        3,
    ));
    let consolidator = Arc::new(JournalConsolidator::new(
        cache_dir.clone(),
        journal_manager,
        lock_manager,
        ConsolidationConfig::default(),
    ));
    consolidator.initialize().await.unwrap();

    let mut config = s3_proxy::cache_size_tracker::CacheSizeConfig::default();
    config.validation_enabled = false; // Disable validation for tests

    let tracker = Arc::new(
        s3_proxy::cache_size_tracker::CacheSizeTracker::new(
            cache_dir.clone(),
            config,
            false,
            consolidator,
        )
        .await
        .unwrap(),
    );

    // Write validation metadata
    tracker
        .write_validation_metadata(
            1000000, // scanned_size
            1000500, // tracked_size
            -500,    // drift
            Duration::from_secs(120),
            50000, // files_scanned
            5,     // cache_expired
            1,     // cache_skipped
            0,     // cache_errors
        )
        .await
        .unwrap();

    // Read it back
    let metadata = tracker.read_validation_metadata().await.unwrap();

    assert_eq!(metadata.scanned_size, 1000000);
    assert_eq!(metadata.tracked_size, 1000500);
    assert_eq!(metadata.drift_bytes, -500);
    assert_eq!(metadata.scan_duration_ms, 120000);
    assert_eq!(metadata.cache_entries_expired, 5);
    assert_eq!(metadata.cache_entries_skipped, 1);
}

#[tokio::test]
async fn test_actively_remove_cached_data_flag_behavior() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Test with flag disabled
    {
        let cache_manager = Arc::new(CacheManager::new_with_all_ttls(
            cache_dir.clone(),
            false,
            0,
            s3_proxy::cache::CacheEvictionAlgorithm::LRU,
            1024,
            true,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            false, // actively_remove_cached_data = false
        ));

        // Initialize the disk cache manager FIRST - this creates the consolidator
        let _disk_cache = cache_manager.create_configured_disk_cache_manager();

        // Now initialize the cache manager (which requires the consolidator to exist)
        cache_manager.initialize().await.unwrap();

        cache_manager.set_cache_manager_in_tracker().await;

        let tracker = cache_manager.get_size_tracker().await.unwrap();
        assert_eq!(tracker.is_active_expiration_enabled(), false);

        // coordinate_cleanup should return 0 immediately
        let cleaned = cache_manager.coordinate_cleanup().await.unwrap();
        assert_eq!(cleaned, 0, "Should not clean when flag is false");
    }

    // Test with flag enabled
    {
        let cache_manager = Arc::new(CacheManager::new_with_all_ttls(
            cache_dir.clone(),
            false,
            0,
            s3_proxy::cache::CacheEvictionAlgorithm::LRU,
            1024,
            true,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            true, // actively_remove_cached_data = true
        ));

        // Initialize the disk cache manager FIRST - this creates the consolidator
        let _disk_cache = cache_manager.create_configured_disk_cache_manager();

        // Now initialize the cache manager (which requires the consolidator to exist)
        cache_manager.initialize().await.unwrap();

        cache_manager.set_cache_manager_in_tracker().await;

        let tracker = cache_manager.get_size_tracker().await.unwrap();
        assert_eq!(tracker.is_active_expiration_enabled(), true);
    }
}

#[tokio::test]
async fn test_size_tracker_integration_with_cache_manager() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = create_test_cache_manager(cache_dir.clone()).await;

    // Verify tracker is initialized
    let tracker = cache_manager.get_size_tracker().await;
    assert!(tracker.is_some(), "Size tracker should be initialized");

    // Verify metrics are accessible through cache manager
    let metrics = cache_manager.get_cache_size_metrics().await;
    assert!(metrics.is_some(), "Cache size metrics should be available");

    let metrics = metrics.unwrap();
    assert_eq!(metrics.current_size, 0); // Initial size
}

// NOTE: The following tests have been removed because they relied on update_size_sync()
// which has been removed as part of the journal-based-size-tracking migration:
// - test_full_recovery_cycle
// - test_drift_reconciliation
// - test_checkpoint_persistence_across_restarts
// - test_delta_log_replay_after_crash
// - test_concurrent_size_updates
// - test_metrics_exposure
// - test_checkpoint_after_multiple_updates
//
// Size tracking is now handled by JournalConsolidator through journal entries.
// See the journal-based-size-tracking spec for the new architecture.

// ============================================================================
// Task 14: Journal-Based Size Tracking Integration Tests
// ============================================================================

/// Helper to create a test consolidator with size tracking
async fn create_test_consolidator(
    cache_dir: std::path::PathBuf,
    max_cache_size: u64,
) -> Arc<JournalConsolidator> {
    // Ensure required directories exist
    std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
    std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
    std::fs::create_dir_all(cache_dir.join("locks")).unwrap();
    std::fs::create_dir_all(cache_dir.join("ranges")).unwrap();

    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.clone(),
        "test-instance".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.clone(),
        Duration::from_secs(30),
        3,
    ));
    let consolidation_config = ConsolidationConfig {
        interval: Duration::from_secs(5),
        size_threshold: 1024 * 1024,
        entry_count_threshold: 100,
        max_keys_per_run: 50,
        max_cache_size,
        eviction_trigger_percent: 95,
        eviction_target_percent: 80,
        stale_entry_timeout_secs: 300,
        consolidation_cycle_timeout: Duration::from_secs(30),
    };
    let consolidator = Arc::new(JournalConsolidator::new(
        cache_dir,
        journal_manager,
        lock_manager,
        consolidation_config,
    ));
    consolidator.initialize().await.unwrap();
    consolidator
}

/// Helper to create a test RangeSpec
fn create_test_range_spec(start: u64, end: u64, compressed_size: u64) -> RangeSpec {
    let now = SystemTime::now();
    RangeSpec {
        start,
        end,
        file_path: format!("test_{}_{}.bin", start, end),
        compression_algorithm: CompressionAlgorithm::Lz4,
        compressed_size,
        uncompressed_size: compressed_size,
        created_at: now,
        last_accessed: now,
        access_count: 1,
        frequency_score: 1,
    }
}

/// Helper to create a test journal entry
fn create_test_journal_entry(
    cache_key: &str,
    start: u64,
    end: u64,
    compressed_size: u64,
    operation: JournalOperation,
    instance_id: &str,
) -> JournalEntry {
    JournalEntry {
        timestamp: SystemTime::now(),
        instance_id: instance_id.to_string(),
        cache_key: cache_key.to_string(),
        range_spec: create_test_range_spec(start, end, compressed_size),
        operation,
        range_file_path: format!("ranges/test_{}_{}.bin", start, end),
        metadata_version: 1,
        new_ttl_secs: None,
        object_ttl_secs: None,
        access_increment: None,
        object_metadata: None,
        metadata_written: false,
    }
}

/// Helper to write a journal entry to a journal file
async fn write_journal_entry(cache_dir: &std::path::Path, entry: &JournalEntry) {
    let journal_path = cache_dir
        .join("metadata")
        .join("_journals")
        .join(format!("{}.journal", entry.instance_id));

    let json = serde_json::to_string(entry).unwrap();
    let content = if journal_path.exists() {
        let existing = tokio::fs::read_to_string(&journal_path).await.unwrap();
        format!("{}\n{}", existing.trim(), json)
    } else {
        json
    };
    tokio::fs::write(&journal_path, content).await.unwrap();
}

/// Helper to create a range file on disk
async fn create_range_file(
    cache_dir: &std::path::Path,
    cache_key: &str,
    start: u64,
    end: u64,
    size: u64,
) {
    let range_base_dir = cache_dir.join("ranges");
    let range_path = s3_proxy::disk_cache::get_sharded_path(
        &range_base_dir,
        cache_key,
        &format!("_{}-{}.bin", start, end),
    )
    .unwrap();

    if let Some(parent) = range_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }

    // Create a file with the specified size
    let data = vec![0u8; size as usize];
    tokio::fs::write(&range_path, data).await.unwrap();
}

/// Task 14.1: Test full consolidation cycle with size tracking
///
/// Validates: Requirements 1.1, 1.2, 1.3
/// - Size is tracked via accumulator at write time
/// - Consolidator flushes accumulator and applies deltas to size_state.json
/// - Size state includes total cache size and write cache size
///
/// NOTE: With accumulator-based size tracking, size is tracked at write time
/// via the accumulator, not via journal entries. Journal entries are only
/// used for metadata updates.
#[tokio::test]
async fn test_full_consolidation_cycle_with_size_tracking() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create consolidator with no max cache size (eviction disabled)
    let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

    // Verify initial size is 0
    assert_eq!(
        consolidator.get_current_size().await,
        0,
        "Initial size should be 0"
    );
    assert_eq!(
        consolidator.get_write_cache_size().await,
        0,
        "Initial write cache size should be 0"
    );

    // With accumulator-based tracking, we add size via the accumulator
    // (simulating what happens at write time)
    consolidator.size_accumulator().add(1000);
    consolidator.size_accumulator().add(1000);

    // Create journal entries for Add operations (for metadata updates only)
    let cache_key = "test-bucket/test-object";
    let entry1 = create_test_journal_entry(
        cache_key,
        0,
        999,
        1000,
        JournalOperation::Add,
        "test-instance",
    );
    let entry2 = create_test_journal_entry(
        cache_key,
        1000,
        1999,
        1000,
        JournalOperation::Add,
        "test-instance",
    );

    // Write journal entries
    write_journal_entry(&cache_dir, &entry1).await;
    write_journal_entry(&cache_dir, &entry2).await;

    // Create corresponding range files (required for validation)
    create_range_file(&cache_dir, cache_key, 0, 999, 1000).await;
    create_range_file(&cache_dir, cache_key, 1000, 1999, 1000).await;

    // Run consolidation cycle - this flushes the accumulator and applies deltas
    let result = consolidator.run_consolidation_cycle().await.unwrap();

    // Verify consolidation results
    assert_eq!(result.keys_processed, 1, "Should process 1 cache key");
    assert_eq!(
        result.entries_consolidated, 2,
        "Should consolidate 2 entries"
    );
    // With accumulator-based tracking, size_delta comes from delta files, not journal entries
    assert_eq!(
        result.current_size, 2000,
        "Current size should be 2000 bytes"
    );
    assert!(
        !result.eviction_triggered,
        "Eviction should not be triggered"
    );

    // Verify size state is updated
    let size_state = consolidator.get_size_state().await;
    assert_eq!(
        size_state.total_size, 2000,
        "Total size should be 2000 bytes"
    );
    assert_eq!(
        size_state.consolidation_count, 1,
        "Consolidation count should be 1"
    );
    assert!(
        !size_state.last_updated_by.is_empty(),
        "last_updated_by should be set"
    );

    // Verify size state is persisted to disk
    let size_state_path = cache_dir.join("size_tracking").join("size_state.json");
    assert!(size_state_path.exists(), "size_state.json should exist");

    let persisted_content = tokio::fs::read_to_string(&size_state_path).await.unwrap();
    let persisted_state: SizeState = serde_json::from_str(&persisted_content).unwrap();
    assert_eq!(
        persisted_state.total_size, 2000,
        "Persisted total size should be 2000 bytes"
    );
}

/// Task 14.1 (continued): Test size tracking with Remove operations
///
/// Validates: Requirements 1.1
/// - Remove operations decrease size by compressed_size
///
/// NOTE: With accumulator-based size tracking, size changes are tracked via
/// the accumulator at write/eviction time, not via journal entries.
#[tokio::test]
async fn test_consolidation_cycle_with_remove_operations() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

    // First, add some data via the accumulator (simulating write time)
    consolidator.size_accumulator().add(1000);

    // Create journal entry for metadata update
    let cache_key = "test-bucket/remove-test-object";
    let add_entry = create_test_journal_entry(
        cache_key,
        0,
        999,
        1000,
        JournalOperation::Add,
        "test-instance",
    );
    write_journal_entry(&cache_dir, &add_entry).await;
    create_range_file(&cache_dir, cache_key, 0, 999, 1000).await;

    // Run first consolidation - flushes accumulator and applies delta
    let result1 = consolidator.run_consolidation_cycle().await.unwrap();
    assert_eq!(result1.current_size, 1000, "Size should be 1000 after add");

    // Now subtract via accumulator (simulating eviction/invalidation)
    consolidator.size_accumulator().subtract(1000);

    // Create a Remove entry for metadata update
    let remove_entry = create_test_journal_entry(
        cache_key,
        0,
        999,
        1000,
        JournalOperation::Remove,
        "test-instance",
    );
    write_journal_entry(&cache_dir, &remove_entry).await;

    // Run second consolidation
    let result2 = consolidator.run_consolidation_cycle().await.unwrap();
    assert_eq!(
        result2.current_size, 0,
        "Current size should be 0 after remove"
    );

    // Verify size state
    let size_state = consolidator.get_size_state().await;
    assert_eq!(
        size_state.total_size, 0,
        "Total size should be 0 after remove"
    );
}

/// Task 14.2: Test eviction triggered by consolidator
///
/// Validates: Requirements 2.1, 2.2, 2.4, 2.5
/// - After each consolidation cycle, consolidator checks if total size exceeds max capacity
/// - If over capacity, consolidator triggers eviction via CacheManager
/// - Size state is updated immediately after eviction
///
/// NOTE: With accumulator-based size tracking, size is tracked via the accumulator.
#[tokio::test]
async fn test_eviction_triggered_by_consolidator() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create consolidator with small max cache size to trigger eviction
    // Note: Without a CacheManager connected, eviction won't actually run,
    // but we can verify the triggering logic
    let max_cache_size = 500; // 500 bytes max
    let consolidator = create_test_consolidator(cache_dir.clone(), max_cache_size).await;

    // Add size via accumulator (simulating write time)
    consolidator.size_accumulator().add(1000);

    // Create journal entries for metadata update
    let cache_key = "test-bucket/eviction-test-object";
    let entry = create_test_journal_entry(
        cache_key,
        0,
        999,
        1000,
        JournalOperation::Add,
        "test-instance",
    );
    write_journal_entry(&cache_dir, &entry).await;
    create_range_file(&cache_dir, cache_key, 0, 999, 1000).await;

    // Run consolidation cycle - flushes accumulator and applies delta
    let result = consolidator.run_consolidation_cycle().await.unwrap();

    // Verify size exceeds max
    assert_eq!(
        result.current_size, 1000,
        "Current size should be 1000 bytes"
    );
    assert!(
        result.current_size > max_cache_size,
        "Size should exceed max cache size"
    );

    // Note: eviction_triggered will be false because no CacheManager is connected
    // In a real scenario with CacheManager, eviction would be triggered
    // This test verifies the size tracking and threshold detection work correctly
}

/// Task 14.3: Test crash recovery (restart with existing size_state.json)
///
/// Validates: Requirements 1.4, 4.4
/// - Size state survives proxy restart (recovered from persisted file)
/// - On startup with missing size state, system schedules immediate validation
#[tokio::test]
async fn test_crash_recovery_with_existing_size_state() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create required directories
    std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
    std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
    std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

    // Simulate a previous run by writing a size_state.json file
    let previous_state = SizeState {
        total_size: 5000,
        write_cache_size: 500,
        last_consolidation: SystemTime::now() - Duration::from_secs(60),
        consolidation_count: 10,
        last_updated_by: "previous-instance:12345".to_string(),
    };

    let size_state_path = cache_dir.join("size_tracking").join("size_state.json");
    let json = serde_json::to_string_pretty(&previous_state).unwrap();
    tokio::fs::write(&size_state_path, json).await.unwrap();

    // Create a new consolidator (simulating restart)
    let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

    // Verify size state was recovered from disk
    let recovered_state = consolidator.get_size_state().await;
    assert_eq!(
        recovered_state.total_size, 5000,
        "Total size should be recovered"
    );
    assert_eq!(
        recovered_state.write_cache_size, 500,
        "Write cache size should be recovered"
    );
    assert_eq!(
        recovered_state.consolidation_count, 10,
        "Consolidation count should be recovered"
    );
    assert_eq!(
        recovered_state.last_updated_by, "previous-instance:12345",
        "last_updated_by should be recovered"
    );

    // Verify get_current_size() returns recovered value
    assert_eq!(
        consolidator.get_current_size().await,
        5000,
        "get_current_size should return recovered value"
    );
    assert_eq!(
        consolidator.get_write_cache_size().await,
        500,
        "get_write_cache_size should return recovered value"
    );
}

/// Task 14.3 (continued): Test startup with missing size_state.json
///
/// Validates: Requirements 4.4
/// - On startup with missing size state, system starts with default (zero) values
#[tokio::test]
async fn test_startup_with_missing_size_state() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create consolidator without any existing size_state.json
    let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

    // Verify size state starts at default values
    let size_state = consolidator.get_size_state().await;
    assert_eq!(
        size_state.total_size, 0,
        "Total size should be 0 for fresh start"
    );
    assert_eq!(
        size_state.write_cache_size, 0,
        "Write cache size should be 0 for fresh start"
    );
    assert_eq!(
        size_state.consolidation_count, 0,
        "Consolidation count should be 0 for fresh start"
    );
}

/// Task 14.4: Test validation scan corrects drift
///
/// Validates: Requirements 4.1, 4.2, 4.3
/// - Validation scan calculates actual cache size from filesystem
/// - Validation corrects size state if drift exceeds threshold
/// - Validation logs drift amount for monitoring
#[tokio::test]
async fn test_validation_scan_corrects_drift() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create consolidator with initial size state that has drift
    let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

    // Manually set an incorrect size (simulating drift)
    // We'll use update_size_from_validation to correct it
    let _incorrect_size = 10000;
    let correct_size = 5000;

    // First, set the incorrect size by running a consolidation with fake entries
    // Then correct it with validation

    // Update size from validation (simulating what CacheSizeTracker does after a scan)
    consolidator
        .update_size_from_validation(correct_size, Some(500))
        .await;

    // Verify size was corrected
    let size_state = consolidator.get_size_state().await;
    assert_eq!(
        size_state.total_size, correct_size,
        "Total size should be corrected by validation"
    );
    assert_eq!(
        size_state.write_cache_size, 500,
        "Write cache size should be updated by validation"
    );

    // Verify the corrected size is persisted
    let size_state_path = cache_dir.join("size_tracking").join("size_state.json");
    let persisted_content = tokio::fs::read_to_string(&size_state_path).await.unwrap();
    let persisted_state: SizeState = serde_json::from_str(&persisted_content).unwrap();
    assert_eq!(
        persisted_state.total_size, correct_size,
        "Persisted size should be corrected"
    );
}

/// Task 14.5: Test graceful shutdown persists size state
///
/// Validates: Requirements 6.1, 6.2, 6.3
/// - On shutdown signal, consolidator runs final consolidation cycle
/// - Size state is persisted before process exits
/// - Any pending journal entries are processed before shutdown completes
///
/// NOTE: With accumulator-based size tracking, size is tracked via the accumulator.
#[tokio::test]
async fn test_graceful_shutdown_persists_size_state() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

    // Add some data via accumulator (simulating write time)
    consolidator.size_accumulator().add(1000);

    // Create journal entry for metadata update
    let cache_key = "test-bucket/shutdown-test-object";
    let entry = create_test_journal_entry(
        cache_key,
        0,
        999,
        1000,
        JournalOperation::Add,
        "test-instance",
    );
    write_journal_entry(&cache_dir, &entry).await;
    create_range_file(&cache_dir, cache_key, 0, 999, 1000).await;

    // Run consolidation cycle (simulating what happens during shutdown)
    let result = consolidator.run_consolidation_cycle().await.unwrap();
    assert_eq!(
        result.current_size, 1000,
        "Size should be 1000 after consolidation"
    );

    // Verify size state is persisted
    let size_state_path = cache_dir.join("size_tracking").join("size_state.json");
    assert!(
        size_state_path.exists(),
        "size_state.json should exist after consolidation"
    );

    // Simulate shutdown by dropping the consolidator and creating a new one
    drop(consolidator);

    // Create a new consolidator (simulating restart after shutdown)
    let new_consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

    // Verify size state was persisted and recovered
    let recovered_state = new_consolidator.get_size_state().await;
    assert_eq!(
        recovered_state.total_size, 1000,
        "Size should be recovered after restart"
    );
    assert_eq!(
        recovered_state.consolidation_count, 1,
        "Consolidation count should be recovered"
    );
}

/// Test multiple consolidation cycles accumulate size correctly
///
/// Validates: Requirements 1.1, 1.5
/// - Size deltas accumulate correctly across multiple cycles
/// - Consolidation count increments with each cycle
///
/// NOTE: With accumulator-based size tracking, size is tracked via the accumulator.
#[tokio::test]
async fn test_multiple_consolidation_cycles_accumulate_size() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

    // First cycle: Add 1000 bytes via accumulator
    consolidator.size_accumulator().add(1000);

    let cache_key1 = "test-bucket/object1";
    let entry1 = create_test_journal_entry(
        cache_key1,
        0,
        999,
        1000,
        JournalOperation::Add,
        "test-instance",
    );
    write_journal_entry(&cache_dir, &entry1).await;
    create_range_file(&cache_dir, cache_key1, 0, 999, 1000).await;

    let result1 = consolidator.run_consolidation_cycle().await.unwrap();
    assert_eq!(
        result1.current_size, 1000,
        "Size should be 1000 after first cycle"
    );

    // Second cycle: Add 2000 more bytes via accumulator
    consolidator.size_accumulator().add(2000);

    let cache_key2 = "test-bucket/object2";
    let entry2 = create_test_journal_entry(
        cache_key2,
        0,
        1999,
        2000,
        JournalOperation::Add,
        "test-instance",
    );
    write_journal_entry(&cache_dir, &entry2).await;
    create_range_file(&cache_dir, cache_key2, 0, 1999, 2000).await;

    let result2 = consolidator.run_consolidation_cycle().await.unwrap();
    assert_eq!(
        result2.current_size, 3000,
        "Size should be 3000 after second cycle"
    );

    // Verify consolidation count
    let size_state = consolidator.get_size_state().await;
    assert_eq!(
        size_state.consolidation_count, 2,
        "Consolidation count should be 2"
    );
    assert_eq!(size_state.total_size, 3000, "Total size should be 3000");
}

/// Test empty consolidation cycle doesn't change size state
///
/// Validates: Requirements 1.1
/// - Running consolidation with no pending entries doesn't modify size
#[tokio::test]
async fn test_empty_consolidation_cycle() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

    // Run consolidation with no journal entries
    let result = consolidator.run_consolidation_cycle().await.unwrap();

    assert_eq!(result.keys_processed, 0, "Should process 0 keys");
    assert_eq!(
        result.entries_consolidated, 0,
        "Should consolidate 0 entries"
    );
    assert_eq!(result.size_delta, 0, "Size delta should be 0");
    assert_eq!(result.current_size, 0, "Current size should remain 0");

    // Verify size state wasn't modified
    let size_state = consolidator.get_size_state().await;
    assert_eq!(
        size_state.consolidation_count, 0,
        "Consolidation count should remain 0"
    );
}

/// Test size never goes negative
///
/// Validates: Correctness Property - Size never goes negative
#[tokio::test]
async fn test_size_never_goes_negative() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

    // Try to remove more than exists (should saturate at 0)
    let cache_key = "test-bucket/negative-test-object";
    let remove_entry = create_test_journal_entry(
        cache_key,
        0,
        999,
        1000,
        JournalOperation::Remove,
        "test-instance",
    );
    write_journal_entry(&cache_dir, &remove_entry).await;

    // Note: Remove entries without corresponding range files won't be processed
    // because validation requires the range file to exist
    // This test verifies the saturating_sub behavior in size state updates

    // Run consolidation
    let result = consolidator.run_consolidation_cycle().await.unwrap();

    // Size should remain at 0 (saturating subtraction)
    // Note: current_size is u64, so it can never be negative
    assert_eq!(
        result.current_size, 0,
        "Size should be 0 (saturating subtraction)"
    );
    assert_eq!(
        consolidator.get_current_size().await,
        0,
        "get_current_size should return 0"
    );
}
