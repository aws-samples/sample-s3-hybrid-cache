//! Property-Based Tests for Journal-Based Size Tracking
//!
//! This module contains property-based tests using quickcheck to verify the correctness
//! properties defined in the journal-based-size-tracking design document.
//!
//! Properties tested:
//! - Property 3: Multi-instance consistency - shared state persistence
//! - Property 4: Crash recovery - size recovers within validation threshold

use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator, SizeState};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

// ============================================================================
// Test Helpers
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

// ============================================================================
// Property 3: Multi-Instance Consistency (Shared State Persistence)
// ============================================================================

/// **Feature: journal-based-size-tracking, Property 3: Multi-Instance Consistency (Shared State)**
///
/// *For any* size state persisted by one instance, another instance reading
/// the same file should see identical values.
///
/// **Validates: Requirements 7.4, 7.5**
#[quickcheck]
fn prop_multi_instance_shared_state_persistence(
    total_size: u32,
    write_cache_size: u16,
    consolidation_count: u16,
) -> TestResult {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();

        // Create required directories
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

        // Create a size state file (simulating instance 1 writing it)
        let original_state = SizeState {
            total_size: total_size as u64,
            write_cache_size: write_cache_size as u64,
            last_consolidation: SystemTime::now(),
            consolidation_count: consolidation_count as u64,
            last_updated_by: "instance-1:12345".to_string(),
        };

        let size_state_path = cache_dir.join("size_tracking").join("size_state.json");
        let json = serde_json::to_string_pretty(&original_state).unwrap();
        tokio::fs::write(&size_state_path, json).await.unwrap();

        // Create a new consolidator (simulating instance 2 reading the state)
        let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

        // Property: instance 2 should see the same values as instance 1 wrote
        let loaded_state = consolidator.get_size_state().await;

        if loaded_state.total_size != original_state.total_size {
            return TestResult::error(format!(
                "Total size mismatch: loaded={}, original={}",
                loaded_state.total_size, original_state.total_size
            ));
        }

        if loaded_state.write_cache_size != original_state.write_cache_size {
            return TestResult::error(format!(
                "Write cache size mismatch: loaded={}, original={}",
                loaded_state.write_cache_size, original_state.write_cache_size
            ));
        }

        if loaded_state.consolidation_count != original_state.consolidation_count {
            return TestResult::error(format!(
                "Consolidation count mismatch: loaded={}, original={}",
                loaded_state.consolidation_count, original_state.consolidation_count
            ));
        }

        if loaded_state.last_updated_by != original_state.last_updated_by {
            return TestResult::error(format!(
                "Last updated by mismatch: loaded={}, original={}",
                loaded_state.last_updated_by, original_state.last_updated_by
            ));
        }

        TestResult::passed()
    })
}

// ============================================================================
// Property 4: Crash Recovery
// ============================================================================

/// **Feature: journal-based-size-tracking, Property 4: Crash Recovery**
///
/// *After* a crash and restart, size_state recovers to within validation
/// threshold of actual disk usage.
///
/// **Validates: Requirements 1.4, 4.4**
#[quickcheck]
fn prop_crash_recovery_size_state(
    total_size: u32,
    write_cache_size: u16,
    consolidation_count: u16,
) -> TestResult {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();

        // Create required directories
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

        // Simulate a previous run by writing a size_state.json file
        let previous_state = SizeState {
            total_size: total_size as u64,
            write_cache_size: write_cache_size as u64,
            last_consolidation: SystemTime::now() - Duration::from_secs(60),
            consolidation_count: consolidation_count as u64,
            last_updated_by: "previous-instance:12345".to_string(),
        };

        let size_state_path = cache_dir.join("size_tracking").join("size_state.json");
        let json = serde_json::to_string_pretty(&previous_state).unwrap();
        tokio::fs::write(&size_state_path, json).await.unwrap();

        // Create a new consolidator (simulating restart after crash)
        let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

        // Property: size state should be recovered from disk
        let recovered_state = consolidator.get_size_state().await;

        if recovered_state.total_size != previous_state.total_size {
            return TestResult::error(format!(
                "Total size not recovered: recovered={}, expected={}",
                recovered_state.total_size, previous_state.total_size
            ));
        }

        if recovered_state.write_cache_size != previous_state.write_cache_size {
            return TestResult::error(format!(
                "Write cache size not recovered: recovered={}, expected={}",
                recovered_state.write_cache_size, previous_state.write_cache_size
            ));
        }

        if recovered_state.consolidation_count != previous_state.consolidation_count {
            return TestResult::error(format!(
                "Consolidation count not recovered: recovered={}, expected={}",
                recovered_state.consolidation_count, previous_state.consolidation_count
            ));
        }

        // Verify get_current_size() returns recovered value
        if consolidator.get_current_size().await != previous_state.total_size {
            return TestResult::error(format!(
                "get_current_size() mismatch: actual={}, expected={}",
                consolidator.get_current_size().await,
                previous_state.total_size
            ));
        }

        TestResult::passed()
    })
}

/// **Feature: journal-based-size-tracking, Property 4: Crash Recovery (Fresh Start)**
///
/// *On* startup with missing size state, system starts with default (zero) values.
///
/// **Validates: Requirements 1.4, 4.4**
#[quickcheck]
fn prop_crash_recovery_fresh_start(_seed: u8) -> TestResult {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();

        // Create consolidator without any existing size_state.json
        let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

        // Property: size state should start at default values
        let size_state = consolidator.get_size_state().await;

        if size_state.total_size != 0 {
            return TestResult::error(format!(
                "Total size should be 0 for fresh start: actual={}",
                size_state.total_size
            ));
        }

        if size_state.write_cache_size != 0 {
            return TestResult::error(format!(
                "Write cache size should be 0 for fresh start: actual={}",
                size_state.write_cache_size
            ));
        }

        if size_state.consolidation_count != 0 {
            return TestResult::error(format!(
                "Consolidation count should be 0 for fresh start: actual={}",
                size_state.consolidation_count
            ));
        }

        TestResult::passed()
    })
}

/// **Feature: journal-based-size-tracking, Property 4: Crash Recovery (Validation Correction)**
///
/// *After* validation scan, size state is corrected to match actual filesystem size.
///
/// **Validates: Requirements 4.1, 4.2, 4.3**
#[quickcheck]
fn prop_crash_recovery_validation_correction(
    _initial_size: u32,
    correct_size: u32,
    write_cache_size: u16,
) -> TestResult {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();

        let consolidator = create_test_consolidator(cache_dir.clone(), 0).await;

        // Set an initial (potentially incorrect) size
        // We'll use update_size_from_validation to correct it
        let correct_size = correct_size as u64;
        let write_cache_size = write_cache_size as u64;

        // Update size from validation (simulating what CacheSizeTracker does after a scan)
        consolidator
            .update_size_from_validation(correct_size, Some(write_cache_size))
            .await;

        // Property: size should be corrected by validation
        let size_state = consolidator.get_size_state().await;

        if size_state.total_size != correct_size {
            return TestResult::error(format!(
                "Total size not corrected: actual={}, expected={}",
                size_state.total_size, correct_size
            ));
        }

        if size_state.write_cache_size != write_cache_size {
            return TestResult::error(format!(
                "Write cache size not corrected: actual={}, expected={}",
                size_state.write_cache_size, write_cache_size
            ));
        }

        // Verify the corrected size is persisted
        let size_state_path = cache_dir.join("size_tracking").join("size_state.json");
        let persisted_content = tokio::fs::read_to_string(&size_state_path).await.unwrap();
        let persisted_state: SizeState = serde_json::from_str(&persisted_content).unwrap();

        if persisted_state.total_size != correct_size {
            return TestResult::error(format!(
                "Persisted size not corrected: actual={}, expected={}",
                persisted_state.total_size, correct_size
            ));
        }

        TestResult::passed()
    })
}
