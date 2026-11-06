//! Property-based tests for coordinated cache initialization
//!
//! Tests universal properties that should hold across all valid executions
//! of the coordinated cache initialization system.

use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use s3_proxy::cache_initialization_coordinator::CacheInitializationCoordinator;
use s3_proxy::cache_size_tracker::{CacheSizeConfig, CacheSizeTracker};
use s3_proxy::config::{CacheConfig, EvictionAlgorithm, InitializationConfig, SharedStorageConfig};
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use s3_proxy::write_cache_manager::WriteCacheManager;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// Helper function to create a test CacheSizeTracker with JournalConsolidator
async fn create_test_size_tracker(
    cache_dir: &std::path::Path,
) -> (Arc<CacheSizeTracker>, Arc<JournalConsolidator>) {
    // Create required directories
    std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
    std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
    std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

    // Create mock dependencies for JournalConsolidator
    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.to_path_buf(),
        "test-instance".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.join("locks"),
        Duration::from_secs(30),
        3,
    ));
    let consolidation_config = ConsolidationConfig::default();

    // Create the consolidator
    let consolidator = Arc::new(JournalConsolidator::new(
        cache_dir.to_path_buf(),
        journal_manager,
        lock_manager,
        consolidation_config,
    ));

    // Initialize the consolidator
    consolidator.initialize().await.unwrap();

    let size_config = CacheSizeConfig {
        checkpoint_interval: Duration::from_secs(300),
        validation_time_of_day: "00:00".to_string(),
        validation_enabled: true,
        incomplete_upload_ttl: Duration::from_secs(86400),
        validation_max_duration: Duration::from_secs(4 * 3600),
        ..Default::default()
    };

    let tracker = Arc::new(
        CacheSizeTracker::new(
            cache_dir.to_path_buf(),
            size_config,
            false,
            consolidator.clone(),
        )
        .await
        .unwrap(),
    );

    (tracker, consolidator)
}

/// Create a test cache configuration
fn create_test_cache_config() -> CacheConfig {
    CacheConfig {
        cache_dir: PathBuf::from("/tmp/test"),
        max_cache_size: 1024 * 1024 * 1024, // 1GB
        ram_cache_enabled: false,
        max_ram_cache_size: 0,
        eviction_algorithm: EvictionAlgorithm::TinyLFU,
        write_cache_enabled: true,
        write_cache_percent: 0.5,
        write_cache_max_object_size: 1024 * 1024 * 100, // 100MB
        disk_safety_free_space_floor: 1024 * 1024 * 1024, // 1 GiB
        put_ttl: Duration::from_secs(86400),            // 1 day
        get_ttl: Duration::from_secs(3600),             // 1 hour
        head_ttl: Duration::from_secs(300),             // 5 minutes
        actively_remove_cached_data: false,
        shared_storage: SharedStorageConfig::default(),
        range_merge_gap_threshold: 1024 * 1024, // 1MB
        eviction_buffer_percent: 5,
        ram_cache_flush_interval: Duration::from_secs(60),
        ram_cache_verification_interval: Duration::from_secs(1),
        incomplete_upload_ttl: Duration::from_secs(86400), // 1 day
        initialization: InitializationConfig::default(),
        cache_bypass_headers_enabled: true,
        metadata_cache: s3_proxy::config::MetadataCacheConfig::default(),
        eviction_trigger_percent: 95, // Trigger eviction at 95% capacity
        eviction_target_percent: 80,  // Reduce to 80% after eviction
        full_object_check_threshold: 67_108_864, // 64 MiB
        disk_streaming_threshold: 1_048_576, // 1 MiB
        compression_batch_size: 1_048_576, // 1 MiB
        read_cache_enabled: true,
        bucket_settings_staleness_threshold: Duration::from_secs(60),
        download_coordination: s3_proxy::config::DownloadCoordinationConfig::default(),
        evaluate_conditions_from_cache: false,
        trust_unverified_ranges: false,
        ram_cache_shard_count: 64,
        max_complete_body_bytes: 10 * 1024 * 1024, // 10 MiB
        max_metadata_file_bytes: 4 * 1024 * 1024,  // 4 MiB
        metadata_io_concurrency: 32,
        partial_range_commit_ratio: 0.5,
    }
}

/// **Feature: write-cache-accounting-and-eviction, Requirements 3.3, 7.1, 7.2**
///
/// *For any* pair of Inflight_Bytes and Resident_Bytes figures, cross-validation reports
/// consistency and raises no warning.
///
/// This replaces `prop_cross_validation_threshold_behavior`, which asserted that a
/// discrepancy above 5% warned and above 20% escalated to `error!`. Task 27 removed the
/// seeding of `WriteCacheManager::current_size` from scan results, because seeding an
/// admission counter from persisted state was the defect behind a months-long
/// write-caching outage on the fleet. The two figures the old property compared therefore
/// measure different things:
///
/// - `write_cache_manager_size` — Inflight_Bytes: bytes held by uploads streaming on THIS
///   instance, released on drop. Zero on a freshly started process, by design.
/// - `size_tracker_write_cache_size` — Resident_Bytes: staged bytes on the SHARED volume.
///
/// There is no correct expected relationship between them, so no threshold can be right,
/// and the old 20% rule would have fired `error!` on every startup of any deployment
/// holding staged data.
///
/// Asserting the retirement across the *same generated input space* is deliberately
/// stronger than deleting the test: if the comparison is ever reinstated, this fails for
/// essentially every generated pair rather than only for a hand-picked fixture.
#[quickcheck]
fn prop_inflight_and_resident_are_never_cross_validated(
    manager_size: u32,
    tracker_size: u32,
) -> TestResult {
    let manager_size = (manager_size % 10000) as u64;
    let tracker_size = (tracker_size % 10000) as u64;

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_cache_config();

        let coordinator = CacheInitializationCoordinator::new(
            temp_dir.path().to_path_buf(),
            true,
            config.clone(),
        );

        let write_cache_manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            1024 * 1024,
            50.0,
            config.put_ttl,
            config.incomplete_upload_ttl,
            s3_proxy::cache::CacheEvictionAlgorithm::TinyLFU,
            config.write_cache_max_object_size,
        );

        // Put `manager_size` bytes genuinely in flight, rather than seeding a counter —
        // `initialize_from_scan_results` no longer exists, and a real reservation is what
        // the figure now means. Held for the duration of the assertion.
        let _reservation = if manager_size > 0 {
            write_cache_manager.try_reserve(manager_size).await
        } else {
            None
        };

        let (size_tracker, consolidator) = create_test_size_tracker(temp_dir.path()).await;

        // `total_size` must be at least the write-cache figure: the staged figure is a
        // SUBSET of the total and `update_size_from_validation_with_kind` clamps it with a WARN
        // otherwise (R6.4).
        if tracker_size > 0 {
            consolidator
                .update_size_from_validation_with_kind(
                    tracker_size,
                    Some(tracker_size),
                    None,
                    s3_proxy::cache_size_tracker::ValidationSampleKind::FullScan,
                )
                .await;
        }

        let validation_results = coordinator
            .cross_validate_subsystems(Some(&write_cache_manager), Some(&size_tracker))
            .await
            .unwrap();

        // Validation still runs and still reports both figures — retiring the comparison
        // must not lose the observability.
        if !validation_results.was_performed() {
            return TestResult::failed();
        }
        if validation_results.size_tracker_write_cache_size != Some(tracker_size) {
            return TestResult::failed();
        }

        // The retirement itself: no discrepancy, no warning, never inconsistent, for any
        // combination of the two figures.
        if !validation_results.write_cache_consistent {
            return TestResult::failed();
        }
        if validation_results.size_discrepancy_percent != 0.0 {
            return TestResult::failed();
        }
        if !validation_results.validation_warnings.is_empty() {
            return TestResult::failed();
        }
        if validation_results.has_serious_discrepancy()
            || validation_results.has_minor_discrepancy()
        {
            return TestResult::failed();
        }

        TestResult::passed()
    })
}

/// Simple test to verify the property test functions compile and can be called
#[tokio::test]
async fn test_property_test_compilation() {
    // This test just verifies that the property test functions compile correctly
    // The actual property testing is done by quickcheck when the functions are called
    // with the #[quickcheck] attribute

    // We can't easily test the property functions directly since they require
    // specific input generation, but we can verify they compile
    let _ = (); // placeholder
}

/// Additional property tests for edge cases
mod edge_case_properties {
    use super::*;

    /// Test that validation results are consistent for zero sizes
    #[quickcheck]
    fn prop_zero_size_validation_consistency() -> TestResult {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let config = create_test_cache_config();

            let coordinator = CacheInitializationCoordinator::new(
                temp_dir.path().to_path_buf(),
                true,
                config.clone(),
            );

            // Create write cache manager with zero size
            let write_cache_manager = WriteCacheManager::new(
                temp_dir.path().to_path_buf(),
                1024 * 1024,
                50.0,
                config.put_ttl,
                config.incomplete_upload_ttl,
                s3_proxy::cache::CacheEvictionAlgorithm::TinyLFU,
                config.write_cache_max_object_size,
            );

            // Create size tracker with JournalConsolidator (zero size)
            let (size_tracker, _consolidator) = create_test_size_tracker(temp_dir.path()).await;

            let validation_results = coordinator
                .cross_validate_subsystems(Some(&write_cache_manager), Some(&size_tracker))
                .await
                .unwrap();

            // Zero sizes should be consistent
            if !validation_results.was_performed()
                || !validation_results.write_cache_consistent
                || validation_results.size_discrepancy_percent != 0.0
                || validation_results.has_serious_discrepancy()
                || validation_results.has_minor_discrepancy()
            {
                return TestResult::failed();
            }

            TestResult::passed()
        })
    }
}
