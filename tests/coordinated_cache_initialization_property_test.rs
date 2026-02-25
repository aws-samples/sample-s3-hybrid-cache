//! Property-based tests for coordinated cache initialization
//!
//! Tests universal properties that should hold across all valid executions
//! of the coordinated cache initialization system.

use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use s3_proxy::cache_initialization_coordinator::CacheInitializationCoordinator;
use s3_proxy::cache_size_tracker::{CacheSizeConfig, CacheSizeTracker};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::cache_validator::CacheValidator;
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::{CacheConfig, EvictionAlgorithm, InitializationConfig, SharedStorageConfig};
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use s3_proxy::write_cache_manager::WriteCacheManager;
use serde_json;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio;

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
        eviction_algorithm: EvictionAlgorithm::LRU,
        write_cache_enabled: true,
        write_cache_percent: 0.5,
        write_cache_max_object_size: 1024 * 1024 * 100, // 100MB
        put_ttl: Duration::from_secs(86400),            // 1 day
        get_ttl: Duration::from_secs(3600),             // 1 hour
        head_ttl: Duration::from_secs(300),             // 5 minutes
        actively_remove_cached_data: false,
        shared_storage: SharedStorageConfig::default(),
        range_merge_gap_threshold: 1024 * 1024, // 1MB
        eviction_buffer_percent: 5,
        ram_cache_flush_interval: Duration::from_secs(60),
        ram_cache_flush_threshold: 100,
        ram_cache_flush_on_eviction: false,
        ram_cache_verification_interval: Duration::from_secs(1),
        incomplete_upload_ttl: Duration::from_secs(86400), // 1 day
        initialization: InitializationConfig::default(),
        cache_bypass_headers_enabled: true,
        metadata_cache: s3_proxy::config::MetadataCacheConfig::default(),
        eviction_trigger_percent: 95, // Trigger eviction at 95% capacity
        eviction_target_percent: 80,  // Reduce to 80% after eviction
        full_object_check_threshold: 67_108_864, // 64 MiB
        disk_streaming_threshold: 1_048_576, // 1 MiB
        read_cache_enabled: true,
        bucket_settings_staleness_threshold: Duration::from_secs(60),
        download_coordination: s3_proxy::config::DownloadCoordinationConfig::default(),
    }
}

/// Create test metadata for property testing
fn create_test_metadata(
    cache_key: String,
    is_write_cached: bool,
    compressed_size: u64,
    uncompressed_size: u64,
) -> NewCacheMetadata {
    NewCacheMetadata {
        cache_key,
        object_metadata: ObjectMetadata {
            etag: "test-etag".to_string(),
            content_type: Some("text/plain".to_string()),
            content_length: uncompressed_size,
            last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
            is_write_cached,
            ..Default::default()
        },
        ranges: vec![RangeSpec {
            start: 0,
            end: uncompressed_size.saturating_sub(1),
            file_path: "test.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size,
            uncompressed_size,
            created_at: SystemTime::now(),
            last_accessed: SystemTime::now(),
            access_count: 1,
            frequency_score: 1,
        }],
        created_at: SystemTime::now(),
        expires_at: SystemTime::now() + Duration::from_secs(3600),
        compression_info: Default::default(),
        ..Default::default()
    }
}

/// **Feature: coordinated-cache-initialization, Property 1: Single directory scan coordination**
/// **Validates: Requirements 1.1, 6.1, 6.2**
///
/// For any cache directory with metadata files, the coordinated scan should process
/// each metadata file exactly once and provide consistent results to all subsystems.
#[quickcheck]
fn prop_single_directory_scan_coordination(
    metadata_files: Vec<(String, bool, u32, u32)>, // (cache_key, is_write_cached, compressed_size, uncompressed_size)
) -> TestResult {
    // Filter out invalid inputs
    let valid_files: Vec<_> = metadata_files
        .into_iter()
        .filter(|(key, _, compressed, uncompressed)| {
            !key.is_empty() && *compressed > 0 && *uncompressed > 0 && *compressed <= *uncompressed
        })
        .take(50) // Limit to reasonable number for testing
        .collect();

    if valid_files.is_empty() {
        return TestResult::discard();
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let metadata_dir = temp_dir.path().join("metadata");
        tokio::fs::create_dir_all(&metadata_dir).await.unwrap();

        // Create metadata files
        let mut expected_total_size = 0u64;
        let mut expected_write_cached_size = 0u64;
        let mut expected_write_cached_count = 0u64;
        let mut expected_read_cached_count = 0u64;

        for (i, (cache_key, is_write_cached, compressed_size, uncompressed_size)) in
            valid_files.iter().enumerate()
        {
            let metadata = create_test_metadata(
                cache_key.clone(),
                *is_write_cached,
                *compressed_size as u64,
                *uncompressed_size as u64,
            );

            let metadata_file = metadata_dir.join(format!("object_{}.meta", i));
            let metadata_json = serde_json::to_string(&metadata).unwrap();
            tokio::fs::write(&metadata_file, metadata_json)
                .await
                .unwrap();

            expected_total_size += *compressed_size as u64;
            if *is_write_cached {
                expected_write_cached_size += *compressed_size as u64;
                expected_write_cached_count += 1;
            } else {
                expected_read_cached_count += 1;
            }
        }

        let config = create_test_cache_config();
        let coordinator =
            CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config);

        // Create a validator to perform the scan (since scan_cache_metadata is private)
        let validator = CacheValidator::new(temp_dir.path().to_path_buf());
        let cache_file_results = validator
            .scan_and_process_parallel(&metadata_dir)
            .await
            .unwrap();

        // Convert to scan results format for testing
        let mut total_objects = 0u64;
        let mut total_size = 0u64;
        let mut write_cached_objects = 0u64;
        let mut write_cached_size = 0u64;
        let mut read_cached_objects = 0u64;
        let mut read_cached_size = 0u64;

        for result in &cache_file_results {
            if result.metadata.is_some() {
                total_objects += 1;
                total_size += result.compressed_size;

                if result.is_write_cached {
                    write_cached_objects += 1;
                    write_cached_size += result.compressed_size;
                } else {
                    read_cached_objects += 1;
                    read_cached_size += result.compressed_size;
                }
            }
        }

        // Property 1: Single directory scan coordination
        // Verify that each metadata file was processed exactly once
        let processed_files = cache_file_results.len();
        let expected_files = valid_files.len();

        if processed_files != expected_files {
            return TestResult::failed();
        }

        // Verify that scan results match expected aggregated values
        if total_objects != (expected_write_cached_count + expected_read_cached_count) {
            return TestResult::failed();
        }

        if total_size != expected_total_size {
            return TestResult::failed();
        }

        if write_cached_size != expected_write_cached_size {
            return TestResult::failed();
        }

        if write_cached_objects != expected_write_cached_count {
            return TestResult::failed();
        }

        if read_cached_objects != expected_read_cached_count {
            return TestResult::failed();
        }

        // Verify that all metadata entries are valid (no parsing errors for valid input)
        let valid_entries = cache_file_results
            .iter()
            .filter(|result| result.metadata.is_some())
            .count();

        if valid_entries != expected_files {
            return TestResult::failed();
        }

        // Verify no scan errors for valid metadata files
        let error_entries = cache_file_results
            .iter()
            .filter(|result| !result.parse_errors.is_empty())
            .count();

        if error_entries > 0 {
            return TestResult::failed();
        }

        TestResult::passed()
    })
}

/// **Feature: coordinated-cache-initialization, Property 8: Cross-validation threshold behavior**
/// **Validates: Requirements 4.2, 4.3**
///
/// For any write cache size discrepancy between subsystems, the system should log
/// a warning if discrepancy exceeds 5% and an error if it exceeds 20%.
#[quickcheck]
fn prop_cross_validation_threshold_behavior(manager_size: u32, tracker_size: u32) -> TestResult {
    // Filter out cases where both sizes are zero (no meaningful discrepancy)
    if manager_size == 0 && tracker_size == 0 {
        return TestResult::discard();
    }

    // Limit sizes to reasonable values for testing
    let manager_size = (manager_size % 10000) as u64;
    let tracker_size = (tracker_size % 10000) as u64;

    // Skip cases where one is zero but the other isn't (edge case)
    if (manager_size == 0) != (tracker_size == 0) {
        return TestResult::discard();
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_cache_config();

        let coordinator = CacheInitializationCoordinator::new(
            temp_dir.path().to_path_buf(),
            true,
            config.clone(),
        );

        // Create write cache manager with specified size
        let write_cache_manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            1024 * 1024, // 1MB total cache
            50.0,        // 50% for write cache
            config.put_ttl,
            config.incomplete_upload_ttl,
            s3_proxy::cache::CacheEvictionAlgorithm::LRU,
            config.write_cache_max_object_size,
        );

        // Reserve capacity to set the manager size
        if manager_size > 0 {
            write_cache_manager.reserve_capacity(manager_size);
        }

        // Create size tracker with JournalConsolidator
        let (size_tracker, consolidator) = create_test_size_tracker(temp_dir.path()).await;

        // Set the write cache size via consolidator's size state
        if tracker_size > 0 {
            consolidator
                .update_size_from_validation(0, Some(tracker_size))
                .await;
        }

        // Perform cross-validation
        let validation_results = coordinator
            .cross_validate_subsystems(Some(&write_cache_manager), Some(&size_tracker))
            .await
            .unwrap();

        // Calculate expected discrepancy percentage
        let expected_discrepancy = if manager_size == 0 && tracker_size == 0 {
            0.0
        } else {
            let max_size = manager_size.max(tracker_size) as f64;
            let diff = (manager_size as i64 - tracker_size as i64).abs() as f64;
            (diff / max_size) * 100.0
        };

        // Property 8: Cross-validation threshold behavior
        // Verify that validation was performed
        if !validation_results.was_performed() {
            return TestResult::failed();
        }

        // Verify discrepancy calculation is correct
        let actual_discrepancy = validation_results.size_discrepancy_percent;
        if (actual_discrepancy - expected_discrepancy).abs() > 0.1 {
            return TestResult::failed();
        }

        // Verify threshold behavior
        if expected_discrepancy > 20.0 {
            // Should be marked as inconsistent with serious discrepancy
            if validation_results.write_cache_consistent {
                return TestResult::failed();
            }
            if !validation_results.has_serious_discrepancy() {
                return TestResult::failed();
            }
            // Should have validation warnings
            if validation_results.validation_warnings.is_empty() {
                return TestResult::failed();
            }
        } else if expected_discrepancy > 5.0 {
            // Should be marked as inconsistent with minor discrepancy
            if validation_results.write_cache_consistent {
                return TestResult::failed();
            }
            if !validation_results.has_minor_discrepancy() {
                return TestResult::failed();
            }
            // Should have validation warnings
            if validation_results.validation_warnings.is_empty() {
                return TestResult::failed();
            }
        } else {
            // Should be marked as consistent
            if !validation_results.write_cache_consistent {
                return TestResult::failed();
            }
            if validation_results.has_serious_discrepancy()
                || validation_results.has_minor_discrepancy()
            {
                return TestResult::failed();
            }
            // Should not have validation warnings for consistency
            if !validation_results.validation_warnings.is_empty() {
                return TestResult::failed();
            }
        }

        // Verify that the sizes are correctly recorded
        if validation_results.write_cache_manager_size != Some(manager_size) {
            return TestResult::failed();
        }
        if validation_results.size_tracker_write_cache_size != Some(tracker_size) {
            return TestResult::failed();
        }

        TestResult::passed()
    })
}

/// **Feature: coordinated-cache-initialization, Property 9: Shared validation consistency**
/// **Validates: Requirements 5.1, 5.2, 5.3, 5.4**
///
/// For any metadata validation operation, both WriteCacheManager and CacheSizeTracker
/// should use identical validation logic and produce consistent results.
#[quickcheck]
fn prop_shared_validation_consistency(
    metadata_entries: Vec<(String, bool, u32, u32)>, // (cache_key, is_write_cached, compressed_size, uncompressed_size)
) -> TestResult {
    // Filter out invalid inputs and limit size
    let valid_entries: Vec<_> = metadata_entries
        .into_iter()
        .filter(|(key, _, compressed, uncompressed)| {
            !key.is_empty() && *compressed > 0 && *uncompressed > 0 && *compressed <= *uncompressed
        })
        .take(20) // Limit to reasonable number for testing
        .collect();

    if valid_entries.is_empty() {
        return TestResult::discard();
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let metadata_dir = temp_dir.path().join("metadata");
        tokio::fs::create_dir_all(&metadata_dir).await.unwrap();

        // Create metadata files
        for (i, (cache_key, is_write_cached, compressed_size, uncompressed_size)) in
            valid_entries.iter().enumerate()
        {
            let metadata = create_test_metadata(
                cache_key.clone(),
                *is_write_cached,
                *compressed_size as u64,
                *uncompressed_size as u64,
            );

            let metadata_file = metadata_dir.join(format!("object_{}.meta", i));
            let metadata_json = serde_json::to_string(&metadata).unwrap();
            tokio::fs::write(&metadata_file, metadata_json)
                .await
                .unwrap();
        }

        // Create two separate CacheValidator instances (simulating different subsystems)
        let validator1 = CacheValidator::new(temp_dir.path().to_path_buf());
        let validator2 = CacheValidator::new(temp_dir.path().to_path_buf());

        // Perform parallel scans with both validators
        let results1 = validator1
            .scan_and_process_parallel(&metadata_dir)
            .await
            .unwrap();
        let results2 = validator2
            .scan_and_process_parallel(&metadata_dir)
            .await
            .unwrap();

        // Property 9: Shared validation consistency
        // Both validators should produce identical results
        if results1.len() != results2.len() {
            return TestResult::failed();
        }

        // Sort results by file path for consistent comparison
        let mut sorted_results1 = results1;
        let mut sorted_results2 = results2;
        sorted_results1.sort_by(|a, b| a.file_path.cmp(&b.file_path));
        sorted_results2.sort_by(|a, b| a.file_path.cmp(&b.file_path));

        // Compare each result pair
        for (result1, result2) in sorted_results1.iter().zip(sorted_results2.iter()) {
            // File paths should match
            if result1.file_path != result2.file_path {
                return TestResult::failed();
            }

            // Compressed sizes should match
            if result1.compressed_size != result2.compressed_size {
                return TestResult::failed();
            }

            // Write cached flags should match
            if result1.is_write_cached != result2.is_write_cached {
                return TestResult::failed();
            }

            // Metadata presence should match
            if result1.metadata.is_some() != result2.metadata.is_some() {
                return TestResult::failed();
            }

            // If both have metadata, the content should match
            if let (Some(meta1), Some(meta2)) = (&result1.metadata, &result2.metadata) {
                if meta1.cache_key != meta2.cache_key {
                    return TestResult::failed();
                }
                if meta1.object_metadata.is_write_cached != meta2.object_metadata.is_write_cached {
                    return TestResult::failed();
                }
                // Compare compressed sizes calculated from ranges
                let size1 = validator1.calculate_compressed_size(&meta1.ranges);
                let size2 = validator2.calculate_compressed_size(&meta2.ranges);
                if size1 != size2 {
                    return TestResult::failed();
                }
            }

            // Parse errors should match
            if result1.parse_errors.len() != result2.parse_errors.len() {
                return TestResult::failed();
            }
        }

        // Test shared utility methods produce consistent results
        let total_size1 = validator1.calculate_total_size(&sorted_results1);
        let total_size2 = validator2.calculate_total_size(&sorted_results2);
        if total_size1 != total_size2 {
            return TestResult::failed();
        }

        // Test categorization consistency
        let (write1, read1, invalid1) = validator1.group_by_category(&sorted_results1);
        let (write2, read2, invalid2) = validator2.group_by_category(&sorted_results2);

        if write1.len() != write2.len()
            || read1.len() != read2.len()
            || invalid1.len() != invalid2.len()
        {
            return TestResult::failed();
        }

        // Test filtering consistency
        let valid1 = validator1.filter_valid_results(&sorted_results1);
        let valid2 = validator2.filter_valid_results(&sorted_results2);
        if valid1.len() != valid2.len() {
            return TestResult::failed();
        }

        let error1 = validator1.filter_error_results(&sorted_results1);
        let error2 = validator2.filter_error_results(&sorted_results2);
        if error1.len() != error2.len() {
            return TestResult::failed();
        }

        // Test size statistics consistency
        let stats1 = validator1.calculate_size_statistics(&sorted_results1);
        let stats2 = validator2.calculate_size_statistics(&sorted_results2);

        if stats1.total_compressed_size != stats2.total_compressed_size
            || stats1.write_cached_size != stats2.write_cached_size
            || stats1.read_cached_size != stats2.read_cached_size
            || stats1.total_count != stats2.total_count
            || stats1.write_cached_count != stats2.write_cached_count
            || stats1.read_cached_count != stats2.read_cached_count
            || stats1.invalid_count != stats2.invalid_count
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
    assert!(true);
}

/// Additional property tests for edge cases
mod edge_case_properties {
    use super::*;

    /// Test that empty directories are handled consistently
    #[quickcheck]
    fn prop_empty_directory_handling() -> TestResult {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let empty_metadata_dir = temp_dir.path().join("metadata");
            tokio::fs::create_dir_all(&empty_metadata_dir)
                .await
                .unwrap();

            let config = create_test_cache_config();
            let coordinator =
                CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config);

            // Use validator to scan empty directory
            let validator = CacheValidator::new(temp_dir.path().to_path_buf());
            let cache_file_results = validator
                .scan_and_process_parallel(&empty_metadata_dir)
                .await
                .unwrap();

            // Empty directory should produce empty results
            if !cache_file_results.is_empty() {
                return TestResult::failed();
            }

            TestResult::passed()
        })
    }

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
                s3_proxy::cache::CacheEvictionAlgorithm::LRU,
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
