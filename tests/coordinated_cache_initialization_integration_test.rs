//! Integration tests for coordinated cache initialization
//!
//! Tests the full initialization process including multi-instance coordination
//! and configuration-driven behavior.

// Removed unused import
use s3_proxy::cache_initialization_coordinator::CacheInitializationCoordinator;
use s3_proxy::cache_size_tracker::{CacheSizeConfig, CacheSizeTracker};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
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
    create_test_size_tracker_with_write_cache(cache_dir, 0).await
}

/// Create a test size tracker with a pre-initialized write cache size.
/// This is needed for cross-validation tests where WriteCacheManager is initialized
/// from scan results but CacheSizeTracker reads from size_state.json.
async fn create_test_size_tracker_with_write_cache(
    cache_dir: &std::path::Path,
    write_cache_size: u64,
) -> (Arc<CacheSizeTracker>, Arc<JournalConsolidator>) {
    // Create required directories
    std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
    std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
    std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

    // Create size_state.json with the expected write cache size
    // This ensures cross-validation passes when comparing WriteCacheManager (from scan)
    // with CacheSizeTracker (from size_state.json)
    if write_cache_size > 0 {
        // Note: last_consolidation uses Unix timestamp (seconds since epoch), not ISO 8601
        let size_state = serde_json::json!({
            "total_size": write_cache_size,
            "write_cache_size": write_cache_size,
            "last_consolidation": 1704067200u64,  // 2024-01-01 00:00:00 UTC as Unix timestamp
            "consolidation_count": 0u64,
            "last_updated_by": "test-instance"
        });
        std::fs::write(
            cache_dir.join("size_tracking/size_state.json"),
            serde_json::to_string_pretty(&size_state).unwrap(),
        )
        .unwrap();
    }

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
fn create_test_cache_config(cache_dir: PathBuf, write_cache_enabled: bool) -> CacheConfig {
    CacheConfig {
        cache_dir,
        max_cache_size: 1024 * 1024 * 1024, // 1GB
        ram_cache_enabled: false,
        max_ram_cache_size: 0,
        eviction_algorithm: EvictionAlgorithm::LRU,
        write_cache_enabled,
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

/// Create test metadata for integration testing
fn create_test_metadata(
    cache_key: &str,
    is_write_cached: bool,
    compressed_size: u64,
    uncompressed_size: u64,
) -> NewCacheMetadata {
    NewCacheMetadata {
        cache_key: cache_key.to_string(),
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
            end: uncompressed_size - 1,
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
/// Test full initialization process with both subsystems
#[tokio::test]
async fn test_full_initialization_process() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_cache_config(temp_dir.path().to_path_buf(), true);

    // Create metadata directory with test metadata
    let metadata_dir = temp_dir.path().join("metadata");
    tokio::fs::create_dir_all(&metadata_dir).await.unwrap();

    // Create test metadata files
    let metadata1 = create_test_metadata("bucket/object1", true, 512, 1024);
    let metadata2 = create_test_metadata("bucket/object2", false, 256, 512);
    let metadata3 = create_test_metadata("bucket/object3", true, 1024, 2048);

    let file1 = metadata_dir.join("object1.meta");
    let file2 = metadata_dir.join("object2.meta");
    let file3 = metadata_dir.join("object3.meta");

    tokio::fs::write(&file1, serde_json::to_string(&metadata1).unwrap())
        .await
        .unwrap();
    tokio::fs::write(&file2, serde_json::to_string(&metadata2).unwrap())
        .await
        .unwrap();
    tokio::fs::write(&file3, serde_json::to_string(&metadata3).unwrap())
        .await
        .unwrap();

    // Create coordinator
    let coordinator =
        CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config.clone());

    // Create subsystems
    let mut write_cache_manager = WriteCacheManager::new(
        temp_dir.path().to_path_buf(),
        1024 * 1024, // 1MB total cache
        50.0,        // 50% for write cache
        config.put_ttl,
        config.incomplete_upload_ttl,
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        config.write_cache_max_object_size,
    );

    // Create size tracker with JournalConsolidator
    // Pre-initialize with expected write cache size (1536 = 512 + 1024) for cross-validation
    let (tracker, _consolidator) =
        create_test_size_tracker_with_write_cache(temp_dir.path(), 1536).await;
    let mut size_tracker = Some(tracker);

    // Perform initialization
    let summary = coordinator
        .initialize(Some(&mut write_cache_manager), &mut size_tracker)
        .await
        .unwrap();

    // Verify initialization summary
    assert!(summary.total_duration > Duration::ZERO);
    assert_eq!(summary.total_errors, 0);
    assert_eq!(summary.scan_summary.total_objects, 3);
    assert_eq!(summary.scan_summary.total_size, 1792); // 512 + 256 + 1024
    assert_eq!(summary.scan_summary.write_cached_objects, 2);
    assert_eq!(summary.scan_summary.write_cached_size, 1536); // 512 + 1024
    assert_eq!(summary.scan_summary.read_cached_objects, 1);
    assert_eq!(summary.scan_summary.read_cached_size, 256);

    // Verify subsystem initialization
    assert!(summary.subsystem_results.write_cache_initialized);
    assert!(summary.subsystem_results.size_tracker_initialized);

    // Verify cross-validation was performed
    assert!(summary.validation_results.is_some());
    let validation = summary.validation_results.unwrap();
    assert!(validation.was_performed());
    assert!(validation.write_cache_consistent);
}

/// Test initialization with write cache disabled
#[tokio::test]
async fn test_initialization_write_cache_disabled() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_cache_config(temp_dir.path().to_path_buf(), false);

    // Create metadata directory with test metadata
    let metadata_dir = temp_dir.path().join("metadata");
    tokio::fs::create_dir_all(&metadata_dir).await.unwrap();

    // Create test metadata files (all should be treated as read-cached)
    let metadata1 = create_test_metadata("bucket/object1", true, 512, 1024); // Will be ignored
    let metadata2 = create_test_metadata("bucket/object2", false, 256, 512);

    let file1 = metadata_dir.join("object1.meta");
    let file2 = metadata_dir.join("object2.meta");

    tokio::fs::write(&file1, serde_json::to_string(&metadata1).unwrap())
        .await
        .unwrap();
    tokio::fs::write(&file2, serde_json::to_string(&metadata2).unwrap())
        .await
        .unwrap();

    // Create coordinator with write cache disabled
    let coordinator =
        CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), false, config.clone());

    // Create only size tracker (no write cache manager)
    // Create size tracker with JournalConsolidator
    let (tracker, _consolidator) = create_test_size_tracker(temp_dir.path()).await;
    let mut size_tracker = Some(tracker);

    // Perform initialization without write cache manager
    let summary = coordinator
        .initialize(None, &mut size_tracker)
        .await
        .unwrap();

    // Verify initialization summary
    assert_eq!(summary.total_errors, 0);
    assert_eq!(summary.scan_summary.total_objects, 2);
    assert_eq!(summary.scan_summary.total_size, 768); // 512 + 256

    // With write cache disabled, objects are still categorized based on their metadata
    // but the write cache manager is not initialized
    assert_eq!(summary.scan_summary.write_cached_objects, 1); // Only the one marked as write-cached
    assert_eq!(summary.scan_summary.read_cached_objects, 1); // The one marked as read-cached

    // Verify subsystem initialization
    assert!(!summary.subsystem_results.write_cache_initialized);
    assert!(summary.subsystem_results.size_tracker_initialized);

    // Cross-validation should not be performed without write cache manager
    assert!(summary.validation_results.is_some());
    let validation = summary.validation_results.unwrap();
    assert!(!validation.was_performed());
}

/// Test initialization with empty cache directory
#[tokio::test]
async fn test_initialization_empty_cache() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_cache_config(temp_dir.path().to_path_buf(), true);

    // Create empty metadata directory
    let metadata_dir = temp_dir.path().join("metadata");
    tokio::fs::create_dir_all(&metadata_dir).await.unwrap();

    // Create coordinator
    let coordinator =
        CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config.clone());

    // Create subsystems
    let mut write_cache_manager = WriteCacheManager::new(
        temp_dir.path().to_path_buf(),
        1024 * 1024,
        50.0,
        config.put_ttl,
        config.incomplete_upload_ttl,
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        config.write_cache_max_object_size,
    );

    // Create size tracker with JournalConsolidator
    let (tracker, _consolidator) = create_test_size_tracker(temp_dir.path()).await;
    let mut size_tracker = Some(tracker);

    // Perform initialization
    let summary = coordinator
        .initialize(Some(&mut write_cache_manager), &mut size_tracker)
        .await
        .unwrap();

    // Verify empty cache initialization
    assert_eq!(summary.total_errors, 0);
    assert_eq!(summary.scan_summary.total_objects, 0);
    assert_eq!(summary.scan_summary.total_size, 0);
    assert_eq!(summary.scan_summary.write_cached_objects, 0);
    assert_eq!(summary.scan_summary.read_cached_objects, 0);
    assert!(summary.scan_summary.metadata_entries.is_empty());
    assert!(!summary.scan_summary.has_errors());

    // Verify subsystems were still initialized
    assert!(summary.subsystem_results.write_cache_initialized);
    assert!(summary.subsystem_results.size_tracker_initialized);

    // Cross-validation should still be performed (with zero sizes)
    assert!(summary.validation_results.is_some());
    let validation = summary.validation_results.unwrap();
    assert!(validation.was_performed());
    assert!(validation.write_cache_consistent);
    assert_eq!(validation.size_discrepancy_percent, 0.0);
}
/// Test initialization with corrupted metadata files
#[tokio::test]
async fn test_initialization_with_corrupted_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_cache_config(temp_dir.path().to_path_buf(), true);

    // Create metadata directory with mixed valid and invalid metadata
    let metadata_dir = temp_dir.path().join("metadata");
    tokio::fs::create_dir_all(&metadata_dir).await.unwrap();

    // Create valid metadata file
    let valid_metadata = create_test_metadata("bucket/valid", true, 512, 1024);
    let valid_file = metadata_dir.join("valid.meta");
    tokio::fs::write(&valid_file, serde_json::to_string(&valid_metadata).unwrap())
        .await
        .unwrap();

    // Create corrupted metadata files
    let corrupted_file1 = metadata_dir.join("corrupted1.meta");
    let corrupted_file2 = metadata_dir.join("corrupted2.meta");
    let empty_file = metadata_dir.join("empty.meta");

    tokio::fs::write(&corrupted_file1, "invalid json content")
        .await
        .unwrap();
    tokio::fs::write(&corrupted_file2, "{\"incomplete\": \"json\"")
        .await
        .unwrap();
    tokio::fs::write(&empty_file, "").await.unwrap();

    // Create coordinator
    let coordinator =
        CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config.clone());

    // Create subsystems
    let mut write_cache_manager = WriteCacheManager::new(
        temp_dir.path().to_path_buf(),
        1024 * 1024,
        50.0,
        config.put_ttl,
        config.incomplete_upload_ttl,
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        config.write_cache_max_object_size,
    );

    // Create size tracker with JournalConsolidator
    // Pre-initialize with expected write cache size (512) for cross-validation
    let (tracker, _consolidator) =
        create_test_size_tracker_with_write_cache(temp_dir.path(), 512).await;
    let mut size_tracker = Some(tracker);

    // Perform initialization
    let summary = coordinator
        .initialize(Some(&mut write_cache_manager), &mut size_tracker)
        .await
        .unwrap();

    // Verify initialization handled errors gracefully
    assert_eq!(summary.total_errors, 0); // Errors should be handled gracefully
    assert!(summary.total_warnings > 0); // But warnings should be generated

    // Only valid metadata should be counted
    assert_eq!(summary.scan_summary.total_objects, 1);
    assert_eq!(summary.scan_summary.total_size, 512);
    assert_eq!(summary.scan_summary.write_cached_objects, 1);
    assert_eq!(summary.scan_summary.write_cached_size, 512);

    // Should have processed all files (valid and invalid)
    assert_eq!(summary.scan_summary.metadata_entries.len(), 4);

    // Should have scan errors for corrupted files
    assert!(summary.scan_summary.has_errors());
    assert!(summary.scan_summary.error_count() > 0);

    // Subsystems should still be initialized
    assert!(summary.subsystem_results.write_cache_initialized);
    assert!(summary.subsystem_results.size_tracker_initialized);
}

/// Test multi-instance coordination with distributed locking
#[tokio::test]
async fn test_multi_instance_coordination() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_cache_config(temp_dir.path().to_path_buf(), true);

    // Create metadata directory with test metadata
    let metadata_dir = temp_dir.path().join("metadata");
    tokio::fs::create_dir_all(&metadata_dir).await.unwrap();

    // Create test metadata
    let metadata = create_test_metadata("bucket/shared", true, 1024, 2048);
    let metadata_file = metadata_dir.join("shared.meta");
    tokio::fs::write(&metadata_file, serde_json::to_string(&metadata).unwrap())
        .await
        .unwrap();

    // Create two coordinators (simulating two instances)
    let coordinator1 =
        CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config.clone());

    let coordinator2 =
        CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config.clone());

    // Create subsystems for both instances
    let mut write_cache_manager1 = WriteCacheManager::new(
        temp_dir.path().to_path_buf(),
        1024 * 1024,
        50.0,
        config.put_ttl,
        config.incomplete_upload_ttl,
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        config.write_cache_max_object_size,
    );

    let mut write_cache_manager2 = WriteCacheManager::new(
        temp_dir.path().to_path_buf(),
        1024 * 1024,
        50.0,
        config.put_ttl,
        config.incomplete_upload_ttl,
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        config.write_cache_max_object_size,
    );

    // Create size trackers with JournalConsolidator
    let (tracker1, _consolidator1) = create_test_size_tracker(temp_dir.path()).await;
    let mut size_tracker1 = Some(tracker1);

    let (tracker2, _consolidator2) = create_test_size_tracker(temp_dir.path()).await;
    let mut size_tracker2 = Some(tracker2);

    // Run both initializations concurrently with locking
    let (summary1, summary2) = tokio::join!(
        coordinator1.initialize_with_locking(Some(&mut write_cache_manager1), &mut size_tracker1,),
        coordinator2.initialize_with_locking(Some(&mut write_cache_manager2), &mut size_tracker2,)
    );

    // Both should succeed
    let summary1 = summary1.unwrap();
    let summary2 = summary2.unwrap();

    // Both should have consistent results
    assert_eq!(
        summary1.scan_summary.total_objects,
        summary2.scan_summary.total_objects
    );
    assert_eq!(
        summary1.scan_summary.total_size,
        summary2.scan_summary.total_size
    );
    assert_eq!(
        summary1.scan_summary.write_cached_size,
        summary2.scan_summary.write_cached_size
    );

    // Both should have successful subsystem initialization
    assert!(summary1.subsystem_results.write_cache_initialized);
    assert!(summary1.subsystem_results.size_tracker_initialized);
    assert!(summary2.subsystem_results.write_cache_initialized);
    assert!(summary2.subsystem_results.size_tracker_initialized);

    // Both should have performed cross-validation
    assert!(summary1.validation_results.is_some());
    assert!(summary2.validation_results.is_some());
}

/// Test configuration-driven behavior
#[tokio::test]
async fn test_configuration_driven_behavior() {
    let temp_dir = TempDir::new().unwrap();

    // Test with custom initialization configuration
    let mut config = create_test_cache_config(temp_dir.path().to_path_buf(), true);
    config.initialization = InitializationConfig {
        parallel_scan: true,
        scan_timeout: Duration::from_secs(30),
        progress_logging: true,
    };
    // Validation thresholds are now in shared_storage
    config.shared_storage.validation_threshold_warn = 3.0; // Lower warning threshold
    config.shared_storage.validation_threshold_error = 15.0; // Lower error threshold

    // Create metadata directory with test metadata
    let metadata_dir = temp_dir.path().join("metadata");
    tokio::fs::create_dir_all(&metadata_dir).await.unwrap();

    // Create test metadata
    let metadata = create_test_metadata("bucket/config-test", true, 512, 1024);
    let metadata_file = metadata_dir.join("config-test.meta");
    tokio::fs::write(&metadata_file, serde_json::to_string(&metadata).unwrap())
        .await
        .unwrap();

    // Create coordinator with custom config
    let coordinator =
        CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config.clone());

    // Create subsystems
    let mut write_cache_manager = WriteCacheManager::new(
        temp_dir.path().to_path_buf(),
        1024 * 1024,
        50.0,
        config.put_ttl,
        config.incomplete_upload_ttl,
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        config.write_cache_max_object_size,
    );

    // Create size tracker with JournalConsolidator
    // Pre-initialize with expected write cache size (512) for cross-validation
    let (tracker, _consolidator) =
        create_test_size_tracker_with_write_cache(temp_dir.path(), 512).await;
    let mut size_tracker = Some(tracker);

    // Perform initialization
    let summary = coordinator
        .initialize(Some(&mut write_cache_manager), &mut size_tracker)
        .await
        .unwrap();

    // Verify initialization respects configuration
    assert_eq!(summary.total_errors, 0);
    assert_eq!(summary.scan_summary.total_objects, 1);
    assert_eq!(summary.scan_summary.total_size, 512);

    // Verify cross-validation was performed (as configured)
    assert!(summary.validation_results.is_some());
    let validation = summary.validation_results.unwrap();
    assert!(validation.was_performed());

    // With matching sizes, should be consistent regardless of thresholds
    assert!(validation.write_cache_consistent);
}
