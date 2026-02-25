//! Cache Size Metrics Integration Test
//!
//! Tests that cache size metrics are properly integrated into the metrics system.
//!
//! NOTE: Tests have been updated because size tracking is now handled by
//! JournalConsolidator through journal entries. The update_size() method
//! has been removed from CacheSizeTracker.

use s3_proxy::cache_size_tracker::{CacheSizeConfig, CacheSizeTracker};
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use s3_proxy::metrics::MetricsManager;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// Helper function to create a test CacheSizeTracker with JournalConsolidator
async fn create_test_tracker(
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

    let config = CacheSizeConfig::default();
    let tracker = Arc::new(
        CacheSizeTracker::new(cache_dir.to_path_buf(), config, false, consolidator.clone())
            .await
            .unwrap(),
    );

    (tracker, consolidator)
}

#[tokio::test]
async fn test_cache_size_metrics_integration() {
    // Create temporary cache directory
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache size tracker with JournalConsolidator
    let (tracker, _consolidator) = create_test_tracker(&cache_dir).await;

    // NOTE: update_size() has been removed - size tracking is now handled by JournalConsolidator
    // Size starts at 0 for a fresh tracker

    // Create metrics manager and set the tracker
    let mut metrics_manager = MetricsManager::new();
    metrics_manager.set_cache_size_tracker(Arc::clone(&tracker));

    // Collect metrics
    let metrics = metrics_manager.collect_metrics().await;

    // Verify cache size metrics are present
    assert!(
        metrics.cache_size.is_some(),
        "Cache size metrics should be present"
    );

    let cache_size_metrics = metrics.cache_size.unwrap();

    // Verify the size starts at 0 (no update_size calls)
    assert_eq!(
        cache_size_metrics.current_size, 0,
        "Current size should be 0 for fresh tracker"
    );

    // Verify other fields are present
    assert_eq!(
        cache_size_metrics.checkpoint_count, 0,
        "Should have no checkpoints yet"
    );
    assert!(
        cache_size_metrics.last_validation.is_none(),
        "Should have no validation yet"
    );

    println!("Cache size metrics: {:?}", cache_size_metrics);
}

#[tokio::test]
async fn test_cache_size_metrics_json_serialization() {
    // Create temporary cache directory
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache size tracker with JournalConsolidator
    let (tracker, _consolidator) = create_test_tracker(&cache_dir).await;

    // NOTE: update_size() has been removed - size tracking is now handled by JournalConsolidator

    // Create metrics manager and set the tracker
    let mut metrics_manager = MetricsManager::new();
    metrics_manager.set_cache_size_tracker(Arc::clone(&tracker));

    // Collect metrics
    let metrics = metrics_manager.collect_metrics().await;

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&metrics).expect("Failed to serialize metrics to JSON");

    println!("Metrics JSON:\n{}", json);

    // Verify JSON contains cache_size field
    assert!(
        json.contains("cache_size"),
        "JSON should contain cache_size field"
    );
    assert!(
        json.contains("current_size"),
        "JSON should contain current_size field"
    );
    assert!(
        json.contains("last_checkpoint"),
        "JSON should contain last_checkpoint field"
    );
    assert!(
        json.contains("checkpoint_count"),
        "JSON should contain checkpoint_count field"
    );

    // Parse JSON back to verify it's valid
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Failed to parse JSON");

    // Verify cache_size object exists and has expected fields
    let cache_size = parsed
        .get("cache_size")
        .expect("cache_size field should exist");

    assert!(cache_size.is_object(), "cache_size should be an object");

    let cache_size_obj = cache_size.as_object().unwrap();
    assert!(
        cache_size_obj.contains_key("current_size"),
        "Should have current_size"
    );
    assert!(
        cache_size_obj.contains_key("last_checkpoint"),
        "Should have last_checkpoint"
    );
    assert!(
        cache_size_obj.contains_key("checkpoint_count"),
        "Should have checkpoint_count"
    );
    assert!(
        cache_size_obj.contains_key("delta_log_size"),
        "Should have delta_log_size"
    );

    // Verify the current_size value is 0 (no update_size calls)
    let current_size = cache_size_obj
        .get("current_size")
        .and_then(|v| v.as_u64())
        .expect("current_size should be a number");

    assert_eq!(
        current_size, 0,
        "Current size should be 0 for fresh tracker"
    );
}

#[tokio::test]
async fn test_cache_size_metrics_without_tracker() {
    // Create metrics manager without setting a tracker
    let metrics_manager = MetricsManager::new();

    // Collect metrics
    let metrics = metrics_manager.collect_metrics().await;

    // Verify cache size metrics are None when tracker is not set
    assert!(
        metrics.cache_size.is_none(),
        "Cache size metrics should be None when tracker is not set"
    );

    // Serialize to JSON to ensure it doesn't break
    let json = serde_json::to_string_pretty(&metrics).expect("Failed to serialize metrics to JSON");

    println!("Metrics JSON (without tracker):\n{}", json);

    // Verify JSON contains cache_size field as null
    assert!(
        json.contains("cache_size"),
        "JSON should contain cache_size field"
    );
    assert!(
        json.contains("\"cache_size\": null"),
        "cache_size should be null"
    );
}
