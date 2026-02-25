//! Unit tests for eviction coordination metrics tracking
//!
//! Tests that metrics increment correctly during lock operations
//! and are exposed via HTTP endpoint.
//!
//! Requirements: 7.1, 7.2, 7.3, 7.4, 7.5

use s3_proxy::metrics::MetricsManager;

/// Test that successful lock acquisition metric increments correctly
/// Requirement 7.1: Track the number of successful lock acquisitions
#[tokio::test]
async fn test_record_lock_acquisition_successful() {
    let metrics_manager = MetricsManager::new();

    // Initial state should be 0
    let initial_stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        initial_stats.lock_acquisitions_successful, 0,
        "Initial successful acquisitions should be 0"
    );

    // Record a successful acquisition
    metrics_manager.record_lock_acquisition_successful().await;

    // Verify increment
    let stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        stats.lock_acquisitions_successful, 1,
        "Successful acquisitions should be 1 after recording"
    );

    // Record another successful acquisition
    metrics_manager.record_lock_acquisition_successful().await;

    // Verify increment
    let stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        stats.lock_acquisitions_successful, 2,
        "Successful acquisitions should be 2 after second recording"
    );
}

/// Test that failed lock acquisition metric increments correctly
/// Requirement 7.2: Track the number of failed lock acquisitions
#[tokio::test]
async fn test_record_lock_acquisition_failed() {
    let metrics_manager = MetricsManager::new();

    // Initial state should be 0
    let initial_stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        initial_stats.lock_acquisitions_failed, 0,
        "Initial failed acquisitions should be 0"
    );

    // Record a failed acquisition
    metrics_manager.record_lock_acquisition_failed().await;

    // Verify increment
    let stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        stats.lock_acquisitions_failed, 1,
        "Failed acquisitions should be 1 after recording"
    );

    // Record multiple failed acquisitions
    metrics_manager.record_lock_acquisition_failed().await;
    metrics_manager.record_lock_acquisition_failed().await;

    // Verify increment
    let stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        stats.lock_acquisitions_failed, 3,
        "Failed acquisitions should be 3 after multiple recordings"
    );
}

/// Test that stale lock recovery metric increments correctly
/// Requirement 7.3: Track the number of stale locks forcibly acquired
#[tokio::test]
async fn test_record_stale_lock_recovered() {
    let metrics_manager = MetricsManager::new();

    // Initial state should be 0
    let initial_stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        initial_stats.stale_locks_recovered, 0,
        "Initial stale locks recovered should be 0"
    );

    // Record a stale lock recovery
    metrics_manager.record_stale_lock_recovered().await;

    // Verify increment
    let stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        stats.stale_locks_recovered, 1,
        "Stale locks recovered should be 1 after recording"
    );
}

/// Test that lock hold time metric accumulates correctly
/// Requirement 7.4: Track the total time spent holding the eviction lock
#[tokio::test]
async fn test_record_lock_hold_time() {
    let metrics_manager = MetricsManager::new();

    // Initial state should be 0
    let initial_stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        initial_stats.total_lock_hold_time_ms, 0,
        "Initial lock hold time should be 0"
    );

    // Record lock hold time
    metrics_manager.record_lock_hold_time(100).await;

    // Verify accumulation
    let stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        stats.total_lock_hold_time_ms, 100,
        "Lock hold time should be 100ms after recording"
    );

    // Record additional lock hold time
    metrics_manager.record_lock_hold_time(250).await;

    // Verify accumulation
    let stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        stats.total_lock_hold_time_ms, 350,
        "Lock hold time should be 350ms after second recording"
    );
}

/// Test that coordinated eviction metric increments correctly
/// Requirement 7.5: Track evictions performed while holding the lock
#[tokio::test]
async fn test_record_eviction_coordinated() {
    let metrics_manager = MetricsManager::new();

    // Initial state should be 0
    let initial_stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        initial_stats.evictions_coordinated, 0,
        "Initial coordinated evictions should be 0"
    );

    // Record a coordinated eviction
    metrics_manager.record_eviction_coordinated().await;

    // Verify increment
    let stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        stats.evictions_coordinated, 1,
        "Coordinated evictions should be 1 after recording"
    );
}

/// Test that eviction skipped metric increments correctly
/// Requirement 7.5: Track evictions skipped because another instance holds the lock
#[tokio::test]
async fn test_record_eviction_skipped_lock_held() {
    let metrics_manager = MetricsManager::new();

    // Initial state should be 0
    let initial_stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        initial_stats.evictions_skipped_lock_held, 0,
        "Initial evictions skipped should be 0"
    );

    // Record an eviction skipped
    metrics_manager.record_eviction_skipped_lock_held().await;

    // Verify increment
    let stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        stats.evictions_skipped_lock_held, 1,
        "Evictions skipped should be 1 after recording"
    );
}

/// Test that all metrics can be recorded independently
/// Requirements: 7.1, 7.2, 7.3, 7.4, 7.5
#[tokio::test]
async fn test_all_metrics_independent() {
    let metrics_manager = MetricsManager::new();

    // Record various metrics
    metrics_manager.record_lock_acquisition_successful().await;
    metrics_manager.record_lock_acquisition_successful().await;
    metrics_manager.record_lock_acquisition_failed().await;
    metrics_manager.record_stale_lock_recovered().await;
    metrics_manager.record_lock_hold_time(500).await;
    metrics_manager.record_eviction_coordinated().await;
    metrics_manager.record_eviction_coordinated().await;
    metrics_manager.record_eviction_coordinated().await;
    metrics_manager.record_eviction_skipped_lock_held().await;

    // Verify all metrics are tracked independently
    let stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(stats.lock_acquisitions_successful, 2);
    assert_eq!(stats.lock_acquisitions_failed, 1);
    assert_eq!(stats.stale_locks_recovered, 1);
    assert_eq!(stats.total_lock_hold_time_ms, 500);
    assert_eq!(stats.evictions_coordinated, 3);
    assert_eq!(stats.evictions_skipped_lock_held, 1);
}

/// Test that metrics are included in collected system metrics
/// Requirement 7.5: Expose metrics via the metrics endpoint
#[tokio::test]
async fn test_metrics_included_in_system_metrics() {
    let metrics_manager = MetricsManager::new();

    // Record some metrics
    metrics_manager.record_lock_acquisition_successful().await;
    metrics_manager.record_lock_acquisition_failed().await;
    metrics_manager.record_stale_lock_recovered().await;
    metrics_manager.record_lock_hold_time(200).await;
    metrics_manager.record_eviction_coordinated().await;
    metrics_manager.record_eviction_skipped_lock_held().await;

    // Collect system metrics
    let system_metrics = metrics_manager.collect_metrics().await;

    // Verify eviction coordination metrics are present
    assert!(
        system_metrics.eviction_coordination.is_some(),
        "Eviction coordination metrics should be present in system metrics"
    );

    let eviction_metrics = system_metrics.eviction_coordination.unwrap();
    assert_eq!(eviction_metrics.lock_acquisitions_successful, 1);
    assert_eq!(eviction_metrics.lock_acquisitions_failed, 1);
    assert_eq!(eviction_metrics.stale_locks_recovered, 1);
    assert_eq!(eviction_metrics.total_lock_hold_time_ms, 200);
    assert_eq!(eviction_metrics.evictions_coordinated, 1);
    assert_eq!(eviction_metrics.evictions_skipped_lock_held, 1);
}

/// Test that metrics can be serialized to JSON for HTTP endpoint
/// Requirement 7.5: Expose metrics via the metrics endpoint
#[tokio::test]
async fn test_metrics_serialization_for_http_endpoint() {
    let metrics_manager = MetricsManager::new();

    // Record some metrics
    metrics_manager.record_lock_acquisition_successful().await;
    metrics_manager.record_lock_hold_time(150).await;
    metrics_manager.record_eviction_coordinated().await;

    // Collect system metrics
    let system_metrics = metrics_manager.collect_metrics().await;

    // Serialize to JSON (as done by handle_metrics_request)
    let json = serde_json::to_string_pretty(&system_metrics)
        .expect("System metrics should be serializable to JSON");

    // Verify JSON contains eviction coordination metrics
    assert!(
        json.contains("eviction_coordination"),
        "JSON should contain eviction_coordination section"
    );
    assert!(
        json.contains("lock_acquisitions_successful"),
        "JSON should contain lock_acquisitions_successful field"
    );
    assert!(
        json.contains("lock_acquisitions_failed"),
        "JSON should contain lock_acquisitions_failed field"
    );
    assert!(
        json.contains("stale_locks_recovered"),
        "JSON should contain stale_locks_recovered field"
    );
    assert!(
        json.contains("total_lock_hold_time_ms"),
        "JSON should contain total_lock_hold_time_ms field"
    );
    assert!(
        json.contains("evictions_coordinated"),
        "JSON should contain evictions_coordinated field"
    );
    assert!(
        json.contains("evictions_skipped_lock_held"),
        "JSON should contain evictions_skipped_lock_held field"
    );

    // Parse JSON back to verify structure
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("JSON should be parseable");

    // Verify eviction_coordination section exists and has correct values
    let eviction_section = &parsed["eviction_coordination"];
    assert!(
        !eviction_section.is_null(),
        "eviction_coordination section should exist"
    );
    assert_eq!(
        eviction_section["lock_acquisitions_successful"], 1,
        "lock_acquisitions_successful should be 1"
    );
    assert_eq!(
        eviction_section["total_lock_hold_time_ms"], 150,
        "total_lock_hold_time_ms should be 150"
    );
    assert_eq!(
        eviction_section["evictions_coordinated"], 1,
        "evictions_coordinated should be 1"
    );
}

/// Test that EvictionCoordinationStats implements Clone correctly
#[tokio::test]
async fn test_eviction_coordination_stats_clone() {
    let metrics_manager = MetricsManager::new();

    // Record some metrics
    metrics_manager.record_lock_acquisition_successful().await;
    metrics_manager.record_lock_acquisition_failed().await;

    // Get stats (which clones internally)
    let stats1 = metrics_manager.get_eviction_coordination_stats().await;
    let stats2 = metrics_manager.get_eviction_coordination_stats().await;

    // Both should have same values
    assert_eq!(
        stats1.lock_acquisitions_successful,
        stats2.lock_acquisitions_successful
    );
    assert_eq!(
        stats1.lock_acquisitions_failed,
        stats2.lock_acquisitions_failed
    );
}

/// Test concurrent metric recording
/// Requirements: 7.1, 7.2, 7.3, 7.4, 7.5
#[tokio::test]
async fn test_concurrent_metric_recording() {
    use std::sync::Arc;

    let metrics_manager = Arc::new(MetricsManager::new());

    // Spawn multiple tasks to record metrics concurrently
    let mut handles = vec![];

    for _ in 0..10 {
        let mm = Arc::clone(&metrics_manager);
        handles.push(tokio::spawn(async move {
            mm.record_lock_acquisition_successful().await;
        }));
    }

    for _ in 0..5 {
        let mm = Arc::clone(&metrics_manager);
        handles.push(tokio::spawn(async move {
            mm.record_lock_acquisition_failed().await;
        }));
    }

    for _ in 0..3 {
        let mm = Arc::clone(&metrics_manager);
        handles.push(tokio::spawn(async move {
            mm.record_stale_lock_recovered().await;
        }));
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.expect("Task should complete successfully");
    }

    // Verify all metrics were recorded
    let stats = metrics_manager.get_eviction_coordination_stats().await;
    assert_eq!(
        stats.lock_acquisitions_successful, 10,
        "Should have 10 successful acquisitions"
    );
    assert_eq!(
        stats.lock_acquisitions_failed, 5,
        "Should have 5 failed acquisitions"
    );
    assert_eq!(
        stats.stale_locks_recovered, 3,
        "Should have 3 stale locks recovered"
    );
}
