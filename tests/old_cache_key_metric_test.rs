use s3_proxy::metrics::MetricsManager;

#[tokio::test]
async fn test_old_cache_key_encounter_metric() {
    // Create metrics manager
    let metrics_manager = MetricsManager::new();

    // Initially, old cache key encounters should be 0
    let initial_count = metrics_manager.get_old_cache_key_encounters().await;
    assert_eq!(
        initial_count, 0,
        "Initial old cache key encounters should be 0"
    );

    // Record an old cache key encounter
    metrics_manager.record_old_cache_key_encounter().await;

    // Verify the counter incremented
    let count_after_one = metrics_manager.get_old_cache_key_encounters().await;
    assert_eq!(count_after_one, 1, "Should have 1 old cache key encounter");

    // Record multiple encounters
    for _ in 0..5 {
        metrics_manager.record_old_cache_key_encounter().await;
    }

    // Verify the counter incremented correctly
    let count_after_six = metrics_manager.get_old_cache_key_encounters().await;
    assert_eq!(count_after_six, 6, "Should have 6 old cache key encounters");

    println!(
        "Old cache key encounters metric test passed: {}",
        count_after_six
    );
}

#[tokio::test]
async fn test_old_cache_key_metric_in_cache_metrics() {
    // Create metrics manager
    let metrics_manager = MetricsManager::new();

    // Record some old cache key encounters
    for _ in 0..3 {
        metrics_manager.record_old_cache_key_encounter().await;
    }

    // Collect system metrics
    let _system_metrics = metrics_manager.collect_metrics().await;

    // Verify the metric is tracked
    let count = metrics_manager.get_old_cache_key_encounters().await;
    assert_eq!(count, 3, "Should have 3 old cache key encounters");

    println!("Old cache key metric in system metrics test passed");
}
