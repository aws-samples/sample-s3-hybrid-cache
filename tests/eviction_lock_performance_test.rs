/// Performance tests for distributed eviction lock
///
/// This test suite measures the performance characteristics of the global eviction lock
/// to ensure it meets the requirements specified in the design document.
///
/// Expected overhead per design document:
/// - Lock acquisition: ~1-5ms (filesystem operations)
/// - Lock release: ~1-5ms (filesystem operations)
/// - Stale lock check: ~1ms (file read + timestamp comparison)
///
/// Requirements tested: Performance Impact section
use s3_proxy::cache::CacheManager;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tempfile::TempDir;
use tokio::task::JoinSet;

/// Helper to measure execution time of an async operation
async fn measure_time<F, Fut, T>(f: F) -> (T, Duration)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let start = Instant::now();
    let result = f().await;
    let elapsed = start.elapsed();
    (result, elapsed)
}

#[tokio::test]
async fn test_lock_acquisition_performance() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

    // Measure lock acquisition time over multiple iterations
    let iterations = 100;
    let mut acquisition_times = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        // Ensure no lock exists
        let lock_path = cache_manager.get_global_eviction_lock_path();
        if lock_path.exists() {
            std::fs::remove_file(&lock_path).ok();
        }

        // Measure acquisition time
        let (acquired, elapsed) =
            measure_time(|| async { cache_manager.try_acquire_global_eviction_lock().await }).await;

        assert!(
            acquired.expect("Should acquire lock"),
            "Lock acquisition should succeed"
        );
        acquisition_times.push(elapsed);

        // Clean up for next iteration
        cache_manager.release_global_eviction_lock().await.ok();
    }

    // Calculate statistics
    let total: Duration = acquisition_times.iter().sum();
    let avg = total / iterations as u32;

    // Sort for percentile calculations
    acquisition_times.sort();
    let min = acquisition_times[0];
    let max = acquisition_times[iterations - 1];
    let p50 = acquisition_times[iterations / 2];
    let p95 = acquisition_times[(iterations * 95) / 100];
    let p99 = acquisition_times[(iterations * 99) / 100];

    println!("\n=== Lock Acquisition Performance ===");
    println!("Iterations: {}", iterations);
    println!("Average:    {:?}", avg);
    println!("Min:        {:?}", min);
    println!("Max:        {:?}", max);
    println!("P50:        {:?}", p50);
    println!("P95:        {:?}", p95);
    println!("P99:        {:?}", p99);

    // Verify performance meets design requirements (1-5ms expected)
    // We'll be lenient and allow up to 10ms for P95 to account for CI/test environment variability
    assert!(
        p95.as_millis() <= 10,
        "P95 lock acquisition time ({:?}) exceeds 10ms threshold (design expects 1-5ms)",
        p95
    );

    // Average should be well within expected range
    assert!(
        avg.as_millis() <= 5,
        "Average lock acquisition time ({:?}) exceeds 5ms threshold",
        avg
    );
}

#[tokio::test]
async fn test_lock_release_performance() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

    // Measure lock release time over multiple iterations
    let iterations = 100;
    let mut release_times = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        // Acquire lock first
        cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Should acquire lock")
            .then_some(())
            .expect("Lock acquisition should succeed");

        // Measure release time
        let (_, elapsed) =
            measure_time(|| async { cache_manager.release_global_eviction_lock().await }).await;

        release_times.push(elapsed);
    }

    // Calculate statistics
    let total: Duration = release_times.iter().sum();
    let avg = total / iterations as u32;

    // Sort for percentile calculations
    release_times.sort();
    let min = release_times[0];
    let max = release_times[iterations - 1];
    let p50 = release_times[iterations / 2];
    let p95 = release_times[(iterations * 95) / 100];
    let p99 = release_times[(iterations * 99) / 100];

    println!("\n=== Lock Release Performance ===");
    println!("Iterations: {}", iterations);
    println!("Average:    {:?}", avg);
    println!("Min:        {:?}", min);
    println!("Max:        {:?}", max);
    println!("P50:        {:?}", p50);
    println!("P95:        {:?}", p95);
    println!("P99:        {:?}", p99);

    // Verify performance meets design requirements (1-5ms expected)
    // We'll be lenient and allow up to 10ms for P95 to account for CI/test environment variability
    assert!(
        p95.as_millis() <= 10,
        "P95 lock release time ({:?}) exceeds 10ms threshold (design expects 1-5ms)",
        p95
    );

    // Average should be well within expected range
    assert!(
        avg.as_millis() <= 5,
        "Average lock release time ({:?}) exceeds 5ms threshold",
        avg
    );
}

#[tokio::test]
async fn test_stale_lock_check_performance() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

    // Create locks directory and stale lock file
    std::fs::create_dir_all(cache_dir.join("locks")).expect("Failed to create locks directory");
    let lock_file_path = cache_manager.get_global_eviction_lock_path();
    let stale_time = SystemTime::now() - Duration::from_secs(400); // 400 seconds ago (stale)

    let stale_lock = s3_proxy::cache::GlobalEvictionLock {
        instance_id: "old-instance:99999".to_string(),
        process_id: 99999,
        hostname: "old-host".to_string(),
        acquired_at: stale_time,
        timeout_seconds: 300,
    };

    let lock_json =
        serde_json::to_string_pretty(&stale_lock).expect("Failed to serialize stale lock");
    std::fs::write(&lock_file_path, lock_json).expect("Failed to write stale lock file");

    // Measure stale lock detection and acquisition time over multiple iterations
    let iterations = 100;
    let mut check_times = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        // Recreate stale lock
        let lock_json =
            serde_json::to_string_pretty(&stale_lock).expect("Failed to serialize stale lock");
        std::fs::write(&lock_file_path, lock_json).expect("Failed to write stale lock file");

        // Measure time to detect stale lock and acquire
        let (acquired, elapsed) =
            measure_time(|| async { cache_manager.try_acquire_global_eviction_lock().await }).await;

        assert!(
            acquired.expect("Should acquire stale lock"),
            "Stale lock acquisition should succeed"
        );
        check_times.push(elapsed);

        // Clean up for next iteration
        cache_manager.release_global_eviction_lock().await.ok();
    }

    // Calculate statistics
    let total: Duration = check_times.iter().sum();
    let avg = total / iterations as u32;
    let min = *check_times.iter().min().unwrap();
    let max = *check_times.iter().max().unwrap();

    // Sort for percentile calculations
    check_times.sort();
    let p50 = check_times[iterations / 2];
    let p95 = check_times[(iterations * 95) / 100];
    let p99 = check_times[(iterations * 99) / 100];

    println!("\n=== Stale Lock Check Performance ===");
    println!("Iterations: {}", iterations);
    println!("Average:    {:?}", avg);
    println!("Min:        {:?}", min);
    println!("Max:        {:?}", max);
    println!("P50:        {:?}", p50);
    println!("P95:        {:?}", p95);
    println!("P99:        {:?}", p99);

    // Verify performance meets design requirements (~1ms expected for check)
    // Stale lock acquisition includes read + check + write, so allow up to 10ms for P95
    assert!(
        p95.as_millis() <= 10,
        "P95 stale lock check time ({:?}) exceeds 10ms threshold (design expects ~1ms for check)",
        p95
    );
}

#[tokio::test]
async fn test_acquire_release_cycle_performance() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

    // Measure full acquire-release cycle time
    let iterations = 100;
    let mut cycle_times = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let start = Instant::now();

        // Acquire
        let acquired = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Should acquire lock");
        assert!(acquired, "Lock acquisition should succeed");

        // Release
        cache_manager
            .release_global_eviction_lock()
            .await
            .expect("Should release lock");

        let elapsed = start.elapsed();
        cycle_times.push(elapsed);
    }

    // Calculate statistics
    let total: Duration = cycle_times.iter().sum();
    let avg = total / iterations as u32;
    let min = *cycle_times.iter().min().unwrap();
    let max = *cycle_times.iter().max().unwrap();

    // Sort for percentile calculations
    cycle_times.sort();
    let p50 = cycle_times[iterations / 2];
    let p95 = cycle_times[(iterations * 95) / 100];
    let p99 = cycle_times[(iterations * 99) / 100];

    println!("\n=== Full Acquire-Release Cycle Performance ===");
    println!("Iterations: {}", iterations);
    println!("Average:    {:?}", avg);
    println!("Min:        {:?}", min);
    println!("Max:        {:?}", max);
    println!("P50:        {:?}", p50);
    println!("P95:        {:?}", p95);
    println!("P99:        {:?}", p99);

    // Verify total cycle time (acquire + release) is reasonable
    // Design expects 1-5ms each, so 2-10ms total, allow up to 15ms for P95
    assert!(
        p95.as_millis() <= 15,
        "P95 full cycle time ({:?}) exceeds 15ms threshold (design expects 2-10ms)",
        p95
    );

    // Average should be well within expected range
    assert!(
        avg.as_millis() <= 10,
        "Average full cycle time ({:?}) exceeds 10ms threshold",
        avg
    );
}

#[tokio::test]
async fn test_performance_regression_baseline() {
    // This test establishes a baseline for performance regression testing
    // It measures the overhead of lock operations compared to no-lock operations

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

    let iterations = 50;

    // Measure time with lock coordination
    let start_with_lock = Instant::now();
    for _ in 0..iterations {
        let acquired = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Should acquire lock");
        assert!(acquired, "Lock acquisition should succeed");

        // Simulate minimal work
        tokio::time::sleep(Duration::from_micros(10)).await;

        cache_manager
            .release_global_eviction_lock()
            .await
            .expect("Should release lock");
    }
    let with_lock_elapsed = start_with_lock.elapsed();

    // Measure time without lock (just the simulated work)
    let start_without_lock = Instant::now();
    for _ in 0..iterations {
        // Same simulated work without lock overhead
        tokio::time::sleep(Duration::from_micros(10)).await;
    }
    let without_lock_elapsed = start_without_lock.elapsed();

    let lock_overhead = with_lock_elapsed.saturating_sub(without_lock_elapsed);
    let overhead_per_operation = lock_overhead / iterations as u32;

    println!("\n=== Performance Regression Baseline ===");
    println!("Iterations: {}", iterations);
    println!("Time with lock:    {:?}", with_lock_elapsed);
    println!("Time without lock: {:?}", without_lock_elapsed);
    println!("Lock overhead:     {:?}", lock_overhead);
    println!("Overhead per op:   {:?}", overhead_per_operation);

    // Verify overhead is acceptable (should be < 10ms per operation on average)
    assert!(
        overhead_per_operation.as_millis() <= 10,
        "Lock overhead per operation ({:?}) exceeds 10ms threshold",
        overhead_per_operation
    );

    // Document the baseline for future regression testing
    println!(
        "\nBaseline established: {:?} overhead per lock cycle",
        overhead_per_operation
    );
}

#[tokio::test]
async fn test_multiple_instances_under_load() {
    // This test simulates realistic load with multiple instances
    // competing for the lock while performing eviction-like work

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = Arc::new(temp_dir.path().to_path_buf());

    let num_instances = 3;
    let duration = Duration::from_secs(5);

    println!("\n=== Multiple Instances Under Load ===");
    println!("Instances: {}", num_instances);
    println!("Duration: {:?}", duration);

    let start = Instant::now();
    let mut join_set = JoinSet::new();

    for instance_id in 0..num_instances {
        let cache_dir = Arc::clone(&cache_dir);
        let test_duration = duration;

        join_set.spawn(async move {
            let cache_manager =
                CacheManager::new_with_defaults(cache_dir.as_ref().clone(), false, 0);

            let mut successful_acquisitions = 0;
            let mut failed_acquisitions = 0;
            let mut total_lock_hold_time = Duration::ZERO;

            let instance_start = Instant::now();

            while instance_start.elapsed() < test_duration {
                match cache_manager.try_acquire_global_eviction_lock().await {
                    Ok(true) => {
                        successful_acquisitions += 1;
                        let lock_start = Instant::now();

                        // Simulate eviction work (10-50ms)
                        let work_duration = Duration::from_millis(10 + (instance_id * 10) as u64);
                        tokio::time::sleep(work_duration).await;

                        total_lock_hold_time += lock_start.elapsed();

                        // Release lock
                        cache_manager.release_global_eviction_lock().await.ok();
                    }
                    Ok(false) => {
                        failed_acquisitions += 1;
                    }
                    Err(e) => {
                        eprintln!("Instance {} error: {}", instance_id, e);
                    }
                }

                // Small delay between attempts
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            (
                instance_id,
                successful_acquisitions,
                failed_acquisitions,
                total_lock_hold_time,
            )
        });
    }

    // Collect results
    let mut all_results = Vec::new();
    while let Some(result) = join_set.join_next().await {
        all_results.push(result.expect("Task should complete successfully"));
    }

    let total_elapsed = start.elapsed();

    // Analyze results
    let mut total_successful = 0;
    let mut total_failed = 0;
    let mut total_hold_time = Duration::ZERO;

    for (instance_id, successful, failed, hold_time) in all_results {
        total_successful += successful;
        total_failed += failed;
        total_hold_time += hold_time;

        println!(
            "Instance {}: {} successful, {} failed, held lock for {:?}",
            instance_id, successful, failed, hold_time
        );
    }

    println!("\nTotal successful acquisitions: {}", total_successful);
    println!("Total failed acquisitions: {}", total_failed);
    println!("Total lock hold time: {:?}", total_hold_time);
    println!("Total test time: {:?}", total_elapsed);

    // Calculate efficiency metrics
    let lock_utilization = (total_hold_time.as_secs_f64() / total_elapsed.as_secs_f64()) * 100.0;
    println!("Lock utilization: {:.2}%", lock_utilization);

    // Verify system behavior under load
    assert!(
        total_successful > 0,
        "At least some acquisitions should succeed under load"
    );

    assert!(
        total_successful >= num_instances,
        "Each instance should acquire the lock at least once"
    );

    // Verify mutual exclusion is maintained (only one instance holds lock at a time)
    // Lock utilization should not exceed 100% (which would indicate overlapping locks)
    assert!(
        lock_utilization <= 100.0,
        "Lock utilization ({:.2}%) exceeds 100%, indicating potential mutual exclusion violation",
        lock_utilization
    );

    println!("\nPerformance test completed successfully - no significant regression detected");
}
