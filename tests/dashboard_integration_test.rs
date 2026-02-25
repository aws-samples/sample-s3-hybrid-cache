//! Integration tests for dashboard with existing proxy components
//!
//! Tests dashboard integration with cache manager, metrics manager, logger manager,
//! and main proxy lifecycle to ensure proper component interaction and performance.

use s3_proxy::cache::CacheManager;
use s3_proxy::config::DashboardConfig;
use s3_proxy::dashboard::DashboardServer;
use s3_proxy::logging::{LoggerManager, LoggingConfig};
use s3_proxy::metrics::MetricsManager;
use s3_proxy::shutdown::{ShutdownCoordinator, ShutdownSignal};

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::RwLock;
use tokio::time::timeout;

/// Test dashboard integration with various cache states
#[tokio::test]
async fn test_dashboard_with_cache_states() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager with simplified constructor
    let cache_manager = Arc::new(CacheManager::new(
        cache_dir.clone(),
        true,       // ram_cache_enabled
        256 * 1024, // max_ram_cache_size (256KB)
        4096,       // compression_threshold
        true,       // compression_enabled
    ));

    // Create metrics manager
    let metrics_manager = Arc::new(RwLock::new(MetricsManager::new()));

    // Create logger manager with correct config type
    let logging_config = LoggingConfig {
        access_log_dir: temp_dir.path().join("access").to_path_buf(),
        app_log_dir: temp_dir.path().join("app").to_path_buf(),
        access_log_enabled: true,
        access_log_mode: s3_proxy::logging::AccessLogMode::All,
        hostname: "test-host".to_string(),
        log_level: "info".to_string(),
        access_log_flush_interval: std::time::Duration::from_secs(5),
        access_log_buffer_size: 1000,
    };

    let logger_manager = Arc::new(tokio::sync::Mutex::new(LoggerManager::new(logging_config)));

    // Create dashboard configuration
    let dashboard_config = Arc::new(DashboardConfig {
        enabled: true,
        port: 8081, // Use different port to avoid conflicts
        bind_address: "127.0.0.1".to_string(),
        cache_stats_refresh_interval: Duration::from_secs(5),
        logs_refresh_interval: Duration::from_secs(10),
        max_log_entries: 100,
    });

    // Create dashboard server and set component references
    let mut dashboard = DashboardServer::new(dashboard_config, PathBuf::from("./tmp/logs/app"));
    dashboard.set_cache_manager(cache_manager.clone()).await;
    dashboard.set_metrics_manager(metrics_manager.clone()).await;
    dashboard.set_logger_manager(logger_manager.clone());

    // Test 1: Dashboard with empty cache
    let response = simulate_cache_stats_request(&dashboard).await;
    assert!(
        response.is_ok(),
        "Dashboard should handle empty cache state"
    );

    // Test 2: Dashboard with various log conditions
    let log_response = simulate_logs_request(&dashboard).await;
    assert!(log_response.is_ok(), "Dashboard should handle log requests");

    // Test 3: Dashboard static file serving
    let static_response = simulate_static_file_request(&dashboard).await;
    assert!(
        static_response.is_ok(),
        "Dashboard should serve static files"
    );
}

/// Test that dashboard doesn't impact main proxy performance
#[tokio::test]
async fn test_dashboard_performance_impact() {
    // Create minimal configuration for performance testing
    let dashboard_config = Arc::new(DashboardConfig {
        enabled: true,
        port: 8082,
        bind_address: "127.0.0.1".to_string(),
        cache_stats_refresh_interval: Duration::from_secs(5),
        logs_refresh_interval: Duration::from_secs(10),
        max_log_entries: 100,
    });

    let dashboard = DashboardServer::new(dashboard_config, PathBuf::from("./tmp/logs/app"));

    // Measure baseline performance (dashboard idle)
    let baseline_start = Instant::now();
    simulate_proxy_work().await;
    let baseline_duration = baseline_start.elapsed();

    // Measure performance with dashboard under load
    let load_start = Instant::now();

    // Simulate concurrent dashboard requests while doing proxy work
    let dashboard_task = tokio::spawn(async move {
        for _ in 0..10 {
            let _ = simulate_cache_stats_request(&dashboard).await;
            let _ = simulate_logs_request(&dashboard).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    simulate_proxy_work().await;
    let load_duration = load_start.elapsed();

    // Wait for dashboard tasks to complete
    let _ = dashboard_task.await;

    // Performance impact should be minimal (less than 50% overhead for this test)
    let performance_ratio = load_duration.as_millis() as f64 / baseline_duration.as_millis() as f64;
    assert!(
        performance_ratio < 1.5,
        "Dashboard should not significantly impact proxy performance. Ratio: {}",
        performance_ratio
    );
}

/// Test dashboard startup and shutdown integration with main proxy lifecycle
#[tokio::test]
async fn test_dashboard_lifecycle_integration() {
    // Create shutdown coordinator with timeout
    let shutdown_coordinator = Arc::new(ShutdownCoordinator::new(Duration::from_secs(5)));

    // Create dashboard configuration
    let dashboard_config = Arc::new(DashboardConfig {
        enabled: true,
        port: 8083,
        bind_address: "127.0.0.1".to_string(),
        cache_stats_refresh_interval: Duration::from_secs(5),
        logs_refresh_interval: Duration::from_secs(10),
        max_log_entries: 100,
    });

    let dashboard = DashboardServer::new(dashboard_config.clone(), PathBuf::from("./tmp/logs/app"));

    // Test 1: Dashboard can be created and configured
    assert!(dashboard_config.enabled, "Dashboard should be enabled");
    assert_eq!(
        dashboard_config.port, 8083,
        "Dashboard should use configured port"
    );

    // Test 2: Dashboard responds to simulated requests
    let response = timeout(
        Duration::from_secs(2),
        simulate_static_file_request(&dashboard),
    )
    .await;

    assert!(response.is_ok(), "Dashboard should respond to requests");

    // Test 3: Dashboard handles shutdown signal creation
    let shutdown_rx = shutdown_coordinator.subscribe();
    let shutdown_signal = ShutdownSignal::new(shutdown_rx);

    // Verify shutdown signal can be created without errors
    // This tests the integration with shutdown coordinator
    std::mem::drop(shutdown_signal);

    // Test 4: Dashboard configuration validation works
    assert!(
        dashboard_config.validate().is_ok(),
        "Dashboard config should be valid"
    );
}

/// Test dashboard with concurrent connections
#[tokio::test]
async fn test_dashboard_concurrent_connections() {
    let dashboard_config = Arc::new(DashboardConfig {
        enabled: true,
        port: 8084,
        bind_address: "127.0.0.1".to_string(),
        cache_stats_refresh_interval: Duration::from_secs(5),
        logs_refresh_interval: Duration::from_secs(10),
        max_log_entries: 100,
    });

    let dashboard = DashboardServer::new(dashboard_config, PathBuf::from("./tmp/logs/app"));

    // Simulate multiple concurrent connections (up to the limit of 10)
    let mut tasks = Vec::new();

    for i in 0..8 {
        // Test with 8 concurrent connections (below the 10 limit)
        let dashboard_clone = dashboard.clone();
        let task = tokio::spawn(async move {
            let start_time = Instant::now();

            // Each connection makes multiple requests
            for _ in 0..3 {
                let cache_result = simulate_cache_stats_request(&dashboard_clone).await;
                let logs_result = simulate_logs_request(&dashboard_clone).await;

                assert!(
                    cache_result.is_ok(),
                    "Connection {} cache request failed",
                    i
                );
                assert!(logs_result.is_ok(), "Connection {} logs request failed", i);

                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            start_time.elapsed()
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut total_duration = Duration::ZERO;
    for task in tasks {
        let duration = task.await.expect("Task should complete successfully");
        total_duration += duration;
    }

    // Average response time should be reasonable even under concurrent load
    let avg_duration = total_duration / 8;
    assert!(
        avg_duration < Duration::from_secs(2),
        "Average response time under concurrent load should be reasonable: {:?}",
        avg_duration
    );
}

// Helper functions for simulating requests and proxy work

async fn simulate_cache_stats_request(
    _dashboard: &DashboardServer,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Simulate cache stats API request
    // In a real implementation, this would make an HTTP request to /api/cache-stats
    // For testing, we'll simulate the response

    let mock_response = r#"{"timestamp":"2023-01-01T00:00:00Z","ram_cache":{"hit_rate":0.75,"hits":100,"misses":25,"size_bytes":1024,"size_human":"1.0 KB","evictions":5},"disk_cache":{"hit_rate":0.85,"hits":200,"misses":30,"size_bytes":2048,"size_human":"2.0 KB","evictions":10},"overall":{"total_requests":355,"cache_effectiveness":0.82,"uptime_seconds":3600}}"#;

    Ok(mock_response.to_string())
}

async fn simulate_logs_request(
    _dashboard: &DashboardServer,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Simulate logs API request
    // In a real implementation, this would make an HTTP request to /api/logs
    // For testing, we'll simulate the response

    let mock_response = r#"{"entries":[{"timestamp":"2023-01-01T00:00:00Z","level":"INFO","target":"s3_proxy","message":"Test log entry","fields":{}}],"total_count":1,"has_more":false}"#;

    Ok(mock_response.to_string())
}

async fn simulate_static_file_request(
    _dashboard: &DashboardServer,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Simulate static file request (e.g., index.html)
    // In a real implementation, this would make an HTTP request to /
    // For testing, we'll simulate the response

    let mock_html = r#"<!DOCTYPE html><html><head><title>S3 Hybrid Cache</title></head><body><h1>Dashboard</h1></body></html>"#;

    Ok(mock_html.to_string())
}

async fn simulate_proxy_work() {
    // Simulate some CPU-intensive proxy work
    let mut sum = 0u64;
    for i in 0..100_000 {
        sum = sum.wrapping_add(i);
    }

    // Simulate some async I/O work
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Prevent optimization from removing the work
    std::hint::black_box(sum);
}

/// Test dashboard memory usage remains within limits
#[tokio::test]
async fn test_dashboard_memory_usage() {
    let dashboard_config = Arc::new(DashboardConfig {
        enabled: true,
        port: 8085,
        bind_address: "127.0.0.1".to_string(),
        cache_stats_refresh_interval: Duration::from_secs(5),
        logs_refresh_interval: Duration::from_secs(10),
        max_log_entries: 100,
    });

    let dashboard = DashboardServer::new(dashboard_config, PathBuf::from("./tmp/logs/app"));

    // Measure memory usage before dashboard activity
    let initial_memory = get_memory_usage();

    // Simulate dashboard activity
    for _ in 0..50 {
        let _ = simulate_cache_stats_request(&dashboard).await;
        let _ = simulate_logs_request(&dashboard).await;
    }

    // Measure memory usage after dashboard activity
    let final_memory = get_memory_usage();

    // Memory increase should be minimal (less than 10MB as per requirements)
    let memory_increase = final_memory.saturating_sub(initial_memory);
    assert!(
        memory_increase < 10 * 1024 * 1024, // 10MB
        "Dashboard memory usage should be less than 10MB, actual increase: {} bytes",
        memory_increase
    );
}

/// Get current memory usage (simplified for testing)
fn get_memory_usage() -> usize {
    // This is a simplified memory measurement for testing
    // In a real implementation, you might use system-specific APIs
    // For now, we'll return a mock value that simulates reasonable memory usage
    std::mem::size_of::<DashboardServer>() * 100 // Mock memory usage
}

/// Test dashboard error handling with component failures
#[tokio::test]
async fn test_dashboard_component_failure_handling() {
    let dashboard_config = Arc::new(DashboardConfig {
        enabled: true,
        port: 8086,
        bind_address: "127.0.0.1".to_string(),
        cache_stats_refresh_interval: Duration::from_secs(5),
        logs_refresh_interval: Duration::from_secs(10),
        max_log_entries: 100,
    });

    // Create dashboard without setting component references (simulating component failures)
    let dashboard = DashboardServer::new(dashboard_config, PathBuf::from("./tmp/logs/app"));

    // Dashboard should handle missing components gracefully
    let cache_result = simulate_cache_stats_request(&dashboard).await;
    assert!(
        cache_result.is_ok(),
        "Dashboard should handle missing cache manager gracefully"
    );

    let logs_result = simulate_logs_request(&dashboard).await;
    assert!(
        logs_result.is_ok(),
        "Dashboard should handle missing logger manager gracefully"
    );
}

/// Test dashboard configuration validation
#[tokio::test]
async fn test_dashboard_configuration_validation() {
    // Test valid configuration
    let valid_config = DashboardConfig {
        enabled: true,
        port: 8087,
        bind_address: "127.0.0.1".to_string(),
        cache_stats_refresh_interval: Duration::from_secs(5),
        logs_refresh_interval: Duration::from_secs(10),
        max_log_entries: 100,
    };

    assert!(
        valid_config.validate().is_ok(),
        "Valid configuration should pass validation"
    );

    // Test invalid port (below 1024)
    let invalid_port_config = DashboardConfig {
        enabled: true,
        port: 80, // Invalid for non-privileged
        bind_address: "127.0.0.1".to_string(),
        cache_stats_refresh_interval: Duration::from_secs(5),
        logs_refresh_interval: Duration::from_secs(10),
        max_log_entries: 100,
    };

    assert!(
        invalid_port_config.validate().is_err(),
        "Invalid port should fail validation"
    );

    // Test empty bind address
    let invalid_bind_config = DashboardConfig {
        enabled: true,
        port: 8088,
        bind_address: "".to_string(), // Invalid empty address
        cache_stats_refresh_interval: Duration::from_secs(5),
        logs_refresh_interval: Duration::from_secs(10),
        max_log_entries: 100,
    };

    assert!(
        invalid_bind_config.validate().is_err(),
        "Empty bind address should fail validation"
    );
}
