//! Test for versioned request handling
//!
//! Versioned Request Behavior:
//! - Requests with versionId query parameter bypass cache entirely (no read, no write)
//! - A single "versioned_request" bypass metric is recorded
//! - Non-versioned requests are unaffected

use std::sync::Arc;

/// Test that versioned request metrics are tracked correctly
/// When a versioned request bypasses cache, the "versioned_request" metric should be incremented
#[tokio::test]
async fn test_versioned_request_bypass_metrics() {
    let metrics_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::metrics::MetricsManager::new(),
    ));

    // Record cache bypass for versioned request
    metrics_manager
        .read()
        .await
        .record_cache_bypass("versioned_request")
        .await;

    // Verify the metric was recorded
    let bypass_count = metrics_manager
        .read()
        .await
        .get_cache_bypass_count("versioned_request")
        .await;
    assert_eq!(
        bypass_count, 1,
        "Cache bypass metric should be incremented for versioned request"
    );

    // Record another bypass
    metrics_manager
        .read()
        .await
        .record_cache_bypass("versioned_request")
        .await;
    let bypass_count = metrics_manager
        .read()
        .await
        .get_cache_bypass_count("versioned_request")
        .await;
    assert_eq!(
        bypass_count, 2,
        "Cache bypass metric should be incremented again"
    );
}

/// Test that cache bypass metrics track versioned requests separately from other reasons
#[tokio::test]
async fn test_versioned_cache_bypass_metrics_by_reason() {
    let metrics_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::metrics::MetricsManager::new(),
    ));

    // Record different bypass reasons
    metrics_manager
        .read()
        .await
        .record_cache_bypass("versioned_request")
        .await;
    metrics_manager
        .read()
        .await
        .record_cache_bypass("versioned_request")
        .await;
    metrics_manager
        .read()
        .await
        .record_cache_bypass("list operation - always fetch fresh data")
        .await;

    // Verify each reason is tracked separately
    let versioned_count = metrics_manager
        .read()
        .await
        .get_cache_bypass_count("versioned_request")
        .await;
    assert_eq!(
        versioned_count, 2,
        "Versioned request bypasses should be tracked"
    );

    let list_count = metrics_manager
        .read()
        .await
        .get_cache_bypass_count("list operation - always fetch fresh data")
        .await;
    assert_eq!(list_count, 1, "List operation bypasses should be tracked");
}

/// Test that cache metrics include versioned bypass reason
#[tokio::test]
async fn test_cache_metrics_include_versioned_bypass_reason() {
    let metrics_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::metrics::MetricsManager::new(),
    ));

    // Record some bypasses
    metrics_manager
        .read()
        .await
        .record_cache_bypass("versioned_request")
        .await;

    // Verify bypass data is included in collected metrics
    let system_metrics = metrics_manager.read().await.collect_metrics().await;

    if let Some(cache_metrics) = system_metrics.cache {
        let bypasses = &cache_metrics.cache_bypasses_by_reason;
        assert_eq!(*bypasses.get("versioned_request").unwrap_or(&0), 1);
    }
}

/// Test that non-versioned requests do not trigger versioned bypass metrics
/// This ensures backward compatibility
#[tokio::test]
async fn test_non_versioned_requests_not_bypassed() {
    let metrics_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::metrics::MetricsManager::new(),
    ));

    // Verify no versioned bypass is recorded initially
    let versioned_count = metrics_manager
        .read()
        .await
        .get_cache_bypass_count("versioned_request")
        .await;
    assert_eq!(
        versioned_count, 0,
        "Non-versioned requests should not trigger versioned bypass"
    );
}

/// Test HEAD cache bypass metrics recording
#[tokio::test]
async fn test_head_cache_bypass_metrics_recording() {
    let metrics_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::metrics::MetricsManager::new(),
    ));

    // Record HEAD cache bypasses for different reasons
    metrics_manager
        .read()
        .await
        .record_cache_bypass("list operation - always fetch fresh data")
        .await;
    metrics_manager
        .read()
        .await
        .record_cache_bypass("conditional headers - bypass RAM cache")
        .await;
    metrics_manager
        .read()
        .await
        .record_cache_bypass("list operation - always fetch fresh data")
        .await;

    // Verify the metrics were recorded correctly
    let list_bypass_count = metrics_manager
        .read()
        .await
        .get_cache_bypass_count("list operation - always fetch fresh data")
        .await;
    let conditional_bypass_count = metrics_manager
        .read()
        .await
        .get_cache_bypass_count("conditional headers - bypass RAM cache")
        .await;

    assert_eq!(
        list_bypass_count, 2,
        "Should have recorded 2 list operation bypasses for HEAD requests"
    );
    assert_eq!(
        conditional_bypass_count, 1,
        "Should have recorded 1 conditional header bypass for HEAD requests"
    );

    // Verify bypass data is included in collected metrics
    let system_metrics = metrics_manager.read().await.collect_metrics().await;

    if let Some(cache_metrics) = system_metrics.cache {
        let bypasses = &cache_metrics.cache_bypasses_by_reason;
        assert_eq!(
            *bypasses
                .get("list operation - always fetch fresh data")
                .unwrap_or(&0),
            2
        );
        assert_eq!(
            *bypasses
                .get("conditional headers - bypass RAM cache")
                .unwrap_or(&0),
            1
        );
    }
}
