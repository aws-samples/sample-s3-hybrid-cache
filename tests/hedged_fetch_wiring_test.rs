//! Integration tests for the hedged fetch wiring in `forward_get_head_to_s3_and_cache`.
//!
//! Validates spec: hedged-upstream-requests, Task 7:
//! - Rule-enabled key + slow original → hedge serves (Req 7.1)
//! - Key not matched by enabling rule → no hedge (Req 1.3)
//! - Original first-byte timeout + hedge in flight → hedge wins (Req 9.4)
//! - A PUT never increments `issued` (Req 2.2)
//! - Per-rule `hedge_trigger_after` override is honoured (Req 3.1)

use async_trait::async_trait;
use bytes::Bytes;
use hyper::{Method, StatusCode};
use s3_proxy::bucket_settings::ResolvedSettings;
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{CacheMetadata, ObjectMetadata};
use s3_proxy::config::Config;
use s3_proxy::connection_pool::ConnectionPoolManager;
use s3_proxy::hedged_fetch;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::{Result, S3ClientApi, S3RequestContext, S3Response, S3ResponseBody};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Configurable delayed stub
// ---------------------------------------------------------------------------

/// S3 client stub that delays its responses. The first call to
/// `forward_request_pinned` uses `original_delay`; the second call (the hedge)
/// uses `hedge_delay`. This works regardless of IP pinning.
#[derive(Clone)]
struct DelayedHedgeStub {
    original_delay: Duration,
    hedge_delay: Duration,
    original_status: u16,
    hedge_status: u16,
    call_count: Arc<AtomicUsize>,
}

impl DelayedHedgeStub {
    fn new(
        original_delay: Duration,
        hedge_delay: Duration,
        original_status: u16,
        hedge_status: u16,
    ) -> Self {
        Self {
            original_delay,
            hedge_delay,
            original_status,
            hedge_status,
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }
}

fn make_response(status: u16) -> S3Response {
    S3Response {
        status: StatusCode::from_u16(status).unwrap(),
        headers: {
            let mut h = HashMap::new();
            h.insert("content-length".to_string(), "2".to_string());
            h.insert("etag".to_string(), "\"abc123\"".to_string());
            h
        },
        body: Some(S3ResponseBody::Buffered(Bytes::from("ok"))),
        request_duration: Duration::from_millis(1),
    }
}

#[async_trait]
impl S3ClientApi for DelayedHedgeStub {
    async fn forward_request(&self, _context: S3RequestContext) -> Result<S3Response> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.original_delay).await;
        Ok(make_response(self.original_status))
    }

    async fn forward_request_pinned(
        &self,
        _context: S3RequestContext,
        _pinned_ip: Option<IpAddr>,
    ) -> Result<S3Response> {
        // First call = original (slow), subsequent calls = hedge (fast).
        let call_num = self.call_count.fetch_add(1, Ordering::SeqCst);
        let (delay, status) = if call_num == 0 {
            (self.original_delay, self.original_status)
        } else {
            (self.hedge_delay, self.hedge_status)
        };
        tokio::time::sleep(delay).await;
        Ok(make_response(status))
    }

    fn extract_metadata_from_response(&self, _headers: &HashMap<String, String>) -> CacheMetadata {
        CacheMetadata {
            content_length: 2,
            etag: "\"abc123\"".to_string(),
            last_modified: String::new(),
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: SystemTime::now(),
        }
    }

    fn extract_object_metadata_from_response(
        &self,
        _headers: &HashMap<String, String>,
    ) -> ObjectMetadata {
        ObjectMetadata::default()
    }

    fn get_connection_pool(&self) -> Arc<tokio::sync::RwLock<ConnectionPoolManager>> {
        Arc::new(tokio::sync::RwLock::new(
            ConnectionPoolManager::new_with_config(
                s3_proxy::config::ConnectionPoolConfig::default(),
            )
            .unwrap(),
        ))
    }

    fn has_endpoint_overrides(&self) -> bool {
        false
    }

    async fn set_metrics_manager(
        &self,
        _: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
    ) {
    }

    async fn register_endpoint(&self, _: &str) {}
    async fn refresh_dns(&self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn test_config(first_byte_timeout: Duration, retries: usize) -> Arc<Config> {
    let mut config = Config::default();
    config.connection_pool.upstream_first_byte_timeout = first_byte_timeout;
    config.connection_pool.upstream_idle_retries = retries;
    config.connection_pool.hedged_requests.max_inflight_fraction = 1.0; // allow all hedges
    Arc::new(config)
}

async fn make_test_cache(config: &Config) -> (TempDir, Arc<CacheManager>, Arc<RangeHandler>) {
    let temp_dir = TempDir::new().expect("tempdir");
    let cache_dir = temp_dir.path().to_path_buf();
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        cache_dir,
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::LRU,
        1024,
        config.compression.enabled,
        config.cache.get_ttl,
        config.cache.head_ttl,
        config.cache.put_ttl,
        config.cache.actively_remove_cached_data,
        config.cache.shared_storage.clone(),
        config.cache.write_cache_percent,
        config.cache.write_cache_enabled,
        config.cache.incomplete_upload_ttl,
        config.cache.metadata_cache.clone(),
        config.cache.eviction_trigger_percent,
        config.cache.eviction_target_percent,
        config.cache.read_cache_enabled,
        config.cache.bucket_settings_staleness_threshold,
        config.cache.compression_batch_size,
        config.cache.evaluate_conditions_from_cache,
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    ));
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));
    cache_manager.initialize().await.expect("cache init");

    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        disk_cache_manager,
    ));
    (temp_dir, cache_manager, range_handler)
}

fn resolved_with_hedging(
    enabled: bool,
    trigger_after: Duration,
    max_per_request: usize,
) -> ResolvedSettings {
    ResolvedSettings {
        hedging_enabled: enabled,
        hedge_trigger_after: trigger_after,
        hedge_max_per_request: max_per_request,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Req 1.3: key not matched by an enabling rule → no hedge, byte-identical behaviour.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hedging_disabled_no_hedge_issued() {
    // Ensure globals are initialized.
    hedged_fetch::init_global_hedging();

    let stub = DelayedHedgeStub::new(
        Duration::from_millis(10), // fast original
        Duration::from_millis(5),  // fast hedge (shouldn't be called)
        200,
        200,
    );
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stub.clone());

    let config = test_config(Duration::from_secs(5), 2);
    let (_temp_dir, cache_manager, range_handler) = make_test_cache(&config).await;
    let resolved = resolved_with_hedging(false, Duration::from_millis(50), 1);

    let metrics = hedged_fetch::get_global_metrics().unwrap();
    let issued_before = metrics.issued.load(Ordering::Relaxed);

    let response = HttpProxy::forward_get_head_to_s3_and_cache(
        Method::GET,
        "/test-bucket/no-hedge-key.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        HashMap::new(),
        "test-bucket/no-hedge-key.bin".to_string(),
        cache_manager,
        s3_client,
        range_handler,
        config,
        &resolved,
        &None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    // No hedge was issued.
    let issued_after = metrics.issued.load(Ordering::Relaxed);
    assert_eq!(
        issued_after, issued_before,
        "No hedge should be issued when hedging is disabled"
    );
    // Only one call (the original).
    assert_eq!(stub.calls(), 1);
}

/// Req 7.1, 3.2: rule-enabled key + slow original → hedge issued and serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hedging_enabled_slow_original_hedge_wins() {
    hedged_fetch::init_global_hedging();

    let stub = DelayedHedgeStub::new(
        Duration::from_millis(500), // slow original (will trigger hedge)
        Duration::from_millis(10),  // fast hedge
        200,
        200,
    );
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stub.clone());

    let config = test_config(Duration::from_secs(5), 2);
    let (_temp_dir, cache_manager, range_handler) = make_test_cache(&config).await;
    // Hedging enabled with 50ms trigger.
    let resolved = resolved_with_hedging(true, Duration::from_millis(50), 1);

    let metrics = hedged_fetch::get_global_metrics().unwrap();
    let issued_before = metrics.issued.load(Ordering::Relaxed);
    let won_before = metrics.won.load(Ordering::Relaxed);

    let start = Instant::now();
    let response = HttpProxy::forward_get_head_to_s3_and_cache(
        Method::GET,
        "/test-bucket/hedged-key.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        HashMap::new(),
        "test-bucket/hedged-key.bin".to_string(),
        cache_manager,
        s3_client,
        range_handler,
        config,
        &resolved,
        &None,
        None,
    )
    .await
    .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    // Should complete much faster than the 500ms original delay.
    assert!(
        elapsed < Duration::from_millis(300),
        "Hedge should serve faster than original; elapsed={:?}",
        elapsed
    );
    // Hedge was issued and won.
    let issued_after = metrics.issued.load(Ordering::Relaxed);
    let won_after = metrics.won.load(Ordering::Relaxed);
    assert!(
        issued_after > issued_before,
        "Hedge should have been issued"
    );
    assert!(won_after > won_before, "Hedge should have won");
}

/// Req 9.4: original first-byte timeout + hedge in flight → hedge wins, not a 504.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_original_timeout_hedge_in_flight_wins() {
    hedged_fetch::init_global_hedging();

    // Original will exceed the 200ms first-byte timeout.
    // Hedge responds at ~150ms (trigger_after=50ms + 100ms delay).
    let stub = DelayedHedgeStub::new(
        Duration::from_millis(500), // original — will time out at 200ms
        Duration::from_millis(100), // hedge responds within first_byte_timeout
        200,
        200,
    );
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stub.clone());

    let config = test_config(Duration::from_millis(200), 0); // 200ms timeout, 0 retries
    let (_temp_dir, cache_manager, range_handler) = make_test_cache(&config).await;
    let resolved = resolved_with_hedging(true, Duration::from_millis(50), 1);

    let response = HttpProxy::forward_get_head_to_s3_and_cache(
        Method::GET,
        "/test-bucket/timeout-hedge.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        HashMap::new(),
        "test-bucket/timeout-hedge.bin".to_string(),
        cache_manager,
        s3_client,
        range_handler,
        config,
        &resolved,
        &None,
        None,
    )
    .await
    .unwrap();

    // Hedge should win — not a 504.
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Hedge in flight should become the winner, not 504"
    );
}

/// Req 2.2: a PUT never increments `issued` (mutations reach different handlers,
/// and even if they reach here, hedging_eligible = false for non-GET/HEAD).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_put_never_hedges() {
    hedged_fetch::init_global_hedging();

    let stub = DelayedHedgeStub::new(
        Duration::from_millis(10),
        Duration::from_millis(5),
        200,
        200,
    );
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stub.clone());

    let config = test_config(Duration::from_secs(5), 2);
    let (_temp_dir, cache_manager, range_handler) = make_test_cache(&config).await;
    // Hedging enabled, but method is PUT — should still not hedge.
    let resolved = resolved_with_hedging(true, Duration::from_millis(10), 1);

    let metrics = hedged_fetch::get_global_metrics().unwrap();
    let issued_before = metrics.issued.load(Ordering::Relaxed);

    // PUT through the function — normally PUTs go through different handlers,
    // but this verifies the hedging_eligible guard.
    let response = HttpProxy::forward_get_head_to_s3_and_cache(
        Method::PUT,
        "/test-bucket/put-key.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        HashMap::new(),
        "test-bucket/put-key.bin".to_string(),
        cache_manager,
        s3_client,
        range_handler,
        config,
        &resolved,
        &None,
        None,
    )
    .await
    .unwrap();

    // Response should still succeed (function works for any method).
    assert!(response.status().is_success());
    // No hedge issued.
    let issued_after = metrics.issued.load(Ordering::Relaxed);
    assert_eq!(
        issued_after, issued_before,
        "PUT must never increment issued"
    );
}

/// Req 3.1: per-rule hedge_trigger_after override is honoured.
/// With a 300ms trigger and a 200ms original, no hedge should fire.
/// With a 50ms trigger and a 200ms original, a hedge should fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_per_rule_trigger_override_honoured() {
    hedged_fetch::init_global_hedging();

    // Original responds at 200ms. Trigger at 300ms → no hedge.
    let stub = DelayedHedgeStub::new(
        Duration::from_millis(200),
        Duration::from_millis(10),
        200,
        200,
    );
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stub.clone());

    let config = test_config(Duration::from_secs(5), 0);
    let (_temp_dir, cache_manager, range_handler) = make_test_cache(&config).await;
    // trigger_after = 300ms, longer than original's 200ms → no hedge
    let resolved = resolved_with_hedging(true, Duration::from_millis(300), 1);

    let metrics = hedged_fetch::get_global_metrics().unwrap();

    let response = HttpProxy::forward_get_head_to_s3_and_cache(
        Method::GET,
        "/test-bucket/trigger-300.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        HashMap::new(),
        "test-bucket/trigger-300.bin".to_string(),
        cache_manager.clone(),
        s3_client.clone(),
        range_handler.clone(),
        config.clone(),
        &resolved,
        &None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    // No hedge should have fired for THIS request (trigger > original latency).
    // Use stub.calls() to verify only one forward_request_pinned was made.
    assert_eq!(
        stub.calls(),
        1,
        "With trigger_after=300ms > original's 200ms, only the original should be called"
    );

    // Now with trigger_after = 50ms, shorter than original's 200ms → hedge fires.
    let stub2 = DelayedHedgeStub::new(
        Duration::from_millis(200),
        Duration::from_millis(10),
        200,
        200,
    );
    let s3_client2: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stub2.clone());
    let (_temp_dir2, cache_manager2, range_handler2) = make_test_cache(&config).await;
    let resolved2 = resolved_with_hedging(true, Duration::from_millis(50), 1);

    let issued_before_50 = metrics.issued.load(Ordering::Relaxed);

    let response2 = HttpProxy::forward_get_head_to_s3_and_cache(
        Method::GET,
        "/test-bucket/trigger-50.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        HashMap::new(),
        "test-bucket/trigger-50.bin".to_string(),
        cache_manager2,
        s3_client2,
        range_handler2,
        config,
        &resolved2,
        &None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(response2.status(), StatusCode::OK);
    let issued_after_50 = metrics.issued.load(Ordering::Relaxed);
    // Two calls to stub2: original + hedge.
    assert_eq!(
        stub2.calls(),
        2,
        "With trigger_after=50ms < original's 200ms, both original and hedge should be called"
    );
    // The issued metric should have incremented.
    assert!(
        issued_after_50 > issued_before_50,
        "Issued metric should increment when hedge fires"
    );
}

// ---------------------------------------------------------------------------
// Range-path hedge budget sharing tests (Task 8)
// ---------------------------------------------------------------------------

/// A stub that always responds slowly with 206 Partial Content,
/// counting calls per fetch type for budget assertion.
#[derive(Clone)]
struct SlowRangeStub {
    delay: Duration,
    call_count: Arc<AtomicUsize>,
}

impl SlowRangeStub {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }
}

fn make_range_response() -> S3Response {
    S3Response {
        status: StatusCode::PARTIAL_CONTENT,
        headers: {
            let mut h = HashMap::new();
            h.insert("content-length".to_string(), "4".to_string());
            h.insert("etag".to_string(), "\"range-etag\"".to_string());
            h.insert("content-range".to_string(), "bytes 0-3/100".to_string());
            h
        },
        body: Some(S3ResponseBody::Buffered(Bytes::from("data"))),
        request_duration: Duration::from_millis(1),
    }
}

#[async_trait]
impl S3ClientApi for SlowRangeStub {
    async fn forward_request(&self, _context: S3RequestContext) -> Result<S3Response> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        Ok(make_range_response())
    }

    async fn forward_request_pinned(
        &self,
        _context: S3RequestContext,
        _pinned_ip: Option<IpAddr>,
    ) -> Result<S3Response> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        Ok(make_range_response())
    }

    fn extract_metadata_from_response(&self, _headers: &HashMap<String, String>) -> CacheMetadata {
        CacheMetadata {
            content_length: 100,
            etag: "\"range-etag\"".to_string(),
            last_modified: String::new(),
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: std::time::SystemTime::now(),
        }
    }

    fn extract_object_metadata_from_response(
        &self,
        _headers: &HashMap<String, String>,
    ) -> s3_proxy::cache_types::ObjectMetadata {
        s3_proxy::cache_types::ObjectMetadata::default()
    }

    fn get_connection_pool(
        &self,
    ) -> Arc<tokio::sync::RwLock<s3_proxy::connection_pool::ConnectionPoolManager>> {
        Arc::new(tokio::sync::RwLock::new(
            s3_proxy::connection_pool::ConnectionPoolManager::new_with_config(
                s3_proxy::config::ConnectionPoolConfig::default(),
            )
            .unwrap(),
        ))
    }

    fn has_endpoint_overrides(&self) -> bool {
        false
    }

    async fn set_metrics_manager(
        &self,
        _mm: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
    ) {
    }

    async fn register_endpoint(&self, _: &str) {}
    async fn refresh_dns(&self) -> Result<()> {
        Ok(())
    }
}

/// Req 2.3, 6.1, 6.5: budget `1` with 3 slow parallel missing ranges →
/// at most one hedge across the set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_range_hedge_budget_shared_across_subfetches() {
    use s3_proxy::range_handler::RangeSpec;
    use std::sync::atomic::AtomicUsize as StdAtomicUsize;

    hedged_fetch::init_global_hedging();

    // All ranges respond slowly (300ms) — longer than trigger_after (50ms),
    // so every sub-fetch will attempt to hedge.
    let stub = SlowRangeStub::new(Duration::from_millis(300));
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stub.clone());

    let config = test_config(Duration::from_secs(5), 0);
    let (_temp_dir, _cache_manager, range_handler) = make_test_cache(&config).await;

    // 3 missing ranges, budget of 1 → at most 1 hedge across all 3 sub-fetches.
    let missing_ranges = vec![
        RangeSpec { start: 0, end: 3 },
        RangeSpec { start: 10, end: 13 },
        RangeSpec { start: 20, end: 23 },
    ];

    let hedge_budget = Arc::new(StdAtomicUsize::new(1));
    let trigger_after = Duration::from_millis(50);
    let max_inflight_fraction = 1.0; // Don't suppress via governor for this test.

    let result = range_handler
        .fetch_missing_ranges(
            "test-bucket/budget-test.bin",
            &missing_ranges,
            &s3_client,
            "s3.amazonaws.com",
            &"/test-bucket/budget-test.bin".parse().unwrap(),
            &HashMap::new(),
            Some(&hedge_budget),
            trigger_after,
            max_inflight_fraction,
        )
        .await;

    assert!(result.is_ok(), "fetch_missing_ranges should succeed");
    let fetched = result.unwrap();
    assert_eq!(fetched.len(), 3, "All 3 ranges should be fetched");

    // With budget 1 and 3 slow ranges: 3 originals + at most 1 hedge = at most 4 calls.
    // (Less is possible if timing causes the hedge to not fire for some sub-fetches.)
    let total_calls = stub.calls();
    assert!(
        total_calls <= 4,
        "Budget 1 + 3 ranges: expected at most 4 calls (3 originals + 1 hedge), got {}",
        total_calls
    );
    assert!(
        total_calls >= 3,
        "At least 3 calls (originals) expected, got {}",
        total_calls
    );
}

/// Req 6.5: budget `2` with 3 slow ranges → at most two hedges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_range_hedge_budget_2_allows_two_hedges() {
    use s3_proxy::range_handler::RangeSpec;
    use std::sync::atomic::AtomicUsize as StdAtomicUsize;

    hedged_fetch::init_global_hedging();

    let stub = SlowRangeStub::new(Duration::from_millis(300));
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stub.clone());

    let config = test_config(Duration::from_secs(5), 0);
    let (_temp_dir, _cache_manager, range_handler) = make_test_cache(&config).await;

    let missing_ranges = vec![
        RangeSpec { start: 0, end: 3 },
        RangeSpec { start: 10, end: 13 },
        RangeSpec { start: 20, end: 23 },
    ];

    let hedge_budget = Arc::new(StdAtomicUsize::new(2));
    let trigger_after = Duration::from_millis(50);
    let max_inflight_fraction = 1.0;

    let result = range_handler
        .fetch_missing_ranges(
            "test-bucket/budget-2-test.bin",
            &missing_ranges,
            &s3_client,
            "s3.amazonaws.com",
            &"/test-bucket/budget-2-test.bin".parse().unwrap(),
            &HashMap::new(),
            Some(&hedge_budget),
            trigger_after,
            max_inflight_fraction,
        )
        .await;

    assert!(result.is_ok());
    let fetched = result.unwrap();
    assert_eq!(fetched.len(), 3);

    // Budget 2 + 3 slow ranges: 3 originals + at most 2 hedges = at most 5 calls.
    let total_calls = stub.calls();
    assert!(
        total_calls <= 5,
        "Budget 2 + 3 ranges: expected at most 5 calls (3 originals + 2 hedges), got {}",
        total_calls
    );
    assert!(
        total_calls >= 3,
        "At least 3 calls (originals) expected, got {}",
        total_calls
    );
}

/// Req 1.3: not rule-enabled (None budget) → byte-identical to previous behaviour.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_range_no_hedging_when_budget_none() {
    use s3_proxy::range_handler::RangeSpec;

    hedged_fetch::init_global_hedging();

    let stub = SlowRangeStub::new(Duration::from_millis(100));
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stub.clone());

    let config = test_config(Duration::from_secs(5), 0);
    let (_temp_dir, _cache_manager, range_handler) = make_test_cache(&config).await;

    let missing_ranges = vec![
        RangeSpec { start: 0, end: 3 },
        RangeSpec { start: 10, end: 13 },
    ];

    // No hedge budget → no hedging.
    let result = range_handler
        .fetch_missing_ranges(
            "test-bucket/no-hedge.bin",
            &missing_ranges,
            &s3_client,
            "s3.amazonaws.com",
            &"/test-bucket/no-hedge.bin".parse().unwrap(),
            &HashMap::new(),
            None, // Not rule-enabled
            Duration::from_millis(50),
            0.1,
        )
        .await;

    assert!(result.is_ok());
    let fetched = result.unwrap();
    assert_eq!(fetched.len(), 2);

    // With no budget, only originals should fire: exactly 2 calls.
    assert_eq!(
        stub.calls(),
        2,
        "Without hedging, exactly 2 calls (one per range) expected"
    );
}

// ---------------------------------------------------------------------------
// Signed range request hedging (fleet-verification-gaps, T38d regression)
// ---------------------------------------------------------------------------
//
// A client that signs its Range header (e.g. the AWS CLI's
// `aws s3api get-object --range`, which includes `range` in SignedHeaders)
// routes to `forward_signed_range_request` rather than
// `stream_range_from_s3_with_caching`/`fetch_missing_ranges`. Before this fix,
// that path never called into `hedged_fetch` at all, regardless of
// `resolved.hedging_enabled` — a real coverage gap discovered when the fleet
// deployment-verification suite's T38d assertion (range GET through a
// 400ms-delayed origin) failed to observe `hedged_requests.issued`
// incrementing, while the equivalent full-object GET (T38b) did.
//
// These tests dispatch through `forward_range_with_coordination` with
// `is_signed: true` and coordination disabled, which is the public entry
// point that reaches the private `forward_signed_range_request` — the same
// function real signed-range traffic reaches once download coordination
// (enabled by default in production) has decided this request is the sole
// fetcher.

/// Req 1.2, 1.3, 2.1, 2.3, 6.1, 6.5: a signed range request with hedging
/// enabled and a slow original hedges and serves via the hedge, exactly like
/// the unsigned full-object and range-miss paths already do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_signed_range_hedging_enabled_slow_original_hedge_wins() {
    hedged_fetch::init_global_hedging();

    let stub = DelayedHedgeStub::new(
        Duration::from_millis(500), // slow original (will trigger hedge)
        Duration::from_millis(10),  // fast hedge
        206,
        206,
    );
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stub.clone());

    let mut config_inner = Config::default();
    config_inner.connection_pool.upstream_first_byte_timeout = Duration::from_secs(5);
    config_inner.connection_pool.upstream_idle_retries = 2;
    config_inner
        .connection_pool
        .hedged_requests
        .max_inflight_fraction = 1.0;
    config_inner.cache.download_coordination.enabled = false;
    let config = Arc::new(config_inner);

    let (_temp_dir, cache_manager_for_call, range_handler) = make_test_cache(&config).await;

    let resolved = resolved_with_hedging(true, Duration::from_millis(50), 1);
    let range_spec = s3_proxy::range_handler::RangeSpec { start: 0, end: 3 };
    let overlap = s3_proxy::range_handler::RangeOverlap {
        cached_ranges: Vec::new(),
        missing_ranges: vec![range_spec.clone()],
        can_serve_from_cache: false,
    };
    let mut signed_headers = HashMap::new();
    signed_headers.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20250101/us-west-2/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=deadbeef".to_string(), // #gitleaks:allow
    );
    signed_headers.insert("range".to_string(), "bytes=0-3".to_string());

    let metrics = hedged_fetch::get_global_metrics().unwrap();
    let issued_before = metrics.issued.load(Ordering::Relaxed);
    let won_before = metrics.won.load(Ordering::Relaxed);

    let start = Instant::now();
    let response = HttpProxy::forward_range_with_coordination(
        Method::GET,
        "/test-bucket/signed-range-hedge-key.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers,
        "test-bucket/signed-range-hedge-key.bin".to_string(),
        range_spec,
        overlap,
        cache_manager_for_call,
        range_handler,
        s3_client,
        config,
        true, // is_signed
        None,
        Arc::new(s3_proxy::inflight_tracker::InFlightTracker::new()),
        None,
        &resolved,
        &None,
    )
    .await
    .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert!(
        elapsed < Duration::from_millis(300),
        "Signed range hedge should serve faster than the 500ms original; elapsed={:?}",
        elapsed
    );
    let issued_after = metrics.issued.load(Ordering::Relaxed);
    let won_after = metrics.won.load(Ordering::Relaxed);
    assert!(
        issued_after > issued_before,
        "Signed range request should have issued a hedge (Req 2.3 applies to signed ranges too)"
    );
    assert!(
        won_after > won_before,
        "Hedge should have won the signed range race"
    );
}

/// Req 1.3: hedging disabled for a signed range request → byte-identical to
/// pre-fix behaviour, exactly one upstream call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_signed_range_hedging_disabled_no_hedge_issued() {
    hedged_fetch::init_global_hedging();

    let stub = DelayedHedgeStub::new(
        Duration::from_millis(10),
        Duration::from_millis(5),
        206,
        206,
    );
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stub.clone());

    let mut config_inner = Config::default();
    config_inner.connection_pool.upstream_first_byte_timeout = Duration::from_secs(5);
    config_inner.connection_pool.upstream_idle_retries = 2;
    config_inner.cache.download_coordination.enabled = false;
    let config = Arc::new(config_inner);

    let (_temp_dir, cache_manager_for_call, range_handler) = make_test_cache(&config).await;

    let resolved = resolved_with_hedging(false, Duration::from_millis(50), 1);
    let range_spec = s3_proxy::range_handler::RangeSpec { start: 0, end: 3 };
    let overlap = s3_proxy::range_handler::RangeOverlap {
        cached_ranges: Vec::new(),
        missing_ranges: vec![range_spec.clone()],
        can_serve_from_cache: false,
    };
    let mut signed_headers = HashMap::new();
    signed_headers.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20250101/us-west-2/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=deadbeef".to_string(), // #gitleaks:allow
    );
    signed_headers.insert("range".to_string(), "bytes=0-3".to_string());

    let metrics = hedged_fetch::get_global_metrics().unwrap();
    let issued_before = metrics.issued.load(Ordering::Relaxed);

    let response = HttpProxy::forward_range_with_coordination(
        Method::GET,
        "/test-bucket/signed-range-no-hedge-key.bin"
            .parse()
            .unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers,
        "test-bucket/signed-range-no-hedge-key.bin".to_string(),
        range_spec,
        overlap,
        cache_manager_for_call,
        range_handler,
        s3_client.clone(),
        config,
        true, // is_signed
        None,
        Arc::new(s3_proxy::inflight_tracker::InFlightTracker::new()),
        None,
        &resolved,
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let issued_after = metrics.issued.load(Ordering::Relaxed);
    assert_eq!(
        issued_after, issued_before,
        "No hedge should be issued when hedging is disabled for a signed range request"
    );
    let _ = s3_client; // dropped after use; call count is read from the original `stub` handle
    assert_eq!(
        stub.calls(),
        1,
        "Only the original upstream call should happen"
    );
}
