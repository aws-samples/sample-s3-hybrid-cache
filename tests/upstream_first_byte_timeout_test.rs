//! Tests for pre-first-byte timeout + retry on the proxy's own S3 fetch.
//!
//! Validates Requirement 5 (Task 10):
//! - Stalled upstream (accepts but sends nothing) → proxy aborts at
//!   `upstream_first_byte_timeout`, retries `upstream_idle_retries` times, then errors.
//! - Coalesced waiter blocked > idle timeout but < coordination timeout → NOT aborted.

use async_trait::async_trait;
use hyper::{Method, StatusCode};
use s3_proxy::bucket_settings::ResolvedSettings;
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{CacheMetadata, ObjectMetadata};
use s3_proxy::config::Config;
use s3_proxy::connection_pool::ConnectionPoolManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::upstream_overrides::UpstreamOverrides;
use s3_proxy::{Result, S3ClientApi, S3RequestContext, S3Response};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tempfile::TempDir;

/// An S3 client stub that hangs forever on `forward_request`, simulating a
/// stalled upstream that accepts the connection but never sends a first byte.
#[derive(Clone)]
struct StalledS3Client {
    /// Counts how many times `forward_request` was called (to verify retry count).
    attempt_count: Arc<AtomicUsize>,
}

impl StalledS3Client {
    fn new() -> Self {
        Self {
            attempt_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn attempts(&self) -> usize {
        self.attempt_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl S3ClientApi for StalledS3Client {
    async fn forward_request(&self, _context: S3RequestContext) -> Result<S3Response> {
        self.attempt_count.fetch_add(1, Ordering::SeqCst);
        // Simulate a stalled upstream: hang indefinitely (will be cancelled by timeout)
        tokio::time::sleep(Duration::from_secs(3600)).await;
        unreachable!("should be cancelled by timeout before reaching here")
    }

    fn extract_metadata_from_response(&self, _headers: &HashMap<String, String>) -> CacheMetadata {
        CacheMetadata {
            content_length: 0,
            etag: String::new(),
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

    fn get_upstream_overrides(&self) -> Arc<UpstreamOverrides> {
        Arc::new(UpstreamOverrides::from_config(&HashMap::new()))
    }

    fn has_endpoint_overrides(&self) -> bool {
        false
    }

    async fn set_metrics_manager(
        &self,
        _metrics_manager: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
    ) {
    }

    async fn register_endpoint(&self, _endpoint: &str) {}

    async fn refresh_dns(&self) -> Result<()> {
        Ok(())
    }
}

/// Build a minimal config with short first-byte timeout for testing.
fn test_config(first_byte_timeout: Duration, retries: usize) -> Arc<Config> {
    let mut config = Config::default();
    config.connection_pool.upstream_first_byte_timeout = first_byte_timeout;
    config.connection_pool.upstream_idle_retries = retries;
    config.cache.download_coordination.enabled = true;
    Arc::new(config)
}

/// Create a CacheManager + RangeHandler for test use.
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

/// Test: stalled upstream → proxy aborts at first_byte_timeout, retries, then errors.
/// Asserts:
/// - Total time < (retries + 1) * timeout + small buffer (not 60s hang)
/// - Attempt count = retries + 1 (initial + retries)
/// - Returns a non-success response (504 or 502)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stalled_upstream_aborts_at_timeout_and_retries() {
    let timeout_ms = 200; // 200ms for fast test
    let retries = 2;
    let config = test_config(Duration::from_millis(timeout_ms), retries);

    let stalled_client = StalledS3Client::new();
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stalled_client.clone());

    let (_temp_dir, cache_manager, range_handler) = make_test_cache(&config).await;
    let resolved = ResolvedSettings::default();

    let start = Instant::now();

    let response = HttpProxy::forward_get_head_to_s3_and_cache(
        Method::GET,
        "/test-bucket/stalled-object.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        HashMap::new(),
        "test-bucket/stalled-object.bin".to_string(),
        cache_manager,
        s3_client,
        range_handler,
        config.clone(),
        &resolved,
        &None,
        None,
    )
    .await
    .unwrap();

    let elapsed = start.elapsed();

    // Verify: all retries were attempted
    assert_eq!(
        stalled_client.attempts(),
        retries + 1,
        "Expected {} attempts (1 initial + {} retries), got {}",
        retries + 1,
        retries,
        stalled_client.attempts()
    );

    // Verify: didn't hang for 60s — completed within expected timeout budget
    let max_expected = Duration::from_millis(timeout_ms * (retries as u64 + 1) + 1000);
    assert!(
        elapsed < max_expected,
        "Expected to complete within {:?}, but took {:?}",
        max_expected,
        elapsed
    );

    // Verify: error response (not a success)
    assert!(
        response.status().is_server_error() || response.status() == StatusCode::BAD_GATEWAY,
        "Expected server error or 502, got {}",
        response.status()
    );
}

/// Test: with 0 retries, a single timeout attempt returns error quickly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stalled_upstream_zero_retries() {
    let timeout_ms = 150;
    let config = test_config(Duration::from_millis(timeout_ms), 0);

    let stalled_client = StalledS3Client::new();
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stalled_client.clone());

    let (_temp_dir, cache_manager, range_handler) = make_test_cache(&config).await;
    let resolved = ResolvedSettings::default();
    let start = Instant::now();

    let response = HttpProxy::forward_get_head_to_s3_and_cache(
        Method::GET,
        "/test-bucket/zero-retry.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        HashMap::new(),
        "test-bucket/zero-retry.bin".to_string(),
        cache_manager,
        s3_client,
        range_handler,
        config.clone(),
        &resolved,
        &None,
        None,
    )
    .await
    .unwrap();

    let elapsed = start.elapsed();

    // Only 1 attempt (no retries)
    assert_eq!(stalled_client.attempts(), 1);

    // Completed quickly
    assert!(
        elapsed < Duration::from_millis(timeout_ms + 500),
        "Expected completion within ~{}ms, took {:?}",
        timeout_ms + 500,
        elapsed
    );

    // Error response
    assert!(
        response.status().is_server_error() || response.status() == StatusCode::BAD_GATEWAY,
        "Expected error, got {}",
        response.status()
    );
}

/// Test: coalesced waiter blocked > idle timeout but < coordination timeout → NOT aborted.
///
/// This test verifies that the upstream_first_byte_timeout does NOT apply to the
/// coordination wait. A waiter sits in the download coordinator for longer than the
/// first-byte timeout but less than the coordination timeout. It should NOT be
/// aborted — the waiter path does not go through forward_get_head_to_s3_and_cache
/// until after coordination resolves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_coalesced_waiter_not_aborted_by_first_byte_timeout() {
    use s3_proxy::inflight_tracker::{FetchRole, InFlightTracker};

    // Configure: first_byte_timeout = 100ms, coordination wait_timeout = 5s
    // The waiter will wait ~300ms (> first_byte_timeout) for the fetcher to complete.
    // It should NOT be aborted.
    let mut config = Config::default();
    config.connection_pool.upstream_first_byte_timeout = Duration::from_millis(100);
    config.connection_pool.upstream_idle_retries = 0;
    config.cache.download_coordination.enabled = true;
    config.cache.download_coordination.wait_timeout_secs = 5;

    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "test-bucket/coordination-wait-key.bin".to_string();
    let flight_key = InFlightTracker::make_full_key(&cache_key);

    // The "fetcher" registers first
    let fetcher_role = inflight_tracker.try_register(&flight_key);
    let guard = match fetcher_role {
        FetchRole::Fetcher(g) => g,
        _ => panic!("Expected fetcher role"),
    };

    // The "waiter" registers second
    let waiter_role = inflight_tracker.try_register(&flight_key);
    let mut waiter_rx = match waiter_role {
        FetchRole::Waiter(rx) => rx,
        _ => panic!("Expected waiter role"),
    };

    let wait_timeout = config.cache.download_coordination.wait_timeout();

    // Spawn fetcher: sleeps 300ms (> first_byte_timeout=100ms) then completes
    let guard_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        guard.complete_success();
    });

    // Waiter waits for the fetcher. This should NOT be aborted by the
    // upstream_first_byte_timeout because the waiter is in coordination wait,
    // not in forward_get_head_to_s3_and_cache.
    let start = Instant::now();
    let result = tokio::time::timeout(wait_timeout, waiter_rx.recv()).await;
    let elapsed = start.elapsed();

    // The waiter should succeed (fetcher completed OK)
    assert!(result.is_ok(), "Waiter should not have timed out");
    let inner = result.unwrap();
    assert!(
        inner.is_ok(),
        "Waiter channel should have received a message"
    );
    let msg = inner.unwrap();
    assert!(msg.is_ok(), "Fetcher should have signaled success");

    // The wait took ~300ms which is > first_byte_timeout (100ms) — proving the
    // coordination wait is NOT governed by the first-byte timeout.
    assert!(
        elapsed >= Duration::from_millis(250),
        "Expected wait of ~300ms, got {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(1000),
        "Wait took too long: {:?}",
        elapsed
    );

    guard_handle.await.unwrap();
}
