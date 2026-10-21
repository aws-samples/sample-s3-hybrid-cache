//! Cache Size Metrics Integration Test
//!
//! Covers the `cache_size` section of `/metrics`, and specifically the STARTUP
//! ORDERING that made it report `null` for the life of every process from v1.1.x
//! until 2.8.3.
//!
//! ## Why these tests drive a real `CacheManager`
//!
//! The previous version of this file called a `MetricsManager::set_cache_size_tracker`
//! setter directly, handing it a `CacheSizeTracker` built inline. That passed
//! continuously while production was broken, because it bypassed the wiring
//! production actually used — the exact "tests assert a path customers never take"
//! trap. The setter and the field behind it no longer exist; `MetricsManager` reads
//! the tracker through the cache manager at collection time.
//!
//! So every test here obtains its metrics the way `/metrics` does: attach an
//! `Arc<CacheManager>` via `set_cache_manager`, then call `collect_metrics()`.
//!
//! ## The regression guard, and why its order matters
//!
//! `test_cache_size_present_when_manager_attached_before_initialize` reproduces
//! `main()`'s real sequence: the metrics manager is attached to the cache manager
//! BEFORE `CacheManager::initialize()` runs, because in production the metrics
//! wiring happens hundreds of lines above the `HttpProxy::start()` spawn that calls
//! `initialize()`. Under the old design — an `Arc<CacheSizeTracker>` captured at
//! wiring time — that ordering yields `None` forever, since the tracker does not
//! exist yet and nothing retries. Under the current design it yields `Some`.
//!
//! That is the red side: this test fails against every release from v1.1.x through
//! 2.8.2 — which holds for BOTH builds carrying that number, the internal one and the
//! public issue-19 hotfix, since the fix postdates both and first reaches a customer in
//! 3.0.0 — and against any future change that reintroduces startup-time tracker capture.
//! Keep the `set_cache_manager` call BEFORE the `initialize()` call — swapping them
//! makes the test pass under both designs and silently destroys its value.

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::metrics::MetricsManager;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// Build a cache manager over a fresh temp dir. Deliberately NOT initialized —
/// each test decides when `initialize()` runs relative to the metrics wiring,
/// because that ordering is the thing under test.
///
/// The `create_configured_disk_cache_manager()` call mirrors production: `HttpProxy::new`
/// makes it once to construct the journal singletons, and `initialize()` requires the
/// `JournalConsolidator` it installs.
fn create_test_cache_manager(temp_dir: &TempDir) -> Arc<CacheManager> {
    let manager = build_cache_manager(temp_dir);
    let _ = manager.create_configured_disk_cache_manager();
    Arc::new(manager)
}

fn build_cache_manager(temp_dir: &TempDir) -> CacheManager {
    CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,                   // RAM cache disabled — irrelevant to cache_size reporting
        0,                       // max_ram_cache_size
        10 * 1024 * 1024 * 1024, // max_cache_size
        CacheEvictionAlgorithm::TinyLFU,
        1024,                           // compression_threshold
        true,                           // compression_enabled
        Duration::from_secs(315360000), // GET TTL (~10 years)
        Duration::from_secs(3600),      // HEAD TTL
        Duration::from_secs(3600),      // PUT TTL
        false,                          // actively_remove_cached_data
        SharedStorageConfig::default(),
        10.0,                       // write_cache_percent
        false,                      // write_cache_enabled
        Duration::from_secs(86400), // incomplete_upload_ttl
        MetadataCacheConfig::default(),
        95,                      // eviction_trigger_percent
        80,                      // eviction_target_percent
        true,                    // read_cache_enabled
        Duration::from_secs(60), // bucket_settings_staleness_threshold
        1_048_576,               // compression_batch_size
        false,                   // evaluate_conditions_from_cache
        Duration::from_secs(10), // ram_cache_flush_interval
        64,                      // ram_cache_shard_count
        Duration::from_secs(5),  // upstream_first_byte_timeout
    )
}

/// THE REGRESSION GUARD. Reproduces production's ordering: metrics manager wired to
/// the cache manager first, `initialize()` second. Fails on every release from v1.1.x
/// through 2.8.2 (both the internal build and the public hotfix of that number; fixed
/// for customers in 3.0.0), and on any reintroduction of startup-time tracker capture.
#[tokio::test]
async fn test_cache_size_present_when_manager_attached_before_initialize() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_manager = create_test_cache_manager(&temp_dir);

    let mut metrics_manager = MetricsManager::new();

    // ORDER IS THE EXPERIMENT — wire BEFORE initialize, as main() does.
    metrics_manager.set_cache_manager(Arc::clone(&cache_manager));

    // Only now does the size tracker come into existence.
    cache_manager
        .initialize()
        .await
        .expect("cache manager initialize");

    let metrics = metrics_manager.collect_metrics().await;

    assert!(
        metrics.cache_size.is_some(),
        "cache_size must be populated after CacheManager::initialize(), even though \
         the metrics manager was attached before it. A None here means the tracker is \
         being captured at wiring time again — the v1.1.x-through-2.8.2 defect, fixed in 3.0.0."
    );

    let cache_size = metrics.cache_size.unwrap();

    // A fresh cache is empty; the point is that the figure is REACHABLE, not its value.
    assert_eq!(
        cache_size.current_size, 0,
        "fresh cache should report zero tracked bytes"
    );

    println!("cache_size after initialize: {:?}", cache_size);
}

/// The other half of the two-sided assertion. `null` must still mean something
/// honest — namely that initialization has not happened yet — rather than the
/// section having become unconditionally present.
#[tokio::test]
async fn test_cache_size_absent_before_initialize() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_manager = create_test_cache_manager(&temp_dir);

    let mut metrics_manager = MetricsManager::new();
    metrics_manager.set_cache_manager(Arc::clone(&cache_manager));

    // No initialize() call — the tracker genuinely does not exist.
    let metrics = metrics_manager.collect_metrics().await;

    assert!(
        metrics.cache_size.is_none(),
        "cache_size must be None before initialize() creates the tracker; a Some here \
         means the section no longer reflects real tracker availability"
    );
}

/// `cache_size` is None when no cache manager is attached at all.
#[tokio::test]
async fn test_cache_size_metrics_without_cache_manager() {
    let metrics_manager = MetricsManager::new();

    let metrics = metrics_manager.collect_metrics().await;

    assert!(
        metrics.cache_size.is_none(),
        "Cache size metrics should be None when no cache manager is attached"
    );

    let json = serde_json::to_string_pretty(&metrics).expect("Failed to serialize metrics to JSON");
    assert!(
        json.contains("cache_size"),
        "JSON should still contain the cache_size key"
    );
    assert!(
        json.contains("\"cache_size\": null"),
        "cache_size should serialize as null"
    );
}

/// Serialized shape of the section, collected through the production path.
#[tokio::test]
async fn test_cache_size_metrics_json_serialization() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_manager = create_test_cache_manager(&temp_dir);

    let mut metrics_manager = MetricsManager::new();
    metrics_manager.set_cache_manager(Arc::clone(&cache_manager));
    cache_manager
        .initialize()
        .await
        .expect("cache manager initialize");

    let metrics = metrics_manager.collect_metrics().await;
    let json = serde_json::to_string_pretty(&metrics).expect("Failed to serialize metrics to JSON");

    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Failed to parse JSON");
    let cache_size = parsed
        .get("cache_size")
        .expect("cache_size field should exist");

    assert!(
        cache_size.is_object(),
        "cache_size should be an object once the tracker exists, got: {cache_size}"
    );

    let cache_size_obj = cache_size.as_object().unwrap();
    for field in [
        "current_size",
        "write_cache_size",
        "last_checkpoint",
        "checkpoint_count",
        "delta_log_size",
        "last_validation",
    ] {
        assert!(
            cache_size_obj.contains_key(field),
            "cache_size should carry the documented field {field} \
             (docs/METRICS_REFERENCE.md § cache_size)"
        );
    }

    let current_size = cache_size_obj
        .get("current_size")
        .and_then(|v| v.as_u64())
        .expect("current_size should be a number");
    assert_eq!(current_size, 0, "fresh cache should report zero bytes");
}
