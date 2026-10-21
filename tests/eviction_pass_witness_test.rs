//! The read-tier eviction pass's own two figures, asserted at the PRODUCTION end.
//!
//! # Why this is a separate file from `metrics_wire_shape_test.rs`
//!
//! That file proves the figures reach the wire once something has populated them. This one proves
//! the real entry point populates them — `CacheManager::enforce_disk_cache_limits` →
//! `perform_eviction_with_lock`, the background eviction path, reached with nothing stubbed. A
//! field can be serialised and never written (this payload had one such figure) just as easily as
//! it can be written and never serialised (it had two), so neither end substitutes for the other.
//!
//! # Why its own test BINARY, and why the assertion is a delta
//!
//! `eviction_passes_total` is a process-global `AtomicU64` and libtest runs a binary's tests as
//! parallel threads in one process, so an exact-value assertion would be a race against any other
//! test in the same binary that evicts. An integration-test file is its own process, and this file
//! deliberately contains **one** eviction-driving test. The assertion is still written as a delta
//! rather than an absolute so that adding a second such test here degrades it to imprecise rather
//! than to flaky.
//!
//! Spec: cache-eviction-at-scale. Requirements: 12.1, 12.3.

use s3_proxy::cache::{collect_eviction_executor_gauges, CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_size_tracker::{AccountingDriftSample, ValidationSampleKind};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::disk_cache::get_sharded_path;
use s3_proxy::eviction_result::{EvictionPassResult, WriteCacheCountPassResult};
use s3_proxy::metrics::MetricsManager;
use s3_proxy::Result;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

fn create_test_cache_manager(temp_dir: &TempDir, max_cache_size: u64) -> CacheManager {
    CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,          // RAM cache disabled
        0,              // max_ram_cache_size
        max_cache_size, // max_cache_size (sets max_cache_size_limit)
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
        95,                                 // eviction_trigger_percent
        80,                                 // eviction_target_percent
        true,                               // read_cache_enabled
        Duration::from_secs(60),            // bucket_settings_staleness_threshold
        1_048_576,                          // compression_batch_size
        false,                              // evaluate_conditions_from_cache
        std::time::Duration::from_secs(10), // ram_cache_flush_interval
        64,                                 // ram_cache_shard_count
        std::time::Duration::from_secs(5),  // upstream_first_byte_timeout
    )
}

#[allow(clippy::too_many_arguments)]
async fn store_test_range(
    cache_manager: &CacheManager,
    cache_key: &str,
    start: u64,
    end: u64,
    data_size: usize,
    created_at: SystemTime,
    last_accessed: SystemTime,
) -> Result<()> {
    let data = vec![0u8; data_size];
    let ranges_base_dir = cache_manager.get_cache_dir().join("ranges");
    let suffix = format!("_{}-{}.bin", start, end);
    let range_path = get_sharded_path(&ranges_base_dir, cache_key, &suffix)
        .expect("Failed to get sharded path for range file");
    let relative_range_path = range_path
        .strip_prefix(&ranges_base_dir)
        .unwrap_or(&range_path)
        .to_string_lossy()
        .to_string();
    let mut range_spec = RangeSpec::new(
        start,
        end,
        relative_range_path,
        CompressionAlgorithm::Lz4,
        data_size as u64,
        data_size as u64,
    );
    range_spec.created_at = created_at;
    range_spec.last_accessed = last_accessed;
    range_spec.access_count = 10;

    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata::new(
            "test-etag".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            end - start + 1,
            Some("application/octet-stream".to_string()),
        ),
        ranges: vec![range_spec],
        created_at,
        expires_at: SystemTime::now() + Duration::from_secs(3600),
        compression_info: Default::default(),
        ..Default::default()
    };

    std::fs::create_dir_all(range_path.parent().unwrap())?;
    std::fs::write(&range_path, &data)?;
    let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
    std::fs::create_dir_all(metadata_path.parent().unwrap())?;
    std::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?)?;
    Ok(())
}

/// A real eviction pass increments the per-instance pass witness and records its active duration.
///
/// # What a failure means
///
/// `eviction_passes_total` flat implicates the `fetch_add` in
/// `CacheManager::perform_eviction_with_lock`, sited at the same point and on the same condition
/// as the result document is built so that "a pass completed here" and "a pass published a result"
/// cannot disagree.
///
/// `duration_ms` absent or `null` implicates the `Instant` started after the nothing-to-free
/// return in the same function. `null` specifically means the document was written without a
/// measurement, which is the UNMEASURED reading reserved for documents from builds predating the
/// field — a freshly written one must never be `null`.
///
/// # The witness is what makes the four executor counters beside it readable
///
/// Those four publish zero on an unpressured cache, and "no pass has run in this process" is
/// otherwise indistinguishable from "no whole object was ever cold". `cache.eviction_last_result`
/// nearly answers it and cannot: it is read from the shared volume and is fleet-wide, so a
/// restarted proxy showing four zeros beside a populated result is genuinely ambiguous.
#[tokio::test]
async fn an_eviction_pass_increments_the_witness_and_records_its_duration() {
    let temp_dir = TempDir::new().unwrap();
    let max_cache_size = 1_000_000u64;
    let cache_manager = create_test_cache_manager(&temp_dir, max_cache_size);

    let now = SystemTime::now();
    // Ten 100 KB ranges take the cache to ~1 MB, which is over the 95% trigger.
    for i in 0..10u64 {
        store_test_range(
            &cache_manager,
            &format!("test-bucket/witness-object-{}", i),
            0,
            99_999,
            100_000,
            now - Duration::from_secs(3600 - i * 60),
            now - Duration::from_secs(1800 - i * 60),
        )
        .await
        .unwrap();
    }

    // No pass has published on this volume yet, so the shared document is absent. Absence is a
    // reading, and asserting it here is what makes the `Some` below attributable to this pass
    // rather than to a document some earlier test left behind.
    assert!(
        EvictionPassResult::load(temp_dir.path()).is_none(),
        "no eviction pass has run on this fresh volume, so no result document should exist"
    );

    let before = collect_eviction_executor_gauges().eviction_passes_total;

    let freed = cache_manager.enforce_disk_cache_limits().await;
    assert!(freed.is_ok(), "eviction should succeed: {:?}", freed);

    let after = collect_eviction_executor_gauges().eviction_passes_total;
    assert!(
        after > before,
        "the per-instance pass witness must move across a real pass (before={before}, \
         after={after}). Without it, the four executor counters beside it on `cache.*` cannot \
         be told apart from an idle process."
    );

    let published = EvictionPassResult::load(temp_dir.path())
        .expect("a completed pass must publish its result document to the shared volume");
    let duration = published.duration_ms.expect(
        "a freshly written result must carry a measured active duration. `null` is reserved \
         for documents from builds predating the field, and a rate consumer skips those rather \
         than dividing by a denominator it never observed.",
    );
    // No upper bound asserted: the duration is a real measurement on a real filesystem and any
    // ceiling would be a timing assertion. The property under test is that it was measured at
    // all, and the `expect` above is that property.
    let _ = duration;

    // And the summary line names it, so an operator reading the pass's own log sees the same
    // figure the rate is computed from.
    assert!(
        published.summary().contains("duration_ms="),
        "the pass's summary line must name the duration: {}",
        published.summary()
    );
}

/// The COLLECTOR reads both persisted documents back onto the payload.
///
/// # This is the assertion a literal-built `SystemMetrics` cannot make
///
/// `tests/metrics_wire_shape_test.rs` constructs `WriteCacheMetrics { count_pass_last_result:
/// Some(..), .. }` by hand, so it proves the field serialises and nothing about whether anything
/// populates it. Reverting the one line in `collect_write_cache_metrics` that loads the document
/// leaves that file entirely green — a figure computed, persisted, and dropped on the way to the
/// wire, which is the exact shape of the two defects already found on this payload.
///
/// So this test goes through `MetricsManager::collect_metrics`, with a real `CacheManager` and the
/// documents on a real volume, and asserts the figures arrive. A failure implicates the two load
/// call sites: `count_pass_last_result` in `collect_write_cache_metrics`, and `accounting_drift` in
/// `CacheSizeTracker::get_metrics`.
///
/// The documents are installed directly rather than produced by a pass: their producers are
/// covered where they are written, and what is untested without this is the *read*. The install
/// call is the same one production uses, so the bytes on disk are production's bytes.
#[tokio::test]
async fn the_collector_reads_the_persisted_pass_documents_onto_the_payload() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = Arc::new(create_test_cache_manager(&temp_dir, 10 * 1024 * 1024));
    let _ = cache_manager.create_configured_disk_cache_manager();

    let mut metrics_manager = MetricsManager::new();
    // Wire BEFORE initialize, as `main()` does — `cache_size` is captured at collection time and
    // a wiring-time capture is a defect this project has already shipped once.
    metrics_manager.set_cache_manager(Arc::clone(&cache_manager));
    cache_manager
        .initialize()
        .await
        .expect("cache manager initialize");

    // AFTER initialize. The removed startup census installed a drift sample of its own here,
    // which overwrote a document planted beforehand; seeding after startup keeps the fixture
    // independent of whatever startup writes.
    WriteCacheCountPassResult::new(
        "collector-host:1".into(),
        1_000_000, // count_budget
        950_000,   // count_trigger
        800_000,   // count_target
        960_000,   // count_at_start
        962_000,   // observed_count_at_start
        160_000,   // ranges_requested
        120_000,   // ranges_freed
        7,         // unreadable_candidate_ranges
        43,        // objects_evicted
        512,       // candidates_considered
        300,       // reclaimable_candidates
        290,       // expired_candidates
        3,         // passes_total
        500_000,   // ranges_freed_total
        11,        // admission_declines_total
    )
    .install(temp_dir.path())
    .expect("install the count-pass result");

    AccountingDriftSample::new(
        "collector-host:1".into(),
        ValidationSampleKind::FullScan,
        true,
        (4000, 1000, 40),
        (4096, Some(1024), Some(41)),
    )
    .install(temp_dir.path())
    .expect("install the accounting drift sample");

    let metrics = metrics_manager.collect_metrics().await;

    let count_pass = metrics.write_cache.count_pass_last_result.as_ref().expect(
        "the collector must read write_cache_count_eviction_last_result.json back onto the \
         payload. The pass has been installing it since it shipped and nothing read it; its \
         read-tier sibling was published all along.",
    );
    assert_eq!(
        count_pass.candidates_considered, 512,
        "the published figures must be the document's, not a default"
    );
    assert_eq!(
        count_pass.instance_id, "collector-host:1",
        "the writer's identity must survive: passes_total, ranges_freed_total and \
         admission_declines_total inside this fleet-wide document are THAT instance's \
         per-process counters, and are unattributable without it"
    );

    let drift = metrics
        .cache_size
        .as_ref()
        .expect("cache_size must be populated after initialize")
        .accounting_drift
        .as_ref()
        .expect(
            "the collector must read accounting_drift_sample.json back onto the payload. \
             R12.6's gauges are unalarmable if the document is written and never read.",
        );
    assert_eq!(
        drift.total_size.drift,
        Some(96),
        "reference 4096 against persisted 4000 is a drift of +96, and it must survive the \
         round trip through the shared volume"
    );
    assert_eq!(drift.sample_kind, "full_scan");
}
