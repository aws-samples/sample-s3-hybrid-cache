//! R12.3's rate accumulators, asserted at the PRODUCTION end — and the published field asserted
//! to be the accumulator's quotient.
//!
//! # Why this file exists beside two others
//!
//! `src/metrics.rs`'s `eviction_rate_gauge_tests` divides two constants. It pins the WINDOW — the
//! correction design § 8 makes to its own specified form — and it proves nothing about wiring: it
//! would pass identically against a build in which no eviction pass ever touched an accumulator and
//! no range credit ever touched the other one.
//!
//! `tests/metrics_wire_shape_test.rs` builds a `SystemMetrics` literal and asserts the names reach
//! the wire. It would pass identically against a build whose collector never read the
//! accumulators — that exact defect was measured on this payload when `count_pass_last_result` was
//! published, and it is why that unit needed a second test binary too.
//!
//! So this file asserts the two ends neither of those can:
//!
//! 1. A **real eviction pass**, driven through `CacheManager::enforce_disk_cache_limits` with
//!    nothing stubbed, moves all three shed accumulator terms.
//! 2. A **real range credit**, through the production `SizeAccumulator::add_range` that all three
//!    production credit sites call, moves the admission accumulator — and a deduplicated repeat
//!    does not, which is the other half of the population claim.
//! 3. `MetricsManager::collect_metrics` publishes the accumulators, and each published rate is the
//!    quotient of the published counters beside it. That is the assertion that makes the counters
//!    worth shipping: a consumer can audit the rate instead of trusting it.
//!
//! # Why deltas, and why ONE eviction-driving test in this binary
//!
//! The accumulators are process-global `AtomicU64`s and libtest runs a binary's tests as parallel
//! threads in one process, so an absolute assertion would race. An integration-test file is its own
//! process; this one contains exactly one eviction-driving test, and the quotient assertions read
//! all their terms out of a **single** `collect_metrics` response so that a concurrent credit
//! cannot fall between two reads.
//!
//! Spec: cache-eviction-at-scale § 8. Requirements: 2.3, 12.3.

use s3_proxy::cache::{collect_eviction_shed_accumulator, CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::disk_cache::get_sharded_path;
use s3_proxy::journal_consolidator::{
    collect_admission_accumulator, ConsolidationConfig, JournalConsolidator,
};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
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

/// A real eviction pass moves all three shed accumulator terms, and the collector publishes them
/// with each rate equal to the quotient of the counters beside it.
///
/// # What each failure means
///
/// `shed_bytes_total` flat implicates the `fetch_add` in
/// `CacheManager::perform_eviction_with_lock`, sited at the same point and read from the same
/// `total_bytes_freed` as `cache.eviction_last_result.bytes_freed`, so the cumulative figure and
/// the last-pass document cannot disagree about what a pass freed.
///
/// `active_micros_total` flat implicates the `Instant` started after the nothing-to-free return in
/// the same function. That figure is the shed rate's **denominator** and the whole subject of this
/// unit: design § 8 specifies an average over the whole window and then says, in the same section,
/// that the form it specifies would set `keeping_pace = false` on a correctly idle cache.
///
/// The published-quotient assertions failing while the deltas pass implicates
/// `MetricsManager::collect_cache_metrics`'s `EvictionRateGauges::build` call — a figure computed
/// and then dropped on the way to the wire, which is a defect this payload has already shipped
/// twice.
#[tokio::test]
async fn a_real_pass_moves_the_shed_accumulator_and_the_published_rates_are_its_quotient() {
    let temp_dir = TempDir::new().unwrap();
    let max_cache_size = 1_000_000u64;
    let cache_manager = Arc::new(create_test_cache_manager(&temp_dir, max_cache_size));

    let now = SystemTime::now();
    // Ten 100 KB ranges take the cache to ~1 MB, which is over the 95% trigger.
    for i in 0..10u64 {
        store_test_range(
            &cache_manager,
            &format!("test-bucket/rate-object-{}", i),
            0,
            99_999,
            100_000,
            now - Duration::from_secs(3600 - i * 60),
            now - Duration::from_secs(1800 - i * 60),
        )
        .await
        .unwrap();
    }

    let before = collect_eviction_shed_accumulator();

    let freed = cache_manager.enforce_disk_cache_limits().await;
    assert!(freed.is_ok(), "eviction should succeed: {:?}", freed);

    let after = collect_eviction_shed_accumulator();
    assert!(
        after.shed_bytes_total > before.shed_bytes_total,
        "a real pass must credit the shed-bytes numerator (before={}, after={}). Without it the \
         published rate is a plausible zero rather than a measurement.",
        before.shed_bytes_total,
        after.shed_bytes_total
    );
    assert!(
        after.shed_ranges_total > before.shed_ranges_total,
        "a real pass must credit the shed-RANGES numerator (before={}, after={}). Ranges, not \
         entries — the published field is named for what it counts.",
        before.shed_ranges_total,
        after.shed_ranges_total
    );
    assert!(
        after.active_micros_total > before.active_micros_total,
        "a real pass must credit the ACTIVE-duration denominator (before={}, after={}). This is \
         the term design § 8 corrects itself to: a rate over wall-clock-since-start reads \
         eviction's correct idle time between passes as falling behind.",
        before.active_micros_total,
        after.active_micros_total
    );

    // And the collector publishes them. Every term below comes from ONE response, so a concurrent
    // credit in another test thread cannot fall between two reads.
    let mut metrics_manager = MetricsManager::new();
    metrics_manager.set_cache_manager(Arc::clone(&cache_manager));
    let metrics = metrics_manager.collect_metrics().await;
    let rates = metrics
        .cache
        .as_ref()
        .expect("cache metrics must be populated with a cache manager wired")
        .eviction_rates;

    assert!(
        rates.eviction_active_micros_total > 0,
        "the collector must read the shed accumulator onto the payload. A zero here with the \
         deltas above passing means the accumulator is written and never published — the same \
         shape as the two populated-but-unserialised figures already found on this endpoint."
    );
    let active_secs = rates.eviction_active_micros_total as f64 / 1_000_000.0;
    assert_eq!(
        rates.eviction_shed_bytes_per_sec,
        Some(rates.eviction_shed_bytes_total as f64 / active_secs),
        "the published rate must be the quotient of the published counters, or a consumer cannot \
         audit it and cannot compute a fleet rate by summing across proxies"
    );
    assert_eq!(
        rates.eviction_shed_ranges_per_sec,
        Some(rates.eviction_shed_ranges_total as f64 / active_secs)
    );

    // The backlog is signed and is measured against the configured trigger, not against the bound.
    // 95% of 1,000,000 is 950,000, and the pass drove the cache to the 80% target, so the healthy
    // reading here is negative.
    let backlog = rates
        .eviction_backlog_bytes
        .expect("a configured max_cache_size gives a backlog");
    assert!(
        backlog < 0,
        "after a pass that reached its target the cache is below its trigger, so the backlog must \
         be NEGATIVE — that is the healthy reading, and it is why the field is signed. Got \
         {backlog}"
    );
}

/// A real range credit moves the admission accumulator; a deduplicated repeat of the same range
/// does not.
///
/// # Both arms are load-bearing
///
/// The first alone would pass against an instrumentation that counted every call, including the
/// duplicate writes a stampede produces on shared storage — which would over-report the ingest
/// rate and make `keeping_pace` read `false` on a healthy cache. The second alone would pass
/// against an instrumentation that counted nothing at all.
///
/// `SizeAccumulator::add_range` is the site under test because it is the single production credit
/// path for the accounted total, reached from four callers — `CacheManager::credit_staged_range`,
/// `DiskCacheManager::store_range`'s hybrid arm, `DiskCacheManager::commit_incremental_range`, and
/// `JournalConsolidator::write_multipart_journal_entries` — so it counts the same population
/// eviction debits, and cannot drift from those four call sites the way a per-caller counter
/// could. The fourth was missed on a first reading of the tree, which is the argument for
/// instrumenting the callee rather than the callers.
#[tokio::test]
async fn a_real_range_credit_moves_the_admission_accumulator_and_a_duplicate_does_not() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();
    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.clone(),
        "admission-wiring".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.clone(),
        Duration::from_secs(30),
        3,
    ));
    let consolidator = JournalConsolidator::new(
        cache_dir,
        journal_manager,
        lock_manager,
        ConsolidationConfig::default(),
    );

    const KEY: &str = "test-bucket/admission-wiring.bin";
    const BYTES: u64 = 4_096;

    let before = collect_admission_accumulator();
    let credited = consolidator
        .size_accumulator()
        .add_range(KEY, 0, BYTES - 1, BYTES);
    assert!(
        credited,
        "fixture: the first credit of a fresh range applies"
    );
    let after_first = collect_admission_accumulator();

    assert_eq!(
        after_first.admission_bytes_total - before.admission_bytes_total,
        BYTES,
        "the production credit path must credit the admission numerator by the range's \
         compressed_size. Without it, R12.3's rate has nothing to race and `keeping_pace` is a \
         verdict on one figure."
    );
    assert_eq!(
        after_first.admission_ranges_total - before.admission_ranges_total,
        1,
        "and exactly one RANGE, which is the unit the field is named for"
    );

    // The dedup arm: another writer already holds these bytes in the accounted total on this
    // volume, so this is not an arrival and must contribute nothing.
    let credited_again = consolidator
        .size_accumulator()
        .add_range(KEY, 0, BYTES - 1, BYTES);
    assert!(
        !credited_again,
        "fixture: a repeat of the same (key, start, end) must be deduplicated, or this arm is \
         testing the wrong branch"
    );
    let after_second = collect_admission_accumulator();
    assert_eq!(
        after_second.admission_bytes_total, after_first.admission_bytes_total,
        "a deduplicated write is not an admission: the bytes are already in the accounted total, \
         and counting them again would over-report ingest and report a healthy cache as falling \
         behind"
    );
    assert_eq!(
        after_second.admission_ranges_total,
        after_first.admission_ranges_total
    );
}
