//! `/metrics`' `cache_hit_buffer` section, asserted at the PRODUCTION end: the figures MOVE.
//!
//! # Why this file exists beside the wire-shape test
//!
//! `tests/metrics_wire_shape_test.rs` builds a `SystemMetrics` literal and asserts the five names
//! and their values reach the wire. It would pass identically against a build whose collector never
//! read the buffer at all — which is exactly the defect this section was added to fix, one level
//! down: `CacheHitUpdateBuffer` computed all four figures and published none of them, because
//! `get_stats()` and `buffer_len()` had no production consumer and `FlushResult`'s per-flush
//! `errors` went to a `debug!` line and was dropped.
//!
//! So a field added with no test that can fail is the `tee_backpressure_*` shape: present,
//! readable, and evidence of nothing. Every assertion below drives a real
//! `CacheHitUpdateBuffer` — the one a real `CacheManager` built — and reads the figure back through
//! `MetricsManager::collect_metrics`, so a failure implicates either the buffer's accounting or the
//! collector, and not serde.
//!
//! # Each figure's red side
//!
//! Taken and recorded verbatim in
//! the investigation record: the flush's
//! `entries_flushed` credit removed, its `flush_errors` credit removed, the gauge hardcoded to
//! zero, and the collector unwired from the cache manager. Four one-hunk defects, four distinct
//! failures.
//!
//! # These figures are non-zero on a running proxy, which is the other half of the point
//!
//! `updates_recorded_total` moves on any cache-hit traffic:
//! `DiskCacheManager::refresh_object_ttl` (three `http_proxy` call sites, on the post-`304` TTL
//! refresh) and `record_range_access`/`update_range_access` (one `range_handler` and one
//! `http_proxy` call site, on range cache hits) are the producers, and each routes into the buffer
//! whenever one is configured. `main.rs` spawns a 5 s flush task unconditionally when the cache
//! manager has a buffer, so `entries_flushed_total` follows within one interval. This file drives
//! both producers rather than reaching past them.
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::metrics::{CacheHitBufferMetrics, MetricsManager};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

const KEY: &str = "test-bucket/cache-hit-buffer-object";

/// A cache manager with its journal components built, which is what creates the buffer.
async fn make_cache_manager(dir: &std::path::Path) -> Arc<CacheManager> {
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        dir.to_path_buf(),
        false,              // ram_cache_enabled — irrelevant here
        0,                  // max_ram_cache_size
        1024 * 1024 * 1024, // max_cache_size
        CacheEvictionAlgorithm::TinyLFU,
        1024,                             // compression_threshold
        true,                             // compression_enabled
        Duration::from_secs(315_360_000), // get_ttl
        Duration::from_secs(3600),        // head_ttl
        Duration::from_secs(3600),        // put_ttl
        false,                            // actively_remove_cached_data
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
    ));
    // Building the disk cache manager is what constructs and publishes the journal components,
    // the cache-hit update buffer among them. Without this the section would correctly be `null`
    // and every assertion below would be about an absent buffer.
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();
    cache_manager.initialize().await.expect("cache init");
    cache_manager
}

/// Collect one `/metrics` response and return the `cache_hit_buffer` section, failing loudly if it
/// is `null` — which means the collector is not reading the buffer.
async fn published_section(manager: &MetricsManager, label: &str) -> CacheHitBufferMetrics {
    manager
        .collect_metrics()
        .await
        .cache_hit_buffer
        .unwrap_or_else(|| {
            panic!(
                "`cache_hit_buffer` is null {label}, but this cache manager has a buffer. The \
                 collector is not reading it, so all five figures are unpublished however \
                 correctly the buffer accounts for them — the defect this section exists to fix."
            )
        })
}

/// The five published figures move with real cache-hit traffic, in the directions they claim.
///
/// # Both producers, not one
///
/// `record_ttl_refresh` and `record_access_update` are separate entry points with separate
/// production callers, and each builds a different `JournalOperation`. Driving only one would leave
/// the other's contribution to `updates_recorded_total` unasserted.
///
/// # The invariant, not just the counters
///
/// `updates_recorded_total` and `entries_flushed_total` count updates; `pending_updates` counts
/// distinct buffered entries, and repeated hits on one stored range inside a flush window fold
/// into one entry. So in general
/// `pending_updates + entries_flushed_total <= updates_recorded_total` while no flush has failed
/// (and a flush in progress holds updates in neither term), with equality exactly when no two
/// recorded updates have coalesced. No figure counts the coalesced-away updates. This test
/// records one TTL refresh and one access update, which are different kinds and never fold, so it
/// asserts the equality. Asserting the identity as well as the three figures is worth the line: a
/// collector that filled all five fields from one figure would satisfy any single counter
/// assertion and cannot satisfy this one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_published_figures_move_with_real_cache_hit_traffic() {
    let tmp = TempDir::new().expect("tempdir");
    let cache_manager = make_cache_manager(tmp.path()).await;
    let mut metrics_manager = MetricsManager::new();
    metrics_manager.set_cache_manager(Arc::clone(&cache_manager));

    let buffer = cache_manager
        .get_cache_hit_update_buffer()
        .await
        .expect("PRECONDITION MISSING: the cache manager built no cache-hit update buffer");

    // BASELINE. A fresh buffer, so all five are zero — and the section is `Some`, which is what
    // distinguishes these zeroes from the "no buffer" case that publishes `null`.
    let before = published_section(&metrics_manager, "on a freshly built buffer").await;
    assert_eq!(
        before,
        CacheHitBufferMetrics::default(),
        "PRECONDITION MISSING: a freshly built buffer must publish five zeroes, or the deltas \
         below are measured against unknown state. Got {before:?}"
    );

    buffer
        .record_ttl_refresh(KEY, Duration::from_secs(3600))
        .await
        .expect("record a TTL refresh, as the post-304 path does");
    buffer
        .record_access_update(KEY, 0, 1023, 1)
        .await
        .expect("record a range access, as a range cache hit does");

    let buffered = published_section(&metrics_manager, "after two recorded updates").await;
    assert_eq!(
        buffered.updates_recorded_total, 2,
        "two updates were accepted into the buffer and `updates_recorded_total` reports {}. This \
         counter is the buffer's own `updates_recorded` atomic and is unaffected by flush timing.",
        buffered.updates_recorded_total
    );
    assert_eq!(
        buffered.pending_updates, 2,
        "`pending_updates` reports {} for a buffer holding two unflushed entries. This is the \
         `pending_disk_updates` queue depth. PRECONDITION, stated because it is the only timing \
         assumption here: the buffer's default flush interval is 5 s and `record_*` flushes early \
         only once that has elapsed, so a nonzero-but-smaller reading means this process stalled \
         for five seconds between the two statements above rather than that the gauge is wrong.",
        buffered.pending_updates
    );
    assert_eq!(
        buffered.entries_flushed_total, 0,
        "nothing has been flushed yet, so `entries_flushed_total` must still be 0, not {}. A \
         nonzero reading here would mean the gauge and the counter are being filled from the same \
         figure.",
        buffered.entries_flushed_total
    );
    assert_eq!(
        buffered.pending_updates + buffered.entries_flushed_total,
        buffered.updates_recorded_total,
        "with no coalescing, every accepted update must be either pending or flushed: {:?}",
        buffered
    );

    let result = buffer.force_flush().await.expect("flush to the journal");
    assert_eq!(
        (result.entries_flushed, result.errors),
        (2, 0),
        "PRECONDITION MISSING: the flush itself did not write both entries cleanly, so the \
         published counters below would be measuring a failed flush rather than a successful one"
    );

    let flushed = published_section(&metrics_manager, "after a successful flush").await;
    assert_eq!(
        flushed.entries_flushed_total, 2,
        "`entries_flushed_total` reports {} after a flush that wrote 2 entries. This is the figure \
         an operator reads against `updates_recorded_total` to see whether the write-behind is \
         keeping up; a counter stuck at 0 while updates accumulate is indistinguishable from a \
         dead flush task.",
        flushed.entries_flushed_total
    );
    assert_eq!(
        flushed.pending_updates, 0,
        "the buffer was drained, so `pending_updates` must fall back to 0, not {}",
        flushed.pending_updates
    );
    assert_eq!(
        flushed.updates_recorded_total, 2,
        "`updates_recorded_total` is cumulative and must not fall on a flush"
    );
    assert_eq!(
        flushed.flush_errors_total, 0,
        "a clean flush must leave `flush_errors_total` at 0, not {}",
        flushed.flush_errors_total
    );
    assert_eq!(
        flushed.pending_updates + flushed.entries_flushed_total,
        flushed.updates_recorded_total,
        "every accepted update must be either pending or flushed: {flushed:?}"
    );
}

/// A flush that cannot write its journal moves `flush_errors_total`.
///
/// # Why this arm is the one worth having
///
/// `flush_errors_total` reads 0 on a healthy proxy, which is the shape this repository has been
/// bitten by — a published figure that is always zero is evidence of nothing. It earns its place on
/// two conditions, and this test is the second: the sibling counters in the same section move, so a
/// reader can tell the section is wired; and the non-zero path is reachable and demonstrated here
/// rather than asserted. Without this arm, an `errors` accumulator that was never credited would
/// look identical to a proxy whose flushes all succeed.
///
/// # The fixture
///
/// A **directory** at the journal file's own path. `flush` creates the parent, then opens the file
/// for append; the open fails with `EISDIR`, so the entries it had already serialised are counted
/// as errors instead of as writes. That exercises the real accounting arm — no injection seam, no
/// stub — and it does not need the flush to return `Err`, which is the point: this failure mode
/// returns `Ok` with a non-zero `errors`, so nothing upstream notices.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flush_that_cannot_write_its_journal_moves_the_published_error_counter() {
    let tmp = TempDir::new().expect("tempdir");
    let cache_manager = make_cache_manager(tmp.path()).await;
    let mut metrics_manager = MetricsManager::new();
    metrics_manager.set_cache_manager(Arc::clone(&cache_manager));

    let buffer = cache_manager
        .get_cache_hit_update_buffer()
        .await
        .expect("PRECONDITION MISSING: the cache manager built no cache-hit update buffer");

    // Occupy the journal file's path with a directory, so the append-open cannot succeed.
    let journal_path = tmp
        .path()
        .join("metadata")
        .join("_journals")
        .join(format!("{}.journal", buffer.instance_id()));
    std::fs::create_dir_all(&journal_path).expect("plant a directory at the journal file's path");
    assert!(
        journal_path.is_dir(),
        "PRECONDITION MISSING: {journal_path:?} is not a directory, so the flush below may well \
         succeed and this test would assert nothing"
    );

    buffer
        .record_access_update(KEY, 0, 1023, 1)
        .await
        .expect("record a range access");

    let result = buffer.force_flush().await.expect(
        "this failure mode returns Ok with a non-zero `errors` — an Err here means the fixture is \
         exercising the directory-creation arm instead of the file-open arm",
    );
    assert_eq!(
        (result.entries_flushed, result.errors),
        (0, 1),
        "PRECONDITION MISSING: the flush did not fail the way this fixture intends. Expected 0 \
         written and 1 error; got {result:?}"
    );

    let published = published_section(&metrics_manager, "after a failed flush").await;
    assert_eq!(
        published.flush_errors_total, 1,
        "`flush_errors_total` reports {} after a flush that lost one entry. This is the figure the \
         deleted `batch_flush_errors` documentation told operators to alert on, and it is \
         load-bearing: the lost entries carry the TTL refreshes and access counts eviction ranks \
         candidates by, so losing them silently skews eviction toward objects that are in fact hot.",
        published.flush_errors_total
    );
    assert_eq!(
        published.entries_flushed_total, 0,
        "nothing reached the journal, so `entries_flushed_total` must stay at 0, not {}. A flush \
         that credits both counters for the same entry would make the two figures unusable against \
         each other.",
        published.entries_flushed_total
    );
    assert_eq!(
        published.updates_recorded_total, 1,
        "the update was accepted into the buffer, so `updates_recorded_total` must report it \
         whatever became of the flush"
    );
}
