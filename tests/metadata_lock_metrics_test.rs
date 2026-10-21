//! The metadata slot lock's contention counters must have a production writer.
//!
//! Four figures are published for the per-key metadata slot lock — acquisitions granted,
//! acquisition attempts refused, callers that exhausted their retries, and cumulative hold
//! time. Every one of them was permanently zero: nothing outside `metrics.rs` called the
//! recorders, and the retry-exhaustion counter's only caller had been deleted along with the
//! function that housed it. A contention counter that cannot move is worse than no counter,
//! because `docs/SHARED_STORAGE.md` tells an operator to act on it.
//!
//! # What these assertions read, and why it is not the atomic
//!
//! Every assertion reads the **published field** out of `MetricsManager::collect_metrics()`,
//! which is the same path `/metrics` serves. Asserting the module's atomic directly would
//! pass even if the collector read a different counter — the failure class this suite has hit
//! four times, most recently where an assertion read a figure adjacent to the one the code
//! branched on.
//!
//! # Why one test function
//!
//! The counters are process-wide `AtomicU64`s, so two `#[test]` functions in this binary would
//! run on separate threads against shared state and pollute each other's deltas. One
//! sequential function, and every assertion is a **delta** rather than an absolute, so a
//! background acquisition elsewhere in the process cannot make a passing run vacuous.
//!
//! # Red side
//!
//! Remove any one increment in `metadata_lock_manager.rs` and the corresponding delta goes to
//! zero while the operation still demonstrably runs — the guard is returned, the refusal is
//! returned, the `.meta` is written. That is exactly the shipped defect, which is why the
//! assertion messages name the increment site rather than only the field.

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::metadata_lock_manager::{try_acquire_slot_lock, MetadataLockManager};
use s3_proxy::metrics::{AtomicMetadataMetrics, MetricsManager};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// A cache manager attached only so that the `cache` section is published at all —
/// `SystemMetrics::cache` is `None` without one, and `metadata_lock_retry_exhausted_total`
/// lives there because that is the field the `cache.metadata_lock_retry_exhausted_total`
/// OTLP gauge reads.
///
/// Deliberately NOT initialized: `initialize()` starts the size-tracker validation task,
/// whose census walks the cache directory, and a background task acquiring slot locks would
/// pollute the exact-equality deltas below.
fn uninitialized_cache_manager(temp_dir: &TempDir) -> Arc<CacheManager> {
    Arc::new(CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,                   // RAM cache disabled — irrelevant here
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
    ))
}

/// The four published metadata-lock figures, read the way `/metrics` reads them.
struct Published {
    successful: u64,
    failed: u64,
    retry_exhausted: u64,
    hold_time_ms_total: u64,
}

async fn published(metrics: &MetricsManager) -> Published {
    let collected = metrics.collect_metrics().await;
    let atomic: AtomicMetadataMetrics = collected
        .atomic_metadata
        .expect("the atomic_metadata section must be present in the published payload");
    let cache = collected
        .cache
        .expect("the cache section must be present in the published payload");
    Published {
        successful: atomic.metadata_lock_acquisitions_successful,
        failed: atomic.metadata_lock_acquisitions_failed,
        retry_exhausted: cache.metadata_lock_retry_exhausted_total,
        hold_time_ms_total: atomic.metadata_lock_hold_time_ms_total,
    }
}

#[tokio::test]
async fn the_metadata_lock_counters_move_when_the_lock_is_actually_used() {
    let temp = TempDir::new().expect("tempdir");
    let cache_dir = temp.path().to_path_buf();
    let mut metrics = MetricsManager::new();
    metrics.set_cache_manager(uninitialized_cache_manager(&temp));
    let metrics = metrics;
    let cache_key = "test-bucket/metadata-lock-metrics.bin";

    // ---- 1. A GRANTED acquisition increments the success counter -------------------------
    //
    // Site: `try_acquire_slot_lock`'s `Ok(())` arm. Without that increment the guard is
    // still returned, so nothing else about this block changes and the delta is 0.
    let before = published(&metrics).await;
    let holder = try_acquire_slot_lock(&cache_dir, cache_key)
        .expect("fixture: an uncontended slot must be grantable");
    let after_grant = published(&metrics).await;
    assert_eq!(
        after_grant.successful - before.successful,
        1,
        "a granted slot-lock acquisition did not move the published \
         atomic_metadata.metadata_lock_acquisitions_successful \
         (before={}, after={}). The increment in try_acquire_slot_lock's Ok arm is missing, \
         or the collector reads a different counter than the one it increments.",
        before.successful,
        after_grant.successful
    );

    // ---- 2. A REFUSED attempt increments the failure counter -----------------------------
    //
    // Real contention, not a simulated refusal: `holder` is live, and `flock` conflicts
    // between two descriptors of one file even inside a single process, which is the
    // property `metadata_lock_manager`'s own nesting test asserts. An uncontended fixture
    // could never reach this arm.
    //
    // Site: `try_acquire_slot_lock`'s `WouldBlock` arm.
    let contended = try_acquire_slot_lock(&cache_dir, cache_key);
    assert!(
        contended.is_err(),
        "fixture: the second acquisition must be REFUSED while the first guard is alive, \
         or nothing below measures contention"
    );
    let after_refusal = published(&metrics).await;
    assert_eq!(
        after_refusal.failed - after_grant.failed,
        1,
        "a refused slot-lock attempt did not move the published \
         atomic_metadata.metadata_lock_acquisitions_failed (before={}, after={}). \
         The increment in try_acquire_slot_lock's WouldBlock arm is missing.",
        after_grant.failed,
        after_refusal.failed
    );

    // ---- 3. RETRY EXHAUSTION increments its own counter, and is not a timeout ------------
    //
    // `max_retries = 0`, so the manager makes one attempt and gives up at
    // `attempt == self.max_retries`. Nothing here waits on a clock: what bounds the caller
    // is the retry count, which is why the counter is named for exhaustion rather than for
    // a timeout the protocol does not have. The `Duration` below is the discarded
    // `lock_timeout` parameter and has no effect — passing zero would behave identically.
    //
    // Site: `MetadataLockManager::acquire_lock`'s max-retries arm.
    let manager = MetadataLockManager::new(cache_dir.clone(), Duration::from_secs(60), 0);
    let exhausted = manager.acquire_lock(cache_key).await;
    assert!(
        exhausted.is_err(),
        "fixture: acquire_lock must FAIL while the slot is held, or exhaustion never happens"
    );
    let after_exhaustion = published(&metrics).await;
    assert_eq!(
        after_exhaustion.retry_exhausted - after_refusal.retry_exhausted,
        1,
        "retry exhaustion did not move the published \
         cache.metadata_lock_retry_exhausted_total (before={}, after={}). This is the \
         counter whose only writer was deleted with acquire_lock_with_retry; the increment \
         belongs in acquire_lock's `attempt == self.max_retries` arm.",
        after_refusal.retry_exhausted,
        after_exhaustion.retry_exhausted
    );
    // Attempts are counted individually and giving up is counted once, so the refused
    // attempt inside acquire_lock also lands on the failure counter. Asserting both keeps
    // the two counters distinguishable: a single increment serving both would fail here.
    assert_eq!(
        after_exhaustion.failed - after_refusal.failed,
        1,
        "the attempt acquire_lock made before giving up was not counted as a refused \
         attempt (before={}, after={}). failed counts ATTEMPTS and retry_exhausted counts \
         CALLERS; if these two are wired to one increment the distinction is lost.",
        after_refusal.failed,
        after_exhaustion.failed
    );

    // ---- 4. HOLD TIME accumulates when the guard is released ----------------------------
    //
    // The guard's lifetime IS the hold, because `Drop` is what releases the lock. Hold a
    // fresh slot for a measurable interval, drop it, and the cumulative sum must have moved
    // by at least that interval. `>=` rather than `==`: the published figure is milliseconds
    // derived from a microsecond accumulator, so it can round down by up to 1ms, and the
    // release above contributes too.
    //
    // Site: `impl Drop for MetadataLock`.
    drop(holder);
    let before_hold = published(&metrics).await;
    let timed = try_acquire_slot_lock(&cache_dir, "test-bucket/metadata-lock-hold-time.bin")
        .expect("fixture: a distinct key's slot must be grantable");
    tokio::time::sleep(Duration::from_millis(60)).await;
    drop(timed);
    let after_hold = published(&metrics).await;
    assert!(
        after_hold.hold_time_ms_total - before_hold.hold_time_ms_total >= 55,
        "a 60ms slot-lock hold added {}ms to the published \
         atomic_metadata.metadata_lock_hold_time_ms_total (before={}, after={}), expected at \
         least 55ms. The accumulation in MetadataLock::drop is missing, or it reads a \
         timestamp other than acquired_at.",
        after_hold.hold_time_ms_total - before_hold.hold_time_ms_total,
        before_hold.hold_time_ms_total,
        after_hold.hold_time_ms_total
    );
}
