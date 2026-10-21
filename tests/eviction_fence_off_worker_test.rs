//! The per-object eviction fence read must not serialise the concurrent eviction region.
//!
//! **What this asserts, and why it is a width assertion rather than a duration one.**
//! `CacheManager::verify_eviction_fence` is consulted once per object by
//! `perform_eviction_with_lock`, and the eviction region dispatches leaf groups with
//! `futures::StreamExt::buffer_unordered` — it does **not** `tokio::spawn` them. Every
//! in-flight leaf group is therefore polled by the single task awaiting that stream, so while
//! the fence read was synchronous `std::fs` it blocked that one task: the fence reads
//! serialised at width **1** and head-of-line blocked the whole region, whatever
//! `LEAF_CONCURRENCY_LIMIT` or the runtime's worker count happened to be. The claim under
//! test is that the fence call is now a yield point, so several of them can be in flight at
//! once.
//!
//! A pass-duration figure is the adjacent measurement and is deliberately not used: the
//! mutation region is a small share of a real eviction pass, so the pass can stay flat while
//! this works, and can move for reasons unrelated to it.
//!
//! **The discriminator is a LITERAL floor of 2, not a comparison against
//! `LEAF_CONCURRENCY_LIMIT`.** An assertion phrased in the constant would be satisfied at 1
//! exactly as at 32. 1 is the value the synchronous shape produced, so 2 is the smallest
//! reading that falsifies it, and it stays falsifying if the constant is ever changed.
//!
//! **Why the bounded retry does not weaken the red side.** Under a synchronous fence the
//! increment/decrement pair around the call cannot overlap with another future's — there is no
//! await between them, so the poller cannot interleave and the peak is **structurally** 1 on
//! every attempt. Retrying can therefore only rescue a green run whose `spawn_blocking` tasks
//! happened to complete before the next future was polled; it cannot turn a red side green.
//!
//! **What this does NOT prove.** It does not measure eviction throughput, it does not exercise
//! `perform_eviction_with_lock` (private), and it says nothing about shared-storage latency —
//! the temp dir here is local. It proves the fence call yields, which is the property the
//! region's achievable width depends on.
//!
//! Left on the default `#[tokio::test]` current-thread runtime on purpose. With one worker
//! thread, a reading above 1 cannot be explained by worker-thread parallelism, which is the
//! mechanism the fence's own doc comment used to (wrongly) name as the ceiling.

use futures::stream::{self, StreamExt};
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::SharedStorageConfig;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// How many fence calls to drive concurrently. Any value above the floor below works; 8 is
/// enough to make a single-threaded serialisation obvious without making the test slow.
const CONCURRENCY: usize = 8;

/// The retired behaviour's peak. See the module banner for why this is a literal.
const SERIALISED_PEAK: usize = 1;

/// Attempts allowed before failing. Sound only because the red side is structurally 1.
const ATTEMPTS: usize = 5;

fn create_cache_manager(cache_dir: std::path::PathBuf) -> CacheManager {
    CacheManager::new_with_shared_storage(
        cache_dir,
        false,
        0,
        1024 * 1024 * 1024,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig {
            eviction_lock_timeout: Duration::from_secs(300),
            ..Default::default()
        },
        10.0,
        false,
        Duration::from_secs(86400),
        s3_proxy::config::MetadataCacheConfig::default(),
        95,
        80,
        true,
        Duration::from_secs(60),
        1_048_576,
        false,
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    )
}

/// Drive `CONCURRENCY` fence calls through the production dispatch shape and report the peak
/// number simultaneously inside the call, plus whether every verdict was `Ok`.
async fn measure_peak_in_flight(cache_manager: &CacheManager) -> (usize, usize) {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let calls = (0..CONCURRENCY).map(|_| {
        let in_flight = Arc::clone(&in_flight);
        let peak = Arc::clone(&peak);
        async move {
            let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            let verdict = cache_manager.verify_eviction_fence().await;
            in_flight.fetch_sub(1, Ordering::SeqCst);
            verdict
        }
    });

    // Exactly how `run_leaf_groups` dispatches: one task polling every in-flight future.
    let verdicts: Vec<_> = stream::iter(calls)
        .buffer_unordered(CONCURRENCY)
        .collect()
        .await;

    let ok_count = verdicts.iter().filter(|v| v.is_ok()).count();
    (peak.load(Ordering::SeqCst), ok_count)
}

#[tokio::test]
async fn eviction_fence_reads_overlap_instead_of_serialising_the_region() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();
    std::fs::create_dir_all(cache_dir.join("locks")).expect("Failed to create locks directory");

    let cache_manager = create_cache_manager(cache_dir);
    assert!(
        cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Lock acquisition should not error"),
        "the fixture must hold the eviction lock, or every fence call fails for that reason \
         instead of measuring anything"
    );

    let mut best_peak = 0usize;
    let mut last_ok = 0usize;
    for _ in 0..ATTEMPTS {
        let (peak, ok_count) = measure_peak_in_flight(&cache_manager).await;
        best_peak = best_peak.max(peak);
        last_ok = ok_count;
        if best_peak > SERIALISED_PEAK {
            break;
        }
    }

    // The fence must still work — a call that overlaps but reports a lost fence would fail the
    // pass for the wrong reason, and a peak reading alone cannot tell the two apart.
    assert_eq!(
        last_ok, CONCURRENCY,
        "the lock is held by this instance, so all {} fence verifications must return Ok; \
         {} did",
        CONCURRENCY, last_ok
    );

    assert!(
        best_peak > SERIALISED_PEAK,
        "at most {} eviction fence verification(s) were ever in flight at once across {} \
         attempts of {} concurrent calls, which is the reading the retired synchronous \
         `std::fs::read_to_string` produced by construction: the fence read is back on the \
         polling task, so it head-of-line blocks the whole `buffer_unordered` eviction region \
         and no value of LEAF_CONCURRENCY_LIMIT can widen it",
        best_peak,
        ATTEMPTS,
        CONCURRENCY
    );

    cache_manager
        .release_global_eviction_lock()
        .await
        .expect("Should release lock");
}
