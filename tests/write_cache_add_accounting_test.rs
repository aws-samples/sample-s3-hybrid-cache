//! Write-through PUT paths must credit BOTH size channels.
//!
//! # The defect these tests cover
//!
//! Both single-PUT write-cache paths publish their `.bin` through
//! `WriteCacheRangeSink::finalize` and then write the `.meta` **directly** via
//! `store_new_metadata`, deliberately bypassing the journal so an immediate
//! post-PUT GET is a cache hit. That is correct for read-after-write, and it
//! silently removed both paths from every accounting mechanism:
//!
//! * `finalize_incremental_range` documents that it performs no size tracking;
//! * `store_new_metadata` writes no journal entry;
//! * `consolidate_key` hardcodes its own `size_delta` to 0, contributing only
//!   graduation's negative `write_cache_delta`.
//!
//! So a write-through PUT credited neither `total_size` nor `write_cache_size`.
//!
//! Measured on the verification fleet on 2026-08-25, before the fix: 1,594
//! accumulator flushes since 2026-05-20, 1,566 of them `write_cache_delta=+0`,
//! **9** positive write-cache credits in total against **1,535** `committed
//! write-cached range` log lines. The 9 came from the paths that DO journal
//! (`CompleteMultipartUpload` and `store_full_object_as_range_new`, both via
//! `JournalConsolidator::write_multipart_journal_entries`).
//!
//! Consequence, and why it outranks "a gauge reads low": with R6 grounding
//! `write_cache_size` from the `.meta` files on disk, the figure returned to ~0 and
//! stayed there, so `write_cache_percent` bounded nothing at all — while eviction
//! and invalidation kept debiting it, saturating at zero and driving the accounting
//! toward undershoot, the direction that over-admits rather than refusing.
//!
//! # Shown failing first
//!
//! Against the pre-fix tree both `write_cache` assertions below fail with
//! `left: 0`, and the `total` assertions fail too. Neither figure moved because
//! nothing on the path touched the accumulator.
//!
//! The staging-flag guard is covered separately by the `credit_staged_range` unit
//! tests in `src/cache.rs`: this path hardcodes `is_write_cached: true` when it
//! builds its `ObjectMetadata`, so the `false` case is not reachable from here and
//! asserting it through this door would prove nothing.
//!
//! Spec: write-cache-accounting-and-eviction. Requirements: 1.1, 6.2

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;

/// Payload size for each PUT. Small enough to keep the tests fast, large enough
/// that a credit of the wrong figure (e.g. uncompressed vs compressed) is visible.
const BODY_LEN: usize = 64 * 1024;

/// Build a `CacheManager` whose journal consolidator is wired, which is what
/// `credit_staged_range` needs to reach the accumulator.
///
/// `create_configured_disk_cache_manager()` is the call that constructs and stores
/// the consolidator (see `src/cache.rs`, the `journal_consolidator.try_write()`
/// block), so it must run before the write path is exercised — exactly as
/// `CacheManager::initialize` arranges in production.
fn setup(cache_dir: &std::path::Path) -> CacheManager {
    for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
        std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
    }

    let manager = CacheManager::new_with_eviction_algorithm(
        cache_dir.to_path_buf(),
        false, // ram_cache_enabled — irrelevant here, and off keeps the test cheap
        0,
        CacheEvictionAlgorithm::LRU,
    );

    // Wires the journal consolidator into the manager as a side effect.
    let _ = manager.create_configured_disk_cache_manager();

    manager
}

async fn accumulator_deltas(manager: &CacheManager) -> (i64, i64) {
    let consolidator = manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator should be wired by create_configured_disk_cache_manager");
    let accumulator = consolidator.size_accumulator();
    (
        accumulator.current_delta(),
        accumulator.current_write_cache_delta(),
    )
}

async fn put_write_cached(manager: &CacheManager, cache_key: &str, body: &[u8]) {
    manager
        .store_put_as_write_cached_range_with_ttl(
            cache_key,
            body,
            "\"write-cache-add-test\"".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            Some("application/octet-stream".to_string()),
            HashMap::new(),
            Duration::from_secs(86_400),
        )
        .await
        .expect("write-cache store should succeed");
}

/// The buffered write-through PUT path credits both channels.
///
/// `store_put_as_write_cached_range_with_ttl` is the whole-body path. It reaches
/// `WriteCacheRangeSink::finalize` and then writes its own `.meta`, so it is one of
/// the two paths that credited nothing.
#[tokio::test]
async fn buffered_write_through_put_credits_total_and_write_cache() {
    let temp = TempDir::new().unwrap();
    let manager = setup(temp.path());
    let body = vec![7u8; BODY_LEN];

    let (total_before, wc_before) = accumulator_deltas(&manager).await;
    assert_eq!(
        (total_before, wc_before),
        (0, 0),
        "accumulator should start clean"
    );

    put_write_cached(&manager, "test-bucket/buffered-write-through.bin", &body).await;

    let (total_after, wc_after) = accumulator_deltas(&manager).await;

    assert!(
        total_after > 0,
        "total_size delta must move for a write-through PUT (got {total_after})"
    );
    // The headline assertion: this is the figure that read +0 on the fleet for
    // 1,535 consecutive staged commits.
    assert!(
        wc_after > 0,
        "write_cache_size delta must move for a write-through PUT (got {wc_after}) — \
         this is the add-side gap found at checkpoint 2"
    );
    // The staged bytes are a SUBSET of the total, never additional to it, so the two
    // credits must describe the same bytes. Asserting equality catches a future
    // change that credits one channel from `compressed_size` and the other from an
    // on-disk `len()` or the uncompressed size — a drift that would otherwise only
    // surface as a slow divergence on the fleet.
    assert_eq!(
        total_after, wc_after,
        "a fully staged object's write-cache credit must equal its total credit"
    );
}

/// A range whose `.bin` already exists is credited **once**, not twice.
///
/// This is the cross-instance over-count guard: on a shared volume another proxy
/// may have already published and credited the identical range. `store_range` and
/// `commit_incremental_range` both gate their credits on `!range_already_existed`,
/// and the write-cache paths now do too — the flag used to be discarded by
/// `finalize`, which is how the credit came to be missing in the first place.
///
/// Note this asserts on the WRITE-CACHE channel specifically. `add_range` dedups
/// internally on `(key_hash, start, end)`, so the total is protected even without
/// the guard; `add_write_cache` has no dedup of its own, so it is the channel that
/// would actually double-count.
#[tokio::test]
async fn re_put_of_the_same_range_does_not_double_credit_write_cache() {
    let temp = TempDir::new().unwrap();
    let manager = setup(temp.path());
    let cache_key = "test-bucket/re-put-same-range.bin";
    let body = vec![3u8; BODY_LEN];

    put_write_cached(&manager, cache_key, &body).await;
    let (total_after_first, wc_after_first) = accumulator_deltas(&manager).await;
    assert!(
        wc_after_first > 0,
        "first PUT must credit the write-cache channel"
    );

    // Same key, same length, so the same `.bin` path. The PUT invalidates the
    // previous entry first (which debits), so the NET figure is what matters rather
    // than the raw credit count.
    put_write_cached(&manager, cache_key, &body).await;
    let (total_after_second, wc_after_second) = accumulator_deltas(&manager).await;

    assert_eq!(
        wc_after_second, wc_after_first,
        "re-PUTting the same range must leave the write-cache figure at one copy of \
         the bytes, not two (invalidate debits, re-publish credits)"
    );
    // Both channels or neither. Asserting only the write-cache figure is what let the
    // total's own asymmetry hide: `add_range` dedups on `(key, start, end)` while
    // `subtract` does not, so adding the debit without a matching dedup-aware
    // subtract drives the total to ZERO across a re-PUT — an undershoot swapped in
    // for the overshoot. The staged bytes are a subset of the total, so a re-PUT that
    // leaves one copy in one channel must leave one copy in the other.
    assert_eq!(
        total_after_second, total_after_first,
        "re-PUTting the same range must leave the total figure at one copy of the \
         bytes too — not zero, and not two"
    );
}
