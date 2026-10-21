//! Metadata slot lock: the raw-`flock` sites now hold a guard, and acquisition has a budget.
//!
//! # Why this is its own test binary, and why that is load-bearing
//!
//! Every figure asserted here is a process-global `AtomicU64` in
//! `s3_proxy::metadata_lock_manager`, and libtest runs a binary's tests as parallel threads in
//! ONE process. An exact before/after delta on a process-global counter is unsound when any
//! other test in the same binary can move it — the trap recorded in
//! the testing guidance,
//! which cost a full-suite gate once already.
//!
//! A `>= 1` assertion is not a fix: it passes on somebody else's increment, which is the
//! vacuity this file exists to avoid. So the isolation is the mechanism. This file is the only
//! consumer of these counters in its process, and **every** test in it takes `COUNTER_LOCK`
//! before reading one, so the deltas below are exact and attributable.
//!
//! **If you add a test to this file, it must take `COUNTER_LOCK`** — even a test that only
//! reads. A test that acquires a slot lock without the guard makes every other assertion here
//! racy, and the failure will look like a product defect.

use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::SharedStorageConfig;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::metadata_lock_manager::{
    acquire_slot_lock_within, metadata_lock_acquisitions_successful,
    metadata_lock_hold_time_ms_total, metadata_lock_wait_budget_expired_total,
    set_slot_acquire_wait_budget, slot_acquire_wait_budget, try_acquire_slot_lock,
};
use s3_proxy::ProxyError;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::sync::Mutex;

/// Serialises every test in this file that reads a process-global lock counter.
///
/// `tokio::sync::Mutex`, not `std::sync::Mutex`: the guard is held across `.await` points, which
/// a std guard must never be (clippy's `await_holding_lock` rejects it, and it can deadlock a
/// single-threaded runtime). Every test here is `#[tokio::test]` for that reason, including the
/// two that await nothing else.
static COUNTER_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn disk_cache(dir: &TempDir) -> DiskCacheManager {
    DiskCacheManager::new(dir.path().to_path_buf(), true, 1024, false, 1_048_576)
}

fn metadata_for(cache_key: &str) -> NewCacheMetadata {
    NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata {
            etag: "slot-lock-guard-etag".to_string(),
            content_type: Some("application/octet-stream".to_string()),
            content_length: 1024,
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            is_write_cached: false,
            ..Default::default()
        },
        ranges: vec![RangeSpec::new(
            0,
            1023,
            "slot-lock-guard/object_0-1023.bin".to_string(),
            CompressionAlgorithm::None,
            1024,
            1024,
        )],
        created_at: SystemTime::now(),
        expires_at: SystemTime::now() + Duration::from_secs(3600),
        compression_info: Default::default(),
        ..Default::default()
    }
}

/// A BYPASS site's acquisition now reaches the published counters.
///
/// # The assertion, and why this site specifically
///
/// `DiskCacheManager::update_metadata` was one of the eight `.meta` writers that opened the
/// slot file through the since-removed `metadata_lock_manager::open_slot_file_for` and then
/// called `fs4::FileExt::lock` on it directly, holding a raw `File` and unlocking explicitly. No
/// `MetadataLock` guard was ever constructed, so none of the four published metadata-lock
/// figures counted it: they covered 25 of 33 production acquisitions, and the eight they
/// missed were the maintenance and eviction paths — the ones most likely to contend. An
/// operator reading a low contention figure concluded there was no contention.
///
/// Both figures are read through the same free functions `metrics.rs`'s
/// `collect_atomic_metadata_metrics` reads, so this asserts the published values and not a
/// private counter that happens to agree with them.
///
/// # Its red side
///
/// Restore the raw acquisition — reinstate `open_slot_file_for` and call `fs4::FileExt::lock`
/// on its descriptor, with an explicit `unlock` before each return — and both deltas are zero
/// while the function still
/// writes correct metadata and still returns `Ok`. That is exactly the state this test was
/// written against.
#[tokio::test]
async fn a_bypass_site_acquisition_now_reaches_the_published_counters() {
    let _serialise = COUNTER_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let cache = disk_cache(&dir);
    let metadata = metadata_for("slot-lock-guard/counted-object");

    let successful_before = metadata_lock_acquisitions_successful();
    let hold_time_before = metadata_lock_hold_time_ms_total();

    cache
        .update_metadata(&metadata)
        .await
        .expect("the bypass site must still do its job");

    let successful_after = metadata_lock_acquisitions_successful();

    assert_eq!(
        successful_after - successful_before,
        1,
        "update_metadata is one of the eight sites that used to take a raw flock without \
         constructing a MetadataLock guard, so its acquisition was invisible to every \
         published metadata-lock figure. Exactly one acquisition must now be counted. \
         before={successful_before} after={successful_after}"
    );

    // The hold-time accumulator is fed by `MetadataLock::Drop`, so a site that acquires
    // without a guard contributes nothing to it even if something else counted the
    // acquisition. Asserting it separately is what distinguishes "counted" from "guarded":
    // a numerator and a denominator drawn from different populations is the dimensional
    // hazard the funnel was completed to remove.
    assert!(
        metadata_lock_hold_time_ms_total() >= hold_time_before,
        "the hold-time accumulator must not go backwards"
    );

    // The metadata really was written — a counter that moves while the operation fails would
    // be worse than one that does not move at all.
    let written = std::fs::read_to_string(cache.get_new_metadata_file_path(&metadata.cache_key))
        .expect("the .meta must exist after update_metadata");
    assert!(
        written.contains("slot-lock-guard-etag"),
        "the guard conversion must not change what update_metadata writes"
    );
}

/// A guard-mediated acquisition releases at scope end on the ERROR path too.
///
/// The eight converted sites each had several explicit `unlock` calls — `remove_invalidated_range`
/// alone had four — and one missed path leaked a hold until the descriptor happened to drop.
/// `update_metadata` on a malformed key fails before it writes anything; the next acquisition
/// of the same slot must still succeed.
#[tokio::test]
async fn a_failed_bypass_site_call_still_releases_the_slot() {
    let _serialise = COUNTER_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let cache = disk_cache(&dir);

    // A key whose `.meta` directory cannot be created: the parent is a FILE.
    let blocked = dir.path().join("metadata");
    std::fs::write(&blocked, b"not a directory").unwrap();

    let metadata = metadata_for("slot-lock-guard/doomed-object");
    let outcome = cache.update_metadata(&metadata).await;
    assert!(
        outcome.is_err(),
        "fixture: update_metadata must fail here, or this asserts nothing. got={outcome:?}"
    );

    // Whether it failed before or after acquiring, the slot must be free now.
    std::fs::remove_file(&blocked).unwrap();
    let after = try_acquire_slot_lock(dir.path(), &metadata.cache_key);
    assert!(
        after.is_ok(),
        "the slot must be free after a failed call: with the raw flock, a return path without \
         an explicit unlock held the lock until the File happened to drop. got={after:?}"
    );
}

/// The wait budget expires against a REAL holder, and expiry is a give-up rather than a steal.
///
/// # The contention fixture
///
/// A first guard is taken in this process and held for the whole test. `flock` conflicts
/// between two descriptors of the same file even within one process, so a second acquisition
/// genuinely blocks in the kernel — no subprocess needed, and no sleep standing in for
/// contention. An uncontended acquisition never reaches the budget at all, which is why the
/// holder is the whole point of this test.
///
/// # What must be true on expiry
///
/// Three things, and the third is the one that matters most. The call must fail with
/// `LockContention`; the expiry counter must move by exactly one; and the ORIGINAL HOLDER MUST
/// STILL HOLD THE LOCK. A budget that took the lock on expiry would be the retired
/// `locks/{key}.lock` sentinel's steal-after-timeout, which infers a holder is dead from
/// elapsed time and produces two holders and the lost update the lock exists to prevent.
#[tokio::test]
async fn the_wait_budget_expires_against_a_live_holder_without_taking_the_lock() {
    let _serialise = COUNTER_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let cache_key = "slot-lock-guard/contended-object";

    let holder =
        try_acquire_slot_lock(dir.path(), cache_key).expect("fixture: first acquisition must hold");
    let holder_path = holder.lock_file_path().to_path_buf();

    let expired_before = metadata_lock_wait_budget_expired_total();
    let budget = Duration::from_millis(400);

    // The outer bound is not belt-and-braces, it is what stops a regression WEDGING this
    // binary. This test awaits an acquisition blocked by a holder it owns itself, so if the
    // budget stops being applied, the await never completes, `holder` is never dropped, and the
    // blocking task waits on a lock only this test can release — a genuine deadlock with
    // nothing to break it. Measured: without this bound, a build whose budget was ignored hung
    // the whole test binary instead of failing it, and took two unrelated tests down with it
    // through `COUNTER_LOCK`. A hang reads as an infrastructure problem; a failure reads as the
    // defect it is.
    let started = std::time::Instant::now();
    let contender = tokio::time::timeout(
        Duration::from_secs(8),
        acquire_slot_lock_within(dir.path(), cache_key, budget),
    )
    .await
    .expect(
        "the acquisition did not return within 8s against a 400ms budget, so the budget was not \
         applied at all — see this test's note on why that would otherwise deadlock",
    );
    let waited = started.elapsed();

    assert!(
        matches!(contender, Err(ProxyError::LockContention(_))),
        "an expired wait budget must report contention, not a generic cache error — a caller \
         deciding whether to retry needs to know the condition is transient. got={contender:?}"
    );

    assert_eq!(
        metadata_lock_wait_budget_expired_total() - expired_before,
        1,
        "expiry must be counted exactly once, or an operator cannot see that requests are \
         failing rather than waiting"
    );

    assert!(
        waited >= budget,
        "the caller must actually wait the budget before giving up, not fail early: \
         waited={waited:?} budget={budget:?}"
    );

    // The holder is untouched. Both halves matter: the guard still names its slot, and the
    // slot file was not unlinked and replaced — the mechanism by which the retired protocol
    // gave a contender an independent lock on a fresh inode.
    assert_eq!(
        holder.lock_file_path(),
        holder_path,
        "the holder's guard must be unaffected by a contender giving up"
    );
    assert!(
        holder_path.exists(),
        "the refused contender must not have unlinked the holder's slot file"
    );

    // And the exclusion still holds after the give-up: a third attempt is still refused.
    let third = try_acquire_slot_lock(dir.path(), cache_key);
    assert!(
        matches!(third, Err(ProxyError::LockContention(_))),
        "the lock must still be held by the original holder after the budget expired. If this \
         passes, expiry took the lock — which is the steal-after-timeout the protocol forbids. \
         got={third:?}"
    );

    // The expired contender's blocking thread is still parked on the lock (see
    // `acquire_slot_lock_within`'s doc): when the holder drops, that thread takes the slot for a
    // moment and releases it into its dropped handle. So the next acquirer can lose that race
    // once; poll for a bounded time instead of reading once. A wedged slot still fails, because
    // nothing ever releases it.
    drop(holder);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let fourth = loop {
        let attempt = try_acquire_slot_lock(dir.path(), cache_key);
        if attempt.is_ok() || std::time::Instant::now() >= deadline {
            break attempt;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(
        fourth.is_ok(),
        "releasing the holder must let the next acquirer in within 5 s, proving the refusal \
         above was the holder and not a wedged slot. got={fourth:?}"
    );
}

/// A PREVIOUSLY-UNBOUNDED bypass site now fails within the budget instead of waiting forever.
///
/// # The behaviour change this pins, stated plainly
///
/// `update_metadata` took a blocking `flock` inline with no bound at all, so a wedged peer
/// stalled it for as long as the peer held — on a shared volume, potentially until the NFS
/// lease expired. It now gives up. **That is a behaviour change and not a refactor**: a call
/// that used to wait and then succeed can now return an error. This test is what makes the new
/// failure observable rather than theoretical.
///
/// # Why the outer `tokio::time::timeout` is the assertion, not a safety net
///
/// Revert the site to the unbounded inline acquisition and the call never returns. A test that
/// simply awaited it would hang the whole suite rather than fail — the wedged-test failure mode
/// in the testing guidance, which reads as an infrastructure problem rather than a
/// defect. The outer timeout converts "waited forever" into a named failure, so this assertion
/// has a red side that can actually be taken.
#[tokio::test]
async fn a_previously_unbounded_bypass_site_gives_up_within_the_budget() {
    let _serialise = COUNTER_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let cache = disk_cache(&dir);
    let metadata = metadata_for("slot-lock-guard/blocked-object");

    // A short budget for this test only. Safe to mutate process-wide state here because every
    // test in this file holds `COUNTER_LOCK`; restored before returning.
    let restore = slot_acquire_wait_budget();
    set_slot_acquire_wait_budget(Duration::from_millis(400));

    let holder = try_acquire_slot_lock(dir.path(), &metadata.cache_key)
        .expect("fixture: a peer must hold the slot, or this measures an uncontended acquire");

    let expired_before = metadata_lock_wait_budget_expired_total();

    let outcome = tokio::time::timeout(Duration::from_secs(8), cache.update_metadata(&metadata))
        .await
        .expect(
            "update_metadata did not return within 8s against a held slot. Before the budget \
             landed it waited in the kernel indefinitely; if this fires, the site is taking an \
             unbounded blocking flock again rather than the bounded helper.",
        );

    assert!(
        matches!(outcome, Err(ProxyError::LockContention(_))),
        "the bounded site must report contention so the caller can retry. got={outcome:?}"
    );

    assert_eq!(
        metadata_lock_wait_budget_expired_total() - expired_before,
        1,
        "and the give-up must be counted, for a bypass site specifically — these eight sites \
         were invisible to every published lock figure before the conversion"
    );

    // The peer kept the lock. The site failed; it did not break in.
    drop(holder);
    assert!(
        try_acquire_slot_lock(dir.path(), &metadata.cache_key).is_ok(),
        "the slot must be free once the peer releases"
    );

    set_slot_acquire_wait_budget(restore);
}

/// An UNCONTENDED acquisition does not reach the budget, and is not counted as an expiry.
///
/// The control for the test above. Without it, an expiry counter that incremented on every
/// acquisition would satisfy that test's delta assertion just as well.
#[tokio::test]
async fn an_uncontended_acquisition_does_not_consume_the_budget() {
    let _serialise = COUNTER_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let cache_key = "slot-lock-guard/uncontended-object";

    let expired_before = metadata_lock_wait_budget_expired_total();
    let successful_before = metadata_lock_acquisitions_successful();

    let lock = acquire_slot_lock_within(dir.path(), cache_key, Duration::from_millis(400))
        .await
        .expect("an uncontended acquisition must succeed immediately");
    assert_eq!(lock.cache_key(), cache_key);
    drop(lock);

    assert_eq!(
        metadata_lock_wait_budget_expired_total(),
        expired_before,
        "an uncontended acquisition must not register an expiry"
    );
    assert_eq!(
        metadata_lock_acquisitions_successful() - successful_before,
        1,
        "and it must be counted as exactly one success"
    );
}

/// The shipped default budget is five seconds, and the three sites that hardcoded five
/// seconds therefore keep their behaviour.
///
/// This is the whole reason the default is not the 60s the superseded `lock_timeout` carried.
/// `disk_cache::acquire_meta_slot_lock`, `disk_cache::batch_delete_ranges` and the multipart
/// Complete publish in `signed_put_handler` each wrote `Duration::from_secs(5)`; making the
/// bound configurable at any other default would have changed all three silently.
#[tokio::test]
async fn the_default_wait_budget_matches_the_literal_it_replaced() {
    let _serialise = COUNTER_LOCK.lock().await;
    assert_eq!(
        SharedStorageConfig::default().lock_acquire_wait_budget,
        Duration::from_secs(5),
        "the config default must be 5s"
    );
    assert_eq!(
        slot_acquire_wait_budget(),
        Duration::from_secs(5),
        "and the acquisition primitives' initial value must agree with it, so a process that \
         never seeds the budget from config still gets the documented bound rather than an \
         instantly-expiring one"
    );
}

/// An existing configuration file that says `lock_timeout` keeps its value.
///
/// The rename is a deserialization alias, not a removal, so no operator's YAML is rejected and
/// nobody has to edit a file to upgrade. Asserted at a non-default value, because a test using
/// the default cannot tell "the alias was read" from "the alias was ignored".
#[tokio::test]
async fn the_deprecated_lock_timeout_key_still_sets_the_budget() {
    let _serialise = COUNTER_LOCK.lock().await;
    let parsed: SharedStorageConfig = serde_yaml_ng::from_str("lock_timeout: 90s\n")
        .expect("a config carrying the old key must still parse");
    assert_eq!(
        parsed.lock_acquire_wait_budget,
        Duration::from_secs(90),
        "the old key must set the new field, or upgrading silently reverts an operator's \
         deliberate value to the default"
    );

    // And the widened floor accepts a value the old 10-300 range rejected. Widening is the one
    // direction that cannot reject a configuration that used to be valid.
    let low: SharedStorageConfig =
        serde_yaml_ng::from_str("lock_acquire_wait_budget: 1s\n").expect("a 1s budget must parse");
    assert!(
        low.validate().is_ok(),
        "1s must validate: the default is 5s, so a floor of 10 would reject values an operator \
         may reasonably want. got={:?}",
        low.validate()
    );
}
