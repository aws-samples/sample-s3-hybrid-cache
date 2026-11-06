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

#[path = "common/proxy_rig.rs"]
mod proxy_rig;
use proxy_rig::{Rig, RigOptions};
use s3_proxy::config::SharedStorageConfig;
use s3_proxy::metadata_lock_manager::set_slot_acquire_wait_budget;
use s3_proxy::metadata_lock_manager::{
    acquire_slot_lock_within, metadata_lock_acquisitions_successful,
    metadata_lock_wait_budget_expired_total, slot_acquire_wait_budget, try_acquire_slot_lock,
};
use s3_proxy::ProxyError;
use std::sync::LazyLock;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::Mutex;

/// Serialises every test in this file that reads a process-global lock counter.
///
/// `tokio::sync::Mutex`, not `std::sync::Mutex`: the guard is held across `.await` points, which
/// a std guard must never be (clippy's `await_holding_lock` rejects it, and it can deadlock a
/// single-threaded runtime). Every test here is `#[tokio::test]` for that reason, including the
/// two that await nothing else.
static COUNTER_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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

/// The request path for a part-numbered GET that misses, on a proxy in front of a two-part
/// origin object: `GET /bucket/key?partNumber=1`. The part is forwarded, served to the client,
/// and stored as a range fill, which takes the key's slot lock to publish.
const OBJECT_PATH: &str = "/slot-lock-guard/object.bin";
const OBJECT_LEN: usize = 64 * 1024;
const PART_ONE_END: u64 = (OBJECT_LEN / 2 - 1) as u64;

/// A proxy with no background consolidation, so the only slot-lock traffic is the request's.
async fn part_rig() -> Rig {
    let rig = Rig::new(RigOptions {
        consolidate: false,
        ..RigOptions::default()
    })
    .await;
    rig.mock.put(OBJECT_PATH, OBJECT_LEN);
    rig
}

/// Wait until the part's journal `Add` is on disk: the last step of the fill's publish.
async fn wait_for_fill_journal(rig: &Rig) {
    let want = Rig::cache_key(OBJECT_PATH);
    let journals = rig.dir.path().join("metadata").join("_journals");
    let until = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let found = std::fs::read_dir(&journals)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| std::fs::read_to_string(e.path()).ok())
            .any(|content| content.contains(&want) && content.contains("\"Add\""));
        if found {
            return;
        }
        assert!(
            std::time::Instant::now() < until,
            "the part fill's journal Add never landed in {journals:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The `.bin` files under the cache's `ranges` directory.
fn range_files(rig: &Rig) -> usize {
    walkdir::WalkDir::new(rig.dir.path().join("ranges"))
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "bin"))
        .count()
}

/// A range fill's publish takes the key's slot lock through the guard, so the published
/// acquisition figure counts it.
///
/// The fill is the one a part-numbered GET miss starts on a real proxy. The figure is read
/// through the same free function `metrics.rs` reads, so this asserts the published value. Its
/// red side removes the increment in the guard's acquisition: the fill still publishes and the
/// client still receives the part, and the delta drops below the expected figure.
#[tokio::test]
async fn a_range_fill_publish_reaches_the_published_acquisition_counter() {
    let _serialise = COUNTER_LOCK.lock().await;
    let rig = part_rig().await;
    let before = metadata_lock_acquisitions_successful();
    let (status, body) = rig.get(&format!("{OBJECT_PATH}?partNumber=1"), &[]).await;
    assert_eq!(status, hyper::StatusCode::PARTIAL_CONTENT);
    assert_eq!(body.len(), OBJECT_LEN / 2, "the client receives part 1");
    wait_for_fill_journal(&rig).await;
    // The publish, journal and credit are done once the Add is journaled; settle any
    // trailing work of the same request before reading the figure.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = metadata_lock_acquisitions_successful();
    assert_eq!(
        after - before,
        FILL_PUBLISH_ACQUISITIONS,
        "a part-numbered GET miss must reach the published slot-lock acquisition counter \
         exactly {FILL_PUBLISH_ACQUISITIONS} time(s). before={before} after={after}"
    );
    assert_eq!(range_files(&rig), 1, "the fill published its range file");
}

/// A part-numbered GET miss takes the key's slot lock twice, measured on this request path
/// (before=2 after=4 with no other traffic on the proxy).
const FILL_PUBLISH_ACQUISITIONS: u64 = 2;

/// A range fill whose slot is held by a peer gives up within the wait budget instead of
/// waiting for the holder, and the give-up is counted. The client's response does not depend on
/// the fill.
///
/// The bounded polls are the assertion: a publish that took an unbounded blocking lock would
/// never count its give-up, and the test would fail at the poll's deadline instead of wedging
/// the binary.
#[tokio::test]
async fn a_range_fill_publish_gives_up_within_the_budget_when_the_slot_is_held() {
    let _serialise = COUNTER_LOCK.lock().await;
    let rig = part_rig().await;
    let key = Rig::cache_key(OBJECT_PATH);
    let restore = slot_acquire_wait_budget();
    set_slot_acquire_wait_budget(Duration::from_millis(400));
    let holder = try_acquire_slot_lock(rig.dir.path(), &key)
        .expect("fixture: a peer must hold the slot, or this measures an uncontended acquire");
    let expired_before = metadata_lock_wait_budget_expired_total();
    let outcome = rig.get(&format!("{OBJECT_PATH}?partNumber=1"), &[]).await;
    let (status, body) = outcome;
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while metadata_lock_wait_budget_expired_total() == expired_before
        && std::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let expired_at_give_up = metadata_lock_wait_budget_expired_total();
    // A late second give-up would show here.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let expired_after = metadata_lock_wait_budget_expired_total();
    let published = range_files(&rig);
    let still_held = try_acquire_slot_lock(rig.dir.path(), &key);
    set_slot_acquire_wait_budget(restore);
    assert_eq!(
        status,
        hyper::StatusCode::PARTIAL_CONTENT,
        "the client's response must not depend on the cache fill"
    );
    assert_eq!(body.len(), OBJECT_LEN / 2, "the client receives part 1");
    assert_eq!(
        expired_at_give_up - expired_before,
        1,
        "the fill must give up within the budget against a held slot and count it once. \
         PART_ONE_END={PART_ONE_END}"
    );
    assert_eq!(
        expired_after, expired_at_give_up,
        "the give-up must be counted exactly once"
    );
    assert_eq!(published, 0, "a fill that gave up publishes no range file");
    assert!(
        matches!(still_held, Err(ProxyError::LockContention(_))),
        "the original holder must still hold the slot after the fill gave up. got={still_held:?}"
    );
    drop(holder);
}
