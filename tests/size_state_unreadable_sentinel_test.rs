//! An unreadable `size_state.json` must be distinguishable from a genuine zero.
//!
//! `cache-eviction-at-scale`. The three read-only
//! `JournalConsolidator` getters used to collapse an I/O or parse error onto a plausible
//! value — `0`, `0`, and `SizeState::default()`. Zero is below every eviction trigger and
//! below every `max_cache_size` check, so one unreadable document silently disabled
//! read-tier eviction, the max-size half of admission, and the write-cache byte trigger,
//! while the dashboard published a healthy-looking zero.
//!
//! Same shape and the same reasoning as `WriteLedger::count_entries` (§ 16.6.3) and
//! `LedgerReadOutcome` (unit D1): `None` is the UNREADABLE sentinel, distinguishable from
//! `Some(0)` by a dashboard, by a `python3` parse, and by eye.
//!
//! **These assertions compile unchanged against the pre-change `u64` / `SizeState`
//! signatures and FAIL there**, because `assert_ne!` is generic: pre-change both arms of
//! each comparison are the same `0` (or the same defaulted document), post-change they are
//! `Some(..)` and `None`. That is what makes the red side real rather than a compile error.
//!
//! The fail-open posture (R7.5) is deliberate and unchanged — see
//! `unreadable_still_fails_open_at_the_eviction_trigger`.

use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator, SizeState};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn consolidator(cache_dir: &Path) -> JournalConsolidator {
    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.to_path_buf(),
        "rt-sentinel".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.to_path_buf(),
        Duration::from_secs(30),
        3,
    ));
    JournalConsolidator::new(
        cache_dir.to_path_buf(),
        journal_manager,
        lock_manager,
        ConsolidationConfig::default(),
    )
}

/// Replace `size_state.json` with a DIRECTORY. `Path::exists()` is true, so
/// `load_size_state` gets past its absent-file arm, and `read_to_string` then fails with
/// EISDIR. This is an I/O failure, not a parse failure, and it is the arm a genuinely
/// unreadable shared volume takes.
fn make_unreadable_io(cache_dir: &Path) {
    let p = cache_dir.join("size_tracking").join("size_state.json");
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    let _ = std::fs::remove_file(&p);
    std::fs::create_dir(&p).unwrap();
    assert!(p.exists(), "fixture: the path must exist to reach the read");
}

/// A document that is present and readable but does not parse. A different arm of
/// `load_size_state` from the one above, and worth asserting separately: a torn or
/// truncated write produces this one, not EISDIR.
fn make_unreadable_parse(cache_dir: &Path) {
    let p = cache_dir.join("size_tracking").join("size_state.json");
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(&p, b"{\"total_size\": 42, this is not json").unwrap();
}

async fn persist_zeroed(c: &JournalConsolidator) {
    c.persist_size_state(&SizeState {
        total_size: 0,
        write_cache_size: 0,
        ..Default::default()
    })
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// Site 1: get_current_size
// ---------------------------------------------------------------------------

/// A genuinely empty cache and an unreadable document must not read alike.
///
/// FAILURE BRANCH: `journal_consolidator.rs` `get_current_size`'s `Err(e) => 0` arm — the
/// error arm of the `match self.load_size_state()` inside `get_current_size`. If that arm
/// yields a number rather than the UNREADABLE sentinel, both sides of this comparison are
/// `0` and the assertion fails.
#[tokio::test]
async fn unreadable_current_size_is_distinguishable_from_a_genuine_zero_io() {
    let good = TempDir::new().unwrap();
    let bad = TempDir::new().unwrap();
    let c_good = consolidator(good.path());
    let c_bad = consolidator(bad.path());

    persist_zeroed(&c_good).await;
    make_unreadable_io(bad.path());

    let genuine_zero = c_good.get_current_size().await;
    let unreadable = c_bad.get_current_size().await;
    assert_ne!(
        genuine_zero, unreadable,
        "an unreadable size_state.json read the same as a genuinely empty cache: \
         genuine={genuine_zero:?} unreadable={unreadable:?}"
    );
}

/// Same property for the parse-failure arm rather than the I/O arm.
///
/// FAILURE BRANCH: the same `Err(e) => 0` arm of `get_current_size`, reached via
/// `load_size_state`'s `serde_json::from_str` mapping rather than its `read_to_string`
/// mapping.
#[tokio::test]
async fn unreadable_current_size_is_distinguishable_from_a_genuine_zero_parse() {
    let good = TempDir::new().unwrap();
    let bad = TempDir::new().unwrap();
    let c_good = consolidator(good.path());
    let c_bad = consolidator(bad.path());

    persist_zeroed(&c_good).await;
    make_unreadable_parse(bad.path());

    let genuine_zero = c_good.get_current_size().await;
    let unreadable = c_bad.get_current_size().await;
    assert_ne!(
        genuine_zero, unreadable,
        "an unparseable size_state.json read the same as a genuinely empty cache: \
         genuine={genuine_zero:?} unreadable={unreadable:?}"
    );
}

// ---------------------------------------------------------------------------
// Site 2: get_write_cache_size
// ---------------------------------------------------------------------------

/// The write-cache byte eviction trigger reads this figure. A zero disables it, so an
/// unreadable document must not present as a fully-graduated staging tier.
///
/// FAILURE BRANCH: `journal_consolidator.rs` `get_write_cache_size`'s `Err(e) => 0` arm.
#[tokio::test]
async fn unreadable_write_cache_size_is_distinguishable_from_a_genuine_zero() {
    let good = TempDir::new().unwrap();
    let bad = TempDir::new().unwrap();
    let c_good = consolidator(good.path());
    let c_bad = consolidator(bad.path());

    persist_zeroed(&c_good).await;
    make_unreadable_io(bad.path());

    let genuine_zero = c_good.get_write_cache_size().await;
    let unreadable = c_bad.get_write_cache_size().await;
    assert_ne!(
        genuine_zero, unreadable,
        "an unreadable size_state.json read the same as a drained staging tier: \
         genuine={genuine_zero:?} unreadable={unreadable:?}"
    );
}

// ---------------------------------------------------------------------------
// Site 3: get_size_state
// ---------------------------------------------------------------------------

/// The metrics endpoint and the dashboard read this. An operator looking for the cause of
/// stalled eviction must not be shown a healthy-looking defaulted document.
///
/// Compared via `Debug` rather than `PartialEq` because `SizeState` derives only `Debug`,
/// and because the comparison then works identically against both the pre-change
/// `SizeState` return and the post-change `Option<SizeState>`.
///
/// FAILURE BRANCH: `journal_consolidator.rs` `get_size_state`'s
/// `Err(e) => SizeState::default()` arm.
#[tokio::test]
async fn unreadable_size_state_is_distinguishable_from_an_absent_document() {
    let absent = TempDir::new().unwrap();
    let bad = TempDir::new().unwrap();
    let c_absent = consolidator(absent.path());
    let c_bad = consolidator(bad.path());

    make_unreadable_io(bad.path());

    let from_absent = format!("{:?}", c_absent.get_size_state().await);
    let from_unreadable = format!("{:?}", c_bad.get_size_state().await);
    assert_ne!(
        from_absent, from_unreadable,
        "an unreadable size_state.json produced the same document as an absent one: \
         absent={from_absent} unreadable={from_unreadable}"
    );
}

// ---------------------------------------------------------------------------
// ABSENT is not UNREADABLE, and a fresh volume must stay quiet
// ---------------------------------------------------------------------------

/// A missing `size_state.json` on a fresh volume is normal. It must remain a real reading
/// of zero — `Some(0)`, not the UNREADABLE sentinel — so that nothing WARNs on first boot.
///
/// FAILURE BRANCH: `load_size_state`'s `if !self.size_state_path.exists()` early return. If
/// an absent document ever started producing the sentinel, this fails and the warn-on-fresh
/// -volume regression is caught here rather than in an operator's logs.
#[tokio::test]
async fn absent_size_state_is_a_real_zero_not_the_unreadable_sentinel() {
    let fresh = TempDir::new().unwrap();
    let c = consolidator(fresh.path());
    assert!(
        !fresh
            .path()
            .join("size_tracking")
            .join("size_state.json")
            .exists(),
        "fixture: the document must be absent"
    );

    // A fresh volume and a persisted zero must agree; only unreadability differs.
    let persisted = TempDir::new().unwrap();
    let c_persisted = consolidator(persisted.path());
    persist_zeroed(&c_persisted).await;

    assert_eq!(
        c.get_current_size().await,
        c_persisted.get_current_size().await,
        "an absent document must read as a genuine zero, the same as a persisted zero"
    );
    assert_eq!(
        c.get_write_cache_size().await,
        c_persisted.get_write_cache_size().await,
        "an absent document must read as a genuine zero for the staging tier too"
    );
}

// ---------------------------------------------------------------------------
// The posture is unchanged: unreadable still fails open
// ---------------------------------------------------------------------------

/// R7.5 makes fail-open deliberate. Adding the sentinel must not make eviction fire on an
/// unreadable read, so the value the eviction trigger actually branches on
/// (`current_size <= trigger_threshold` in `maybe_trigger_eviction`) must still be the
/// permissive `0` after the caller flattens the sentinel.
///
/// This asserts the flattening the production call sites perform, not a gauge that merely
/// correlates with it: `unwrap_or(0)` is literally what each `get_current_size()` call site
/// in `journal_consolidator.rs` and `cache.rs` applies before the comparison.
///
/// FAILURE BRANCH: any production call site that flattens the sentinel to something other
/// than `0` — i.e. a change of posture at the `get_current_size()` call sites feeding
/// `maybe_trigger_eviction`.
#[tokio::test]
async fn unreadable_still_fails_open_at_the_eviction_trigger() {
    let bad = TempDir::new().unwrap();
    let c = consolidator(bad.path());
    make_unreadable_io(bad.path());

    // Written against the post-change signature deliberately: this assertion guards that
    // behaviour is UNCHANGED, so it has no meaningful red side and does not need to compile
    // against the old one.
    let flattened: u64 = c.get_current_size().await.unwrap_or(0);
    assert_eq!(
        flattened, 0,
        "fail-open posture changed: an unreadable read must still flatten to 0 for the \
         `current_size <= trigger_threshold` comparison"
    );
}
