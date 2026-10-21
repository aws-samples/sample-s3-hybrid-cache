//! A process that starts inside the `st_size` fold's interval does not fold the volume again.
//!
//! The fold's cadence timestamp is process-local, so before the volume was consulted every
//! process start folded the whole volume on its first tick, and a rolling restart of N proxies
//! ran N whole-volume traversals while the deploy was in progress. The interval is now a
//! property of the volume: the gate reads the stalest shard's `scanned_at` and folds only once
//! that is older than the interval.
//!
//! One test in this binary, deliberately: the cadence is process-global state, and "the first
//! tick after a process start" exists only until some call in the process seeds or completes
//! it. A second test here would share that state and see a warm process instead of a fresh one.

use std::path::Path;
use std::time::Duration;

use s3_proxy::st_size_fold::{maybe_run_st_size_fold, run_st_size_fold};
use s3_proxy::staged_fold::{read_shard_checkpoints, Shard};
use tempfile::TempDir;

const BEFORE: &str = "host:before-restart";
const AFTER: &str = "host:after-restart";

fn seed_range(cache_dir: &Path, l1: &str, name: &str, size: usize) {
    let dir = cache_dir.join("ranges").join("bucket").join(l1).join("111");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(name), vec![0u8; size]).unwrap();
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Every shard's `(scanned_at, scanned_by)`, in shard order.
fn scans(cache_dir: &Path) -> Vec<(u64, String)> {
    read_shard_checkpoints(cache_dir)
        .iter()
        .map(|(_, c)| {
            (
                c.other.get("scanned_at").and_then(|v| v.as_u64()).unwrap(),
                c.other
                    .get("scanned_by")
                    .and_then(|v| v.as_str())
                    .unwrap()
                    .to_string(),
            )
        })
        .collect()
}

/// Move every shard's `scanned_at` back by `secs`, as if the fold had run that long ago.
fn backdate_scans(cache_dir: &Path, secs: u64) {
    let at = now_nanos() - secs * 1_000_000_000;
    for (_, c) in read_shard_checkpoints(cache_dir).iter() {
        let mut c = c.clone();
        c.other
            .insert("scanned_at".to_string(), serde_json::json!(at));
        c.install(cache_dir).unwrap();
    }
}

/// A fresh process on a volume folded 8 s ago, with a 10 s interval, does not fold; it folds
/// once the stalest shard ages past the interval.
///
/// # What each half discriminates
///
/// - **First call returns `None` and leaves every shard's `scanned_at` and `scanned_by` as the
///   previous process wrote them.** This is the restart-inside-the-interval property. The
///   `scanned_by` check is independent of the return value: a fold that ran would restamp it.
/// - **After 3 s the same process folds, and every shard now names the new instance.** Without
///   this, the first half would pass for a gate that never folds a warm volume at all. It also
///   checks the seed arithmetic: the process-local timestamp was seeded to `now − 8`, so the
///   fold falls due at +2 s, not a whole interval after the restart.
///
/// # RED SIDE
///
/// Delete the `volume_scan_age_secs` block in `maybe_run_st_size_fold` and the first assertion
/// fails with `Some(..)`: the process-local timestamp is 0 at start, so the gate folds.
#[tokio::test]
async fn a_restart_inside_the_interval_does_not_fold_the_volume() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    seed_range(dir, "aa", "one_0-1099.bin", 1_100);
    seed_range(dir, "bb", "two_0-511.bin", 512);

    // The previous process's fold: every shard carries its figure and a scan timestamp. Run
    // through `run_st_size_fold`, which does not touch the cadence statics, so this process is
    // still in its first-tick state.
    let prior = run_st_size_fold(dir, BEFORE, Duration::ZERO).await;
    assert_eq!(
        prior.installed,
        vec![Shard::new("bucket", "aa"), Shard::new("bucket", "bb")],
        "fixture precondition: the prior fold must cover both shards"
    );
    backdate_scans(dir, 8);
    let before = scans(dir);
    assert_eq!(before.len(), 2);

    let interval = Duration::from_secs(10);
    let first = maybe_run_st_size_fold(dir, interval, Duration::ZERO, AFTER).await;
    assert!(
        first.is_none(),
        "a process starting 8s after the volume's last fold, with a 10s interval, folded the \
         volume again: {first:?}"
    );
    assert_eq!(
        scans(dir),
        before,
        "no shard may be restamped by a fold that should not have run"
    );

    tokio::time::sleep(Duration::from_secs(3)).await;
    let second = maybe_run_st_size_fold(dir, interval, Duration::ZERO, AFTER).await;
    let second = second.expect(
        "the stalest shard is now past the interval, so the fold must run: the seed from the \
         volume must not suppress a fold that is due",
    );
    assert_eq!(second.installed.len(), 2);
    assert!(
        scans(dir).iter().all(|(_, by)| by == AFTER),
        "every shard must now carry this process's scan: {:?}",
        scans(dir)
    );
}
