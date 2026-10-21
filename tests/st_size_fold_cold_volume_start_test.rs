//! A process that starts on a volume no fold has reached folds it on its first tick.
//!
//! The companion of `st_size_fold_restart_cadence_test.rs`: seeding the cadence from the
//! volume must not delay the first fold of a volume that has no shard figures, because until
//! one runs the per-shard aggregate does not exist.
//!
//! One test in this binary, for the same reason as its companion: the cadence is
//! process-global, and a first tick exists once per process.

use std::path::Path;
use std::time::Duration;

use s3_proxy::st_size_fold::maybe_run_st_size_fold;
use s3_proxy::staged_fold::{read_shard_checkpoints, Shard};
use tempfile::TempDir;

fn seed_range(cache_dir: &Path, l1: &str, name: &str, size: usize) {
    let dir = cache_dir.join("ranges").join("bucket").join(l1).join("111");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(name), vec![0u8; size]).unwrap();
}

/// A cold volume folds at start, and the process-local cadence then holds the next fold off.
///
/// - **First call folds both shards.** No checkpoint exists, so the volume cannot show it is
///   fresh and the gate must fold.
/// - **An immediate second call does not.** The fold just completed in this process, so the
///   process-local timestamp is inside the interval; this shows the cold path did not turn
///   the gate into "fold on every tick".
#[tokio::test]
async fn a_cold_volume_folds_on_the_first_tick() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    seed_range(dir, "aa", "one_0-1099.bin", 1_100);
    seed_range(dir, "bb", "two_0-511.bin", 512);
    assert_eq!(
        read_shard_checkpoints(dir).iter().count(),
        0,
        "fixture precondition: the volume carries no shard documents"
    );

    let interval = Duration::from_secs(3_600);
    let first = maybe_run_st_size_fold(dir, interval, Duration::ZERO, "host:cold").await;
    let first = first.expect("a volume with no shard figures must fold on the first tick");
    assert_eq!(
        first.installed,
        vec![Shard::new("bucket", "aa"), Shard::new("bucket", "bb")]
    );

    let second = maybe_run_st_size_fold(dir, interval, Duration::ZERO, "host:cold").await;
    assert!(
        second.is_none(),
        "a second tick straight after a completed fold must not fold again: {second:?}"
    );
}
