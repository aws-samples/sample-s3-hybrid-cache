//! A `st_size` fold pass whose shard enumeration fails does not consume the interval.
//!
//! Before, `maybe_run_st_size_fold` stamped its completion after every pass, including one that
//! enumerated nothing, so one failed listing cost a whole interval of fold staleness. The
//! failure now schedules a retry after `enumeration_failure_backoff_secs`.
//!
//! One test in this binary, deliberately: the cadence is process-global state.

use std::path::Path;
use std::time::Duration;

use s3_proxy::st_size_fold::{enumeration_failure_backoff_secs, maybe_run_st_size_fold};
use tempfile::TempDir;

const INSTANCE: &str = "host:enumeration-retry";

fn seed_range(cache_dir: &Path, l1: &str, name: &str, size: usize) {
    let dir = cache_dir.join("ranges").join("bucket").join(l1).join("111");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(name), vec![0u8; size]).unwrap();
}

/// # What each arm discriminates
///
/// - **Immediately after the failure, no pass runs.** The retry is bounded, not a spin on
///   every tick.
/// - **After the backoff, with the volume repaired, the pass runs and installs.** This is the
///   fix: the old stamp made the next pass wait the whole 20 s interval.
///
/// # RED SIDE
///
/// Replace the `enumeration_failed` branch's stamp with `LAST_FOLD_COMPLETED_SECS.store(finished)`
/// and the third call returns `None`.
#[tokio::test]
async fn an_enumeration_failure_retries_after_the_backoff_not_the_interval() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    // `ranges` as a regular file: listing it fails with something other than NotFound.
    std::fs::write(dir.join("ranges"), b"not a directory").unwrap();

    let interval = Duration::from_secs(20);
    let backoff = enumeration_failure_backoff_secs(interval.as_secs());
    assert_eq!(backoff, 2, "FIXTURE: a 20 s interval backs off a tenth");

    let first = maybe_run_st_size_fold(dir, interval, Duration::ZERO, INSTANCE)
        .await
        .expect("FIXTURE: a cold process with no checkpoints must attempt a pass");
    assert!(
        first.enumeration_failed,
        "FIXTURE: the pass must fail its enumeration: {first:?}"
    );

    assert!(
        maybe_run_st_size_fold(dir, interval, Duration::ZERO, INSTANCE)
            .await
            .is_none(),
        "the retry must wait out the backoff, not run on the next tick"
    );

    std::fs::remove_file(dir.join("ranges")).unwrap();
    seed_range(dir, "aa", "one_0-1099.bin", 1_100);
    tokio::time::sleep(Duration::from_secs(backoff + 1)).await;

    let third = maybe_run_st_size_fold(dir, interval, Duration::ZERO, INSTANCE).await;
    let third = third.expect(
        "FAILED-PASS-CONSUMES-INTERVAL: the backoff has elapsed and the volume is readable, but \
         no pass ran: the failed enumeration was stamped as a completion",
    );
    assert!(!third.enumeration_failed);
    assert_eq!(third.installed.len(), 1, "the repaired volume must fold");
}
