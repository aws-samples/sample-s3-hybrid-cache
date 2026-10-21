//! A schedule seeded from the volume's age carries per-instance jitter.
//!
//! Every instance reads the same stalest-shard age, so before the jitter every instance fell
//! due at the same second and walked the volume together. The seed now adds
//! `seed_jitter_secs(instance_id, interval)`, stable per instance and below a tenth of the
//! interval.
//!
//! One test in this binary, deliberately: the cadence is process-global state.

use std::path::Path;
use std::time::Duration;

use s3_proxy::st_size_fold::{maybe_run_st_size_fold, run_st_size_fold, seed_jitter_secs};
use s3_proxy::staged_fold::read_shard_checkpoints;
use tempfile::TempDir;

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

fn backdate_scans(cache_dir: &Path, secs: u64) {
    let at = now_nanos() - secs * 1_000_000_000;
    for (_, c) in read_shard_checkpoints(cache_dir).iter() {
        let mut c = c.clone();
        c.other
            .insert("scanned_at".to_string(), serde_json::json!(at));
        c.install(cache_dir).unwrap();
    }
}

/// A 100 s interval on a volume folded 97 s ago. Unjittered, the fold falls due at +3 s.
///
/// # What each arm discriminates
///
/// - **The jitter spreads instances and is bounded.** Two ids differ, and every id is below a
///   tenth of the interval.
/// - **At +4 s, no fold runs.** The chosen id's jitter is at least 4 s, so its seed is later
///   than the volume age alone puts it.
/// - **Once the jitter has elapsed, the fold runs.** The jitter delays the schedule; it does
///   not suppress a due fold.
///
/// # RED SIDE
///
/// Remove `.saturating_add(jitter)` from the seed in `maybe_run_st_size_fold` and the +4 s
/// call returns `Some(..)`.
#[tokio::test]
async fn a_seeded_schedule_is_delayed_by_this_instances_jitter() {
    const INTERVAL: u64 = 100;
    let ids: Vec<String> = (0..64).map(|i| format!("host:jitter-{i}")).collect();
    let jitters: Vec<u64> = ids
        .iter()
        .map(|id| seed_jitter_secs(id, INTERVAL))
        .collect();
    assert!(
        jitters.iter().all(|&j| j < INTERVAL / 10),
        "jitter must stay below a tenth of the interval: {jitters:?}"
    );
    assert!(
        jitters.iter().any(|&j| j != jitters[0]),
        "jitter must differ between instances: {jitters:?}"
    );
    assert_eq!(
        seed_jitter_secs(&ids[0], INTERVAL),
        jitters[0],
        "jitter must be stable for an instance id"
    );
    let (id, jitter) = ids
        .iter()
        .zip(jitters.iter())
        .find(|(_, &j)| (4..=6).contains(&j))
        .map(|(id, &j)| (id.clone(), j))
        .expect("FIXTURE: some id in 64 must have a 4-6 s jitter");

    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    seed_range(dir, "aa", "one_0-1099.bin", 1_100);
    let prior = run_st_size_fold(dir, "host:prior", Duration::ZERO).await;
    assert_eq!(
        prior.installed.len(),
        1,
        "FIXTURE: the prior fold must install"
    );
    backdate_scans(dir, 97);

    let interval = Duration::from_secs(INTERVAL);
    assert!(
        maybe_run_st_size_fold(dir, interval, Duration::ZERO, &id)
            .await
            .is_none(),
        "FIXTURE: 97 s inside a 100 s interval is not due"
    );

    tokio::time::sleep(Duration::from_secs(4)).await;
    let at_four = maybe_run_st_size_fold(dir, interval, Duration::ZERO, &id).await;
    assert!(
        at_four.is_none(),
        "UNJITTERED-SEED: at +4 s the volume age alone says due, but this instance's {jitter}s \
         jitter must hold it back: {at_four:?}"
    );

    tokio::time::sleep(Duration::from_secs(jitter)).await;
    let due = maybe_run_st_size_fold(dir, interval, Duration::ZERO, &id).await;
    assert!(
        due.is_some_and(|o| o.installed.len() == 1),
        "once the {jitter}s jitter has elapsed the fold must run"
    );
}
