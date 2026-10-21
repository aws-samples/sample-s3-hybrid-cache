//! `cache_rules.json` reloads must not hold a request on a stalled cache volume.
//!
//! `BucketSettingsManager::resolve` is awaited on every GET, HEAD and PUT before the proxy
//! forwards or reads the body. Once `bucket_settings_staleness_threshold` has passed it
//! re-reads `cache_dir/cache_rules.json`. That read must not delay `resolve` past a small
//! bound when the volume stalls, and a changed file must still be picked up by the request
//! that finds the rules stale when the volume is healthy.
//!
//! The stall is the file replaced by a FIFO: `open` blocks in the kernel until a writer
//! appears, with no seam in the code under test. Unix only: `mkfifo` is a POSIX call.
#![cfg(unix)]

use s3_proxy::bucket_settings::{BucketSettingsManager, GlobalDefaults};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// The longest `resolve` may take while the rules file cannot be read.
const RESOLVE_BOUND: Duration = Duration::from_millis(200);

/// How long the stalled read is held before the fixture releases it. Far above the bound,
/// so a `resolve` that waits for the read fails the bound by a wide margin.
const STALL: Duration = Duration::from_secs(3);

fn defaults() -> GlobalDefaults {
    GlobalDefaults {
        get_ttl: Duration::from_secs(300),
        head_ttl: Duration::from_secs(60),
        put_ttl: Duration::from_secs(3600),
        read_cache_enabled: true,
        write_cache_enabled: true,
        compression_enabled: true,
        ram_cache_enabled: true,
        evaluate_conditions_from_cache: true,
        upstream_first_byte_timeout: Duration::from_secs(5),
    }
}

fn rules_with_get_ttl(secs: u64) -> String {
    format!(r#"{{"rules": [{{"pattern": "**", "get_ttl": "{secs}s"}}]}}"#)
}

fn rules_path(dir: &Path) -> PathBuf {
    dir.join("cache_rules.json")
}

/// Replace `path` with a FIFO, so the next reader blocks in `open`.
fn make_fifo(path: &Path) {
    let _ = std::fs::remove_file(path);
    let status = std::process::Command::new("mkfifo")
        .arg(path)
        .status()
        .expect("run mkfifo");
    assert!(status.success(), "mkfifo {:?} failed", path);
}

/// After `after`, release every reader parked on the FIFO at `path` with `content`, then
/// leave a regular file holding `content` in its place. Runs on a detached thread so the
/// test can time `resolve` while the stall is in force.
fn release_fifo_after(path: PathBuf, content: String, after: Duration) {
    std::thread::spawn(move || {
        std::thread::sleep(after);
        let (renamed_tx, renamed_rx) = std::sync::mpsc::channel::<()>();
        let fifo = path.clone();
        let body = content.clone();
        // Opening the write end releases every reader parked in `open`. It stays open until
        // the regular file has replaced the pipe, so a reader arriving in between finds a
        // writer instead of parking with none.
        std::thread::spawn(move || {
            if let Ok(mut w) = std::fs::OpenOptions::new().write(true).open(&fifo) {
                let _ = renamed_rx.recv();
                use std::io::Write;
                let _ = w.write_all(body.as_bytes());
            }
        });
        std::thread::sleep(Duration::from_millis(100));
        let tmp = path.with_extension("scn1-release");
        std::fs::write(&tmp, content.as_bytes()).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        let _ = renamed_tx.send(());
    });
}

/// A stale rule set whose file cannot be read must not hold `resolve`: the request resolves
/// against the in-memory rules within the bound, and the new rules take effect once the
/// volume answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_rules_file_does_not_hold_resolve_past_the_bound() {
    let dir = TempDir::new().unwrap();
    let path = rules_path(dir.path());
    std::fs::write(&path, rules_with_get_ttl(111)).unwrap();
    let threshold = Duration::from_millis(300);
    let mgr = BucketSettingsManager::new(dir.path().to_path_buf(), defaults(), threshold);

    // First load: a healthy volume.
    assert_eq!(mgr.resolve("b/k").await.get_ttl, Duration::from_secs(111));

    // Stall the volume, let the rules go stale, and schedule the release.
    make_fifo(&path);
    tokio::time::sleep(threshold + Duration::from_millis(100)).await;
    release_fifo_after(path.clone(), rules_with_get_ttl(222), STALL);

    let started = Instant::now();
    let resolved = mgr.resolve("b/k").await;
    let waited = started.elapsed();
    assert!(
        waited <= RESOLVE_BOUND,
        "cache_rules.json reload held resolve for {:?} on a stalled cache volume; the bound is {:?}",
        waited,
        RESOLVE_BOUND
    );
    assert_eq!(
        resolved.get_ttl,
        Duration::from_secs(111),
        "a request that cannot reload must resolve against the in-memory rules"
    );

    // Once the volume answers, the new rules take effect without a restart.
    let deadline = Instant::now() + STALL + threshold + Duration::from_secs(3);
    loop {
        if mgr.resolve("b/k").await.get_ttl == Duration::from_secs(222) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "rules were not picked up after the stalled volume answered"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// On a healthy volume the request that finds the rules stale sees the changed file: an edit
/// takes effect within the staleness threshold plus one reload, which is what the suite's
/// `STALENESS_WAIT` (threshold + 5 s) relies on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_rules_file_is_picked_up_by_the_first_stale_request() {
    let dir = TempDir::new().unwrap();
    let path = rules_path(dir.path());
    std::fs::write(&path, rules_with_get_ttl(111)).unwrap();
    let threshold = Duration::from_millis(300);
    let mgr = BucketSettingsManager::new(dir.path().to_path_buf(), defaults(), threshold);
    assert_eq!(mgr.resolve("b/k").await.get_ttl, Duration::from_secs(111));

    std::fs::write(&path, rules_with_get_ttl(222)).unwrap();
    // Within the threshold the in-memory rules are reused without touching disk.
    assert_eq!(mgr.resolve("b/k").await.get_ttl, Duration::from_secs(111));

    tokio::time::sleep(threshold + Duration::from_millis(50)).await;
    let started = Instant::now();
    let resolved = mgr.resolve("b/k").await;
    assert_eq!(
        resolved.get_ttl,
        Duration::from_secs(222),
        "the first request after the threshold must see the changed rules on a healthy volume"
    );
    assert!(started.elapsed() <= RESOLVE_BOUND);
}
