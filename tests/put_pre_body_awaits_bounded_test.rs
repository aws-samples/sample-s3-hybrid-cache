//! The two shared-storage waits `HttpProxy::handle_put_request` makes BEFORE it reads a PUT,
//! `UploadPart` or unsigned-PUT body must each be bounded, so a stalled cache volume cannot hold
//! a client's upload unread until the client gives up.
//!
//! - `CacheManager::origin_verdict`: the per-bucket origin-marker claim, first write per bucket
//!   per process.
//! - `CacheManager::put_capacity_usage`: the whole-cache figure handed to
//!   `check_cache_capacity`, which read `size_state.json` inline on every PUT.
//!
//! # How the stall is made, with no seam
//!
//! Each file the wait reads is replaced by a named pipe. Opening a FIFO for reading blocks until
//! a writer opens it, which is what a `read`/`open` against a volume in NFS lock recovery looks
//! like to the proxy: a thread parked in the kernel, with nothing for an async timeout to cancel.
//! That makes the red side real on unchanged code. Each test releases its readers afterwards
//! (open a writer, then rename a regular file over the pipe), because a tokio runtime waits for
//! its blocking threads when it is dropped.
//!
//! Unix only: `mkfifo` is a POSIX call.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::origin_identity::OriginVerdict;

/// The bound the fix applies to each pre-body wait.
const BOUND: Duration = Duration::from_secs(5);
/// The PUT router's bound on its capacity read.
const ROUTER_BOUND: Duration = Duration::from_millis(250);
/// Scheduling slack on a loaded test machine.
const MARGIN: Duration = Duration::from_millis(2500);
/// How long the stall would last if nothing bounded it: longer than `BOUND + MARGIN`, so an
/// unbounded wait fails the assertion rather than finishing inside it.
const STALL: Duration = Duration::from_secs(20);

async fn make_manager(cache_dir: PathBuf) -> Arc<CacheManager> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.ram_cache_enabled = false;
    config.cache.write_cache_enabled = true;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    let c = &config.cache;
    let manager = Arc::new(CacheManager::new_with_shared_storage(
        c.cache_dir.clone(),
        c.ram_cache_enabled,
        c.max_ram_cache_size,
        c.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        false,
        c.get_ttl,
        c.head_ttl,
        c.put_ttl,
        c.actively_remove_cached_data,
        c.shared_storage.clone(),
        c.write_cache_percent,
        c.write_cache_enabled,
        c.incomplete_upload_ttl,
        c.metadata_cache.clone(),
        c.eviction_trigger_percent,
        c.eviction_target_percent,
        c.read_cache_enabled,
        c.bucket_settings_staleness_threshold,
        c.compression_batch_size,
        c.evaluate_conditions_from_cache,
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    ));
    let disk_cache_manager = manager.create_configured_disk_cache_manager();
    manager.initialize().await.expect("cache init");
    disk_cache_manager
        .initialize()
        .await
        .expect("disk cache init");
    manager
}

/// Replace `path` with a FIFO, so the next reader blocks in `open`.
fn make_fifo(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let _ = std::fs::remove_file(path);
    let status = std::process::Command::new("mkfifo")
        .arg(path)
        .status()
        .expect("run mkfifo");
    assert!(status.success(), "mkfifo {:?} failed", path);
}

/// Release every reader parked on the FIFO at `path`, then leave a regular file holding
/// `content` in its place so later readers do not park.
fn release_fifo(path: &Path, content: &'static [u8]) {
    let fifo = path.to_path_buf();
    let (renamed_tx, renamed_rx) = std::sync::mpsc::channel::<()>();
    // Opening the write end blocks until a reader exists, and releases every reader parked in
    // `open`. The write end stays open until the regular file has replaced the pipe, so a
    // reader that opens the pipe in between finds a writer instead of parking with none. A
    // detached thread, so a pipe nobody is reading cannot hang the test.
    std::thread::spawn(move || {
        if let Ok(mut w) = std::fs::OpenOptions::new().write(true).open(&fifo) {
            let _ = renamed_rx.recv();
            use std::io::Write;
            let _ = w.write_all(content);
        }
    });
    std::thread::sleep(Duration::from_millis(300));
    let tmp = path.with_extension("scn1-release");
    std::fs::write(&tmp, content).unwrap();
    std::fs::rename(&tmp, path).unwrap();
    let _ = renamed_tx.send(());
    // Let the released readers finish before the runtime is dropped.
    std::thread::sleep(Duration::from_millis(300));
}

/// The origin-marker claim on the first write to a bucket must answer within the bound when
/// the marker cannot be read, and the answer must NOT permit caching: without a verdict the
/// proxy cannot say this origin owns the bucket's entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_origin_marker_claim_is_abandoned_within_the_bound_without_caching() {
    let dir = TempDir::new().unwrap();
    let manager = make_manager(dir.path().to_path_buf()).await;
    let marker = dir
        .path()
        .join("metadata")
        .join("stallbucket")
        .join("_origin.json");
    make_fifo(&marker);

    let start = Instant::now();
    let verdict = tokio::time::timeout(
        STALL,
        manager.origin_verdict("stallbucket/object.bin", "s3.us-west-2.amazonaws.com"),
    )
    .await;
    let elapsed = start.elapsed();
    release_fifo(&marker, b"not json");

    assert!(
        elapsed <= BOUND + MARGIN,
        "origin marker claim held the PUT before its body for {:?} on a stalled cache volume; \
         the bound is {:?}",
        elapsed,
        BOUND
    );
    let verdict = verdict.expect("inside STALL by the assertion above");
    assert!(
        !verdict.permits_cache(),
        "an unresolved origin verdict must forward uncached, got {:?}",
        verdict
    );
    assert_ne!(verdict, OriginVerdict::Matches);
}

/// The capacity figure a PUT hands to `check_cache_capacity` must not wait on `size_state.json`
/// past the bound, even on the process's first PUT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_size_state_read_does_not_hold_the_put_past_the_bound() {
    let dir = TempDir::new().unwrap();
    let manager = make_manager(dir.path().to_path_buf()).await;
    let size_state = dir.path().join("size_tracking").join("size_state.json");
    make_fifo(&size_state);

    let start = Instant::now();
    let usage = tokio::time::timeout(STALL, manager.put_capacity_usage()).await;
    let elapsed = start.elapsed();
    release_fifo(&size_state, b"{}");

    assert!(
        elapsed <= ROUTER_BOUND + MARGIN,
        "PUT capacity read held the PUT before its body for {:?} on a stalled cache volume; \
         the bound is {:?}",
        elapsed,
        ROUTER_BOUND
    );
    // No figure: the router forwards the write uncached.
    assert_eq!(usage.expect("inside STALL by the assertion above"), None);
}

/// Control: on a healthy volume the capacity figure is the one `get_cache_size_stats`
/// publishes, so the snapshot does not change what `check_cache_capacity` sees.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_healthy_volume_serves_the_published_total() {
    let dir = TempDir::new().unwrap();
    let manager = make_manager(dir.path().to_path_buf()).await;
    let published = manager
        .get_cache_size_stats()
        .await
        .unwrap()
        .sizes
        .unwrap()
        .total_cache_size;
    let start = Instant::now();
    assert_eq!(manager.put_capacity_usage().await, Some(published));
    assert!(start.elapsed() < BOUND);
}
