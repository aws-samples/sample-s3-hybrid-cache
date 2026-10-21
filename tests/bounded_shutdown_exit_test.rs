//! Process exit must be bounded, even when a blocking task is parked on a shared-volume lock.
//!
//! `Drop for BlockingPool` calls `BlockingPool::shutdown` with no timeout and waits for every
//! blocking task that is currently *running*. Several of this crate's blocking tasks wait on
//! file locks under the cache directory, which is shared storage — so the holder can be a peer
//! instance, or an NFS client that died with the lease. Under `#[tokio::main]` the runtime is
//! dropped implicitly when `main` returns, which makes that untimed wait **process exit**: on
//! SIGTERM the graceful path completes, `main` returns, and the process then sits at 0 % CPU
//! until a peer releases. `config/s3-proxy.service` sets no `TimeoutStopSec`, so systemd waits
//! its 90 s default and `SIGKILL`s.
//!
//! Two tests, covering the two halves of the fix:
//!
//! 1. `a_bounded_shutdown_returns_while_a_blocking_task_is_still_parked` — the mechanism, with
//!    a real holder and a real parked task, asserting on *why* the drop returned.
//! 2. `main_builds_its_runtime_with_a_bounded_shutdown` — binds that mechanism to `src/main.rs`,
//!    which no behavioural test can reach: `fn main` is not callable from a test, and a genuine
//!    red side for it has to WEDGE, which is not something a test binary can survive. The
//!    deterministic red side for the wedge itself is taken out of tree, in `tmp/wedgeprobe/`.

use std::sync::mpsc;
use std::time::{Duration, Instant};

/// How long the probe runtimes are allowed to spend in their blocking-pool wait.
const BOUND: Duration = Duration::from_secs(1);

/// Watchdog slack. An unbounded wait never returns, so this only has to be long enough that a
/// loaded machine cannot fail the test spuriously.
const SLACK: Duration = Duration::from_secs(20);

/// Build a runtime, run `body` on it, bound its shutdown, and report the time the shutdown took.
///
/// Runs on a thread of its own behind a channel watchdog, so an unbounded wait **fails** the
/// test instead of wedging the suite — which is exactly what the unfixed code does.
fn shutdown_elapsed<F>(bounded: bool, body: F) -> Option<Duration>
where
    F: FnOnce(std::path::PathBuf) + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let dir = std::env::temp_dir().join(format!(
        "bounded-shutdown-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&dir).expect("probe dir");
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("probe runtime");
        runtime.block_on(async {
            let d = dir.clone();
            tokio::task::spawn_blocking(move || body(d));
            // Let the blocking task reach its wait before the runtime is dropped.
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        let start = Instant::now();
        if bounded {
            runtime.shutdown_timeout(BOUND);
        } else {
            drop(runtime);
        }
        let _ = tx.send(start.elapsed());
    });
    rx.recv_timeout(BOUND + SLACK).ok()
}

/// A bounded shutdown returns **at the bound**, with the holder still holding.
///
/// **The vacuity trap this avoids**: asserting that the process exits. It exits either way
/// whenever nothing holds the lock, which is every ordinary run. So the lock is held for the
/// whole test and never released, and the assertion is on *why* the shutdown returned —
/// `elapsed >= BOUND` means the timeout fired, and the control arm in the same test returns in
/// milliseconds with the same bound in place, which is what makes that reading mean anything.
#[test]
fn a_bounded_shutdown_returns_while_a_blocking_task_is_still_parked() {
    // The holder: an independent OS thread outside any runtime, standing in for the peer
    // instance. It holds for the whole test, so the parked task cannot finish.
    let path = std::env::temp_dir().join(format!("bounded-shutdown-holder-{}", std::process::id()));
    std::fs::write(&path, b"").expect("lock file");
    let holder = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open holder");
    fs4::FileExt::lock(&holder).expect("holder takes the lock");

    let held = path.clone();
    let parked = shutdown_elapsed(true, move |_| {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&held)
            .expect("open contender");
        // Unbounded, in the kernel, exactly as the crate's own blocking lock waits are.
        let _ = fs4::FileExt::lock(&f);
    })
    .expect(
        "a bounded shutdown must return while the lock is still held — an unbounded one never \
         does, and under `#[tokio::main]` that is process exit",
    );

    assert!(
        parked >= BOUND,
        "the shutdown returned after {parked:?}, inside its own {BOUND:?} bound — so it \
         returned because the blocking task FINISHED, which means the holder released and the \
         fixture proved nothing"
    );

    // Control, same bound, a blocking task that returns immediately: distinguishes "returned
    // because the timeout fired" from "returned because the task finished".
    let finished = shutdown_elapsed(true, |_| {}).expect("a finishing task must not wedge");
    assert!(
        finished < BOUND,
        "a blocking task that returns must not make the shutdown wait out its {BOUND:?} bound \
         (took {finished:?}); the bound is a ceiling, not a delay"
    );

    drop(holder);
    let _ = std::fs::remove_file(&path);
}

/// `src/main.rs` must bound its runtime's shutdown rather than let it be dropped implicitly.
///
/// Asserted against the source text because there is no other way to bind it: `fn main` cannot
/// be called from a test, and the behaviour it fixes is a wedge. Its red side is the real
/// defect — restore `#[tokio::main]` and this fails.
#[test]
fn main_builds_its_runtime_with_a_bounded_shutdown() {
    let source = include_str!("../src/main.rs");
    // Matched as an attribute on its own line, not as a substring: the file explains in prose
    // why it does not use the macro, and a `contains` would match that explanation.
    assert!(
        !source
            .lines()
            .any(|line| line.trim_start() == "#[tokio::main]"),
        "the `tokio::main` attribute drops the runtime implicitly, and that drop is an untimed \
         wait on the blocking pool — i.e. process exit waits on a lock a peer instance may hold"
    );
    assert!(
        source.contains(".shutdown_timeout("),
        "`fn main` must end its runtime with `shutdown_timeout`, bounding the blocking-pool wait"
    );
    assert!(
        source.contains("new_multi_thread"),
        "the explicit runtime must keep the macro's multi-threaded flavour"
    );
    assert!(
        source.contains("enable_all"),
        "the explicit runtime must keep the macro's `enable_all()` — without it the timer and \
         I/O drivers are absent and the proxy does not run at all"
    );
}
