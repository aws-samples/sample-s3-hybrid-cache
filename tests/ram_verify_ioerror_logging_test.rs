//! Part B — `CacheManager::ram_verify` warns once per FAILED `.meta` read, not once
//! per request that reuses the failure.
//!
//! `ram_verify` coalesces verifications per key: inside
//! `ram_cache_verification_interval` a second caller reuses the stored
//! `RamVerifyDiskCheck` instead of reading again. The `IoError` arm used to `warn!` on
//! every call, so during a shared-storage outage the log showed one failed read per
//! request while one read per key per interval happened. The reused path now logs at
//! `debug!`; the `warn!` stays on the call that performed the failing read.
//!
//! # How the log is observed
//!
//! A thread-local subscriber (`tracing::subscriber::set_default`) writing into a buffer.
//! The test body runs on the runtime's `block_on` thread, and `ram_verify` logs from the
//! future it polls there, so the thread-local default sees every line. Lines are filtered
//! to this test's key, so nothing another component logs can move the counts.
//!
//! # Why the reads are counted as well as the lines
//!
//! "One warning" is only the right answer if exactly one read happened. The injected read
//! counts itself and later calls use a closure that panics, so a test that accidentally
//! read three times — and correctly warned three times — cannot pass.

use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use s3_proxy::cache::{CacheManager, RamVerifyDiskCheck};
use s3_proxy::compression::CompressionAlgorithm;
use tempfile::TempDir;

const KEY: &str = "verify-bucket/scn1-io-error-logging.bin";
const ETAG: &str = "\"version-a\"";
const INTERVAL: Duration = Duration::from_secs(2);
const WARN_TEXT: &str = "RAM verify could not read shared metadata for";
const REUSE_TEXT: &str = "RAM verify reusing a shared-metadata read failure for";

#[derive(Clone)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn must_not_read(_k: String) -> RamVerifyDiskCheck {
    panic!(
        "FIXTURE: ram_verify read .meta again inside the verification interval, so the \
         reused-result path this test is about was not reached"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reused_io_error_logs_debug_and_only_the_failed_read_warns() {
    let buf = Buffer(Arc::new(Mutex::new(Vec::new())));
    let writer = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let dir = TempDir::new().expect("temp dir");
    let mut cm = CacheManager::new(dir.path().to_path_buf(), true, 8 * 1024 * 1024, 1024, true);
    cm.set_ram_cache_verification_interval(INTERVAL);
    let cm = Arc::new(cm);

    assert!(
        cm.promote_range_to_ram_cache_frame(
            KEY,
            (0, 999),
            vec![7u8; 1000],
            CompressionAlgorithm::None,
            ETAG.to_string(),
            String::new(),
            1u64 << 40,
        ),
        "PRECONDITION MISSING: the range was not promoted to RAM"
    );
    let hit = cm
        .get_range_from_ram_cache_hit(KEY, 0, 999, Duration::from_secs(3600))
        .expect("PRECONDITION MISSING: the promoted range is not resident");

    // The hit must be past the interval, or ram_verify returns before the lock and
    // neither log line can be reached.
    tokio::time::sleep(INTERVAL + Duration::from_millis(100)).await;
    assert!(
        now_ms().saturating_sub(hit.verified_at_ms) > INTERVAL.as_millis() as u64,
        "PRECONDITION MISSING: the RAM hit is still inside the verification interval"
    );

    let reads = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&reads);
    assert!(
        cm.ram_verify(KEY, &hit, move |_k: String| async move {
            counted.fetch_add(1, Ordering::SeqCst);
            RamVerifyDiskCheck::IoError
        })
        .await,
        "the IoError arm fails open"
    );
    // IoError leaves verified_at untouched, so these calls reach the coalescing lock,
    // find the stored failure inside the interval, and reuse it.
    for _ in 0..3 {
        assert!(cm.ram_verify(KEY, &hit, must_not_read).await);
    }
    assert_eq!(
        reads.load(Ordering::SeqCst),
        1,
        "FIXTURE: exactly one .meta read"
    );

    let log = String::from_utf8(buf.0.lock().unwrap().clone()).expect("utf8 log");
    let lines: Vec<&str> = log.lines().filter(|l| l.contains(KEY)).collect();
    let warns: Vec<&&str> = lines
        .iter()
        .filter(|l| l.contains(" WARN ") && l.contains(WARN_TEXT))
        .collect();
    let reuse_debugs: Vec<&&str> = lines
        .iter()
        .filter(|l| l.contains("DEBUG") && l.contains(REUSE_TEXT))
        .collect();

    assert_eq!(
        warns.len(),
        1,
        "one failed .meta read must produce exactly one WARN; four requests produced {}. \
         A per-request warning overstates the failure rate during an outage.\n{}",
        warns.len(),
        lines.join("\n")
    );
    assert_eq!(
        reuse_debugs.len(),
        3,
        "each of the three reusing requests must log at DEBUG.\n{}",
        lines.join("\n")
    );
    assert!(
        !lines
            .iter()
            .any(|l| l.contains(" WARN ") && l.contains(REUSE_TEXT)),
        "the reused-result line must not be a warning"
    );
}
