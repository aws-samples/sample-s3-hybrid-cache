//! A staging eviction must leave `write_cache.inflight_bytes` alone.
//!
//! `inflight_bytes` is this proxy's total of upload reservations still held
//! (`WriteCacheManager::current_usage`, published by `MetricsManager::collect_write_cache_metrics`).
//! Staged bytes on disk were never in it: a reservation is released when its upload finishes. A
//! staging eviction that released the bytes it freed from that counter drove it to zero under a
//! live upload, logged a capacity underflow, and made the upload's own release underflow again.

use super::staging_pass_pending_lock_timeout_test::fixture;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

#[derive(Clone)]
struct Sink(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Sink {
    type Writer = Sink;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// An upload's reservation held across a staging pass that evicts more than it.
///
/// The underflow WARN is rate-limited process-wide to one a minute, so its absence alone could be
/// a neighbouring test's warning suppressing this one. The gauge assertions do not depend on it.
// One worker thread: the test future is polled on the test thread, where the log capture is
// installed, and the eviction and the reservation's release both run inside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_staging_eviction_leaves_a_held_upload_reservation_in_inflight_bytes() {
    let sink = Sink(Arc::new(Mutex::new(Vec::new())));
    let _log = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .with_max_level(tracing::Level::WARN)
            .without_time()
            .with_ansi(false)
            .finish(),
    );
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (m, _keys) = fixture(dir, 7).await;
    let m = Arc::new(m);
    let mut metrics_manager = s3_proxy::metrics::MetricsManager::new();
    metrics_manager.set_cache_manager(Arc::clone(&m));

    const HELD: u64 = 1024 * 1024;
    let underflows_before = m.write_cache_underflows_for_tests().await;
    let reservation = m
        .try_reserve_write_cache(HELD)
        .await
        .expect("fixture: the upload's reservation is admitted");
    assert_eq!(
        m.get_write_cache_manager_gauges().await.0,
        HELD,
        "fixture: the held reservation is in flight"
    );

    let freed = m.evict_staging_tier().await;
    assert!(
        freed > HELD,
        "fixture: the pass must free more than the held reservation (freed={freed}, \
         held={HELD}), or a release of the freed bytes would not show"
    );
    let gauge = m.get_write_cache_manager_gauges().await.0;
    let published = metrics_manager
        .collect_metrics()
        .await
        .write_cache
        .inflight_bytes;
    drop(reservation);
    let after_release = m.get_write_cache_manager_gauges().await.0;
    let logged = String::from_utf8(sink.0.lock().unwrap().clone()).unwrap();
    let underflows = m.write_cache_underflows_for_tests().await - underflows_before;

    assert_eq!(
        (gauge, published),
        (HELD, HELD),
        "a staging eviction that freed {freed} bytes moved the in-flight figure under a held \
         {HELD}-byte reservation (WriteCacheManager::current_usage={gauge}, \
         /metrics write_cache.inflight_bytes={published})"
    );
    assert_eq!(
        after_release, 0,
        "the upload's own release must return the figure to zero"
    );
    assert_eq!(
        underflows, 0,
        "a staging eviction and a reservation release underflowed the capacity counter, \
         whether or not the rate-limited WARN fired"
    );
    assert!(
        !logged.contains("capacity underflow"),
        "a staging eviction and a reservation release logged a capacity underflow:\n{logged}"
    );
}
