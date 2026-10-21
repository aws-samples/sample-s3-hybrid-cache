//! Per-proxy shared-volume stall breaker.
//!
//! caps the cache work on one write's response at
//! [`crate::cache::WRITE_CACHE_DELAY_BUDGET`]. A DeleteObjects draws on its own budget instead,
//! `DELETE_OBJECTS_CACHE_DELAY_BUDGET` in `src/http_proxy.rs` (3 s after S3 answers), which its
//! origin-marker read and its whole batch of invalidations share. Under a sustained stall
//! of the shared cache volume every write would still pay its whole budget. This breaker notices
//! the stall and, while it is
//! open, writes skip their volume steps with no added delay and GETs and HEADs go to S3 uncached.
//!
//! - **Trip.** [`TRIP_AFTER`] write responses in a row whose cache waits ran out of their bound
//!   ([`crate::cache::CacheDelayBudget`] reports at most one per response), all within
//!   [`TRIP_WINDOW`] of the first, with no volume step finishing in between. The cache waits
//!   that timed out must have started at least [`TRIP_MIN_SPACING`] apart: waits that started
//!   together and ran out together saw one pause, which counts once however many writes it
//!   held and whenever those writes arrived. One slow
//!   request cannot trip it, and neither can two isolated ones minutes apart. A completed
//!   invalidation resets the count. A DeleteObjects counts once, when its batch ends, so serial
//!   stalled DeleteObjects trip the breaker only while the time each request spends beyond its
//!   3 s is under about 2 s: their three counts must fall within [`TRIP_WINDOW`] of the first.
//! - **While open.** Every write's budget is zero, so a cache wait is polled once and an answer
//!   already in memory is still used. The signed and unsigned PUT routers forward the write
//!   uncached. Invalidations clear this proxy's in-memory tiers and go straight to the background
//!   stamp retry without touching the volume, so this proxy forwards the key's GET and HEAD to S3
//!   until the retry lands. Every other GET and HEAD is forwarded to S3 uncached as well.
//! - **Close.** A background probe writes one small file under `locks/` with a
//!   [`PROBE_BOUND`] deadline, backing off from [`PROBE_INITIAL_BACKOFF`] to
//!   [`PROBE_MAX_BACKOFF`] while it fails. It closes the breaker after [`CLOSE_AFTER`]
//!   consecutive successes, [`PROBE_CONFIRM_INTERVAL`] apart, so the volume must answer for at
//!   least a second. A probe still blocked from an earlier attempt counts as a failure and no
//!   second one is started, so a hung mount holds at most one blocking thread for the probe.
//!   The stamp retries then land on their own backoff.
//!
//! In memory and per process; a peer proxy decides for itself.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::{info, warn};

/// Write responses in a row whose cache waits timed out before the breaker trips.
pub const TRIP_AFTER: u32 = 3;
/// The timeouts that trip the breaker must all fall within this long of the first.
pub const TRIP_WINDOW: Duration = Duration::from_secs(10);
/// Counted timeouts come from cache waits that started at least this far apart. A timeout from
/// a wait that started closer than this to an already-counted one is the same pause, and does
/// not count again. So a trip needs a stall that held waits started across at least
/// `(TRIP_AFTER - 1) * TRIP_MIN_SPACING`, plus the shortest wait bound
/// ([`crate::cache::PUT_ROUTER_READ_BOUND`], 250 ms): a pause of about 1.25 s.
pub const TRIP_MIN_SPACING: Duration = Duration::from_millis(500);
/// Consecutive probe successes that close the breaker.
pub const CLOSE_AFTER: u32 = 3;
/// How long one probe may take before it counts as a failure.
pub const PROBE_BOUND: Duration = Duration::from_millis(250);
/// The first wait between failing probes, doubled up to [`PROBE_MAX_BACKOFF`].
pub const PROBE_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
/// The longest wait between failing probes.
pub const PROBE_MAX_BACKOFF: Duration = Duration::from_secs(5);
/// The wait between probes once one has succeeded.
pub const PROBE_CONFIRM_INTERVAL: Duration = Duration::from_millis(500);

/// Breakers open now in this process. One per cache manager, so `0` or `1` in production.
static OPEN_NOW: AtomicU64 = AtomicU64::new(0);
/// Times a breaker in this process has opened.
static TRIPS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// `1` while this proxy's volume-stall breaker is open (`cache.volume_stall_breaker_open`).
pub(crate) fn breaker_open() -> u64 {
    OPEN_NOW.load(Ordering::Relaxed)
}
/// Times this proxy's volume-stall breaker has opened (`cache.volume_stall_breaker_trips_total`).
pub(crate) fn breaker_trips_total() -> u64 {
    TRIPS_TOTAL.load(Ordering::Relaxed)
}

#[derive(Debug, Default)]
struct Streak {
    count: u32,
    first: Option<Instant>,
    /// When each counted timeout's cache wait started.
    starts: Vec<Instant>,
}

/// How far apart two instants are, in either order.
fn spacing(a: Instant, b: Instant) -> Duration {
    a.saturating_duration_since(b)
        .max(b.saturating_duration_since(a))
}

/// One proxy's view of whether its shared cache volume is stalled. See the module docs.
#[derive(Debug)]
pub struct VolumeStallBreaker {
    cache_dir: PathBuf,
    open: AtomicBool,
    streak: Mutex<Streak>,
    trips: AtomicU64,
    probe_in_flight: Arc<AtomicBool>,
    /// Test seam: while set, every probe fails.
    probe_fails: AtomicBool,
    last_trip_warn_secs: AtomicU64,
    last_recover_info_secs: AtomicU64,
}

impl VolumeStallBreaker {
    /// A closed breaker for the volume behind `cache_dir`.
    pub fn new(cache_dir: PathBuf) -> Self {
        Self {
            cache_dir,
            open: AtomicBool::new(false),
            streak: Mutex::new(Streak::default()),
            trips: AtomicU64::new(0),
            probe_in_flight: Arc::new(AtomicBool::new(false)),
            probe_fails: AtomicBool::new(false),
            last_trip_warn_secs: AtomicU64::new(0),
            last_recover_info_secs: AtomicU64::new(0),
        }
    }

    /// Whether the breaker is open: the volume is treated as stalled.
    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// Times this breaker has opened.
    pub fn trips(&self) -> u64 {
        self.trips.load(Ordering::Relaxed)
    }

    /// Test seam: open the breaker now, as three timeouts would, and start the probe.
    #[doc(hidden)]
    pub fn open_for_tests(self: &Arc<Self>) {
        if !self.open.swap(true, Ordering::AcqRel) {
            self.trips.fetch_add(1, Ordering::Relaxed);
            TRIPS_TOTAL.fetch_add(1, Ordering::Relaxed);
            OPEN_NOW.fetch_add(1, Ordering::Relaxed);
            self.spawn_probe();
        }
    }

    /// Test seam: make every probe fail while `fail` is set, as a stalled volume would.
    #[doc(hidden)]
    pub fn set_probe_fails_for_tests(&self, fail: bool) {
        self.probe_fails.store(fail, Ordering::SeqCst);
    }

    fn lock_streak(&self) -> std::sync::MutexGuard<'_, Streak> {
        self.streak.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Count one write response whose cache wait timed out, at `now`, for a wait that started
    /// at `started`. `true` when this call opened the breaker. The caller starts the probe.
    pub(crate) fn record_timeout_at(&self, now: Instant, started: Instant) -> bool {
        if self.is_open() {
            return false;
        }
        let mut streak = self.lock_streak();
        match streak.first {
            Some(first) if now.saturating_duration_since(first) <= TRIP_WINDOW => {
                // Started together with a write already counted: the same pause.
                if streak
                    .starts
                    .iter()
                    .any(|s| spacing(*s, started) < TRIP_MIN_SPACING)
                {
                    return false;
                }
                streak.count += 1;
                streak.starts.push(started);
            }
            _ => {
                streak.first = Some(now);
                streak.count = 1;
                streak.starts.clear();
                streak.starts.push(started);
            }
        }
        if streak.count < TRIP_AFTER {
            return false;
        }
        *streak = Streak::default();
        if self.open.swap(true, Ordering::AcqRel) {
            return false;
        }
        self.trips.fetch_add(1, Ordering::Relaxed);
        TRIPS_TOTAL.fetch_add(1, Ordering::Relaxed);
        OPEN_NOW.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// A volume step finished within its bound: the timeouts so far were not a stall.
    pub(crate) fn record_success(&self) {
        if self.is_open() {
            return;
        }
        *self.lock_streak() = Streak::default();
    }

    /// [`Self::record_timeout_at`] now, for a cache wait that started at `started`; on a trip,
    /// log and start the probe.
    pub(crate) fn record_timeout(self: &Arc<Self>, started: Instant) {
        if self.record_timeout_at(Instant::now(), started) {
            if rate_limit(&self.last_trip_warn_secs) {
                warn!(
                    "Shared cache volume stalled: {} write responses in a row ran out of their \
                     cache wait within {:?}. Until the volume answers again, writes skip the \
                     cache and their invalidations go to the background retry, and GET and HEAD \
                     are forwarded to S3 uncached (cache.volume_stall_breaker_open = 1; at most \
                     one such WARN per minute): cache_dir={:?}",
                    TRIP_AFTER, TRIP_WINDOW, self.cache_dir
                );
            }
            self.spawn_probe();
        }
    }

    fn close(&self) {
        if self.open.swap(false, Ordering::AcqRel) {
            OPEN_NOW.fetch_sub(1, Ordering::Relaxed);
            *self.lock_streak() = Streak::default();
            if rate_limit(&self.last_recover_info_secs) {
                info!(
                    "Shared cache volume answering again after {} consecutive probes; caching \
                     resumes and pending invalidations drain (at most one such INFO per minute): \
                     cache_dir={:?}",
                    CLOSE_AFTER, self.cache_dir
                );
            }
        }
    }

    /// One bounded probe: write a small file under `locks/`. A probe still blocked from an
    /// earlier attempt is a failure, and no second one starts.
    async fn probe_once(&self) -> bool {
        if self.probe_fails.load(Ordering::SeqCst) {
            return false;
        }
        if self.probe_in_flight.swap(true, Ordering::AcqRel) {
            return false;
        }
        let in_flight = Arc::clone(&self.probe_in_flight);
        let dir = self.cache_dir.join("locks");
        let path = dir.join(format!(".volume-stall-probe-{}", std::process::id()));
        let task = tokio::task::spawn_blocking(move || {
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let ok = std::fs::create_dir_all(&dir)
                .and_then(|_| std::fs::write(&path, stamp.to_string()))
                .is_ok();
            in_flight.store(false, Ordering::Release);
            ok
        });
        matches!(tokio::time::timeout(PROBE_BOUND, task).await, Ok(Ok(true)))
    }

    fn spawn_probe(self: &Arc<Self>) {
        let me = Arc::clone(self);
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(async move {
            let mut backoff = PROBE_INITIAL_BACKOFF;
            let mut successes = 0u32;
            while me.is_open() {
                // Only this task holds the breaker: its manager is gone.
                if Arc::strong_count(&me) == 1 {
                    break;
                }
                let wait = if successes > 0 {
                    PROBE_CONFIRM_INTERVAL
                } else {
                    backoff
                };
                tokio::time::sleep(wait).await;
                if me.probe_once().await {
                    successes += 1;
                    backoff = PROBE_INITIAL_BACKOFF;
                    if successes >= CLOSE_AFTER {
                        me.close();
                    }
                } else {
                    successes = 0;
                    backoff = (backoff * 2).min(PROBE_MAX_BACKOFF);
                }
            }
        });
    }
}

impl Drop for VolumeStallBreaker {
    /// A breaker dropped while open no longer counts in `cache.volume_stall_breaker_open`.
    /// The probe task holds the breaker until it sees its manager gone, so this runs then.
    fn drop(&mut self) {
        if *self.open.get_mut() {
            OPEN_NOW.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Serialises every lib test that opens a breaker or reads [`breaker_open`]: `OPEN_NOW` is
/// process-global, and libtest runs a binary's tests as threads of one process. A test anywhere
/// in the crate that can open a breaker (directly, or through three timed-out writes on a
/// `CacheManager`) takes this for its whole body.
#[cfg(test)]
pub(crate) fn open_gauge_test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

/// `true` at most once a minute per `last`.
fn rate_limit(last: &AtomicU64) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let prev = last.load(Ordering::Relaxed);
    (prev == 0 || now.saturating_sub(prev) >= 60)
        && last
            .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn breaker() -> VolumeStallBreaker {
        VolumeStallBreaker::new(PathBuf::from("/nonexistent-case85"))
    }

    /// Every test here takes [`open_gauge_test_lock`]: each can open a breaker if the rule it
    /// checks regresses, and one reads the process-global gauge.
    fn serial() -> std::sync::MutexGuard<'static, ()> {
        open_gauge_test_lock()
    }

    /// A breaker dropped while open leaves the process gauge where it was before it
    /// opened.
    #[test]
    fn dropping_an_open_breaker_clears_the_open_gauge() {
        let _serial = serial();
        let before = breaker_open();
        let b = breaker();
        let t0 = Instant::now();
        for i in 0..3u32 {
            let t = t0 + Duration::from_secs(1) * i;
            b.record_timeout_at(t, t);
        }
        assert!(b.is_open());
        assert_eq!(
            breaker_open(),
            before + 1,
            "fixture: the open breaker counts"
        );
        drop(b);
        assert_eq!(
            breaker_open(),
            before,
            "a breaker dropped while open must no longer count as open"
        );
    }

    /// (d) One slow operation, or two, does not trip it; the third in the window does.
    #[test]
    fn trips_only_after_three_timeouts_in_the_window() {
        let _serial = serial();
        let b = breaker();
        let t0 = Instant::now();
        let at = |d: Duration| t0 + d;
        assert!(
            !b.record_timeout_at(t0, t0),
            "one slow operation must not trip the breaker"
        );
        let s1 = at(Duration::from_secs(1));
        assert!(!b.record_timeout_at(s1, s1));
        assert!(!b.is_open());
        let s2 = at(Duration::from_secs(2));
        assert!(b.record_timeout_at(s2, s2));
        assert!(b.is_open());
        assert_eq!(b.trips(), 1);
        b.close();
        assert!(!b.is_open());
    }

    /// (d) A volume step that finishes between two timeouts resets the count.
    #[test]
    fn a_success_between_timeouts_resets_the_count() {
        let _serial = serial();
        let b = breaker();
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        assert!(!b.record_timeout_at(at(0), at(0)));
        assert!(!b.record_timeout_at(at(600), at(600)));
        b.record_success();
        assert!(!b.record_timeout_at(at(1200), at(1200)));
        assert!(!b.record_timeout_at(at(1800), at(1800)));
        assert!(
            !b.is_open(),
            "two timeouts, a success and two more timeouts must not trip the breaker"
        );
    }

    /// Many writes that started together and ran out together are one pause. They
    /// count once; writes started `TRIP_MIN_SPACING` apart still trip it on the third.
    #[test]
    fn writes_that_started_together_count_as_one_timeout() {
        let _serial = serial();
        let b = breaker();
        let t0 = Instant::now();
        let done = t0 + Duration::from_millis(1050);
        for i in 0..20u64 {
            let started = t0 + Duration::from_millis(i * 10);
            assert!(
                !b.record_timeout_at(done, started),
                "concurrent write {} tripped the breaker",
                i
            );
        }
        assert!(
            !b.is_open(),
            "one pause held 20 concurrent writes: one event"
        );
        let s1 = t0 + TRIP_MIN_SPACING;
        assert!(!b.record_timeout_at(s1 + Duration::from_secs(1), s1));
        let s2 = s1 + TRIP_MIN_SPACING;
        assert!(
            b.record_timeout_at(s2 + Duration::from_secs(1), s2),
            "three writes started TRIP_MIN_SPACING apart must trip the breaker"
        );
    }

    /// (d) Timeouts spread wider than the window do not trip it.
    #[test]
    fn timeouts_further_apart_than_the_window_do_not_trip() {
        let _serial = serial();
        let b = breaker();
        let t0 = Instant::now();
        for i in 0..10u64 {
            let t = t0 + TRIP_WINDOW.mul_f64(0.6) * (i as u32);
            assert!(
                !b.record_timeout_at(t, t),
                "timeout {} tripped the breaker",
                i
            );
        }
        assert!(!b.is_open());
    }
}
