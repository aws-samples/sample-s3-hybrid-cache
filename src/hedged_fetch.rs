//! Hedged upstream request coordinator and cost governor.
//!
//! This module provides the process-global admission control for hedged requests
//! and the [`race_first_byte`] helper that races an original fetch against a
//! conditional hedge. A hedge is a second, identical upstream fetch issued when
//! the original is slow to return its first byte.
//!
//! The [`HedgeGovernor`] tracks in-flight fetches and hedges via atomic counters
//! and RAII guards, enforcing the per-instance `max_inflight_fraction` cap
//! (Requirement 6.2, 6.3).
//!
//! The module is self-contained with no dependencies on `config` or
//! `bucket_settings` — callers pass resolved values.

use crate::logging::mask_presigned_params;
use crate::s3_client::{S3ClientApi, S3RequestContext, S3Response};
use crate::ProxyError;

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::debug;

// ---------------------------------------------------------------------------
// Process-global singletons (same pattern as bandwidth_limiter::GLOBAL_LIMITER)
// ---------------------------------------------------------------------------

/// Process-global hedge governor, initialized once at startup.
static GLOBAL_GOVERNOR: OnceLock<Arc<HedgeGovernor>> = OnceLock::new();

/// Process-global hedge metrics, initialized once at startup.
static GLOBAL_METRICS: OnceLock<Arc<HedgeMetrics>> = OnceLock::new();

/// Initialize the process-global hedge governor and metrics.
///
/// Must be called once before any requests are served. Subsequent calls are
/// silently ignored (the existing instances are reused). Returns the shared
/// `Arc<HedgeMetrics>` so the caller can hand it to `MetricsManager`.
pub fn init_global_hedging() -> Arc<HedgeMetrics> {
    let _ = GLOBAL_GOVERNOR.set(Arc::new(HedgeGovernor::new()));
    let metrics = Arc::new(HedgeMetrics::new());
    let _ = GLOBAL_METRICS.set(Arc::clone(&metrics));
    // Return the metrics Arc even if the set was a no-op (idempotent).
    GLOBAL_METRICS.get().unwrap().clone()
}

/// Get the process-global hedge governor (returns `None` before `init_global_hedging`).
pub fn get_global_governor() -> Option<&'static Arc<HedgeGovernor>> {
    GLOBAL_GOVERNOR.get()
}

/// Get the process-global hedge metrics (returns `None` before `init_global_hedging`).
pub fn get_global_metrics() -> Option<&'static Arc<HedgeMetrics>> {
    GLOBAL_METRICS.get()
}

/// Process-global governor that tracks in-flight fetches and hedges.
///
/// The governor enforces the per-instance `max_inflight_fraction` cap: a new hedge
/// is admitted only when `(inflight_hedges + 1) / max(inflight_fetches, 1)` does
/// not exceed the configured fraction. This prevents hedging from amplifying
/// traffic beyond the operator's cost ceiling during latency spikes.
pub struct HedgeGovernor {
    inflight_fetches: AtomicU64,
    inflight_hedges: AtomicU64,
}

/// RAII guard that increments `inflight_fetches` on creation and decrements on drop.
pub struct FetchGuard<'a> {
    governor: &'a HedgeGovernor,
}

/// RAII guard that increments `inflight_hedges` on creation and decrements on drop.
pub struct HedgeGuard<'a> {
    governor: &'a HedgeGovernor,
}

impl HedgeGovernor {
    /// Create a new governor with both counters at zero.
    pub fn new() -> Self {
        Self {
            inflight_fetches: AtomicU64::new(0),
            inflight_hedges: AtomicU64::new(0),
        }
    }

    /// Begin a fetch and return a guard that tracks it.
    pub fn start_fetch(&self) -> FetchGuard<'_> {
        self.inflight_fetches.fetch_add(1, Ordering::Relaxed);
        FetchGuard { governor: self }
    }

    /// Attempt to admit a new hedge under the per-instance fraction cap.
    ///
    /// Returns `Some(HedgeGuard)` if admitted, `None` if suppressed.
    ///
    /// The first hedge is always admitted when no other hedges are in flight,
    /// regardless of the fraction cap. This prevents single-request workloads
    /// from being permanently suppressed (Requirement 6.2 says "cap the ratio
    /// to bound cost amplification", which only applies when there is meaningful
    /// traffic to amplify). At steady state with many in-flight fetches the
    /// fraction cap governs normally.
    pub fn try_admit_hedge(&self, max_inflight_fraction: f64) -> Option<HedgeGuard<'_>> {
        let fetches = self.inflight_fetches.load(Ordering::Relaxed);
        let hedges = self.inflight_hedges.load(Ordering::Relaxed);

        // Always admit the first hedge (no existing hedges in flight).
        if hedges == 0 {
            self.inflight_hedges.fetch_add(1, Ordering::Relaxed);
            return Some(HedgeGuard { governor: self });
        }

        // For subsequent hedges, enforce the fraction cap.
        let denominator = fetches.max(1) as f64;
        let proposed_ratio = (hedges + 1) as f64 / denominator;

        if proposed_ratio > max_inflight_fraction {
            return None;
        }

        self.inflight_hedges.fetch_add(1, Ordering::Relaxed);
        Some(HedgeGuard { governor: self })
    }

    /// Current number of in-flight fetches (for testing/metrics).
    pub fn inflight_fetches(&self) -> u64 {
        self.inflight_fetches.load(Ordering::Relaxed)
    }

    /// Current number of in-flight hedges (for testing/metrics).
    pub fn inflight_hedges(&self) -> u64 {
        self.inflight_hedges.load(Ordering::Relaxed)
    }
}

impl Default for HedgeGovernor {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> Drop for FetchGuard<'a> {
    fn drop(&mut self) {
        self.governor
            .inflight_fetches
            .fetch_sub(1, Ordering::Relaxed);
    }
}

impl<'a> Drop for HedgeGuard<'a> {
    fn drop(&mut self) {
        self.governor
            .inflight_hedges
            .fetch_sub(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Race outcome + metrics
// ---------------------------------------------------------------------------

/// Outcome of [`race_first_byte`].
pub enum RaceOutcome {
    /// One arm returned a response status (the Winner).
    Winner(S3Response),
    /// Both arms exceeded their `first_byte_timeout` without returning a
    /// status. Only possible when `first_byte_timeout` is `Some`.
    AllTimedOut,
    /// Both arms finished without producing a status (transport errors).
    Error(ProxyError),
}

/// Lightweight hedging metrics (task 6 will wire into `MetricsManager`).
pub struct HedgeMetrics {
    pub issued: AtomicU64,
    pub won: AtomicU64,
    pub suppressed: AtomicU64,
}

impl HedgeMetrics {
    pub fn new() -> Self {
        Self {
            issued: AtomicU64::new(0),
            won: AtomicU64::new(0),
            suppressed: AtomicU64::new(0),
        }
    }

    pub fn record_issued(&self) {
        self.issued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_won(&self) {
        self.won.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_suppressed(&self) {
        self.suppressed.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for HedgeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// race_first_byte — the hedge coordinator
// ---------------------------------------------------------------------------

/// Internal per-arm result type.
enum ArmResult {
    /// The arm returned a response (any status = Winner candidate).
    Response(S3Response),
    /// The arm's first-byte timeout elapsed (no status produced).
    TimedOut,
    /// Transport error before a status was returned.
    TransportError(ProxyError),
}

/// Race an original upstream fetch against an optional hedge.
///
/// Takes **resolved values** — the caller has already resolved
/// `hedge_trigger_after` and `hedge_max_per_request` from `ResolvedSettings`.
///
/// # Structure
///
/// Two pinned futures driven by `tokio::select!` inside a loop, with per-arm
/// "finished" flags. A flat `select!` would drop the losing branch on first
/// completion, breaking the no-status fallback (Req 5.4).
#[allow(clippy::too_many_arguments)]
pub async fn race_first_byte(
    s3_client: &(dyn S3ClientApi + Send + Sync),
    ctx: S3RequestContext,
    first_byte_timeout: Option<Duration>,
    trigger_after: Duration,
    hedge_budget: &AtomicUsize,
    ips: [Option<IpAddr>; 2],
    governor: &HedgeGovernor,
    max_inflight_fraction: f64,
    metrics: &HedgeMetrics,
    cache_key: &str,
) -> RaceOutcome {
    let start = std::time::Instant::now();
    let original_ip = ips[0];
    let hedge_ip = ips[1];
    let original_ctx = ctx.clone();
    let hedge_ctx = ctx;

    // --- Original arm future (with optional first-byte timeout) ---
    let original_fut = async {
        let result = s3_client
            .forward_request_pinned(original_ctx, original_ip)
            .await;
        match result {
            Ok(resp) => ArmResult::Response(resp),
            Err(e) => ArmResult::TransportError(e),
        }
    };
    let original_timed_fut = async {
        match first_byte_timeout {
            Some(t) => match tokio::time::timeout(t, original_fut).await {
                Ok(r) => r,
                Err(_) => ArmResult::TimedOut,
            },
            None => original_fut.await,
        }
    };

    // --- Hedge arm future (two-phase: sleep then conditional request) ---
    // Returns `Option<ArmResult>`: `None` means the hedge was never issued or
    // was suppressed — treat as "this arm is done with no result to offer".
    let hedge_two_phase_fut = async {
        // Phase 1: wait for the trigger delay.
        tokio::time::sleep(trigger_after).await;

        // Phase 2: check conditions and potentially issue.
        // Budget check (atomic decrement).
        let claimed = hedge_budget.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |val| {
            if val > 0 {
                Some(val - 1)
            } else {
                None
            }
        });
        if claimed.is_err() {
            metrics.record_suppressed();
            return None;
        }

        // Governor check.
        let hedge_guard = match governor.try_admit_hedge(max_inflight_fraction) {
            Some(g) => g,
            None => {
                // Restore the budget we claimed.
                hedge_budget.fetch_add(1, Ordering::Relaxed);
                metrics.record_suppressed();
                return None;
            }
        };

        // Issue the hedge.
        metrics.record_issued();
        debug!(
            "Hedged request issued for cache_key={}, elapsed_ttfb={}ms, pinned_ip={:?}",
            mask_presigned_params(cache_key),
            start.elapsed().as_millis(),
            hedge_ip,
        );

        // Run the hedge request with its own independent first-byte timeout.
        let hedge_result = async {
            let result = s3_client.forward_request_pinned(hedge_ctx, hedge_ip).await;
            match result {
                Ok(resp) => ArmResult::Response(resp),
                Err(e) => ArmResult::TransportError(e),
            }
        };

        let arm_result = match first_byte_timeout {
            Some(t) => match tokio::time::timeout(t, hedge_result).await {
                Ok(r) => r,
                Err(_) => ArmResult::TimedOut,
            },
            None => hedge_result.await,
        };

        // Keep guard alive until here.
        drop(hedge_guard);
        Some(arm_result)
    };

    // --- Select loop: pin both futures, track per-arm "done" flags ---
    tokio::pin!(original_timed_fut);
    tokio::pin!(hedge_two_phase_fut);

    let mut original_done = false;
    let mut hedge_done = false;
    let mut last_error: Option<ProxyError> = None;
    let mut all_timed_out = true; // remains true only if every finished arm was TimedOut

    loop {
        tokio::select! {
            result = &mut original_timed_fut, if !original_done => {
                original_done = true;
                match result {
                    ArmResult::Response(resp) => {
                        debug!(
                            "Original arm won for cache_key={}, ttfb={}ms, ip={:?}",
                            mask_presigned_params(cache_key),
                            start.elapsed().as_millis(),
                            original_ip,
                        );
                        return RaceOutcome::Winner(resp);
                    }
                    ArmResult::TimedOut => {
                        // No status — keep awaiting the hedge if it's still alive.
                    }
                    ArmResult::TransportError(e) => {
                        all_timed_out = false;
                        last_error = Some(e);
                    }
                }
            }

            result = &mut hedge_two_phase_fut, if !hedge_done => {
                hedge_done = true;
                match result {
                    Some(ArmResult::Response(resp)) => {
                        metrics.record_won();
                        debug!(
                            "Hedge arm won for cache_key={}, ttfb={}ms, ip={:?}",
                            mask_presigned_params(cache_key),
                            start.elapsed().as_millis(),
                            hedge_ip,
                        );
                        return RaceOutcome::Winner(resp);
                    }
                    Some(ArmResult::TimedOut) => {
                        // No status — keep awaiting the original if still alive.
                    }
                    Some(ArmResult::TransportError(e)) => {
                        all_timed_out = false;
                        last_error = Some(e);
                    }
                    None => {
                        // Hedge was never issued (suppressed or budget exhausted)
                        // or the original already won during the sleep. This arm
                        // did not produce a status but also did not time out;
                        // it's effectively absent from the race. Don't change
                        // `all_timed_out` — if the original timed out, that's
                        // still a timeout of the logical fetch.
                    }
                }
            }
        }

        // Both arms finished without producing a status.
        if original_done && hedge_done {
            if all_timed_out {
                return RaceOutcome::AllTimedOut;
            }
            return RaceOutcome::Error(last_error.unwrap_or_else(|| {
                ProxyError::ConnectionError("Both hedge arms failed without a status".to_string())
            }));
        }
    }
}

// ---------------------------------------------------------------------------
// Call-site helpers
// ---------------------------------------------------------------------------

/// Select up to two distinct healthy upstream IPs for `host`, mapped to the
/// `[original, hedge]` pin pair [`race_first_byte`] expects.
///
/// A short result is never an error and must never suppress a hedge
/// (Requirement 4.2, 4.4):
/// - 2 IPs → the arms are pinned to distinct IPs
/// - 1 IP  → both arms pin to it (still separate connections)
/// - 0 IPs → both arms run unpinned (no distributor yet, e.g. DNS not resolved)
pub async fn select_ip_pair(
    s3_client: &(dyn S3ClientApi + Send + Sync),
    host: &str,
) -> [Option<IpAddr>; 2] {
    let pool_manager = s3_client.get_connection_pool();
    let ips = pool_manager
        .read()
        .await
        .get_distinct_distributed_ips(host, 2);
    match ips.len() {
        0 => [None, None],
        1 => [Some(ips[0]), Some(ips[0])],
        _ => [Some(ips[0]), Some(ips[1])],
    }
}

/// Run a single upstream fetch that has no first-byte timeout of its own,
/// hedging it when `hedge_budget` is `Some` (i.e. a rule enabled hedging for
/// this key) and the process-global governor is initialized.
///
/// This is the shared entry point for the fetch paths that buffer or stream a
/// single upstream response and do **not** impose a first-byte timeout: the
/// complete-range-miss streaming path and each missing-range sub-fetch of a
/// partially cached range GET. `hedge_budget: None` takes the plain
/// `forward_request` path, byte-identical to pre-hedging behaviour
/// (Requirement 1.3).
///
/// Spec: hedged-upstream-requests Requirements 1.3, 2.3, 6.1, 6.5.
pub async fn fetch_maybe_hedged(
    s3_client: &(dyn S3ClientApi + Send + Sync),
    ctx: S3RequestContext,
    host: &str,
    hedge_budget: Option<&AtomicUsize>,
    trigger_after: Duration,
    max_inflight_fraction: f64,
    cache_key: &str,
) -> crate::Result<S3Response> {
    let budget = match hedge_budget {
        Some(b) => b,
        // Not rule-enabled — the existing path (Requirement 1.3).
        None => return s3_client.forward_request(ctx).await,
    };
    let (governor, metrics) = match (get_global_governor(), get_global_metrics()) {
        (Some(g), Some(m)) => (g, m),
        // Governor not initialized (should not happen in production) — degrade to
        // the unhedged path rather than failing the request.
        _ => return s3_client.forward_request(ctx).await,
    };

    let ips = select_ip_pair(s3_client, host).await;

    let _fetch_guard = governor.start_fetch();
    match race_first_byte(
        s3_client,
        ctx,
        // No first-byte timeout on these paths — do not introduce one
        // (Requirement 9.5). `AllTimedOut` is therefore unreachable.
        None,
        trigger_after,
        budget,
        ips,
        governor,
        max_inflight_fraction,
        metrics,
        cache_key,
    )
    .await
    {
        RaceOutcome::Winner(resp) => Ok(resp),
        RaceOutcome::Error(e) => Err(e),
        RaceOutcome::AllTimedOut => Err(ProxyError::ConnectionError(
            "hedged fetch reported AllTimedOut with no first-byte timeout configured".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_types::CacheMetadata;
    use crate::s3_client::S3ResponseBody;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread;
    use std::time::SystemTime;
    use tokio::time::Duration;

    // --- Test stub for S3ClientApi ---

    /// A configurable stub that can delay responses and optionally return errors.
    struct TestStub {
        delay: Duration,
        response: Result<S3Response, ProxyError>,
    }

    impl TestStub {
        fn ok_after(delay: Duration, status: u16) -> Self {
            Self {
                delay,
                response: Ok(make_response(status)),
            }
        }

        fn error_after(delay: Duration, err: ProxyError) -> Self {
            Self {
                delay,
                response: Err(err),
            }
        }
    }

    fn make_response(status: u16) -> S3Response {
        use hyper::StatusCode;
        S3Response {
            status: StatusCode::from_u16(status).unwrap(),
            headers: HashMap::new(),
            body: Some(S3ResponseBody::Buffered(Bytes::from("ok"))),
            request_duration: Duration::from_millis(1),
        }
    }

    /// A stub that serves different responses for original vs hedge (by IP).
    /// If no IP pin is provided, uses the original response.
    struct DualStub {
        original: TestStub,
        hedge: TestStub,
        original_ip: Option<IpAddr>,
        hedge_ip: Option<IpAddr>,
        call_count: AtomicUsize,
    }

    impl DualStub {
        fn new(
            original: TestStub,
            hedge: TestStub,
            original_ip: Option<IpAddr>,
            hedge_ip: Option<IpAddr>,
        ) -> Self {
            Self {
                original,
                hedge,
                original_ip,
                hedge_ip,
                call_count: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.call_count.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl S3ClientApi for DualStub {
        async fn forward_request(&self, _context: S3RequestContext) -> crate::Result<S3Response> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(self.original.delay).await;
            match &self.original.response {
                Ok(r) => Ok(make_response(r.status.as_u16())),
                Err(e) => Err(e.clone()),
            }
        }

        fn get_connection_pool(
            &self,
        ) -> Arc<tokio::sync::RwLock<crate::connection_pool::ConnectionPoolManager>> {
            unimplemented!("not used by race_first_byte")
        }

        fn has_endpoint_overrides(&self) -> bool {
            false
        }

        async fn set_metrics_manager(
            &self,
            _: Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>,
        ) {
        }

        async fn register_endpoint(&self, _: &str) {}
        async fn refresh_dns(&self) -> crate::Result<()> {
            Ok(())
        }

        async fn forward_request_pinned(
            &self,
            _context: S3RequestContext,
            pinned_ip: Option<IpAddr>,
        ) -> crate::Result<S3Response> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            // Determine which stub to use based on the pinned IP.
            let is_hedge = pinned_ip == self.hedge_ip && self.hedge_ip != self.original_ip;
            let stub = if is_hedge {
                &self.hedge
            } else {
                &self.original
            };
            tokio::time::sleep(stub.delay).await;
            match &stub.response {
                Ok(r) => Ok(make_response(r.status.as_u16())),
                Err(e) => Err(e.clone()),
            }
        }

        fn extract_metadata_from_response(
            &self,
            _headers: &HashMap<String, String>,
        ) -> CacheMetadata {
            CacheMetadata {
                etag: String::new(),
                last_modified: String::new(),
                content_length: 0,
                part_number: None,
                cache_control: None,
                access_count: 0,
                last_accessed: SystemTime::now(),
            }
        }

        fn extract_object_metadata_from_response(
            &self,
            _headers: &HashMap<String, String>,
        ) -> crate::cache_types::ObjectMetadata {
            crate::cache_types::ObjectMetadata::default()
        }
    }

    // --- Test helpers ---

    fn test_ctx() -> S3RequestContext {
        use hyper::{Method, Uri};
        S3RequestContext {
            method: Method::GET,
            uri: Uri::from_static("http://bucket.s3.amazonaws.com/key"),
            headers: HashMap::new(),
            body: None,
            host: "bucket.s3.amazonaws.com".to_string(),
            request_size: None,
            operation_type: None,
            allow_streaming: true,
        }
    }

    fn ip_a() -> IpAddr {
        "10.0.0.1".parse().unwrap()
    }

    fn ip_b() -> IpAddr {
        "10.0.0.2".parse().unwrap()
    }

    // --- Governor tests (from task 4, preserved) ---

    #[test]
    fn fetch_guard_increments_and_decrements() {
        let gov = HedgeGovernor::new();
        assert_eq!(gov.inflight_fetches(), 0);
        let guard = gov.start_fetch();
        assert_eq!(gov.inflight_fetches(), 1);
        let guard2 = gov.start_fetch();
        assert_eq!(gov.inflight_fetches(), 2);
        drop(guard);
        assert_eq!(gov.inflight_fetches(), 1);
        drop(guard2);
        assert_eq!(gov.inflight_fetches(), 0);
    }

    #[test]
    fn hedge_guard_increments_and_decrements() {
        let gov = HedgeGovernor::new();
        let _f1 = gov.start_fetch();
        let _f2 = gov.start_fetch();
        let _f3 = gov.start_fetch();
        let hedge = gov.try_admit_hedge(1.0).expect("should admit");
        assert_eq!(gov.inflight_hedges(), 1);
        let hedge2 = gov.try_admit_hedge(1.0).expect("should admit");
        assert_eq!(gov.inflight_hedges(), 2);
        drop(hedge);
        assert_eq!(gov.inflight_hedges(), 1);
        drop(hedge2);
        assert_eq!(gov.inflight_hedges(), 0);
    }

    #[test]
    fn admits_under_cap() {
        let gov = HedgeGovernor::new();
        for _ in 0..10 {
            std::mem::forget(gov.start_fetch());
        }
        assert!(gov.try_admit_hedge(0.1).is_some());
    }

    #[test]
    fn suppresses_over_cap() {
        let gov = HedgeGovernor::new();
        for _ in 0..10 {
            std::mem::forget(gov.start_fetch());
        }
        // First hedge is always admitted (first-is-free rule).
        let _h1 = gov.try_admit_hedge(0.1).expect("first admitted");
        // Second hedge: (1+1)/10 = 0.2 > 0.1 → suppressed.
        assert!(gov.try_admit_hedge(0.1).is_none());
    }

    #[test]
    fn zero_fetches_first_hedge_always_admitted() {
        let gov = HedgeGovernor::new();
        // First hedge is always admitted even with 0 fetches and a low fraction.
        let h1 = gov.try_admit_hedge(0.1);
        assert!(h1.is_some());
        // Second hedge with 0 fetches and 1 hedge in-flight:
        // hedges > 0 → uses fraction check: (1+1)/max(0,1) = 2.0 > 0.1 → suppressed.
        assert!(gov.try_admit_hedge(0.1).is_none());
        // Drop the first guard.
        drop(h1);
        // Now hedges=0 again, so next attempt uses first-is-free.
        assert!(gov.try_admit_hedge(0.1).is_some());
    }

    #[test]
    fn concurrent_admit_is_race_free() {
        let gov = Arc::new(HedgeGovernor::new());
        for _ in 0..100 {
            std::mem::forget(gov.start_fetch());
        }
        let num_threads = 20;
        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let gov = Arc::clone(&gov);
                thread::spawn(move || {
                    let mut admitted = Vec::new();
                    for _ in 0..50 {
                        if let Some(guard) = gov.try_admit_hedge(0.5) {
                            admitted.push(guard);
                        }
                    }
                    admitted.len()
                })
            })
            .collect();
        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(gov.inflight_hedges(), 0);
        assert!(total > 0);
    }

    // --- race_first_byte tests ---

    #[tokio::test]
    async fn fast_original_no_hedge() {
        // Req 3.3: original returns before trigger_after → no hedge issued.
        let stub = DualStub::new(
            TestStub::ok_after(Duration::from_millis(10), 200),
            TestStub::ok_after(Duration::from_millis(10), 200),
            Some(ip_a()),
            Some(ip_b()),
        );
        let gov = HedgeGovernor::new();
        let _fg = gov.start_fetch();
        let metrics = HedgeMetrics::new();
        let budget = AtomicUsize::new(1);

        let result = race_first_byte(
            &stub,
            test_ctx(),
            Some(Duration::from_secs(5)),
            Duration::from_millis(100), // trigger well after original responds
            &budget,
            [Some(ip_a()), Some(ip_b())],
            &gov,
            1.0,
            &metrics,
            "test-key",
        )
        .await;

        assert!(matches!(result, RaceOutcome::Winner(r) if r.status == 200));
        assert_eq!(metrics.issued.load(Ordering::Relaxed), 0);
        assert_eq!(stub.calls(), 1); // only the original was called
    }

    #[tokio::test]
    async fn slow_original_hedge_wins() {
        // Req 3.2, 8.2: original slow → hedge issued, hedge wins, `won` recorded.
        let stub = DualStub::new(
            TestStub::ok_after(Duration::from_millis(500), 200), // slow original
            TestStub::ok_after(Duration::from_millis(10), 200),  // fast hedge
            Some(ip_a()),
            Some(ip_b()),
        );
        let gov = HedgeGovernor::new();
        let _fg = gov.start_fetch();
        let metrics = HedgeMetrics::new();
        let budget = AtomicUsize::new(1);

        let result = race_first_byte(
            &stub,
            test_ctx(),
            Some(Duration::from_secs(5)),
            Duration::from_millis(50), // trigger at 50ms
            &budget,
            [Some(ip_a()), Some(ip_b())],
            &gov,
            1.0,
            &metrics,
            "test-key",
        )
        .await;

        assert!(matches!(result, RaceOutcome::Winner(r) if r.status == 200));
        assert_eq!(metrics.issued.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.won.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn suppressed_over_fraction() {
        // Req 6.2/6.3: governor fraction exceeded → suppressed, original served.
        // Pre-load one hedge so the "first-is-free" rule is exhausted and the
        // fraction cap actually governs.
        let stub = DualStub::new(
            TestStub::ok_after(Duration::from_millis(200), 200),
            TestStub::ok_after(Duration::from_millis(10), 200),
            Some(ip_a()),
            Some(ip_b()),
        );
        let gov = HedgeGovernor::new();
        let _fg = gov.start_fetch();
        // Consume the "first-is-free" slot so race_first_byte faces the real cap.
        let _preload_hedge = gov.try_admit_hedge(1.0).unwrap();
        let metrics = HedgeMetrics::new();
        let budget = AtomicUsize::new(1);

        // fraction 0.0 means no further hedges allowed
        let result = race_first_byte(
            &stub,
            test_ctx(),
            Some(Duration::from_secs(5)),
            Duration::from_millis(50),
            &budget,
            [Some(ip_a()), Some(ip_b())],
            &gov,
            0.0, // impossible to admit (hedges already > 0)
            &metrics,
            "test-key",
        )
        .await;

        assert!(matches!(result, RaceOutcome::Winner(r) if r.status == 200));
        assert_eq!(metrics.issued.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.suppressed.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn budget_zero_never_hedges() {
        // Budget of 0 → never hedges.
        let stub = DualStub::new(
            TestStub::ok_after(Duration::from_millis(200), 200),
            TestStub::ok_after(Duration::from_millis(10), 200),
            Some(ip_a()),
            Some(ip_b()),
        );
        let gov = HedgeGovernor::new();
        let _fg = gov.start_fetch();
        let metrics = HedgeMetrics::new();
        let budget = AtomicUsize::new(0); // zero budget

        let result = race_first_byte(
            &stub,
            test_ctx(),
            Some(Duration::from_secs(5)),
            Duration::from_millis(50),
            &budget,
            [Some(ip_a()), Some(ip_b())],
            &gov,
            1.0,
            &metrics,
            "test-key",
        )
        .await;

        assert!(matches!(result, RaceOutcome::Winner(r) if r.status == 200));
        assert_eq!(metrics.issued.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.suppressed.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn slow_then_404_original_wins_unaltered() {
        // Req 5.1: slow original returns 404 before hedge → 404 is the Winner.
        let stub = DualStub::new(
            TestStub::ok_after(Duration::from_millis(80), 404), // slow-ish 404
            TestStub::ok_after(Duration::from_millis(200), 200), // hedge slower
            Some(ip_a()),
            Some(ip_b()),
        );
        let gov = HedgeGovernor::new();
        let _fg = gov.start_fetch();
        let metrics = HedgeMetrics::new();
        let budget = AtomicUsize::new(1);

        let result = race_first_byte(
            &stub,
            test_ctx(),
            Some(Duration::from_secs(5)),
            Duration::from_millis(50), // trigger at 50ms, original returns at 80ms
            &budget,
            [Some(ip_a()), Some(ip_b())],
            &gov,
            1.0,
            &metrics,
            "test-key",
        )
        .await;

        // The 404 wins — any status is a Winner (Req 5.1).
        assert!(matches!(result, RaceOutcome::Winner(r) if r.status == 404));
        // Hedge was issued (original was still pending at trigger_after=50ms).
        assert_eq!(metrics.issued.load(Ordering::Relaxed), 1);
        // But hedge did NOT win (original won with 404).
        assert_eq!(metrics.won.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn original_error_hedge_becomes_winner() {
        // Req 5.4: original transport error + hedge in flight → hedge wins.
        let stub = DualStub::new(
            TestStub::error_after(
                Duration::from_millis(10),
                ProxyError::ConnectionError("reset".into()),
            ),
            TestStub::ok_after(Duration::from_millis(100), 200),
            Some(ip_a()),
            Some(ip_b()),
        );
        let gov = HedgeGovernor::new();
        let _fg = gov.start_fetch();
        let metrics = HedgeMetrics::new();
        let budget = AtomicUsize::new(1);

        // trigger_after=5ms so the hedge starts before original errors at 10ms
        let result = race_first_byte(
            &stub,
            test_ctx(),
            Some(Duration::from_secs(5)),
            Duration::from_millis(5),
            &budget,
            [Some(ip_a()), Some(ip_b())],
            &gov,
            1.0,
            &metrics,
            "test-key",
        )
        .await;

        assert!(matches!(result, RaceOutcome::Winner(r) if r.status == 200));
        assert_eq!(metrics.issued.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.won.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn both_arms_error_returns_error() {
        // Both arms fail without a status → Error.
        let stub = DualStub::new(
            TestStub::error_after(
                Duration::from_millis(10),
                ProxyError::ConnectionError("original reset".into()),
            ),
            TestStub::error_after(
                Duration::from_millis(30),
                ProxyError::ConnectionError("hedge reset".into()),
            ),
            Some(ip_a()),
            Some(ip_b()),
        );
        let gov = HedgeGovernor::new();
        let _fg = gov.start_fetch();
        let metrics = HedgeMetrics::new();
        let budget = AtomicUsize::new(1);

        let result = race_first_byte(
            &stub,
            test_ctx(),
            Some(Duration::from_secs(5)),
            Duration::from_millis(5),
            &budget,
            [Some(ip_a()), Some(ip_b())],
            &gov,
            1.0,
            &metrics,
            "test-key",
        )
        .await;

        assert!(matches!(result, RaceOutcome::Error(_)));
    }

    #[tokio::test]
    async fn both_stall_returns_all_timed_out() {
        // Both arms exceed first_byte_timeout → AllTimedOut.
        let stub = DualStub::new(
            TestStub::ok_after(Duration::from_secs(10), 200), // will time out
            TestStub::ok_after(Duration::from_secs(10), 200), // will time out
            Some(ip_a()),
            Some(ip_b()),
        );
        let gov = HedgeGovernor::new();
        let _fg = gov.start_fetch();
        let metrics = HedgeMetrics::new();
        let budget = AtomicUsize::new(1);

        let result = race_first_byte(
            &stub,
            test_ctx(),
            Some(Duration::from_millis(100)), // short timeout
            Duration::from_millis(20),
            &budget,
            [Some(ip_a()), Some(ip_b())],
            &gov,
            1.0,
            &metrics,
            "test-key",
        )
        .await;

        assert!(matches!(result, RaceOutcome::AllTimedOut));
        assert_eq!(metrics.issued.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn no_first_byte_timeout_all_timed_out_cannot_occur() {
        // When first_byte_timeout is None, AllTimedOut cannot occur — arms are
        // only bounded by the request_timeout (simulated by eventually completing).
        let stub = DualStub::new(
            TestStub::ok_after(Duration::from_millis(200), 200),
            TestStub::ok_after(Duration::from_millis(100), 200),
            Some(ip_a()),
            Some(ip_b()),
        );
        let gov = HedgeGovernor::new();
        let _fg = gov.start_fetch();
        let metrics = HedgeMetrics::new();
        let budget = AtomicUsize::new(1);

        let result = race_first_byte(
            &stub,
            test_ctx(),
            None, // no first-byte timeout
            Duration::from_millis(50),
            &budget,
            [Some(ip_a()), Some(ip_b())],
            &gov,
            1.0,
            &metrics,
            "test-key",
        )
        .await;

        // Hedge wins (faster than original).
        assert!(matches!(result, RaceOutcome::Winner(r) if r.status == 200));
        assert_eq!(metrics.issued.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.won.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn exactly_one_response_property() {
        // Req 5.5: exactly one client response per original request, regardless
        // of which arm wins. Run multiple scenarios and confirm each produces
        // exactly one Winner or one terminal outcome.
        let scenarios: Vec<(TestStub, TestStub, &str)> = vec![
            (
                TestStub::ok_after(Duration::from_millis(5), 200),
                TestStub::ok_after(Duration::from_millis(5), 200),
                "both fast",
            ),
            (
                TestStub::ok_after(Duration::from_millis(200), 200),
                TestStub::ok_after(Duration::from_millis(5), 200),
                "original slow, hedge fast",
            ),
            (
                TestStub::ok_after(Duration::from_millis(5), 200),
                TestStub::ok_after(Duration::from_millis(200), 200),
                "original fast, hedge slow",
            ),
            (
                TestStub::ok_after(Duration::from_millis(5), 404),
                TestStub::ok_after(Duration::from_millis(5), 200),
                "original 404 fast",
            ),
            (
                TestStub::error_after(
                    Duration::from_millis(5),
                    ProxyError::ConnectionError("err".into()),
                ),
                TestStub::ok_after(Duration::from_millis(50), 200),
                "original error, hedge ok",
            ),
        ];

        for (original, hedge, label) in scenarios {
            let stub = DualStub::new(original, hedge, Some(ip_a()), Some(ip_b()));
            let gov = HedgeGovernor::new();
            let _fg = gov.start_fetch();
            let metrics = HedgeMetrics::new();
            let budget = AtomicUsize::new(1);

            let result = race_first_byte(
                &stub,
                test_ctx(),
                Some(Duration::from_secs(5)),
                Duration::from_millis(20),
                &budget,
                [Some(ip_a()), Some(ip_b())],
                &gov,
                1.0,
                &metrics,
                "test-key",
            )
            .await;

            // Exactly one outcome per scenario.
            match result {
                RaceOutcome::Winner(_) => {} // exactly one response
                RaceOutcome::AllTimedOut => {
                    panic!("{}: unexpected AllTimedOut", label);
                }
                RaceOutcome::Error(_) => {
                    panic!("{}: unexpected Error", label);
                }
            }
        }
    }
}
