//! `InflightLedger`: a process-wide, lock-free byte accounting ledger for
//! buffered request/response bodies.
//!
//! ## Why this exists
//!
//! The proxy bounds concurrency by **request count**
//! (`server.max_concurrent_requests`, enforced via `Semaphore` +
//! [`crate::permit_body::PermitBody`]) and bounds a single buffered body by
//! **size** (`server.max_buffered_request_body_bytes`). Neither sums live
//! buffered bytes *across* concurrent requests against a ceiling — the two
//! existing limits are independent of the actual resident-memory cost of
//! buffering.
//!
//! `InflightLedger` adds that missing dimension: a runtime account of bytes
//! currently reserved by Buffering_Sites (request bodies read into memory,
//! response bodies buffered because streaming isn't possible), consulted
//! immediately before an allocation. Admission is a grant-or-reject, never a
//! wait — the proxy already sheds rather than queues at
//! `max_concurrent_requests` (`try_acquire()`, not `acquire().await`), and this
//! ledger follows the same rule.
//!
//! ## Design
//!
//! - **Lock-free.** `reserved`, `peak`, `rejected_total`, and `aborted_total`
//!   are independent `AtomicU64`s. `try_reserve` is a CAS loop, mirroring
//!   [`crate::bandwidth_limiter::BandwidthLimiter::try_acquire`] — no lock, no
//!   task hop, no `await`.
//! - **Disabled by default.** `ceiling == 0` is `Ledger_Disabled`: `try_reserve`
//!   short-circuits after one relaxed load and returns a `Reservation` that
//!   accounts nothing, so an existing deployment gains no new behaviour on
//!   upgrade (config-compatibility.md).
//! - **RAII release.** [`Reservation::drop`] subtracts its held bytes from
//!   `reserved`. This is what guarantees release on every exit path — early
//!   `return`, `?` propagation, client disconnect, timeout, panic unwind —
//!   without any call site needing to remember to release explicitly.
//!
//! Spec: inflight-memory-accounting, Requirements 1.1, 1.4-1.7, 9.1-9.3, 9.5.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::warn;

/// Process-wide ledger of in-flight buffered bytes.
///
/// Cloned cheaply via `Arc`; pass the same `Arc<InflightLedger>` to every
/// Buffering_Site.
pub struct InflightLedger {
    /// Configured ceiling in bytes. `0` = Ledger_Disabled.
    ceiling: AtomicU64,
    /// Current total reserved bytes across all live `Reservation`s.
    reserved: AtomicU64,
    /// High-water mark of `reserved` observed since construction.
    peak: AtomicU64,
    /// Cumulative count of `try_reserve`/`try_grow` calls rejected because
    /// they would have exceeded the ceiling.
    rejected_total: AtomicU64,
    /// Cumulative count of Unknown_Size_Site accumulations aborted because a
    /// `try_grow` call was rejected mid-accumulation.
    aborted_total: AtomicU64,
    /// Unix timestamp (seconds) of the last rate-limited rejection log line.
    /// Mirrors the `LAST_503_LOG`/`CONCURRENCY_LAST_LOG` discipline in
    /// `http_proxy.rs::shed_request` (Requirement 2.6): at most one `warn!`
    /// per 60s, reporting how many rejections occurred in the elapsed window.
    last_rejection_log_secs: AtomicU64,
}

impl InflightLedger {
    /// Construct a ledger with the given ceiling (bytes). `0` means
    /// Ledger_Disabled.
    pub fn new(ceiling_bytes: u64) -> Self {
        Self {
            ceiling: AtomicU64::new(ceiling_bytes),
            reserved: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            rejected_total: AtomicU64::new(0),
            aborted_total: AtomicU64::new(0),
            last_rejection_log_secs: AtomicU64::new(0),
        }
    }

    /// Rate-limited rejection logging (Requirement 2.6): increments the
    /// rejection counter's contribution to the log window and emits at most
    /// one `warn!` per 60s naming the ceiling, the rejected request size, and
    /// how many rejections occurred since the last log line. Called by every
    /// Admission_Check rejection site instead of each site rolling its own
    /// rate limiter.
    fn log_rejection_rate_limited(&self, requested_bytes: u64) {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let last = self.last_rejection_log_secs.load(Ordering::Relaxed);
        if now_secs >= last + 60
            && self
                .last_rejection_log_secs
                .compare_exchange(last, now_secs, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            warn!(
                ceiling_bytes = self.ceiling.load(Ordering::Relaxed),
                requested_bytes,
                rejected_total = self.rejected_total.load(Ordering::Relaxed),
                "In-flight memory ceiling exceeded, rejecting with Shed_Response"
            );
        }
    }

    /// Construct a `Ledger_Disabled` ledger (ceiling = 0).
    pub fn disabled() -> Self {
        Self::new(0)
    }

    /// `true` when the ledger performs no accounting (ceiling == 0).
    #[inline]
    pub fn is_disabled(&self) -> bool {
        self.ceiling.load(Ordering::Relaxed) == 0
    }

    /// Configured ceiling in bytes (0 when disabled). Reported by the metrics
    /// endpoint even while disabled (Requirement 8.6).
    #[inline]
    pub fn ceiling_bytes(&self) -> u64 {
        self.ceiling.load(Ordering::Relaxed)
    }

    /// Current total reserved bytes.
    #[inline]
    pub fn reserved_bytes(&self) -> u64 {
        self.reserved.load(Ordering::Relaxed)
    }

    /// High-water mark of `reserved_bytes()` observed since construction.
    #[inline]
    pub fn peak_reserved_bytes(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// Cumulative Admission_Check rejections (Requirement 8.4).
    #[inline]
    pub fn rejected_total(&self) -> u64 {
        self.rejected_total.load(Ordering::Relaxed)
    }

    /// Cumulative aborted Unknown_Size_Site accumulations (Requirement 8.5).
    #[inline]
    pub fn aborted_total(&self) -> u64 {
        self.aborted_total.load(Ordering::Relaxed)
    }

    /// Attempt to reserve `bytes` against the ceiling.
    ///
    /// Returns `Some(Reservation)` when granted (or when disabled — a disabled
    /// reservation accounts nothing and its `Drop` is a no-op). Returns `None`
    /// when granting would exceed the ceiling, having first incremented
    /// `rejected_total`.
    ///
    /// This is an immediate grant-or-reject: it never awaits and never queues
    /// (Requirement 9.5).
    pub fn try_reserve(self: &Arc<Self>, bytes: u64) -> Option<Reservation> {
        let ceiling = self.ceiling.load(Ordering::Relaxed);
        if ceiling == 0 {
            // Ledger_Disabled: one relaxed load, no allocation, no CAS.
            return Some(Reservation {
                ledger: None,
                bytes: 0,
            });
        }

        let mut current = self.reserved.load(Ordering::Relaxed);
        loop {
            let Some(new_total) = current.checked_add(bytes) else {
                // Overflow: treat as "would exceed ceiling".
                self.rejected_total.fetch_add(1, Ordering::Relaxed);
                self.log_rejection_rate_limited(bytes);
                return None;
            };
            if new_total > ceiling {
                self.rejected_total.fetch_add(1, Ordering::Relaxed);
                self.log_rejection_rate_limited(bytes);
                return None;
            }
            match self.reserved.compare_exchange_weak(
                current,
                new_total,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.peak.fetch_max(new_total, Ordering::Relaxed);
                    return Some(Reservation {
                        ledger: Some(Arc::clone(self)),
                        bytes,
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Internal: release `bytes` previously granted by `try_reserve`/`try_grow`.
    /// Called only from `Reservation::drop`.
    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.reserved.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// Internal: attempt to grow an existing reservation by `additional` bytes.
    /// Called only from `Reservation::try_grow`. Returns `true` on success.
    fn try_grow(&self, additional: u64) -> bool {
        if additional == 0 {
            return true;
        }
        let ceiling = self.ceiling.load(Ordering::Relaxed);
        // A grow call only ever originates from an *enabled* Reservation (one
        // holding `Some(ledger)`), so ceiling == 0 here would mean the ledger
        // was disabled after the reservation was created — not possible given
        // the ceiling is fixed at construction. Guard anyway for safety.
        if ceiling == 0 {
            return true;
        }
        let mut current = self.reserved.load(Ordering::Relaxed);
        loop {
            let Some(new_total) = current.checked_add(additional) else {
                self.rejected_total.fetch_add(1, Ordering::Relaxed);
                self.log_rejection_rate_limited(additional);
                return false;
            };
            if new_total > ceiling {
                self.rejected_total.fetch_add(1, Ordering::Relaxed);
                self.log_rejection_rate_limited(additional);
                return false;
            }
            match self.reserved.compare_exchange_weak(
                current,
                new_total,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.peak.fetch_max(new_total, Ordering::Relaxed);
                    return true;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Record an aborted Unknown_Size_Site accumulation (Requirement 8.5).
    /// Call sites abort when `Reservation::try_grow` returns `false`.
    pub fn record_aborted_accumulation(&self) {
        self.aborted_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Claim `bytes` for a buffer whose memory **overlaps** a buffer an
    /// enclosing caller already reserved for.
    ///
    /// Some Buffering_Sites are nested: an outer site reserves for the bytes it
    /// will hand to the client, then an inner recovery/repair path buffers the
    /// same bytes again from upstream and the outer buffer ends up *being* that
    /// allocation (`Bytes` is refcounted, and the slice shares it). Calling
    /// `try_reserve` at the inner site would count those bytes twice, so the
    /// pair could not fit under a ceiling that comfortably fits the real
    /// resident cost — turning a transient capacity condition into a permanent
    /// one, because no amount of waiting frees a claim the same request holds.
    ///
    /// This accounts them exactly once without ever under-counting:
    ///
    /// - When the caller already holds `>= bytes`, its claim covers the inner
    ///   buffer in full, so no new claim is taken.
    /// - When the caller holds less (the inner buffer is genuinely larger than
    ///   the outer one), the caller's claim is **grown by the shortfall only**,
    ///   so the ledger reflects `max(outer, inner)` — the real peak — rather
    ///   than their sum. The grown amount stays held until the caller's
    ///   reservation drops, which is correct: the larger allocation is the one
    ///   the outer buffer is a view onto.
    /// - When there is no caller reservation, this is exactly `try_reserve`.
    ///
    /// Returns `None` when the claim (or the grow) would breach the ceiling,
    /// having already counted the rejection, matching `try_reserve`.
    pub fn claim_overlapping(
        self: &Arc<Self>,
        bytes: u64,
        caller_reservation: Option<&mut Reservation>,
    ) -> Option<BufferClaim> {
        match caller_reservation {
            Some(existing) => {
                let shortfall = bytes.saturating_sub(existing.held_bytes());
                if shortfall == 0 || existing.try_grow(shortfall) {
                    Some(BufferClaim::CoveredByCaller)
                } else {
                    None
                }
            }
            None => self.try_reserve(bytes).map(BufferClaim::Held),
        }
    }
}

/// Result of [`InflightLedger::claim_overlapping`].
///
/// `Held` must be kept alive for the buffer's lifetime; `CoveredByCaller`
/// carries nothing because the enclosing caller's reservation already does.
#[must_use]
pub enum BufferClaim {
    /// A fresh reservation this call site owns.
    Held(Reservation),
    /// Accounted for by a caller-held reservation covering the same bytes.
    CoveredByCaller,
}

/// RAII guard on a claim against the `InflightLedger`.
///
/// Dropping a `Reservation` releases its held bytes back to the ledger. This
/// happens on every exit path — normal completion, early `return`, `?`
/// propagation, client disconnect, timeout, and panic unwind — because it is
/// driven by `Drop`, not by an explicit release call any call site could
/// forget (Requirement 1.6).
///
/// `ledger: None` (the Ledger_Disabled case) makes this allocation-free and
/// its `Drop` a true no-op.
pub struct Reservation {
    ledger: Option<Arc<InflightLedger>>,
    bytes: u64,
}

impl Reservation {
    /// A reservation that holds nothing and accounts nothing. Used by callers
    /// that need a `Reservation` value in a code path where no ledger is
    /// configured or attaching one is not applicable (e.g. tests).
    pub fn none() -> Self {
        Self {
            ledger: None,
            bytes: 0,
        }
    }

    /// Bytes currently held by this reservation.
    pub fn held_bytes(&self) -> u64 {
        self.bytes
    }

    /// Attempt to grow this reservation by `additional` bytes against the
    /// ledger's ceiling (Unknown_Size_Site mechanism, Requirement 3.2).
    ///
    /// Returns `true` and grows `self.bytes` on success. Returns `false`
    /// without changing `self.bytes` when growing would exceed the ceiling —
    /// the caller is expected to abort its accumulation, drop this
    /// `Reservation` (releasing what it already held), and call
    /// `InflightLedger::record_aborted_accumulation` (Requirements 3.3, 3.4,
    /// 3.5).
    ///
    /// A no-op ledger (`ledger: None`, i.e. Ledger_Disabled or
    /// [`Reservation::none`]) always succeeds and grows unconditionally,
    /// preserving pre-feature unbounded behaviour when disabled.
    pub fn try_grow(&mut self, additional: u64) -> bool {
        match &self.ledger {
            Some(ledger) => {
                if ledger.try_grow(additional) {
                    self.bytes += additional;
                    true
                } else {
                    false
                }
            }
            None => {
                self.bytes += additional;
                true
            }
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(ledger) = &self.ledger {
            ledger.release(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Concurrent reservations totalling less than the ceiling are all
    /// admitted. Requirement 10.1.
    #[test]
    fn reservations_under_ceiling_all_admitted() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let r1 = ledger.try_reserve(300).expect("should admit");
        let r2 = ledger.try_reserve(300).expect("should admit");
        let r3 = ledger.try_reserve(300).expect("should admit");
        assert_eq!(ledger.reserved_bytes(), 900);
        drop((r1, r2, r3));
        assert_eq!(ledger.reserved_bytes(), 0);
    }

    /// A reservation that would breach the ceiling is rejected. Requirement
    /// 10.2.
    #[test]
    fn breaching_reservation_rejected() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let _r1 = ledger.try_reserve(900).expect("should admit");
        let r2 = ledger.try_reserve(200);
        assert!(r2.is_none(), "900 + 200 > 1000 ceiling, must reject");
    }

    /// A rejected reservation leaves the ledger total unchanged. Requirement
    /// 10.3.
    #[test]
    fn rejected_reservation_leaves_total_unchanged() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let _r1 = ledger.try_reserve(900).expect("should admit");
        let before = ledger.reserved_bytes();
        let r2 = ledger.try_reserve(200);
        assert!(r2.is_none());
        assert_eq!(ledger.reserved_bytes(), before);
    }

    /// The rejected_total counter increments on rejection.
    #[test]
    fn rejection_increments_rejected_total() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let _r1 = ledger.try_reserve(900).expect("should admit");
        assert_eq!(ledger.rejected_total(), 0);
        let _ = ledger.try_reserve(200);
        assert_eq!(ledger.rejected_total(), 1);
        let _ = ledger.try_reserve(200);
        assert_eq!(ledger.rejected_total(), 2);
    }

    /// The ledger total returns to zero after all reservations complete.
    /// Requirement 10.4.
    #[test]
    fn total_returns_to_zero_after_completion() {
        let ledger = Arc::new(InflightLedger::new(1000));
        {
            let _r1 = ledger.try_reserve(500).unwrap();
            let _r2 = ledger.try_reserve(400).unwrap();
            assert_eq!(ledger.reserved_bytes(), 900);
        }
        assert_eq!(ledger.reserved_bytes(), 0);
    }

    /// A disabled ledger (ceiling == 0) accounts nothing: every reservation is
    /// granted regardless of size, and `reserved_bytes` never moves.
    #[test]
    fn disabled_ledger_accounts_nothing() {
        let ledger = Arc::new(InflightLedger::disabled());
        assert!(ledger.is_disabled());
        let r1 = ledger
            .try_reserve(u64::MAX / 2)
            .expect("disabled admits unconditionally");
        let r2 = ledger
            .try_reserve(u64::MAX / 2)
            .expect("disabled admits unconditionally");
        assert_eq!(ledger.reserved_bytes(), 0);
        assert_eq!(ledger.ceiling_bytes(), 0);
        drop((r1, r2));
        assert_eq!(ledger.reserved_bytes(), 0);
        assert_eq!(ledger.rejected_total(), 0);
    }

    /// `Drop` releases held bytes back to the ledger.
    #[test]
    fn drop_releases_bytes() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let r1 = ledger.try_reserve(600).unwrap();
        assert_eq!(ledger.reserved_bytes(), 600);
        drop(r1);
        assert_eq!(ledger.reserved_bytes(), 0);
        // The freed capacity can be reused.
        let r2 = ledger.try_reserve(1000);
        assert!(r2.is_some());
    }

    /// `try_grow` on an enabled reservation succeeds while under the ceiling
    /// and grows the held byte count so `Drop` releases the grown total.
    #[test]
    fn try_grow_success_grows_and_releases_full_amount() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let mut r = ledger.try_reserve(100).unwrap();
        assert!(r.try_grow(200));
        assert_eq!(r.held_bytes(), 300);
        assert_eq!(ledger.reserved_bytes(), 300);
        drop(r);
        assert_eq!(ledger.reserved_bytes(), 0);
    }

    /// `try_grow` fails when it would exceed the ceiling, leaves the
    /// reservation's held bytes and the ledger total unchanged, and
    /// increments `rejected_total`.
    #[test]
    fn try_grow_failure_leaves_state_unchanged() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let mut r = ledger.try_reserve(900).unwrap();
        assert_eq!(ledger.rejected_total(), 0);
        assert!(!r.try_grow(200), "900 + 200 > 1000 ceiling");
        assert_eq!(r.held_bytes(), 900, "held bytes unchanged on failed grow");
        assert_eq!(ledger.reserved_bytes(), 900, "ledger total unchanged");
        assert_eq!(ledger.rejected_total(), 1);
        // Dropping releases only the original 900, not a phantom larger amount.
        drop(r);
        assert_eq!(ledger.reserved_bytes(), 0);
    }

    /// `record_aborted_accumulation` increments `aborted_total`, independent
    /// of `rejected_total` (Requirement 8.5 — the two counters serve
    /// different questions).
    #[test]
    fn record_aborted_accumulation_increments_aborted_total() {
        let ledger = Arc::new(InflightLedger::new(1000));
        assert_eq!(ledger.aborted_total(), 0);
        ledger.record_aborted_accumulation();
        ledger.record_aborted_accumulation();
        assert_eq!(ledger.aborted_total(), 2);
    }

    /// `peak_reserved_bytes` tracks the maximum concurrently-held total, not
    /// the current total — it must not fall when reservations are released.
    #[test]
    fn peak_tracks_maximum_not_current() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let r1 = ledger.try_reserve(400).unwrap();
        let r2 = ledger.try_reserve(400).unwrap();
        assert_eq!(ledger.peak_reserved_bytes(), 800);
        drop(r1);
        drop(r2);
        assert_eq!(ledger.reserved_bytes(), 0);
        assert_eq!(
            ledger.peak_reserved_bytes(),
            800,
            "peak must not fall when reservations release"
        );
        let r3 = ledger.try_reserve(100).unwrap();
        assert_eq!(
            ledger.peak_reserved_bytes(),
            800,
            "peak must not fall below a prior higher watermark"
        );
        drop(r3);
    }

    /// Concurrent reserve/release from many tasks returns the total to zero
    /// (Requirement 10.4, concurrent variant) and never exceeds the ceiling
    /// mid-flight.
    #[tokio::test]
    async fn concurrent_reserve_release_returns_to_zero() {
        let ledger = Arc::new(InflightLedger::new(10_000));
        let mut handles = Vec::new();
        for _ in 0..200 {
            let ledger = Arc::clone(&ledger);
            handles.push(tokio::spawn(async move {
                if let Some(r) = ledger.try_reserve(100) {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    drop(r);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(ledger.reserved_bytes(), 0);
    }

    /// A `Reservation::none()` value (no ledger attached) grows unconditionally
    /// and its `Drop` is a no-op, matching the Ledger_Disabled semantics for
    /// call sites that construct a standalone `Reservation` without a ledger
    /// (e.g. tests, or a call site that legitimately has no ledger to attach).
    #[test]
    fn reservation_none_is_a_true_no_op() {
        let mut r = Reservation::none();
        assert_eq!(r.held_bytes(), 0);
        assert!(r.try_grow(u64::MAX));
        assert_eq!(r.held_bytes(), u64::MAX);
        drop(r); // Must not panic; nothing to release.
    }

    /// `try_reserve` for zero bytes still returns a valid reservation.
    #[test]
    fn zero_byte_reservation_admitted() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let r = ledger
            .try_reserve(0)
            .expect("zero-byte reservation always admitted");
        assert_eq!(ledger.reserved_bytes(), 0);
        drop(r);
        assert_eq!(ledger.reserved_bytes(), 0);
    }

    /// `claim_overlapping` with no caller reservation behaves exactly like
    /// `try_reserve`: it takes its own claim and it can be refused.
    #[test]
    fn claim_overlapping_without_caller_reserves_fresh() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let claim = ledger
            .claim_overlapping(600, None)
            .expect("600 fits under 1000");
        assert!(matches!(claim, BufferClaim::Held(_)));
        assert_eq!(ledger.reserved_bytes(), 600);
        assert!(
            ledger.claim_overlapping(600, None).is_none(),
            "600 + 600 > 1000 must be refused"
        );
        drop(claim);
        assert_eq!(ledger.reserved_bytes(), 0);
    }

    /// The defect this exists to prevent: a nested buffer covering the same
    /// bytes as a caller-held reservation must not be counted twice, so a
    /// ceiling that fits ONE claim of the range admits the pair.
    #[test]
    fn claim_overlapping_reuses_caller_reservation_without_double_counting() {
        let ledger = Arc::new(InflightLedger::new(30));
        let mut caller = ledger.try_reserve(30).expect("30 fits exactly");
        assert_eq!(ledger.reserved_bytes(), 30);

        let claim = ledger
            .claim_overlapping(30, Some(&mut caller))
            .expect("the caller's claim already covers these bytes");
        assert!(matches!(claim, BufferClaim::CoveredByCaller));
        assert_eq!(
            ledger.reserved_bytes(),
            30,
            "the same bytes must be counted once, not twice"
        );
        assert_eq!(ledger.rejected_total(), 0);
        drop(claim);
        drop(caller);
        assert_eq!(ledger.reserved_bytes(), 0);
    }

    /// A nested buffer genuinely larger than the caller's grows the caller's
    /// claim by the shortfall only, so the ledger holds `max`, not the sum —
    /// and never less than the real allocation.
    #[test]
    fn claim_overlapping_grows_caller_by_shortfall_only() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let mut caller = ledger.try_reserve(100).unwrap();
        let claim = ledger
            .claim_overlapping(400, Some(&mut caller))
            .expect("400 fits under 1000");
        assert!(matches!(claim, BufferClaim::CoveredByCaller));
        assert_eq!(
            ledger.reserved_bytes(),
            400,
            "max(100, 400) = 400, not 100 + 400"
        );
        assert_eq!(caller.held_bytes(), 400);
        drop(caller);
        assert_eq!(ledger.reserved_bytes(), 0);
    }

    /// A shortfall that cannot fit is refused, leaving the caller's claim and
    /// the ledger total untouched — the nested site must shed, not proceed
    /// under-counted.
    #[test]
    fn claim_overlapping_refuses_when_shortfall_breaches_ceiling() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let mut caller = ledger.try_reserve(100).unwrap();
        let _other = ledger.try_reserve(800).unwrap();
        assert!(
            ledger.claim_overlapping(400, Some(&mut caller)).is_none(),
            "a 300-byte shortfall on top of 900 held must be refused"
        );
        assert_eq!(caller.held_bytes(), 100, "held bytes unchanged on refusal");
        assert_eq!(ledger.reserved_bytes(), 900, "ledger total unchanged");
        assert_eq!(ledger.rejected_total(), 1);
    }

    /// A disabled ledger accounts nothing through this path either.
    #[test]
    fn claim_overlapping_accounts_nothing_when_disabled() {
        let ledger = Arc::new(InflightLedger::disabled());
        let mut caller = ledger.try_reserve(u64::MAX / 2).unwrap();
        let claim = ledger
            .claim_overlapping(u64::MAX / 2, Some(&mut caller))
            .expect("disabled admits unconditionally");
        assert!(matches!(claim, BufferClaim::CoveredByCaller));
        assert_eq!(ledger.reserved_bytes(), 0);
        assert_eq!(ledger.rejected_total(), 0);
    }

    /// An overflowing reservation request is rejected rather than wrapping.
    #[test]
    fn overflow_is_rejected_not_wrapped() {
        let ledger = Arc::new(InflightLedger::new(1000));
        let _r1 = ledger.try_reserve(500).unwrap();
        let r2 = ledger.try_reserve(u64::MAX);
        assert!(r2.is_none());
        assert_eq!(ledger.reserved_bytes(), 500);
    }
}
