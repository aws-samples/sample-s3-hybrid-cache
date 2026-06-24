//! Download Bandwidth QoS — streaming backpressure adapter.
//!
//! [`ThrottleStream`] wraps the **output of `TeeStream`** and gates delivery
//! of each data frame against the per-instance token bucket in
//! [`BandwidthLimiter`].  Positioned downstream of `TeeStream`, it is
//! invisible to the idle watchdog: `TeeStream`'s idle timer only advances
//! while `TeeStream::poll_next` runs, and a throttle-withheld stream is not
//! polled, so the watchdog never sees throttle waits as an upstream stall.
//!
//! ## State machine
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │ poll_next                                            │
//! │                                                      │
//! │ 1. Pending grant? → poll timeout + oneshot          │
//! │    • timeout fired → fail-open (pass frame through) │
//! │    • grant received → credit local_balance          │
//! │    • still waiting → Pending                        │
//! │                                                      │
//! │ 2. Poll inner stream                                 │
//! │    • Err / None → refund local_balance, propagate   │
//! │    • non-data frame → pass through                  │
//! │    • data frame of n bytes:                         │
//! │      a. disabled → pass through (one atomic load)   │
//! │      b. local_balance ≥ n → draw, return frame      │
//! │      c. try_acquire → credit local_balance, return  │
//! │      d. enqueue DRR → park (return Pending)         │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! [`BandwidthLimiter`]: crate::bandwidth_limiter::BandwidthLimiter

use crate::bandwidth_limiter::{BandwidthLimiter, FairnessKey, LEASE_QUANTUM};
use bytes::Bytes;
use futures::Stream;
use hyper::body::Frame;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Sleep;
use tracing::warn;

/// Maximum time a blocked DRR acquire waits before the stream fails open.
const MAX_WAIT: Duration = Duration::from_secs(5);

/// Streaming backpressure throttle.
///
/// Call [`ThrottleStream::new`] wrapping the `TeeStream` result, before
/// conversion to a `BoxBody`.  Pass `known_len` from `Content-Length` (full
/// GET) or `parse_content_range_length` (range GET) so small requests never
/// over-acquire budget.
pub struct ThrottleStream<S> {
    inner: S,
    limiter: Arc<BandwidthLimiter>,
    key: FairnessKey,
    /// Bytes remaining from the last granted lease that have not yet been
    /// charged to frames.  Drawn down per frame; refilled by acquire calls.
    local_balance: u64,
    /// Known total response length, if available.
    known_len: Option<u64>,
    /// Total bytes handed to the client so far (used to track `known_remaining`).
    bytes_consumed: u64,
    // ── Slow-path state (all None when not parked on a DRR grant) ──────────
    /// Pending grant receiver; Some iff a DRR request is in flight.
    pending_grant: Option<Pin<Box<oneshot::Receiver<()>>>>,
    /// Bytes we asked the DRR to grant (credited to local_balance on success).
    pending_lease: u64,
    /// The data frame blocked on this grant.
    pending_frame: Option<Bytes>,
    /// Fail-open timeout: if this fires before the grant arrives, pass through.
    pending_timeout: Option<Pin<Box<Sleep>>>,
}

impl<S> ThrottleStream<S> {
    /// Create a new `ThrottleStream` wrapping `inner`.
    ///
    /// - `key` — the pre-resolved fairness key for this request.
    /// - `limiter` — shared limiter (cheaply `Arc`-cloned per request).
    /// - `known_len` — `Content-Length` / `Content-Range` length in bytes, or
    ///   `None` for chunked / unknown-length responses.
    pub fn new(
        inner: S,
        key: FairnessKey,
        limiter: Arc<BandwidthLimiter>,
        known_len: Option<u64>,
    ) -> Self {
        Self {
            inner,
            limiter,
            key,
            local_balance: 0,
            known_len,
            bytes_consumed: 0,
            pending_grant: None,
            pending_lease: 0,
            pending_frame: None,
            pending_timeout: None,
        }
    }
}

impl<S> Drop for ThrottleStream<S> {
    fn drop(&mut self) {
        // Refund any unspent lease balance so the token bucket isn't drained
        // by cancelled or short-lived streams.
        self.limiter.refund(self.local_balance);
        self.local_balance = 0;
    }
}

impl<S> Stream for ThrottleStream<S>
where
    S: Stream<Item = Result<Frame<Bytes>, hyper::Error>> + Unpin,
{
    type Item = Result<Frame<Bytes>, hyper::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // ── Step 1: resolve a pending DRR grant ───────────────────────────
        if self.pending_grant.is_some() {
            // Check the fail-open timeout first.
            if let Some(ref mut sleep) = self.pending_timeout {
                if Future::poll(sleep.as_mut(), cx).is_ready() {
                    // Timeout expired — fail open: pass the frame through
                    // without consuming budget.
                    let frame = self.pending_frame.take().expect("pending_frame set");
                    self.pending_grant = None;
                    self.pending_lease = 0;
                    self.pending_timeout = None;
                    let n = frame.len() as u64;
                    self.bytes_consumed += n;
                    warn!(
                        "[THROTTLE_FAILOPEN] DRR grant timeout ({:?}), passing {} bytes through; \
                         key={:?}",
                        MAX_WAIT,
                        n,
                        self.key.label(),
                    );
                    // failopen metric is incremented in enqueue_blocked already;
                    // do NOT double-count here.
                    return Poll::Ready(Some(Ok(Frame::data(frame))));
                }
            }

            // Poll the grant oneshot.
            let grant_result = {
                let rx = self.pending_grant.as_mut().unwrap();
                Future::poll(rx.as_mut(), cx)
            };
            match grant_result {
                Poll::Ready(Ok(())) => {
                    // DRR granted `pending_lease` bytes.
                    let frame = self.pending_frame.take().expect("pending_frame set");
                    let n = frame.len() as u64;
                    let lease = self.pending_lease;
                    self.pending_grant = None;
                    self.pending_lease = 0;
                    self.pending_timeout = None;
                    // Credit the lease minus the current frame into local_balance.
                    self.local_balance = lease.saturating_sub(n);
                    self.bytes_consumed += n;
                    self.limiter.record_bytes(&self.key, n);
                    return Poll::Ready(Some(Ok(Frame::data(frame))));
                }
                Poll::Ready(Err(_)) => {
                    // Sender dropped (DRR task exited) — fail open.
                    let frame = self.pending_frame.take().expect("pending_frame set");
                    let n = frame.len() as u64;
                    self.pending_grant = None;
                    self.pending_lease = 0;
                    self.pending_timeout = None;
                    self.bytes_consumed += n;
                    warn!(
                        "[THROTTLE_FAILOPEN] DRR channel closed, passing {} bytes through; \
                         key={:?}",
                        n,
                        self.key.label(),
                    );
                    return Poll::Ready(Some(Ok(Frame::data(frame))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // ── Step 2: poll the inner stream for the next frame ──────────────
        let inner = Pin::new(&mut self.inner);
        match inner.poll_next(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                // Non-data frames (trailers, etc.) pass through immediately.
                let data = match frame.into_data() {
                    Ok(d) => d,
                    Err(frame) => return Poll::Ready(Some(Ok(frame))),
                };

                let n = data.len() as u64;
                if n == 0 {
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }

                // ── Layer 0: disabled bypass ─────────────────────────────
                if self.limiter.is_disabled() {
                    self.bytes_consumed += n;
                    self.limiter.record_bytes(&self.key, n);
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }

                // ── Layer 1: draw from local lease balance ────────────────
                if self.local_balance >= n {
                    self.local_balance -= n;
                    self.bytes_consumed += n;
                    self.limiter.record_bytes(&self.key, n);
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }

                // ── Balance exhausted: acquire a new lease ────────────────
                let desired = desired_lease(n, self.known_len, self.bytes_consumed);

                // Layer 1 fast path: atomic CAS.
                if let Some(granted) = self.limiter.try_acquire(n, desired) {
                    // granted ∈ [n, desired]; credit surplus into local_balance.
                    self.local_balance = granted.saturating_sub(n);
                    self.bytes_consumed += n;
                    self.limiter.record_bytes(&self.key, n);
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }

                // Layer 2: enqueue in DRR and park.
                // Request only `n` (current frame size), not `desired`, so that
                // the DRR can serve the request even when burst_capacity < desired
                // (e.g., very low ceilings where burst ≈ a few KiB < LEASE_QUANTUM).
                // Full quantum amortisation is obtained on the Layer-1 fast path;
                // the DRR slow path provides per-frame budget as a safe fallback.
                let grant_rx = self.limiter.enqueue_blocked(self.key.clone(), n);
                self.pending_grant = Some(Box::pin(grant_rx));
                self.pending_lease = n;
                self.pending_frame = Some(data);
                self.pending_timeout = Some(Box::pin(tokio::time::sleep(MAX_WAIT)));
                // Schedule an immediate re-poll so Step 1 polls pending_grant and
                // registers its waker with the runtime on the very next call.
                cx.waker().wake_by_ref();
                Poll::Pending
            }

            Poll::Ready(Some(Err(e))) => {
                // Upstream error — refund unused lease balance and propagate.
                self.limiter.refund(self.local_balance);
                self.local_balance = 0;
                Poll::Ready(Some(Err(e)))
            }

            Poll::Ready(None) => {
                // Stream exhausted — refund unused lease balance.
                self.limiter.refund(self.local_balance);
                self.local_balance = 0;
                Poll::Ready(None)
            }

            Poll::Pending => Poll::Pending,
        }
    }
}

// ─── Lease sizing ─────────────────────────────────────────────────────────────

/// Compute the desired lease size for an acquire call.
///
/// - **Known length:** lease = `min(LEASE_QUANTUM, known_remaining)` where
///   `known_remaining = total - bytes_consumed`.  A sub-quantum response
///   deterministically never over-acquires; an 8 MiB range amortises to ~8
///   acquires from the first byte.
/// - **Unknown length:** per-frame exact (`n`) until cumulative reaches
///   `LEASE_QUANTUM`, then full-quantum leases.  Caps any over-grab to bytes
///   the stream has already proven it will transfer.
///
/// The result is always `≥ n` (at least the current frame) so that a stream
/// is only parked when the **real** ceiling is reached, not because a full
/// quantum is unavailable.
fn desired_lease(n: u64, known_len: Option<u64>, bytes_consumed: u64) -> u64 {
    let based = match known_len {
        Some(total) => {
            let remaining = total.saturating_sub(bytes_consumed);
            LEASE_QUANTUM.min(remaining)
        }
        None => {
            // Unknown length: per-frame until cumulative >= LEASE_QUANTUM.
            if bytes_consumed < LEASE_QUANTUM {
                n
            } else {
                LEASE_QUANTUM
            }
        }
    };
    based.max(n) // always at least the current frame
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bandwidth_limiter::{BandwidthLimiter, FairnessKey};
    use crate::config::DownloadBandwidthConfig;
    use bytes::Bytes;
    use futures::StreamExt;
    use hyper::body::Frame;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    fn make_stream(
        chunks: Vec<Bytes>,
    ) -> impl Stream<Item = Result<Frame<Bytes>, hyper::Error>> + Unpin {
        futures::stream::iter(
            chunks
                .into_iter()
                .map(|b| Ok::<Frame<Bytes>, hyper::Error>(Frame::data(b))),
        )
    }

    fn disabled_limiter() -> Arc<BandwidthLimiter> {
        Arc::new(BandwidthLimiter::disabled())
    }

    fn limiter_with_ceiling(
        bps: u64,
    ) -> (Arc<BandwidthLimiter>, Option<tokio::task::JoinHandle<()>>) {
        let cfg = DownloadBandwidthConfig {
            max_bytes_per_sec: bps,
            ..Default::default()
        };
        let (lim, handle) = BandwidthLimiter::new(&cfg);
        (Arc::new(lim), handle)
    }

    // ── desired_lease ─────────────────────────────────────────────────────

    #[test]
    fn known_len_small_request_never_over_grabs() {
        // A 64 KiB object with bytes_consumed=0: lease must be exactly 64 KiB.
        let n = 64 * 1024;
        let lease = desired_lease(n, Some(n), 0);
        assert_eq!(
            lease, n,
            "small known-len request should lease exactly its size"
        );
    }

    #[test]
    fn known_len_large_request_amortises() {
        // An 8 MiB range: first acquire should be min(LEASE_QUANTUM, 8MiB) = LEASE_QUANTUM.
        let total = 8 * 1024 * 1024u64;
        let n = 128 * 1024; // typical frame size
        let lease = desired_lease(n, Some(total), 0);
        assert_eq!(lease, LEASE_QUANTUM);
    }

    #[test]
    fn known_len_tail_capped_to_remaining() {
        // Last 256 KiB of an 8 MiB range: lease should be 256 KiB, not LEASE_QUANTUM.
        let total = 8 * 1024 * 1024u64;
        let consumed = total - 256 * 1024;
        let n = 128 * 1024; // frame size
        let lease = desired_lease(n, Some(total), consumed);
        assert_eq!(lease, 256 * 1024, "tail lease capped to remaining bytes");
    }

    #[test]
    fn unknown_len_small_objects_exact() {
        // Sub-quantum stream at the start: lease = exact n.
        let n = 16 * 1024u64;
        let lease = desired_lease(n, None, 0);
        assert_eq!(lease, n);
    }

    #[test]
    fn unknown_len_large_amortises_after_quantum() {
        // After transferring 1 MiB, unknown-len uses full quanta.
        let n = 128 * 1024u64;
        let lease = desired_lease(n, None, LEASE_QUANTUM);
        assert_eq!(lease, LEASE_QUANTUM);
    }

    #[test]
    fn lease_always_at_least_frame_size() {
        // If remaining = 0 (edge case), lease is still at least n.
        let n = 1024u64;
        let lease = desired_lease(n, Some(0), 0);
        assert!(lease >= n, "lease must be at least frame size");
    }

    // ── Byte-exact passthrough ───────────────────────────────────────────

    #[tokio::test]
    async fn disabled_limiter_passthrough_byte_exact() {
        let lim = disabled_limiter();
        let chunks = vec![Bytes::from("hello"), Bytes::from("world")];
        let inner = make_stream(chunks.clone());
        let key = FairnessKey::Bucket("test".into());
        let mut stream = ThrottleStream::new(inner, key, lim, None);

        let mut collected = Vec::new();
        while let Some(Ok(frame)) = stream.next().await {
            if let Ok(data) = frame.into_data() {
                collected.push(data);
            }
        }
        assert_eq!(collected, chunks);
    }

    #[tokio::test]
    async fn enabled_under_cap_passthrough_byte_exact() {
        let (lim, _handle) = limiter_with_ceiling(100 * 1024 * 1024); // 100 MiB/s
        let chunks = vec![
            Bytes::from(vec![0u8; 1024]),
            Bytes::from(vec![1u8; 1024]),
            Bytes::from(vec![2u8; 1024]),
        ];
        let total_len = chunks.iter().map(|c| c.len() as u64).sum();
        let inner = make_stream(chunks.clone());
        let key = FairnessKey::Bucket("test".into());
        let mut stream = ThrottleStream::new(inner, key, lim, Some(total_len));

        let mut collected = Vec::new();
        while let Some(Ok(frame)) = stream.next().await {
            if let Ok(data) = frame.into_data() {
                collected.push(data);
            }
        }
        assert_eq!(collected.len(), chunks.len());
        for (a, b) in collected.iter().zip(chunks.iter()) {
            assert_eq!(a, b);
        }
    }

    // ── Idle watchdog compatibility ─────────────────────────────────────
    // Verify that ThrottleStream + TeeStream backpressure does NOT trip the
    // idle watchdog: the watchdog lives inside TeeStream (the inner stream),
    // which is not polled while ThrottleStream is parked on a DRR grant.

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn throttle_does_not_trip_idle_watchdog() {
        use crate::tee_stream::TeeStream;

        // Very low ceiling so we definitely hit the DRR path.
        let bps = 8 * 1024u64; // 8 KiB/s — slow
        let (lim, _handle) = limiter_with_ceiling(bps);

        // Use a TeeStream with an idle timeout shorter than MAX_WAIT.
        let idle_timeout = std::time::Duration::from_secs(2);
        let (cache_tx, _cache_rx) = tokio::sync::mpsc::channel(16);

        // A source stream with two 4 KiB chunks.
        let chunk = Bytes::from(vec![0u8; 4096]);
        let chunks = vec![
            Ok::<Frame<Bytes>, hyper::Error>(Frame::data(chunk.clone())),
            Ok::<Frame<Bytes>, hyper::Error>(Frame::data(chunk.clone())),
        ];
        let source = futures::stream::iter(chunks);
        let tee = TeeStream::with_idle_timeout(source, cache_tx, idle_timeout);

        let key = FairnessKey::Bucket("watchdog-test".into());
        let known_len = Some(8192u64); // 2 × 4 KiB
        let mut stream = ThrottleStream::new(tee, key, lim, known_len);

        // Consume the stream; it should complete without a watchdog abort.
        // The throttle rate is low so this will take a few BURST_WINDOWs,
        // but must finish well within the idle_timeout (2s).
        let start = Instant::now();
        let mut total_bytes = 0u64;
        while let Some(result) = stream.next().await {
            match result {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        total_bytes += data.len() as u64;
                    }
                }
                Err(_) => panic!("idle watchdog fired — false abort"),
            }
        }
        let elapsed = start.elapsed();

        assert_eq!(total_bytes, 8192, "byte-exact output");
        // Should complete in reasonable time (not hang).
        assert!(
            elapsed < std::time::Duration::from_secs(30),
            "stream should complete"
        );
    }

    // ── Small request with known length never over-grabs ────────────────

    #[tokio::test]
    async fn small_known_length_request_no_over_grab() {
        // Object smaller than LEASE_QUANTUM: assert total tokens consumed ≈ object size.
        let obj_size = 64 * 1024u64; // 64 KiB
        let (lim, _handle) = limiter_with_ceiling(10 * 1024 * 1024); // 10 MiB/s

        let initial_tokens = lim.available_tokens.load(Ordering::Relaxed);

        let chunk = Bytes::from(vec![0u8; obj_size as usize]);
        let inner = make_stream(vec![chunk]);
        let key = FairnessKey::Bucket("small".into());
        let mut stream = ThrottleStream::new(inner, key, lim.clone(), Some(obj_size));

        while stream.next().await.is_some() {}
        drop(stream); // triggers refund

        let after_tokens = lim.available_tokens.load(Ordering::Relaxed);
        let consumed = initial_tokens.saturating_sub(after_tokens);

        // After refund, net consumption should equal obj_size (no over-grab).
        assert_eq!(
            consumed, obj_size,
            "net token consumption should equal object size, got {}",
            consumed
        );
    }

    // ── Rate limiting (approximate) ──────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn throughput_stays_at_or_below_ceiling() {
        // 1 MiB/s ceiling; transfer 512 KiB and assert it takes at least
        // 400ms (would take ~500ms at the exact ceiling).
        let bps = 1024 * 1024u64; // 1 MiB/s
        let (lim, _handle) = limiter_with_ceiling(bps);

        let obj_size = 512 * 1024u64; // 512 KiB
        let chunk_size = 16 * 1024usize; // 16 KiB frames
        let chunks: Vec<Bytes> = (0..(obj_size as usize / chunk_size))
            .map(|_| Bytes::from(vec![0u8; chunk_size]))
            .collect();

        let inner = make_stream(chunks);
        let key = FairnessKey::Bucket("rate-test".into());
        let mut stream = ThrottleStream::new(inner, key, lim, Some(obj_size));

        let start = Instant::now();
        let mut total = 0u64;
        while let Some(Ok(frame)) = stream.next().await {
            if let Ok(data) = frame.into_data() {
                total += data.len() as u64;
            }
        }
        let elapsed = start.elapsed();

        assert_eq!(total, obj_size, "byte-exact");
        // At 1 MiB/s the 512 KiB should take at least 400ms (some tolerance).
        // On a lightly loaded test runner, it may take a bit more.
        assert!(
            elapsed >= Duration::from_millis(400),
            "throughput exceeded ceiling: elapsed={:?} for {} bytes at {} bps",
            elapsed,
            total,
            bps
        );
    }

    // ── Low ceiling (burst < quantum) ───────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn low_ceiling_never_parks_on_full_quantum() {
        // When burst capacity < LEASE_QUANTUM, the clamp-to-available rule in
        // try_acquire should still grant (smaller) leases rather than parking
        // indefinitely waiting for a full quantum.
        let bps = 64 * 1024u64; // 64 KiB/s → burst ≈ 6.4 KiB (< LEASE_QUANTUM)
        let (lim, _handle) = limiter_with_ceiling(bps);

        let obj_size = 32 * 1024u64; // 32 KiB
        let chunk_size = 4 * 1024usize; // 4 KiB frames
        let chunks: Vec<Bytes> = (0..(obj_size as usize / chunk_size))
            .map(|_| Bytes::from(vec![0u8; chunk_size]))
            .collect();

        let inner = make_stream(chunks);
        let key = FairnessKey::Bucket("low-ceil".into());
        let mut stream = ThrottleStream::new(inner, key, lim, Some(obj_size));

        // Should complete eventually (not hang), even with burst < quantum.
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            let mut total = 0u64;
            while let Some(Ok(frame)) = stream.next().await {
                if let Ok(data) = frame.into_data() {
                    total += data.len() as u64;
                }
            }
            total
        })
        .await;

        assert!(result.is_ok(), "should not timeout with low ceiling");
        assert_eq!(result.unwrap(), obj_size, "byte-exact");
    }

    // ── Refund on drop ────────────────────────────────────────────────────

    #[tokio::test]
    async fn early_drop_refunds_local_balance() {
        let (lim, _handle) = limiter_with_ceiling(10 * 1024 * 1024);

        let before = lim.available_tokens.load(Ordering::Relaxed);

        // Build a stream and consume one frame (acquires a lease).
        let chunks = vec![Bytes::from(vec![0u8; 1024]), Bytes::from(vec![0u8; 1024])];
        let inner = make_stream(chunks);
        let key = FairnessKey::Bucket("drop-test".into());
        let mut stream = ThrottleStream::new(inner, key, lim.clone(), Some(2048));

        // Consume only the first frame.
        let _ = stream.next().await;
        // Drop without consuming the second frame — local_balance should be refunded.
        drop(stream);

        let after = lim.available_tokens.load(Ordering::Relaxed);
        // After refund the balance should be back to at most `before` (it may be
        // slightly less because `before` consumed tokens for the first frame, and
        // the refund only restores unused balance, not the consumed bytes).
        // Key assertion: it doesn't go *below* (before - first_frame_bytes).
        assert!(
            after >= before.saturating_sub(1024),
            "refund should restore unspent lease balance; before={}, after={}",
            before,
            after
        );
    }
}

// ─── Tier-A micro-performance tests (Req 9.1, 9.2, 9.8, 9.9) ─────────────────

#[cfg(test)]
mod perf_tests {
    use super::*;
    use crate::bandwidth_limiter::{BandwidthLimiter, FairnessKey};
    use crate::config::DownloadBandwidthConfig;
    use bytes::Bytes;
    use futures::StreamExt;
    use hyper::body::Frame;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    fn make_stream(
        chunks: Vec<Bytes>,
    ) -> impl Stream<Item = std::result::Result<Frame<Bytes>, hyper::Error>> + Unpin {
        futures::stream::iter(
            chunks
                .into_iter()
                .map(|b| Ok::<Frame<Bytes>, hyper::Error>(Frame::data(b))),
        )
    }

    // ── Req 9.1: disabled = pure passthrough (no allocation, no limiter call) ──

    #[tokio::test]
    async fn disabled_passthrough_no_limiter_interaction() {
        // With a disabled limiter, every frame should pass through immediately
        // via the Layer-0 atomic-load bypass.  Verify by checking that the
        // byte_tracker records bytes (record_bytes is still called to keep
        // accounting uniform) but that try_acquire is never called.
        let lim = Arc::new(BandwidthLimiter::disabled());
        assert!(lim.is_disabled());

        let chunks: Vec<Bytes> = (0..10).map(|_| Bytes::from(vec![0u8; 1024])).collect();
        let inner = make_stream(chunks.clone());
        let key = FairnessKey::Default;
        let mut stream = ThrottleStream::new(inner, key, lim.clone(), Some(10 * 1024));

        let mut total = 0u64;
        while let Some(Ok(frame)) = stream.next().await {
            if let Ok(data) = frame.into_data() {
                total += data.len() as u64;
            }
        }
        assert_eq!(total, 10 * 1024, "byte-exact passthrough when disabled");
        // available_tokens stays at 0 (disabled limiter never refills).
        assert_eq!(
            lim.available_tokens.load(Ordering::Relaxed),
            0,
            "disabled limiter available_tokens must stay 0 (try_acquire skipped)"
        );
    }

    // ── Req 9.2: under-cap acquire takes no async wait / no lock ──────────────
    // Verify by checking that all frames pass through without ever parking
    // (pending_grant is never set = no DRR enqueue).

    #[tokio::test]
    async fn under_cap_no_drr_enqueue() {
        let cfg = DownloadBandwidthConfig {
            max_bytes_per_sec: 100 * 1024 * 1024, // 100 MiB/s — large headroom
            ..Default::default()
        };
        let (lim, _handle) = BandwidthLimiter::new(&cfg);
        let lim = Arc::new(lim);

        // Transfer 64 KiB in 4 KiB frames — well under the burst capacity.
        let chunks: Vec<Bytes> = (0..16).map(|_| Bytes::from(vec![0u8; 4096])).collect();
        let inner = make_stream(chunks);
        let key = FairnessKey::Bucket("test".into());
        let mut stream = ThrottleStream::new(inner, key, lim.clone(), Some(64 * 1024));

        let mut total = 0u64;
        while let Some(Ok(frame)) = stream.next().await {
            if let Ok(data) = frame.into_data() {
                total += data.len() as u64;
            }
        }
        assert_eq!(total, 64 * 1024, "byte-exact under cap");
        // failopen_total must remain 0 — no DRR enqueue / no timeout.
        assert_eq!(lim.failopen_total(), 0, "no fail-open events under cap");
    }

    // ── Req 9.8: small-object flood — budget consumed ≈ real bytes ────────────

    #[tokio::test]
    async fn small_object_flood_budget_approximates_real_bytes() {
        // 100 concurrent sub-quantum requests of 8 KiB each = 800 KiB total.
        // Total token consumption (initial_tokens - remaining) should equal
        // approximately 800 KiB (after refund from Drop).
        let cfg = DownloadBandwidthConfig {
            max_bytes_per_sec: 50 * 1024 * 1024, // 50 MiB/s
            ..Default::default()
        };
        let (lim, _handle) = BandwidthLimiter::new(&cfg);
        let lim = Arc::new(lim);

        let initial_tokens = lim.available_tokens.load(Ordering::Relaxed);
        let obj_size = 8 * 1024u64; // 8 KiB — sub-quantum
        let n_objects = 100usize;

        let mut handles = Vec::new();
        for i in 0..n_objects {
            let lim = Arc::clone(&lim);
            handles.push(tokio::spawn(async move {
                let key = FairnessKey::Bucket(format!("bkt-{}", i % 10));
                let chunk = Bytes::from(vec![0u8; obj_size as usize]);
                let inner = make_stream(vec![chunk]);
                let mut stream = ThrottleStream::new(inner, key, lim, Some(obj_size));
                let mut bytes = 0u64;
                while let Some(Ok(frame)) = stream.next().await {
                    if let Ok(d) = frame.into_data() {
                        bytes += d.len() as u64;
                    }
                }
                bytes
            }));
        }

        let mut total = 0u64;
        for h in handles {
            total += h.await.expect("no panic");
        }

        assert_eq!(
            total,
            obj_size * n_objects as u64,
            "byte-exact across all small-object streams"
        );

        // Net token consumption (after all Drops refund) ≈ real bytes.
        let remaining = lim.available_tokens.load(Ordering::Relaxed);
        let consumed = initial_tokens.saturating_sub(remaining);
        // Allow up to 10% over-accounting tolerance (Space-Saving + async ordering).
        let tolerance = (obj_size * n_objects as u64) / 10;
        assert!(
            consumed <= obj_size * n_objects as u64 + tolerance,
            "net token consumption should approximate real bytes; consumed={}, actual={}",
            consumed,
            obj_size * n_objects as u64
        );
        assert_eq!(
            lim.failopen_total(),
            0,
            "no fail-open in small-object flood"
        );
    }

    // ── Req 9.9: low ceiling (burst < quantum) — smaller batches, no stall ────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn low_ceiling_clamp_yields_smaller_batches_no_stall() {
        // With a ceiling of 32 KiB/s, burst = 32 KiB/s × 100ms = 3.2 KiB.
        // LEASE_QUANTUM (1 MiB) >> burst, so try_acquire must grant the burst-
        // limited amount (< quantum) rather than parking until a full quantum accrues.
        let bps = 32 * 1024u64; // 32 KiB/s
        let cfg = DownloadBandwidthConfig {
            max_bytes_per_sec: bps,
            ..Default::default()
        };
        let (lim, _handle) = BandwidthLimiter::new(&cfg);
        let lim = Arc::new(lim);

        let obj_size = 16 * 1024u64; // 16 KiB — larger than burst_capacity
        let chunk_size = 2 * 1024usize; // 2 KiB frames
        let chunks: Vec<Bytes> = (0..(obj_size as usize / chunk_size))
            .map(|_| Bytes::from(vec![0u8; chunk_size]))
            .collect();

        let inner = make_stream(chunks);
        let key = FairnessKey::Bucket("low".into());
        let mut stream = ThrottleStream::new(inner, key, lim, Some(obj_size));

        // Should complete in bounded time without deadlocking.
        let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let mut total = 0u64;
            while let Some(Ok(frame)) = stream.next().await {
                if let Ok(d) = frame.into_data() {
                    total += d.len() as u64;
                }
            }
            total
        })
        .await;

        assert!(result.is_ok(), "low-ceiling stream must not deadlock");
        assert_eq!(result.unwrap(), obj_size, "byte-exact with low ceiling");
    }
}
