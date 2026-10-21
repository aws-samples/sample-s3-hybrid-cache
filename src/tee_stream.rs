//! Tee Stream Module
//!
//! Provides a stream wrapper that clones data chunks and sends them to a channel
//! while passing the original data through. This enables simultaneous streaming
//! to the client and caching in the background.
//!
//! Uses backpressure-aware `send().await` to guarantee all chunks reach the cache
//! writer. When the channel is full, `poll_next` returns `Poll::Pending`, which
//! applies backpressure through hyper to the S3 response stream, slowing the
//! client download to match disk write speed. This ensures every range is fully
//! cached on first download.
//!
//! ## Mid-stream idle watchdog (Requirement 5, Task 11)
//!
//! When `idle_timeout` is configured, the stream tracks time since the last
//! upstream chunk was received. If the upstream produces no bytes within the
//! timeout, the stream terminates with an error, which aborts the client
//! connection (the client's own retry logic engages). The cache channel is
//! dropped without finalization, so a truncated body is never persisted.
//!
//! **Backpressure exclusion:** The idle timer only ticks while the stream is
//! actively polling the upstream (inner stream). During backpressure (pending
//! cache-channel send), the timer is paused because `poll_next` resolves the
//! pending send before polling the inner stream. This means a slow client cannot
//! trigger a false idle abort — only genuine upstream stalls are detected.

use bytes::Bytes;
use futures::Stream;
use hyper::body::Frame;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::Sleep;
use tracing::{debug, warn};

/// Process-wide cache-tee backpressure counters for the GET-miss path.
///
/// Incremented directly at the point backpressure occurs (inside `poll_next`),
/// rather than read back from a completed `TeeStream` by its caller: the
/// stream is moved into several wrapper layers (`wrap_origin_stream`,
/// `StreamBody`, `PermitBody`) immediately after construction, so there is no
/// call site that still holds the value after the stream finishes. A
/// process-wide atomic pair, mirroring `LAST_OVER_BOUND_WARN_SECS` in
/// `crate::metrics`, avoids needing a completion hook.
///
/// A non-zero value means the cache writer, not the upstream, was the
/// bottleneck for at least one GET-miss request.
///
/// Spec: cache-eviction-at-scale Requirement 18.4
static BACKPRESSURE_EVENTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static BACKPRESSURE_WAIT_MS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Cumulative count of cache-tee backpressure events across all GET-miss
/// `TeeStream` instances since process start. See `BACKPRESSURE_EVENTS_TOTAL`.
pub(crate) fn backpressure_events_total_get() -> u64 {
    BACKPRESSURE_EVENTS_TOTAL.load(Ordering::Relaxed)
}

/// Cumulative milliseconds spent applying cache-tee backpressure across all
/// GET-miss `TeeStream` instances since process start. See
/// `BACKPRESSURE_WAIT_MS_TOTAL`.
pub(crate) fn backpressure_wait_ms_total_get() -> u64 {
    BACKPRESSURE_WAIT_MS_TOTAL.load(Ordering::Relaxed)
}

/// Upstream idle-watchdog counters, process-wide since start. Published as
/// `cache.upstream_idle_abort_total_mid_stream`, `cache.upstream_idle_abort_total_pre_stream`
/// and `cache.upstream_idle_retry_total`.
///
/// Atomics for the same reason as the backpressure pair above: the mid-stream abort fires
/// inside `poll_next`, after the stream has been moved into the response body. The two
/// pre-stream counters are incremented by the first-byte retry loop in
/// `HttpProxy::forward_get_head_to_s3_and_cache`: a retry each time the first byte misses
/// `upstream_first_byte_timeout` with attempts left, and a pre-stream abort when the last
/// attempt misses it and the client gets a 504.
static UPSTREAM_IDLE_ABORT_MID_STREAM: AtomicU64 = AtomicU64::new(0);
static UPSTREAM_IDLE_ABORT_PRE_STREAM: AtomicU64 = AtomicU64::new(0);
static UPSTREAM_IDLE_RETRY: AtomicU64 = AtomicU64::new(0);
/// Record a pre-stream first-byte timeout. `retrying` is whether another attempt follows.
pub(crate) fn note_upstream_first_byte_timeout(retrying: bool) {
    if retrying {
        UPSTREAM_IDLE_RETRY.fetch_add(1, Ordering::Relaxed);
    } else {
        UPSTREAM_IDLE_ABORT_PRE_STREAM.fetch_add(1, Ordering::Relaxed);
    }
}
/// `cache.upstream_idle_abort_total_mid_stream`. See [`UPSTREAM_IDLE_ABORT_MID_STREAM`].
pub(crate) fn upstream_idle_abort_total_mid_stream() -> u64 {
    UPSTREAM_IDLE_ABORT_MID_STREAM.load(Ordering::Relaxed)
}
/// `cache.upstream_idle_abort_total_pre_stream`. See [`UPSTREAM_IDLE_ABORT_MID_STREAM`].
pub(crate) fn upstream_idle_abort_total_pre_stream() -> u64 {
    UPSTREAM_IDLE_ABORT_PRE_STREAM.load(Ordering::Relaxed)
}
/// `cache.upstream_idle_retry_total`. See [`UPSTREAM_IDLE_ABORT_MID_STREAM`].
pub(crate) fn upstream_idle_retry_total() -> u64 {
    UPSTREAM_IDLE_RETRY.load(Ordering::Relaxed)
}
/// Type alias for the pending send future used for backpressure.
type PendingSendFuture =
    Pin<Box<dyn Future<Output = Result<(), mpsc::error::SendError<Bytes>>> + Send + Sync>>;

/// A stream that tees data to a channel while passing it through.
///
/// Uses backpressure-aware sends: when the cache channel is full, the stream
/// pauses (returns Poll::Pending) until capacity is available. This guarantees
/// all chunks reach the cache writer at the cost of throttling client download
/// speed to match disk write speed when the cache writer can't keep up.
///
/// When `idle_timeout` is set, the stream acts as a mid-stream watchdog: if
/// no upstream chunk arrives within the timeout, the stream terminates with an
/// error, aborting the client connection and preventing partial cache commits.
pub struct TeeStream<S> {
    inner: S,
    sender: mpsc::Sender<Bytes>,
    bytes_sent: usize,
    /// Stored send future for backpressure — when the channel is full, we store
    /// the pending send and the frame to return, then poll the send on subsequent calls.
    pending_send: Option<PendingSendFuture>,
    pending_frame: Option<Bytes>,
    /// Track backpressure timing for observability
    backpressure_wait: Duration,
    backpressure_start: Option<Instant>,
    backpressure_events: usize,
    /// Whether the channel has been closed (receiver dropped)
    channel_closed: bool,
    /// Mid-stream idle timeout configuration (None = disabled)
    idle_timeout: Option<Duration>,
    /// Sleep future for the idle watchdog. Reset on every upstream chunk.
    idle_sleep: Option<Pin<Box<Sleep>>>,
}

impl<S> TeeStream<S>
where
    S: Stream<Item = Result<Frame<Bytes>, hyper::Error>>,
{
    /// Create a new tee stream with a channel sender
    pub fn new(inner: S, sender: mpsc::Sender<Bytes>) -> Self {
        Self {
            inner,
            sender,
            bytes_sent: 0,
            pending_send: None,
            pending_frame: None,
            backpressure_wait: Duration::ZERO,
            backpressure_start: None,
            backpressure_events: 0,
            channel_closed: false,
            idle_timeout: None,
            idle_sleep: None,
        }
    }

    /// Create a new tee stream with an idle timeout watchdog.
    ///
    /// If the upstream produces no bytes within `idle_timeout`, the stream
    /// terminates (returns an error frame), aborting the client connection.
    /// The cache channel is dropped without finalization.
    pub(crate) fn with_idle_timeout(
        inner: S,
        sender: mpsc::Sender<Bytes>,
        idle_timeout: Duration,
    ) -> Self {
        let idle_sleep = Some(Box::pin(tokio::time::sleep(idle_timeout)));
        Self {
            inner,
            sender,
            bytes_sent: 0,
            pending_send: None,
            pending_frame: None,
            backpressure_wait: Duration::ZERO,
            backpressure_start: None,
            backpressure_events: 0,
            channel_closed: false,
            idle_timeout: Some(idle_timeout),
            idle_sleep,
        }
    }

    /// Number of times the cache-tee channel was full and the stream had to
    /// await a pending send, applying backpressure through hyper to the
    /// client. A non-zero value means the cache writer, not the upstream, was
    /// the bottleneck for this request.
    ///
    /// Spec: cache-eviction-at-scale Requirement 18.4
    ///
    /// Test seam: the per-instance figure is only read by the backpressure unit
    /// test; production publishes the process-wide `backpressure_events_total_get`.
    #[cfg(test)]
    pub(crate) fn backpressure_events(&self) -> u64 {
        self.backpressure_events as u64
    }

    /// Cumulative time this stream spent awaiting a pending cache-tee send
    /// (i.e. propagating backpressure to the client), in milliseconds.
    ///
    /// Spec: cache-eviction-at-scale Requirement 18.4
    ///
    /// Test seam, as for `backpressure_events`; production publishes
    /// `backpressure_wait_ms_total_get`.
    #[cfg(test)]
    pub(crate) fn backpressure_wait_ms(&self) -> u64 {
        self.backpressure_wait.as_millis() as u64
    }

    /// Resolve a pending backpressure interval: fold the elapsed time into
    /// this stream's own `backpressure_wait` and into the process-wide
    /// `BACKPRESSURE_WAIT_MS_TOTAL` counter. Called from every arm that takes
    /// `self.backpressure_start`, so the global counter never drifts from the
    /// per-instance one.
    fn resolve_backpressure_wait(&mut self) {
        if let Some(start) = self.backpressure_start.take() {
            let elapsed = start.elapsed();
            self.backpressure_wait += elapsed;
            BACKPRESSURE_WAIT_MS_TOTAL.fetch_add(elapsed.as_millis() as u64, Ordering::Relaxed);
        }
    }
}

impl<S> Stream for TeeStream<S>
where
    S: Stream<Item = Result<Frame<Bytes>, hyper::Error>> + Unpin,
{
    type Item = Result<Frame<Bytes>, hyper::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Step 1: If there's a pending send from a previous poll, resolve it first.
        // NOTE: The idle timer is NOT ticking here — we're resolving backpressure,
        // not waiting on the upstream. This is the backpressure exclusion.
        if self.pending_send.is_some() {
            let send_fut = self.pending_send.as_mut().unwrap();
            match send_fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    // Send completed — record backpressure duration
                    self.resolve_backpressure_wait();
                    self.pending_send = None;
                    // Reset idle timer: backpressure time doesn't count as upstream idle.
                    // The upstream was ready (it delivered this chunk), it was the cache
                    // writer that was slow. Don't penalize the upstream for that.
                    if let Some(timeout) = self.idle_timeout {
                        self.idle_sleep = Some(Box::pin(tokio::time::sleep(timeout)));
                    }
                    // Return the frame we were holding
                    let frame = self.pending_frame.take().unwrap();
                    return Poll::Ready(Some(Ok(Frame::data(frame))));
                }
                Poll::Ready(Err(_)) => {
                    // Channel closed — stop sending, continue streaming to client
                    self.resolve_backpressure_wait();
                    self.pending_send = None;
                    self.channel_closed = true;
                    // Reset idle timer after backpressure resolution
                    if let Some(timeout) = self.idle_timeout {
                        self.idle_sleep = Some(Box::pin(tokio::time::sleep(timeout)));
                    }
                    let frame = self.pending_frame.take().unwrap();
                    debug!("Cache channel closed during backpressure, continuing without caching");
                    return Poll::Ready(Some(Ok(Frame::data(frame))));
                }
                Poll::Pending => {
                    // Still waiting for channel capacity — propagate backpressure
                    return Poll::Pending;
                }
            }
        }

        // Step 2: Check the idle watchdog BEFORE polling the inner stream.
        // If the idle sleep has fired, the upstream has stalled — abort.
        if let Some(ref mut sleep) = self.idle_sleep {
            if sleep.as_mut().poll(cx).is_ready() {
                // Idle timeout fired — upstream stalled
                let timeout = self.idle_timeout.unwrap_or(Duration::from_secs(5));
                // Once per stream: the body ends here, and hyper does not poll a body again
                // after it returns `None`.
                UPSTREAM_IDLE_ABORT_MID_STREAM.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "[UPSTREAM_IDLE_ABORT] Mid-stream idle timeout ({:?}) exceeded, aborting stream. \
                     bytes_sent={}, backpressure_events={}",
                    timeout, self.bytes_sent, self.backpressure_events
                );
                // Return None to signal stream end. The hyper body will be incomplete,
                // which causes the client connection to be aborted (RST or incomplete
                // chunked encoding). The cache channel is dropped without all bytes,
                // so commit_incremental_range will fail with "size mismatch" and the
                // partial .bin/.tmp will be cleaned up.
                return Poll::Ready(None);
            }
        }

        // Step 3: Poll the inner stream for the next frame
        let inner = Pin::new(&mut self.inner);
        match inner.poll_next(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                // Reset the idle timer — we received a chunk from upstream
                if let Some(timeout) = self.idle_timeout {
                    self.idle_sleep = Some(Box::pin(tokio::time::sleep(timeout)));
                }

                match frame.into_data() {
                    Ok(data) => {
                        let bytes = data;
                        self.bytes_sent += bytes.len();

                        // If channel is closed, skip sending and just pass through
                        if self.channel_closed {
                            return Poll::Ready(Some(Ok(Frame::data(bytes))));
                        }

                        // Try non-blocking send first (fast path)
                        match self.sender.try_send(bytes.clone()) {
                            Ok(()) => {
                                // Sent successfully — return frame immediately
                                Poll::Ready(Some(Ok(Frame::data(bytes))))
                            }
                            Err(mpsc::error::TrySendError::Full(_returned_bytes)) => {
                                // Channel full — switch to backpressure mode
                                self.backpressure_events += 1;
                                BACKPRESSURE_EVENTS_TOTAL.fetch_add(1, Ordering::Relaxed);
                                self.backpressure_start = Some(Instant::now());

                                // Create a send future and store it
                                let sender = self.sender.clone();
                                let bytes_to_send = bytes.clone();
                                let send_fut =
                                    Box::pin(async move { sender.send(bytes_to_send).await });
                                self.pending_send = Some(send_fut);
                                self.pending_frame = Some(bytes);

                                // Poll the send future immediately
                                let send_fut = self.pending_send.as_mut().unwrap();
                                match send_fut.as_mut().poll(cx) {
                                    Poll::Ready(Ok(())) => {
                                        self.resolve_backpressure_wait();
                                        self.pending_send = None;
                                        let frame = self.pending_frame.take().unwrap();
                                        Poll::Ready(Some(Ok(Frame::data(frame))))
                                    }
                                    Poll::Ready(Err(_)) => {
                                        self.resolve_backpressure_wait();
                                        self.pending_send = None;
                                        self.channel_closed = true;
                                        let frame = self.pending_frame.take().unwrap();
                                        Poll::Ready(Some(Ok(Frame::data(frame))))
                                    }
                                    Poll::Pending => {
                                        // Channel still full — apply backpressure to client
                                        Poll::Pending
                                    }
                                }
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                // Channel closed — stop sending, continue streaming
                                self.channel_closed = true;
                                debug!("Cache channel closed, continuing without caching");
                                Poll::Ready(Some(Ok(Frame::data(bytes))))
                            }
                        }
                    }
                    Err(frame) => {
                        // Non-data frame (trailers, etc), pass through
                        // Don't reset idle timer for non-data frames
                        Poll::Ready(Some(Ok(frame)))
                    }
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                if self.backpressure_events > 0 {
                    debug!(
                        backpressure_events = self.backpressure_events,
                        backpressure_wait_ms = self.backpressure_wait.as_millis() as u64,
                        total_bytes = self.bytes_sent,
                        "Cache write backpressure occurred during stream"
                    );
                }
                Poll::Ready(None)
            }
            Poll::Pending => {
                // The inner stream is not ready — we ARE waiting on upstream here.
                // The idle timer is registered with the waker via the poll in Step 2,
                // so if the timeout fires before inner becomes ready, we'll wake up
                // and detect it in Step 2 on the next poll.
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_tee_stream_basic() {
        let (tx, mut rx) = mpsc::channel(10);

        let data = vec![
            Ok(Frame::data(Bytes::from("hello"))),
            Ok(Frame::data(Bytes::from("world"))),
        ];
        let stream = stream::iter(data);

        let mut tee = TeeStream::new(stream, tx);

        let mut collected = Vec::new();
        while let Some(Ok(frame)) = tee.next().await {
            if let Ok(data) = frame.into_data() {
                collected.push(data);
            }
        }

        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0], Bytes::from("hello"));
        assert_eq!(collected[1], Bytes::from("world"));

        drop(tee);
        let mut teed = Vec::new();
        while let Some(data) = rx.recv().await {
            teed.push(data);
        }
        assert_eq!(teed.len(), 2);
        assert_eq!(teed[0], Bytes::from("hello"));
        assert_eq!(teed[1], Bytes::from("world"));
    }

    #[tokio::test]
    async fn test_tee_stream_backpressure() {
        // Channel capacity 1 — forces backpressure on second chunk
        let (tx, mut rx) = mpsc::channel(1);

        let data = vec![
            Ok(Frame::data(Bytes::from("chunk1"))),
            Ok(Frame::data(Bytes::from("chunk2"))),
            Ok(Frame::data(Bytes::from("chunk3"))),
        ];
        let stream = stream::iter(data);

        let mut tee = TeeStream::new(stream, tx);

        // Consume from both sides concurrently
        let consumer = tokio::spawn(async move {
            let mut received = Vec::new();
            while let Some(data) = rx.recv().await {
                received.push(data);
            }
            received
        });

        let mut client_received = Vec::new();
        while let Some(Ok(frame)) = tee.next().await {
            if let Ok(data) = frame.into_data() {
                client_received.push(data);
            }
        }
        drop(tee);

        let cache_received = consumer.await.unwrap();

        // Both sides should receive all 3 chunks
        assert_eq!(client_received.len(), 3);
        assert_eq!(cache_received.len(), 3);
        assert_eq!(client_received[0], Bytes::from("chunk1"));
        assert_eq!(cache_received[0], Bytes::from("chunk1"));
    }

    /// Spec: cache-eviction-at-scale Requirement 18.4. Asserts the per-instance
    /// getters and the process-wide counters they feed both move when
    /// backpressure genuinely occurs, and that the per-instance figures agree
    /// with what `poll_next`'s own accounting produced.
    ///
    /// Per `testing.md` § "Never assert an exact delta on a process-global
    /// counter", this asserts the global counters moved by AT LEAST this
    /// instance's contribution (`>=`), not an exact value — other tests in
    /// this binary can concurrently increment the same process-wide atomics.
    #[tokio::test]
    async fn test_tee_stream_backpressure_updates_getters_and_global_counters() {
        tokio::time::timeout(
            Duration::from_secs(10),
            run_backpressure_updates_getters_and_global_counters_body(),
        )
        .await
        .expect(
            "test body must complete within 10s; a hang here means the channel \
                 or consumer was not closed correctly, not a slow assertion",
        );
    }

    async fn run_backpressure_updates_getters_and_global_counters_body() {
        let before_events = backpressure_events_total_get();
        let before_wait_ms = backpressure_wait_ms_total_get();

        // Channel capacity 1, and the receiver never drains — every send after the
        // first genuinely backpressures rather than merely taking a fast path.
        let (tx, mut rx) = mpsc::channel(1);

        let data = vec![
            Ok(Frame::data(Bytes::from("chunk1"))),
            Ok(Frame::data(Bytes::from("chunk2"))),
            Ok(Frame::data(Bytes::from("chunk3"))),
        ];
        let stream = stream::iter(data);
        let mut tee = TeeStream::new(stream, tx);

        // Drain the client side and the cache side concurrently, with a small
        // delay on the cache side so the channel genuinely fills at least once.
        let consumer = tokio::spawn(async move {
            let mut received = Vec::new();
            // Deliberately slow: gives poll_next's fast `try_send` path a real
            // chance to hit `Full` rather than racing an always-ready receiver.
            tokio::time::sleep(Duration::from_millis(20)).await;
            while let Some(data) = rx.recv().await {
                received.push(data);
            }
            received
        });

        while (tee.next().await).is_some() {}

        let instance_events = tee.backpressure_events();
        let instance_wait_ms = tee.backpressure_wait_ms();

        // Drop the stream (and its `sender` field) so the channel closes and the
        // consumer's `rx.recv()` loop can observe `None` and return, rather than
        // awaiting forever.
        drop(tee);
        let _ = consumer.await.unwrap();

        assert!(
            instance_events > 0,
            "expected genuine backpressure with a 1-capacity channel and a delayed \
             receiver, got 0 backpressure_events on the instance"
        );

        let after_events = backpressure_events_total_get();
        let after_wait_ms = backpressure_wait_ms_total_get();

        assert!(
            after_events >= before_events + instance_events,
            "process-wide backpressure_events_total_get did not move by at least \
             this instance's contribution: before={before_events} after={after_events} \
             instance={instance_events}"
        );
        // Wait time is >= 0 by construction (u64), so only assert it moved when
        // the instance itself recorded nonzero wait — a zero-wait instance
        // (e.g. the pending send resolves instantly) is a valid outcome and
        // must not force a false failure here.
        if instance_wait_ms > 0 {
            assert!(
                after_wait_ms >= before_wait_ms + instance_wait_ms,
                "process-wide backpressure_wait_ms_total_get did not move by at \
                 least this instance's contribution: before={before_wait_ms} \
                 after={after_wait_ms} instance={instance_wait_ms}"
            );
        }
    }

    #[tokio::test]
    async fn test_tee_stream_channel_closed() {
        let (tx, rx) = mpsc::channel(10);
        drop(rx); // Close receiver immediately

        let data = vec![
            Ok(Frame::data(Bytes::from("hello"))),
            Ok(Frame::data(Bytes::from("world"))),
        ];
        let stream = stream::iter(data);

        let mut tee = TeeStream::new(stream, tx);

        // Client should still receive all data even though cache channel is closed
        let mut collected = Vec::new();
        while let Some(Ok(frame)) = tee.next().await {
            if let Ok(data) = frame.into_data() {
                collected.push(data);
            }
        }

        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0], Bytes::from("hello"));
        assert_eq!(collected[1], Bytes::from("world"));
    }

    /// Test: mid-stream stall → stream terminated at idle timeout, no hanging.
    /// The idle timer fires when the upstream produces no chunks within the timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tee_stream_idle_timeout_fires_on_stall() {
        use std::sync::Arc;
        use tokio::sync::Notify;

        let (tx, _rx) = mpsc::channel(10);
        let idle_timeout = Duration::from_millis(100);

        // Create a stream that sends one chunk then stalls forever
        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();
        let stalling_stream = futures::stream::unfold(0u32, move |state| {
            let notify = notify_clone.clone();
            Box::pin(async move {
                match state {
                    0 => {
                        // First chunk: deliver immediately
                        Some((Ok(Frame::data(Bytes::from("first-chunk"))), 1))
                    }
                    _ => {
                        // Stall forever (simulates upstream hang)
                        notify.notified().await;
                        None
                    }
                }
            })
        });

        let mut tee = TeeStream::with_idle_timeout(stalling_stream, tx, idle_timeout);
        let aborts_before = upstream_idle_abort_total_mid_stream();
        let start = Instant::now();
        let mut received = Vec::new();
        while let Some(result) = tee.next().await {
            match result {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        received.push(data);
                    }
                }
                Err(_) => break,
            }
        }
        let elapsed = start.elapsed();

        // Should have received the first chunk
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], Bytes::from("first-chunk"));

        // Should have aborted within idle_timeout + small buffer (not hanging)
        assert!(
            elapsed < Duration::from_millis(500),
            "Expected abort within ~100ms idle timeout, took {:?}",
            elapsed
        );

        // The inner stream stalls forever after its first chunk, so the tee
        // ending at all (asserted by the loop exiting within the bound above)
        // is the idle abort.
        // Process-global counter: a lower bound on the delta, since other tests in this
        // binary may abort concurrently.
        let aborts_after = upstream_idle_abort_total_mid_stream();
        assert!(
            aborts_after > aborts_before,
            "the idle abort fired but cache.upstream_idle_abort_total_mid_stream did not move \
             ({aborts_before} -> {aborts_after})"
        );
    }

    /// Test: slow-but-steady transfer (bytes every < timeout) completes successfully.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tee_stream_slow_but_steady_completes() {
        let (tx, mut rx) = mpsc::channel(10);
        let idle_timeout = Duration::from_millis(200);

        // Stream that delivers chunks slowly (every 50ms) — well within the 200ms timeout
        let slow_stream = futures::stream::unfold(0u32, |state| {
            Box::pin(async move {
                if state >= 5 {
                    return None; // Done after 5 chunks
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                Some((
                    Ok(Frame::data(Bytes::from(format!("chunk-{}", state)))),
                    state + 1,
                ))
            })
        });

        let mut tee = TeeStream::with_idle_timeout(slow_stream, tx, idle_timeout);

        // Consume from cache side concurrently
        let consumer = tokio::spawn(async move {
            let mut received = Vec::new();
            while let Some(data) = rx.recv().await {
                received.push(data);
            }
            received
        });

        let mut client_received = Vec::new();
        while let Some(Ok(frame)) = tee.next().await {
            if let Ok(data) = frame.into_data() {
                client_received.push(data);
            }
        }
        drop(tee);

        let cache_received = consumer.await.unwrap();

        // All 5 chunks should be received on both sides
        assert_eq!(
            client_received.len(),
            5,
            "Client should receive all 5 chunks"
        );
        assert_eq!(cache_received.len(), 5, "Cache should receive all 5 chunks");

        // Receiving all chunks proves the stream was not idle-aborted.
    }

    /// Test: backpressure does NOT trigger idle abort.
    /// A slow cache writer (channel capacity 1) should not cause a false idle abort.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tee_stream_backpressure_not_misaborted() {
        // Channel capacity 1 — forces backpressure
        let (tx, mut rx) = mpsc::channel(1);
        let idle_timeout = Duration::from_millis(100);

        // Stream delivers chunks instantly (no upstream delay)
        let data = vec![
            Ok(Frame::data(Bytes::from("chunk1"))),
            Ok(Frame::data(Bytes::from("chunk2"))),
            Ok(Frame::data(Bytes::from("chunk3"))),
        ];
        let stream = stream::iter(data);

        let mut tee = TeeStream::with_idle_timeout(stream, tx, idle_timeout);

        // Slow consumer: reads from cache channel with delay > idle_timeout
        // This tests that backpressure doesn't trigger idle abort
        let consumer = tokio::spawn(async move {
            let mut received = Vec::new();
            while let Some(data) = rx.recv().await {
                received.push(data);
                // Simulate slow cache writer — takes longer than idle_timeout per chunk
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            received
        });

        let mut client_received = Vec::new();
        while let Some(Ok(frame)) = tee.next().await {
            if let Ok(data) = frame.into_data() {
                client_received.push(data);
            }
        }

        // All chunks delivered to client: an idle abort would have ended the
        // stream early, so this is also the proof that backpressure waits longer
        // than idle_timeout did NOT trigger the abort.
        assert_eq!(
            client_received.len(),
            3,
            "All chunks should reach client; fewer means backpressure triggered an idle abort"
        );

        drop(tee);
        let cache_received = consumer.await.unwrap();
        assert_eq!(cache_received.len(), 3, "All chunks should reach cache");
    }
}
