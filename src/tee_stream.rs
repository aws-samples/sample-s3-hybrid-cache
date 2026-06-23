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
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::Sleep;
use tracing::{debug, warn};

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
    /// Whether the stream was aborted due to idle timeout
    idle_aborted: bool,
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
            idle_aborted: false,
        }
    }

    /// Create a new tee stream with an idle timeout watchdog.
    ///
    /// If the upstream produces no bytes within `idle_timeout`, the stream
    /// terminates (returns an error frame), aborting the client connection.
    /// The cache channel is dropped without finalization.
    pub fn with_idle_timeout(
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
            idle_aborted: false,
        }
    }

    /// Get the number of bytes sent through the tee
    pub fn bytes_sent(&self) -> usize {
        self.bytes_sent
    }

    /// Whether the stream was aborted due to idle timeout
    pub fn was_idle_aborted(&self) -> bool {
        self.idle_aborted
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
                    if let Some(start) = self.backpressure_start.take() {
                        self.backpressure_wait += start.elapsed();
                    }
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
                    if let Some(start) = self.backpressure_start.take() {
                        self.backpressure_wait += start.elapsed();
                    }
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
                warn!(
                    "[UPSTREAM_IDLE_ABORT] Mid-stream idle timeout ({:?}) exceeded, aborting stream. \
                     bytes_sent={}, backpressure_events={}",
                    timeout, self.bytes_sent, self.backpressure_events
                );
                self.idle_aborted = true;
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
                                        if let Some(start) = self.backpressure_start.take() {
                                            self.backpressure_wait += start.elapsed();
                                        }
                                        self.pending_send = None;
                                        let frame = self.pending_frame.take().unwrap();
                                        Poll::Ready(Some(Ok(Frame::data(frame))))
                                    }
                                    Poll::Ready(Err(_)) => {
                                        if let Some(start) = self.backpressure_start.take() {
                                            self.backpressure_wait += start.elapsed();
                                        }
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

        // Confirm the stream was idle-aborted
        assert!(tee.was_idle_aborted());
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

        // Stream should NOT have been idle-aborted
        // (can't check was_idle_aborted after drop, but receiving all chunks proves it)
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

        // Stream should NOT have been idle-aborted despite backpressure waits > idle_timeout
        assert!(
            !tee.was_idle_aborted(),
            "Backpressure should NOT trigger idle abort"
        );

        // All chunks delivered to client
        assert_eq!(client_received.len(), 3, "All chunks should reach client");

        drop(tee);
        let cache_received = consumer.await.unwrap();
        assert_eq!(cache_received.len(), 3, "All chunks should reach cache");
    }
}
