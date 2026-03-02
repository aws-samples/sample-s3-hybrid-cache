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

use bytes::Bytes;
use futures::Stream;
use hyper::body::Frame;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::debug;

/// A stream that tees data to a channel while passing it through.
///
/// Uses backpressure-aware sends: when the cache channel is full, the stream
/// pauses (returns Poll::Pending) until capacity is available. This guarantees
/// all chunks reach the cache writer at the cost of throttling client download
/// speed to match disk write speed when the cache writer can't keep up.
pub struct TeeStream<S> {
    inner: S,
    sender: mpsc::Sender<Bytes>,
    bytes_sent: usize,
    /// Stored send future for backpressure — when the channel is full, we store
    /// the pending send and the frame to return, then poll the send on subsequent calls.
    pending_send: Option<Pin<Box<dyn Future<Output = Result<(), mpsc::error::SendError<Bytes>>> + Send + Sync>>>,
    pending_frame: Option<Bytes>,
    /// Track backpressure timing for observability
    backpressure_wait: Duration,
    backpressure_start: Option<Instant>,
    backpressure_events: usize,
    /// Whether the channel has been closed (receiver dropped)
    channel_closed: bool,
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
        }
    }

    /// Get the number of bytes sent through the tee
    pub fn bytes_sent(&self) -> usize {
        self.bytes_sent
    }
}

impl<S> Stream for TeeStream<S>
where
    S: Stream<Item = Result<Frame<Bytes>, hyper::Error>> + Unpin,
{
    type Item = Result<Frame<Bytes>, hyper::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Step 1: If there's a pending send from a previous poll, resolve it first
        if self.pending_send.is_some() {
            let send_fut = self.pending_send.as_mut().unwrap();
            match send_fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    // Send completed — record backpressure duration
                    if let Some(start) = self.backpressure_start.take() {
                        self.backpressure_wait += start.elapsed();
                    }
                    self.pending_send = None;
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

        // Step 2: Poll the inner stream for the next frame
        let inner = Pin::new(&mut self.inner);
        match inner.poll_next(cx) {
            Poll::Ready(Some(Ok(frame))) => {
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
                                let send_fut = Box::pin(async move {
                                    sender.send(bytes_to_send).await
                                });
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
            Poll::Pending => Poll::Pending,
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
}
