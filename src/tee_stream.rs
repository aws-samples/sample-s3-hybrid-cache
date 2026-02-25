//! Tee Stream Module
//!
//! Provides a stream wrapper that clones data chunks and sends them to a channel
//! while passing the original data through. This enables simultaneous streaming
//! to the client and caching in the background.

use bytes::Bytes;
use futures::Stream;
use hyper::body::Frame;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// A stream that tees data to a channel while passing it through
pub struct TeeStream<S> {
    inner: S,
    sender: mpsc::Sender<Bytes>,
    bytes_sent: usize,
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
        let inner = Pin::new(&mut self.inner);

        match inner.poll_next(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                // Check if this is a data frame
                match frame.into_data() {
                    Ok(data) => {
                        let bytes = data;
                        self.bytes_sent += bytes.len();

                        // Try to send to cache channel (non-blocking)
                        // If channel is full or closed, just log and continue
                        if let Err(e) = self.sender.try_send(bytes.clone()) {
                            match e {
                                mpsc::error::TrySendError::Full(_) => {
                                    warn!("Cache channel full, dropping chunk");
                                }
                                mpsc::error::TrySendError::Closed(_) => {
                                    debug!("Cache channel closed, stopping tee");
                                }
                            }
                        }

                        // Return the original data frame
                        Poll::Ready(Some(Ok(Frame::data(bytes))))
                    }
                    Err(frame) => {
                        // Non-data frame (trailers, etc), pass through
                        Poll::Ready(Some(Ok(frame)))
                    }
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
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

        // Create a simple stream
        let data = vec![
            Ok(Frame::data(Bytes::from("hello"))),
            Ok(Frame::data(Bytes::from("world"))),
        ];
        let stream = stream::iter(data);

        let mut tee = TeeStream::new(stream, tx);

        // Collect from tee stream
        let mut collected = Vec::new();
        while let Some(Ok(frame)) = tee.next().await {
            if let Ok(data) = frame.into_data() {
                collected.push(data);
            }
        }

        // Check main stream data
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0], Bytes::from("hello"));
        assert_eq!(collected[1], Bytes::from("world"));

        // Check tee'd data
        drop(tee);
        let mut teed = Vec::new();
        while let Some(data) = rx.recv().await {
            teed.push(data);
        }
        assert_eq!(teed.len(), 2);
        assert_eq!(teed[0], Bytes::from("hello"));
        assert_eq!(teed[1], Bytes::from("world"));
    }
}
