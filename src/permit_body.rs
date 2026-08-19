//! `PermitBody`: an [`http_body::Body`] wrapper that holds a request-concurrency
//! permit for the body's entire lifetime.
//!
//! ## Why this exists
//!
//! Before this wrapper, `server.max_concurrent_requests`' semaphore permit was
//! released as soon as `handle_request` returned the `Response` — i.e. at
//! response-head construction (Setup_Phase), not once the body had actually
//! finished streaming to the client (Transfer_Phase). A handful of slow,
//! concurrent large-object downloads could therefore accumulate unboundedly
//! with no admission control, because each permit was already back in the
//! pool by the time the slow part of the request (the transfer) happened.
//!
//! `PermitBody` closes that gap: it wraps any existing response body and holds
//! an `Arc<OwnedSemaphorePermit>` for as long as the wrapper is alive. The
//! permit is released exactly when the body is dropped — whether that is
//! normal completion, an early client disconnect, or an error partway through
//! — because `Arc`'s `Drop` does the releasing, not any explicit call site.
//!
//! Spec: transfer-concurrency-admission, Requirements 1.2, 1.3, 1.4, 6.1, 6.2.

use crate::inflight_ledger::Reservation;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::OwnedSemaphorePermit;

/// Wraps a body `B`, holding a permit for the body's lifetime.
///
/// `permit: None` is a no-op wrapper — `poll_frame`/`is_end_stream`/`size_hint`
/// delegate straight through with no behavioural change. This lets call sites
/// that legitimately have no permit to attach (see call-site comments
/// justifying each such case) use the same type without a separate code path.
pub struct PermitBody<B> {
    inner: B,
    // Held only for its Drop side effect; never read. `Arc` (not a bare
    // `OwnedSemaphorePermit`) because Commit_Phase tasks (cache-write,
    // RAM-promotion) need to hold a share of the same permit alongside the
    // client-facing body, so the permit must be cloneable.
    #[allow(dead_code)]
    permit: Option<Arc<OwnedSemaphorePermit>>,
    // A cached response range can be fully buffered before its response head
    // is returned. Keep its ledger reservation with the body so the claimed
    // bytes remain accounted until the client drains or drops the response.
    #[allow(dead_code)]
    reservation: Option<Reservation>,
}

impl<B> PermitBody<B> {
    pub fn new(inner: B, permit: Option<Arc<OwnedSemaphorePermit>>) -> Self {
        Self {
            inner,
            permit,
            reservation: None,
        }
    }

    /// Retain a buffered-byte ledger reservation until this body is dropped.
    pub fn with_reservation(mut self, reservation: Reservation) -> Self {
        self.reservation = Some(reservation);
        self
    }
}

/// Frame size for [`ChunkedBytes`]. Small enough that hyper's write-buffer
/// watermark engages between frames (its h1 buffer strategy stops polling the
/// body while the previous frames are still unflushed), large enough that a
/// multi-MiB range costs tens of polls, not thousands.
const CHUNKED_BYTES_FRAME_SIZE: usize = 64 * 1024;

/// An already-buffered payload served as a sequence of `Bytes` slices instead
/// of one `Full` frame.
///
/// ## Why this exists
///
/// A single-frame body (`Full<Bytes>`) is exhausted by hyper's FIRST
/// `poll_frame`: hyper moves the whole payload into its own write pipeline,
/// the next poll returns `None`, and hyper drops the body object — even if the
/// client has not read one byte yet. Any RAII state attached to the body
/// (`PermitBody`'s ledger `Reservation`) is therefore released at "handed to
/// hyper", not at "delivered to the client", and can never span a slow client
/// read. This is exactly the gap observed live: a stalled client holding a
/// 12 MiB unread response saw `inflight_memory.reserved_bytes = 0` because the
/// `Full` frame had already been yielded and the body dropped.
///
/// Yielding the same `Bytes` as `CHUNKED_BYTES_FRAME_SIZE`-sized slices (pure
/// refcount operations — no copy) gives hyper's own flow control something to
/// withhold: it stops polling while its write buffer is above the watermark,
/// so the body — and the `Reservation` riding on it via
/// [`PermitBody::with_reservation`] — genuinely lives until the client drains
/// or disconnects. That is the contract the buffered-range serve documents.
///
/// Spec: inflight-memory-accounting (response-side reservation lifetime);
/// unsigned-write-path-streaming Requirement 6.2.
pub struct ChunkedBytes {
    data: Bytes,
    pos: usize,
}

impl ChunkedBytes {
    pub fn new(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }
}

impl Body for ChunkedBytes {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.pos >= self.data.len() {
            return Poll::Ready(None);
        }
        let end = self
            .pos
            .saturating_add(CHUNKED_BYTES_FRAME_SIZE)
            .min(self.data.len());
        let frame = self.data.slice(self.pos..end);
        self.pos = end;
        Poll::Ready(Some(Ok(Frame::data(frame))))
    }

    fn is_end_stream(&self) -> bool {
        self.pos >= self.data.len()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact((self.data.len() - self.pos) as u64)
    }
}

impl<B> Body for PermitBody<B>
where
    B: Body<Data = Bytes> + Unpin,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{BodyExt, Full};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    /// Frames pass through unchanged, in the same order, with no data loss.
    #[tokio::test]
    async fn frames_pass_through_unchanged() {
        let inner = Full::new(Bytes::from_static(b"hello world"));
        let sem = Arc::new(Semaphore::new(1));
        let permit = Arc::new(sem.try_acquire_owned().unwrap());
        let body = PermitBody::new(inner, Some(permit));

        let collected = body.collect().await.unwrap().to_bytes();
        assert_eq!(collected, Bytes::from_static(b"hello world"));
    }

    /// The permit is released when the body is dropped after being fully
    /// consumed (normal completion).
    #[tokio::test]
    async fn permit_released_on_completion() {
        let sem = Arc::new(Semaphore::new(1));
        let permit = Arc::new(sem.clone().try_acquire_owned().unwrap());
        assert_eq!(sem.available_permits(), 0, "permit held before body starts");

        let inner = Full::new(Bytes::from_static(b"data"));
        let body = PermitBody::new(inner, Some(permit));
        let _ = body.collect().await.unwrap();
        // `body` (and its `Arc<OwnedSemaphorePermit>`) is dropped by the time
        // `collect()` returns and control reaches here, since `collect`
        // consumes `body` by value.
        assert_eq!(
            sem.available_permits(),
            1,
            "permit released after completion"
        );
    }

    /// The permit is released on an early drop, before the body is fully
    /// consumed — e.g. a client disconnecting mid-transfer.
    #[tokio::test]
    async fn permit_released_on_early_drop() {
        let sem = Arc::new(Semaphore::new(1));
        let permit = Arc::new(sem.clone().try_acquire_owned().unwrap());
        assert_eq!(sem.available_permits(), 0);

        let inner = Full::new(Bytes::from_static(b"data"));
        let body = PermitBody::new(inner, Some(permit));
        // Never poll the body at all — simulates a connection dying before
        // any frame is read.
        drop(body);
        assert_eq!(sem.available_permits(), 1, "permit released on early drop");
    }

    /// `permit: None` is a true no-op: no panic, no behavioural change, and
    /// (since there's nothing to release) nothing to assert on except that
    /// frames still pass through correctly.
    #[tokio::test]
    async fn none_permit_is_a_no_op() {
        let inner = Full::new(Bytes::from_static(b"unattached"));
        let body: PermitBody<Full<Bytes>> = PermitBody::new(inner, None);
        let collected = body.collect().await.unwrap().to_bytes();
        assert_eq!(collected, Bytes::from_static(b"unattached"));
    }

    /// A shared permit (cloned `Arc`) held by a second "task" alongside the
    /// body is not released until BOTH drop — modeling a Commit_Phase task
    /// holding a share of the same permit as the client-facing PermitBody.
    #[tokio::test]
    async fn shared_permit_not_released_until_all_holders_drop() {
        let sem = Arc::new(Semaphore::new(1));
        let permit = Arc::new(sem.clone().try_acquire_owned().unwrap());
        let commit_phase_share = Arc::clone(&permit);

        let inner = Full::new(Bytes::from_static(b"data"));
        let body = PermitBody::new(inner, Some(permit));
        let _ = body.collect().await.unwrap();
        assert_eq!(
            sem.available_permits(),
            0,
            "still held: commit-phase share outlives the body"
        );

        drop(commit_phase_share);
        assert_eq!(sem.available_permits(), 1, "released once all shares drop");
    }

    /// Body errors propagate through unchanged (permit release is independent
    /// of the frame's Ok/Err payload — it only depends on the wrapper being
    /// dropped, which happens on an error path exactly as on a success path).
    #[tokio::test]
    async fn error_frames_pass_through() {
        use futures::stream;
        use http_body_util::StreamBody;

        let counter = Arc::new(AtomicUsize::new(0));
        counter.fetch_add(1, Ordering::SeqCst);
        // `stream::iter` (unlike `stream::once(async {...})`) has no inner
        // future to poll, so it stays `Unpin` — required by `PermitBody`'s
        // `B: Unpin` bound. A single already-resolved `Err` item models an
        // upstream error frame just as well as an async block would.
        let stream = stream::iter(vec![Err::<Frame<Bytes>, &'static str>("boom")]);
        let inner = StreamBody::new(stream);
        let sem = Arc::new(Semaphore::new(1));
        let permit = Arc::new(sem.clone().try_acquire_owned().unwrap());
        let mut body = PermitBody::new(inner, Some(permit));

        let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        assert!(matches!(frame, Some(Err("boom"))));
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        drop(body);
        assert_eq!(sem.available_permits(), 1);
    }

    #[test]
    fn ledger_reservation_is_released_when_response_body_drops() {
        let ledger = Arc::new(crate::inflight_ledger::InflightLedger::new(1024));
        let reservation = ledger.try_reserve(512).expect("reservation admitted");
        let body = PermitBody::new(Full::new(Bytes::from_static(b"data")), None)
            .with_reservation(reservation);

        assert_eq!(ledger.reserved_bytes(), 512, "body retains ledger claim");
        drop(body);
        assert_eq!(ledger.reserved_bytes(), 0, "dropping body releases claim");
    }

    /// `ChunkedBytes` reproduces the payload byte-exactly across multiple
    /// frames, and reports an exact size hint so hyper emits Content-Length
    /// framing rather than chunked transfer-encoding.
    #[tokio::test]
    async fn chunked_bytes_round_trips_multi_frame_payload() {
        let payload = Bytes::from(
            (0..3 * CHUNKED_BYTES_FRAME_SIZE + 17)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let body = ChunkedBytes::new(payload.clone());
        assert_eq!(body.size_hint().exact(), Some(payload.len() as u64));
        assert!(!body.is_end_stream());
        let collected = body.collect().await.unwrap().to_bytes();
        assert_eq!(collected, payload);
    }

    /// A `ChunkedBytes` payload is NOT exhausted by a single poll — the
    /// property that keeps a `PermitBody` reservation alive until the client
    /// drains. `Full` fails this by design (one frame, then end-of-stream).
    #[tokio::test]
    async fn chunked_bytes_is_not_exhausted_by_first_poll() {
        let payload = Bytes::from(vec![7u8; 2 * CHUNKED_BYTES_FRAME_SIZE]);
        let mut body = ChunkedBytes::new(payload);

        let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .expect("first frame")
            .expect("infallible");
        assert_eq!(
            frame.into_data().unwrap().len(),
            CHUNKED_BYTES_FRAME_SIZE,
            "first poll yields one frame-sized slice, not the whole payload"
        );
        assert!(
            !body.is_end_stream(),
            "body must remain live after the first poll so an attached \
             Reservation is not released at 'handed to hyper'"
        );
    }

    /// Empty payload ends immediately (HEAD-style bodies stay valid).
    #[tokio::test]
    async fn chunked_bytes_empty_payload_is_end_of_stream() {
        let body = ChunkedBytes::new(Bytes::new());
        assert!(body.is_end_stream());
        let collected = body.collect().await.unwrap().to_bytes();
        assert!(collected.is_empty());
    }
}
