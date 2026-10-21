//! `connection_pool.max_lifetime` enforcement.
//!
//! The property under test is **retirement**, not that the configured value was
//! read: an upstream connection that has reached `max_lifetime` must not be
//! handed to another request, and one inside its lifetime must still be reused.
//! Asserting only that the cap value arrives somewhere would pass against a
//! value that is then discarded, which is exactly how this field came to be
//! validated, logged and inert.
//!
//! Two things every test here does deliberately:
//!
//! * **`pool_idle_timeout` is set to an hour, far above any cap used below, and
//!   no test sleeps between requests.** `idle_timeout` (55s in production, and
//!   honoured) retires an idle connection on its own, so a fixture that waited
//!   out a lifetime could not tell the cap from `idle_timeout`. Here the
//!   connection is idle for microseconds and the pool's own expiry cannot fire.
//! * **Age is supplied by stamping the connection as established in the past**,
//!   not by waiting. That keeps the case the feature exists for — a connection
//!   in continuous use, which `idle_timeout` will never reclaim — reachable in
//!   milliseconds.
//!
//! Reuse is observed at the server: it counts accepted TCP connections, so
//! "reused" and "reconnected" are distinguishable from outside the client.

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::Request;
use hyper_util::client::legacy::connect::{capture_connection, Connection};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use s3_proxy::https_connector::{connection_age, retire_if_over_max_lifetime, HttpsStream};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tower::Service;

/// A connector that dials a local listener and stamps the resulting connection
/// with an establishment instant chosen by the test.
///
/// This is the production `HttpsStream`, reached through its explicit-instant
/// constructor — so `HttpsStream::connected`, the stamping that the cap depends
/// on, is the code under test rather than a copy of it.
#[derive(Clone)]
struct AgedConnector {
    addr: SocketAddr,
    established_at: Instant,
}

impl Service<hyper::Uri> for AgedConnector {
    type Response = HttpsStream;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<HttpsStream, std::io::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: hyper::Uri) -> Self::Future {
        let addr = self.addr;
        let established_at = self.established_at;
        Box::pin(async move {
            let tcp = TcpStream::connect(addr).await?;
            Ok(HttpsStream::plain_with_established_at(tcp, established_at))
        })
    }
}

/// Minimal keepalive-capable HTTP/1.1 origin. Returns its address and a counter
/// of accepted TCP connections — the observation that distinguishes a reused
/// connection from a new one.
async fn spawn_counting_origin() -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepts);

    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => {
                            if sock
                                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });

    (addr, accepts)
}

fn build_client(connector: AgedConnector) -> Client<AgedConnector, Empty<Bytes>> {
    Client::builder(TokioExecutor::new())
        // An hour, deliberately: far above every cap used in this file, so no
        // assertion below can be satisfied by hyper's own idle expiry.
        .pool_idle_timeout(Duration::from_secs(3600))
        .pool_max_idle_per_host(10)
        .build(connector)
}

/// Issue one request, drain the body so the connection goes back to the pool,
/// and apply the cap. Returns whether the connection was retired.
async fn request_then_apply_cap(
    client: &Client<AgedConnector, Empty<Bytes>>,
    addr: SocketAddr,
    max_lifetime: Duration,
) -> (hyper::StatusCode, bool) {
    let mut request = Request::builder()
        .uri(format!("http://{}/object", addr))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let capture = capture_connection(&mut request);

    let response = client.request(request).await.expect("request failed");
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"ok", "origin body should arrive intact");

    let retired = retire_if_over_max_lifetime(&capture, max_lifetime, Instant::now());
    (status, retired)
}

/// Control. Without this, the reconnect asserted in the next test proves
/// nothing: a fixture that never reuses a connection would satisfy it whether
/// or not the cap works.
#[tokio::test]
async fn a_connection_inside_its_lifetime_is_reused() {
    let (addr, accepts) = spawn_counting_origin().await;
    let client = build_client(AgedConnector {
        addr,
        established_at: Instant::now(),
    });

    let (status, retired) = request_then_apply_cap(&client, addr, Duration::from_secs(300)).await;
    assert_eq!(status, hyper::StatusCode::OK);
    assert!(!retired, "a connection stamped as new must not be retired");

    let (status, retired) = request_then_apply_cap(&client, addr, Duration::from_secs(300)).await;
    assert_eq!(status, hyper::StatusCode::OK);
    assert!(!retired);

    assert_eq!(
        accepts.load(Ordering::SeqCst),
        1,
        "both requests should have run on one connection; if this is 2 the fixture \
         is not reusing connections and the retirement assertions mean nothing"
    );
}

/// The assertion the feature exists for: an over-age connection is taken out of
/// the pool, so the next request has to open a new one.
#[tokio::test]
async fn a_connection_that_has_reached_max_lifetime_is_not_reused() {
    let (addr, accepts) = spawn_counting_origin().await;
    // Established ten minutes ago against a five-minute cap. The connection is
    // never idle for more than the microseconds between these two requests.
    let client = build_client(AgedConnector {
        addr,
        established_at: Instant::now() - Duration::from_secs(600),
    });

    let (status, retired) = request_then_apply_cap(&client, addr, Duration::from_secs(300)).await;
    assert_eq!(status, hyper::StatusCode::OK);
    assert!(
        retired,
        "a connection past max_lifetime must be retired from the pool"
    );
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        1,
        "the request that discovered the over-age connection must still be served on it"
    );

    let (status, _) = request_then_apply_cap(&client, addr, Duration::from_secs(300)).await;
    assert_eq!(
        status,
        hyper::StatusCode::OK,
        "retiring a connection must not fail the next request"
    );
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        2,
        "the retired connection must not have been handed out again"
    );
}

/// The stamp is a property of the connection, and the production constructor
/// applies it. Without this, the cap could be reading an instant that only the
/// test-only constructor ever supplies.
#[tokio::test]
async fn the_connector_stamps_when_the_connection_was_established() {
    let (addr, _accepts) = spawn_counting_origin().await;

    let fresh = HttpsStream::plain(TcpStream::connect(addr).await.unwrap());
    assert!(
        fresh.established_at().elapsed() < Duration::from_secs(5),
        "the stream should record its own establishment instant"
    );
    let age = connection_age(&fresh.connected(), Instant::now())
        .expect("a connection built by the production constructor must carry its instant");
    assert!(
        age < Duration::from_secs(5),
        "a just-connected stream should read as new, got {:?}",
        age
    );

    let old = HttpsStream::plain_with_established_at(
        TcpStream::connect(addr).await.unwrap(),
        Instant::now() - Duration::from_secs(900),
    );
    let age = connection_age(&old.connected(), Instant::now()).expect("stamp should be present");
    assert!(
        age >= Duration::from_secs(900),
        "an explicitly aged stream should read as old, got {:?}",
        age
    );
}

/// Retirement must not touch a transfer already running on the connection. The
/// origin here sends the response head and half the body, pauses, then sends the
/// rest; the cap is applied in the pause. A mechanism that signalled EOF or an
/// error on an over-age connection would truncate this body — which on a proxy
/// whose workload is long transfers would be a data-path defect, not a tuning
/// regression.
#[tokio::test]
async fn retiring_a_connection_does_not_truncate_a_body_already_streaming() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let _ = sock.read(&mut buf).await;
        sock.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\n\r\nhalf")
            .await
            .unwrap();
        sock.flush().await.unwrap();
        // Wide enough that the cap below is applied while these bytes are still
        // outstanding. The assertion is on the bytes received, not on timing.
        tokio::time::sleep(Duration::from_millis(500)).await;
        sock.write_all(b"rest").await.unwrap();
        sock.flush().await.unwrap();
    });

    let client = build_client(AgedConnector {
        addr,
        established_at: Instant::now() - Duration::from_secs(600),
    });

    let mut request = Request::builder()
        .uri(format!("http://{}/streaming", addr))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let capture = capture_connection(&mut request);

    let response = client.request(request).await.expect("request failed");
    assert_eq!(response.status(), hyper::StatusCode::OK);

    // Retire the connection with the body still in flight.
    assert!(
        retire_if_over_max_lifetime(&capture, Duration::from_secs(300), Instant::now()),
        "the connection is over-age, so this must retire it"
    );

    let body = response.into_body().collect().await;
    let body = body.expect("body must complete after the connection was retired");
    assert_eq!(
        &body.to_bytes()[..],
        b"halfrest",
        "the whole body must arrive; a truncated body means retirement acted mid-request"
    );
}
