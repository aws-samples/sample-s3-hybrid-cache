//! Unit tests for request body cap (Requirement 11)
//!
//! Tests for Requirements 11.1, 11.2, 32.3:
//! - Request body within the cap is accepted (no error)
//! - Request body exceeding the cap returns ProxyError::RequestBodyTooLarge
//! - Content-Length header exceeding the cap is fast-rejected before reading body
//!
//! These tests exercise `read_request_body_bounded` from `src/signed_request_proxy.rs`
//! by spinning up a local hyper HTTP/1 server that receives real requests with
//! `hyper::body::Incoming` bodies.

use bytes::Bytes;
use http_body_util::Full;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use s3_proxy::signed_request_proxy::read_request_body_bounded;
use s3_proxy::ProxyError;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Helper: start a local HTTP/1 server that calls `read_request_body_bounded`
/// with the given `max_bytes` cap and returns the result as an HTTP response.
///
/// Returns (server_addr, shutdown_sender).
async fn start_body_cap_server(max_bytes: u64) -> (SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let max_bytes = Arc::new(max_bytes);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    let (stream, _) = match accept_result {
                        Ok(v) => v,
                        Err(_) => break,
                    };
                    let io = TokioIo::new(stream);
                    let max_bytes = Arc::clone(&max_bytes);

                    tokio::spawn(async move {
                        let max_bytes = *max_bytes;
                        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                            let max_bytes = max_bytes;
                            async move {
                                match read_request_body_bounded(req, max_bytes).await {
                                    Ok(body) => {
                                        let len = body.len().to_string();
                                        Ok::<_, Infallible>(
                                            Response::builder()
                                                .status(StatusCode::OK)
                                                .header("x-body-len", len)
                                                .body(Full::new(body))
                                                .unwrap(),
                                        )
                                    }
                                    Err(ProxyError::RequestBodyTooLarge {
                                        content_length,
                                        max_bytes,
                                    }) => {
                                        let msg = format!(
                                            "content_length={:?},max_bytes={}",
                                            content_length, max_bytes
                                        );
                                        Ok::<_, Infallible>(
                                            Response::builder()
                                                .status(StatusCode::PAYLOAD_TOO_LARGE)
                                                .body(Full::new(Bytes::from(msg)))
                                                .unwrap(),
                                        )
                                    }
                                    Err(e) => {
                                        Ok::<_, Infallible>(
                                            Response::builder()
                                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                                .body(Full::new(Bytes::from(format!("{}", e))))
                                                .unwrap(),
                                        )
                                    }
                                }
                            }
                        });

                        if let Err(e) = http1::Builder::new()
                            .serve_connection(io, service)
                            .await
                        {
                            eprintln!("server connection error: {}", e);
                        }
                    });
                }
                _ = &mut shutdown_rx => {
                    break;
                }
            }
        }
    });

    (addr, shutdown_tx)
}

/// Helper: send a PUT request with the given body to the test server.
async fn send_put_request(
    addr: SocketAddr,
    body: Vec<u8>,
    content_length_header: Option<u64>,
) -> (StatusCode, String) {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();

    let uri = format!("http://{}/test-object", addr);
    let mut builder = Request::builder().method("PUT").uri(&uri);

    if let Some(cl) = content_length_header {
        builder = builder.header("content-length", cl.to_string());
    }

    let req = builder
        .body(Full::new(Bytes::from(body)))
        .expect("failed to build request");

    let resp = client.request(req).await.expect("request failed");
    let status = resp.status();

    use http_body_util::BodyExt;
    let body_bytes = resp
        .into_body()
        .collect()
        .await
        .expect("failed to read response body")
        .to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes).to_string();

    (status, body_str)
}

// =============================================================================
// Test 1: Request body within the cap is accepted
// =============================================================================

#[tokio::test]
async fn body_within_cap_succeeds() {
    let max_bytes: u64 = 1024; // 1 KiB cap
    let (addr, shutdown_tx) = start_body_cap_server(max_bytes).await;

    // Send a body that is exactly at the cap
    let body = vec![0x42u8; 1024];
    let (status, _body_str) = send_put_request(addr, body, Some(1024)).await;

    assert_eq!(status, StatusCode::OK, "Body at cap should succeed");

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn small_body_within_cap_succeeds() {
    let max_bytes: u64 = 1024; // 1 KiB cap
    let (addr, shutdown_tx) = start_body_cap_server(max_bytes).await;

    // Send a small body well under the cap
    let body = b"hello world".to_vec();
    let (status, _body_str) = send_put_request(addr, body, Some(11)).await;

    assert_eq!(status, StatusCode::OK, "Small body should succeed");

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn empty_body_within_cap_succeeds() {
    let max_bytes: u64 = 1024; // 1 KiB cap
    let (addr, shutdown_tx) = start_body_cap_server(max_bytes).await;

    // Send an empty body
    let body = vec![];
    let (status, _body_str) = send_put_request(addr, body, Some(0)).await;

    assert_eq!(status, StatusCode::OK, "Empty body should succeed");

    let _ = shutdown_tx.send(());
}

// =============================================================================
// Test 2: Request body exceeding the cap returns HTTP 413
// =============================================================================

#[tokio::test]
async fn body_exceeding_cap_returns_413() {
    let max_bytes: u64 = 1024; // 1 KiB cap
    let (addr, shutdown_tx) = start_body_cap_server(max_bytes).await;

    // Send a body that exceeds the cap by 1 byte
    let body = vec![0x42u8; 1025];
    let (status, body_str) = send_put_request(addr, body, Some(1025)).await;

    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "Body exceeding cap should return 413"
    );
    assert!(
        body_str.contains("max_bytes=1024"),
        "Response should include max_bytes: {}",
        body_str
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn large_body_exceeding_cap_returns_413() {
    let max_bytes: u64 = 512; // 512 byte cap
    let (addr, shutdown_tx) = start_body_cap_server(max_bytes).await;

    // Send a body much larger than the cap
    let body = vec![0xAB; 4096];
    let (status, body_str) = send_put_request(addr, body, Some(4096)).await;

    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "Large body should return 413"
    );
    assert!(
        body_str.contains("max_bytes=512"),
        "Response should include max_bytes: {}",
        body_str
    );

    let _ = shutdown_tx.send(());
}

// =============================================================================
// Test 3: Content-Length header exceeding cap is fast-rejected before reading body
// =============================================================================

#[tokio::test]
async fn content_length_exceeding_cap_fast_rejects() {
    let max_bytes: u64 = 1024; // 1 KiB cap
    let (addr, shutdown_tx) = start_body_cap_server(max_bytes).await;

    // Send a request with Content-Length header claiming a large body,
    // but the actual body is small. The function should reject based on
    // the Content-Length header alone without reading the full body.
    let body = vec![0x42u8; 10]; // actual body is small
    let (status, body_str) = send_put_request(addr, body, Some(999_999)).await;

    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "Content-Length exceeding cap should fast-reject with 413"
    );
    assert!(
        body_str.contains("content_length=Some(999999)"),
        "Response should include the declared content_length: {}",
        body_str
    );
    assert!(
        body_str.contains("max_bytes=1024"),
        "Response should include max_bytes: {}",
        body_str
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn content_length_at_boundary_plus_one_fast_rejects() {
    let max_bytes: u64 = 1024; // 1 KiB cap
    let (addr, shutdown_tx) = start_body_cap_server(max_bytes).await;

    // Content-Length is exactly cap + 1
    let body = vec![0x42u8; 10];
    let (status, body_str) = send_put_request(addr, body, Some(1025)).await;

    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "Content-Length at cap+1 should fast-reject"
    );
    assert!(
        body_str.contains("content_length=Some(1025)"),
        "Response should include content_length=Some(1025): {}",
        body_str
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn content_length_at_exact_cap_is_accepted() {
    let max_bytes: u64 = 1024; // 1 KiB cap
    let (addr, shutdown_tx) = start_body_cap_server(max_bytes).await;

    // Content-Length is exactly at the cap — should be accepted
    let body = vec![0x42u8; 1024];
    let (status, _body_str) = send_put_request(addr, body, Some(1024)).await;

    assert_eq!(
        status,
        StatusCode::OK,
        "Content-Length at exact cap should be accepted"
    );

    let _ = shutdown_tx.send(());
}
