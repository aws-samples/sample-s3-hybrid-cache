//! A request that comes back to the proxy that forwarded it is refused, not recursed.
//!
//! Routing a `Host` at the proxy fleet is the customer's DNS responsibility. The proxy
//! appends a per-process `Via` entry to every request it forwards and refuses with
//! `508 Loop Detected` any request that already carries its own entry. An entry from
//! another proxy (chaining) is forwarded intact with this process's entry appended.
//!
//! # What these tests assert
//!
//! The 508 test asserts three things together: the status, the error body, and that the
//! upstream fixture received **zero** calls. The status alone passes on any early return.
//! The prefix tests use tokens that are a prefix of, and an extension of, this process's
//! pseudonym, since a prefix match would produce false loops between instances.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::sync::Semaphore;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::signed_request_proxy::via_pseudonym;
use s3_proxy::S3ClientApi;

use common::{CapturedRequest, StubResponse, StubS3Client};

const HOST: &str = "minio.example.com";
const PATH: &str = "/mybucket/data.bin";

const AUTH_NOT_SIGNING_VIA: &str = "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20260101/us-west-2/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=sig";
const AUTH_SIGNING_VIA: &str = "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20260101/us-west-2/s3/aws4_request, SignedHeaders=host;via;x-amz-date, Signature=sig";

fn test_config(cache_dir: std::path::PathBuf) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    config.cache.read_cache_enabled = true;
    config.cache.write_cache_enabled = true;
    Arc::new(config)
}

async fn make_cache_infra(config: &Arc<Config>) -> (Arc<CacheManager>, Arc<RangeHandler>) {
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        config.cache.cache_dir.clone(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        false,
        config.cache.get_ttl,
        config.cache.head_ttl,
        config.cache.put_ttl,
        config.cache.actively_remove_cached_data,
        config.cache.shared_storage.clone(),
        config.cache.write_cache_percent,
        config.cache.write_cache_enabled,
        config.cache.incomplete_upload_ttl,
        config.cache.metadata_cache.clone(),
        config.cache.eviction_trigger_percent,
        config.cache.eviction_target_percent,
        config.cache.read_cache_enabled,
        config.cache.bucket_settings_staleness_threshold,
        config.cache.compression_batch_size,
        config.cache.evaluate_conditions_from_cache,
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    ));
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));
    cache_manager.initialize().await.expect("cache init");
    disk_cache_manager
        .write()
        .await
        .initialize()
        .await
        .expect("disk cache init");
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    (cache_manager, range_handler)
}

struct ProxyServer {
    addr: SocketAddr,
    _shutdown_tx: oneshot::Sender<()>,
}

async fn spawn_proxy_server(
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
    s3_client: Arc<dyn S3ClientApi + Send + Sync>,
) -> ProxyServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let request_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_requests));
    let inflight_tracker = Arc::new(InFlightTracker::new());

    tokio::spawn(async move {
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let (stream, peer) = match accept {
                        Ok(v) => v,
                        Err(_) => break,
                    };
                    let io = TokioIo::new(stream);
                    let config = Arc::clone(&config);
                    let cache_manager = Arc::clone(&cache_manager);
                    let range_handler = Arc::clone(&range_handler);
                    let s3_client = Arc::clone(&s3_client);
                    let inflight_tracker = Arc::clone(&inflight_tracker);
                    let request_semaphore = Arc::clone(&request_semaphore);
                    tokio::spawn(async move {
                        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                            let config = Arc::clone(&config);
                            let cache_manager = Arc::clone(&cache_manager);
                            let range_handler = Arc::clone(&range_handler);
                            let s3_client = Arc::clone(&s3_client);
                            let inflight_tracker = Arc::clone(&inflight_tracker);
                            let request_semaphore = Arc::clone(&request_semaphore);
                            let inflight_ledger = s3_client.get_inflight_ledger();
                            async move {
                                HttpProxy::handle_request(
                                    req,
                                    peer,
                                    config,
                                    cache_manager,
                                    s3_client,
                                    range_handler,
                                    request_semaphore,
                                    None,
                                    None,
                                    inflight_tracker,
                                    None,
                                    None,
                                    None,
                                    inflight_ledger,
                                )
                                .await
                            }
                        });
                        let _ = http1::Builder::new().serve_connection(io, service).await;
                    });
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    ProxyServer {
        addr,
        _shutdown_tx: shutdown_tx,
    }
}

/// One GET through the proxy with optional `Via` and `Authorization` headers.
async fn get(addr: SocketAddr, via: Option<&str>, auth: &str) -> (StatusCode, Bytes) {
    let stream = TcpStream::connect(addr).await.expect("connect");
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .expect("handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let mut builder = Request::builder()
        .method("GET")
        .uri(PATH)
        .header("host", HOST)
        .header("authorization", auth);
    if let Some(via) = via {
        builder = builder.header("via", via);
    }
    let req = builder.body(Full::new(Bytes::new())).expect("request");
    let resp = sender.send_request(req).await.expect("proxy request");
    let status = resp.status();
    let body = resp.into_body().collect().await.expect("body").to_bytes();
    (status, body)
}

struct Fixture {
    _dir: TempDir,
    stub: StubS3Client,
    server: ProxyServer,
}

async fn fixture() -> Fixture {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path().to_path_buf());
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let body = b"loop-test-body";
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::OK)
            .with_header("content-length", body.len().to_string())
            .with_header("etag", "\"loop-etag\"")
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .with_header("content-type", "application/octet-stream")
            .with_body(Bytes::from_static(body)),
    );
    let server = spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        stub.clone().into_trait_object(),
    )
    .await;
    Fixture {
        _dir: dir,
        stub,
        server,
    }
}

fn header_of<'a>(req: &'a CapturedRequest, name: &str) -> Option<&'a str> {
    req.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

#[tokio::test]
async fn own_token_inbound_gives_508_and_no_upstream_call() {
    let fx = fixture().await;
    let via = format!("1.1 {}", via_pseudonym());
    let (status, body) = get(fx.server.addr, Some(&via), AUTH_NOT_SIGNING_VIA).await;
    let text = String::from_utf8_lossy(&body);

    assert_eq!(status, StatusCode::LOOP_DETECTED, "body: {}", text);
    assert!(
        text.contains("<Code>LoopDetected</Code>"),
        "body is not the loop error: {}",
        text
    );
    assert_eq!(
        fx.stub.captured().len(),
        0,
        "the upstream fixture must not be called for a looped request"
    );
}

#[tokio::test]
async fn own_token_in_a_chained_list_is_still_a_loop() {
    let fx = fixture().await;
    let via = format!("1.1 upstream-proxy, 1.1 {}", via_pseudonym());
    let (status, _) = get(fx.server.addr, Some(&via), AUTH_NOT_SIGNING_VIA).await;
    assert_eq!(status, StatusCode::LOOP_DETECTED);
    assert_eq!(fx.stub.captured().len(), 0);
}

#[tokio::test]
async fn foreign_via_is_forwarded_with_ours_appended() {
    let fx = fixture().await;
    let (status, _) = get(
        fx.server.addr,
        Some("1.1 upstream-proxy"),
        AUTH_NOT_SIGNING_VIA,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let captured = fx.stub.captured();
    assert_eq!(captured.len(), 1, "exactly one upstream call");
    assert_eq!(
        header_of(&captured[0], "via"),
        Some(format!("1.1 upstream-proxy, 1.1 {}", via_pseudonym()).as_str())
    );
}

#[tokio::test]
async fn request_without_via_is_forwarded_with_ours() {
    let fx = fixture().await;
    let (status, _) = get(fx.server.addr, None, AUTH_NOT_SIGNING_VIA).await;
    assert_eq!(status, StatusCode::OK);
    let captured = fx.stub.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(
        header_of(&captured[0], "via"),
        Some(format!("1.1 {}", via_pseudonym()).as_str())
    );
}

#[tokio::test]
async fn token_that_is_prefix_or_extension_of_ours_is_not_a_loop() {
    let ours = via_pseudonym();
    let prefix = &ours[..ours.len() - 1];
    let extension = format!("{}x", ours);
    assert_ne!(prefix, ours);
    for other in [prefix.to_string(), extension] {
        let fx = fixture().await;
        let via = format!("1.1 {}", other);
        let (status, _) = get(fx.server.addr, Some(&via), AUTH_NOT_SIGNING_VIA).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{} must not be read as a loop",
            other
        );
        assert_eq!(fx.stub.captured().len(), 1);
    }
}

#[tokio::test]
async fn signed_via_is_forwarded_byte_identical() {
    let fx = fixture().await;
    let (status, _) = get(fx.server.addr, Some("1.1 signed-value"), AUTH_SIGNING_VIA).await;
    assert_eq!(status, StatusCode::OK);
    let captured = fx.stub.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(header_of(&captured[0], "via"), Some("1.1 signed-value"));
}

/// One request of any method through the proxy, with optional `Via` and `Authorization`.
async fn send(
    addr: SocketAddr,
    method: &str,
    via: Option<&str>,
    auth: Option<&str>,
    body: &'static [u8],
) -> StatusCode {
    let stream = TcpStream::connect(addr).await.expect("connect");
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .expect("handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let mut builder = Request::builder()
        .method(method)
        .uri(PATH)
        .header("host", HOST)
        .header("content-length", body.len().to_string());
    if let Some(auth) = auth {
        builder = builder.header("authorization", auth);
    }
    if let Some(via) = via {
        builder = builder.header("via", via);
    }
    let req = builder.body(Full::new(Bytes::from(body))).expect("request");
    let resp = sender.send_request(req).await.expect("proxy request");
    let status = resp.status();
    let _ = resp.into_body().collect().await;
    status
}

#[tokio::test]
async fn looped_unsigned_put_and_delete_give_508_and_no_upstream_call() {
    let fx = fixture().await;
    let via = format!("1.1 {}", via_pseudonym());
    for method in ["PUT", "DELETE"] {
        let status = send(fx.server.addr, method, Some(&via), None, b"payload").await;
        assert_eq!(status, StatusCode::LOOP_DETECTED, "{}", method);
    }
    assert_eq!(
        fx.stub.captured().len(),
        0,
        "no upstream call for a looped unsigned PUT or DELETE"
    );
}

/// DELETE reaches the upstream through `handle_other_request`, which builds its own
/// request context. (A PUT streams to the upstream directly and is covered by the
/// streaming serializer tests in `signed_request_proxy.rs`.)
#[tokio::test]
async fn unsigned_delete_is_forwarded_with_via() {
    let fx = fixture().await;
    let status = send(
        fx.server.addr,
        "DELETE",
        Some("1.1 upstream-proxy"),
        None,
        b"",
    )
    .await;
    assert!(status.is_success(), "DELETE gave {}", status);
    let captured = fx.stub.captured();
    assert_eq!(captured.len(), 1, "exactly one upstream call");
    assert_eq!(
        header_of(&captured[0], "via"),
        Some(format!("1.1 upstream-proxy, 1.1 {}", via_pseudonym()).as_str()),
        "our Via must be appended on the generic forward path"
    );
}

// ---------------------------------------------------------------------------------------
// Repeated request headers.
//
// A client may send one header on several lines. SigV4 signs such a header as its values
// trimmed and joined with `,` (no space), so the request S3 receives must carry every
// value: either the lines as sent or one line whose value is that join. These tests use
// this file's harness because it drives the real `HttpProxy::handle_request` and the stub
// records the header map each upstream request is built from.
// ---------------------------------------------------------------------------------------

const AUTH_SIGNING_ATTRIBUTES: &str = "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20260101/us-west-2/s3/aws4_request, SignedHeaders=host;x-amz-date;x-amz-object-attributes, Signature=sig";

/// One request through the proxy with the given header lines, in order. A name listed
/// twice is sent on two lines.
async fn send_with_headers(
    addr: SocketAddr,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
) -> StatusCode {
    let stream = TcpStream::connect(addr).await.expect("connect");
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .expect("handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("host", HOST);
    for (name, value) in headers {
        // `Builder::header` appends, so a repeated name becomes a second line.
        builder = builder.header(*name, *value);
    }
    let req = builder.body(Full::new(Bytes::new())).expect("request");
    let resp = tokio::time::timeout(Duration::from_secs(30), sender.send_request(req))
        .await
        .expect("proxy answered within 30s")
        .expect("proxy request");
    let status = resp.status();
    let _ = resp.into_body().collect().await;
    status
}

/// GetObjectAttributes as the AWS SDK for Rust sends it: the attribute list on two
/// lines, each signed as part of the one canonical `x-amz-object-attributes`.
#[tokio::test]
async fn repeated_signed_header_reaches_upstream_with_every_value() {
    let fx = fixture().await;
    let uri = format!("{}?attributes", PATH);
    let status = send_with_headers(
        fx.server.addr,
        "GET",
        &uri,
        &[
            ("authorization", AUTH_SIGNING_ATTRIBUTES),
            ("x-amz-date", "20260101T000000Z"),
            ("x-amz-object-attributes", "ETag"),
            ("x-amz-object-attributes", "ObjectSize"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let captured = fx.stub.captured();
    assert_eq!(captured.len(), 1, "exactly one upstream call");
    assert_eq!(
        header_of(&captured[0], "x-amz-object-attributes"),
        Some("ETag,ObjectSize"),
        "the upstream request must carry every value of the repeated header, joined \
         as SigV4 canonicalises it"
    );
    // A header sent once is forwarded unchanged.
    assert_eq!(
        header_of(&captured[0], "x-amz-date"),
        Some("20260101T000000Z")
    );
}

/// Control: the same values on one line, as the AWS CLI sends them, pass unchanged.
#[tokio::test]
async fn single_line_list_header_is_forwarded_unchanged() {
    let fx = fixture().await;
    let uri = format!("{}?attributes", PATH);
    let status = send_with_headers(
        fx.server.addr,
        "GET",
        &uri,
        &[
            ("authorization", AUTH_SIGNING_ATTRIBUTES),
            ("x-amz-date", "20260101T000000Z"),
            ("x-amz-object-attributes", "ETag,ObjectSize"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let captured = fx.stub.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(
        header_of(&captured[0], "x-amz-object-attributes"),
        Some("ETag,ObjectSize")
    );
}

/// The generic forward path (`handle_other_request`) builds its own header map.
#[tokio::test]
async fn repeated_header_on_unsigned_delete_reaches_upstream_with_every_value() {
    let fx = fixture().await;
    let status = send_with_headers(
        fx.server.addr,
        "DELETE",
        PATH,
        &[
            ("x-amz-meta-list", "a"),
            ("x-amz-meta-list", "b"),
            ("x-amz-meta-single", "only"),
        ],
    )
    .await;
    assert!(status.is_success(), "DELETE gave {}", status);
    let captured = fx.stub.captured();
    assert_eq!(captured.len(), 1, "exactly one upstream call");
    assert_eq!(header_of(&captured[0], "x-amz-meta-list"), Some("a,b"));
    assert_eq!(header_of(&captured[0], "x-amz-meta-single"), Some("only"));
}

/// Control: loop detection reads every `Via` line, so this process's entry on the
/// second of two lines is still a loop.
#[tokio::test]
async fn own_token_on_a_second_via_line_is_still_a_loop() {
    let fx = fixture().await;
    let ours = format!("1.1 {}", via_pseudonym());
    let status = send_with_headers(
        fx.server.addr,
        "GET",
        PATH,
        &[
            ("authorization", AUTH_NOT_SIGNING_VIA),
            ("via", "1.1 upstream-proxy"),
            ("via", &ours),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::LOOP_DETECTED);
    assert_eq!(fx.stub.captured().len(), 0);
}

/// Two foreign `Via` lines: both hops are forwarded, with this process's entry appended.
#[tokio::test]
async fn repeated_foreign_via_keeps_every_hop() {
    let fx = fixture().await;
    let status = send_with_headers(
        fx.server.addr,
        "GET",
        PATH,
        &[
            ("authorization", AUTH_NOT_SIGNING_VIA),
            ("via", "1.1 first-proxy"),
            ("via", "1.1 second-proxy"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let captured = fx.stub.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(
        header_of(&captured[0], "via"),
        Some(format!("1.1 first-proxy,1.1 second-proxy, 1.1 {}", via_pseudonym()).as_str())
    );
}
