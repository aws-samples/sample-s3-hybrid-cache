//! In-flight-memory ledger integration tests for response-side buffering.
//!
//! `InflightLedger`/`Reservation` themselves are exhaustively unit-tested in
//! `src/inflight_ledger.rs` (concurrent reserve/release, rejection leaves the
//! total unchanged, peak tracking, `try_grow` success/failure, disabled-ledger
//! no-op). These tests prove the production response path consults the ledger
//! at a real buffering site: a small range read from a warm cache reaches
//! `HttpProxy::serve_range_from_cache_buffered` through
//! `HttpProxy::handle_request`.
//!
//! The former unsigned-PUT fixtures were intentionally retired. Unsigned writes
//! stream after the unsigned-write-path-streaming change and no longer reserve
//! against this ledger, so keeping those tests would leave them green while
//! exercising nothing. A mid-body disconnect and unknown-size request-body
//! accumulation are likewise request-side-only properties and belong to the
//! retired path rather than contrived response-side cases.
//!
//! Requirement coverage:
//! - 10.1: concurrent response-side reservations under the ceiling are admitted.
//! - 10.2: a breaching response-side reservation is rejected with the Shed_Response.
//! - 10.3: a rejected reservation leaves the ledger total unchanged.
//! - 10.4: the ledger total returns to zero after a response completes.
//! - 10.7: a config omitting the ceiling parses and yields Ledger_Disabled.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Request, StatusCode};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_ledger::InflightLedger;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::S3ClientApi;

use common::{StubResponse, StubS3Client};

const CACHED_OBJECT_SIZE: usize = 4 * 1024;
const RANGE_SIZE: usize = 1_000;
const RANGE_HEADER: &str = "bytes=0-999";
const CACHE_PATH: &str = "/mybucket/ledger-response-object.bin";

/// Config with RAM disabled and a disk-streaming threshold above the test
/// range. After the warm GET below creates the cache entry, the range read
/// therefore takes `serve_range_from_cache_buffered`, not the disk-streaming
/// branch. The high request limit keeps permit admission out of these tests.
fn ledger_test_config(cache_dir: std::path::PathBuf) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    config.cache.read_cache_enabled = true;
    config.cache.disk_streaming_threshold = (CACHED_OBJECT_SIZE + 1) as u64;
    config.server.max_concurrent_requests = 100;
    Arc::new(config)
}

/// Build the cache infrastructure the same way `HttpProxy::new` wires it.
async fn make_cache_infra(
    config: &Arc<Config>,
) -> (
    Arc<CacheManager>,
    Arc<tokio::sync::RwLock<DiskCacheManager>>,
    Arc<RangeHandler>,
) {
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        config.cache.cache_dir.clone(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::LRU,
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

    (cache_manager, disk_cache_manager, range_handler)
}

struct ProxyServer {
    addr: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
}

/// Spin up a local HTTP/1 server whose service calls the real
/// `HttpProxy::handle_request`. Range reads made through it exercise the
/// response-side ledger obtained from `s3_client`.
async fn spawn_proxy_server(
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
    s3_client: Arc<dyn S3ClientApi + Send + Sync>,
) -> ProxyServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_requests,
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let inflight_ledger = s3_client.get_inflight_ledger();

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
                    let request_semaphore = Arc::clone(&semaphore);
                    let inflight_ledger = Arc::clone(&inflight_ledger);

                    tokio::spawn(async move {
                        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                            let config = Arc::clone(&config);
                            let cache_manager = Arc::clone(&cache_manager);
                            let range_handler = Arc::clone(&range_handler);
                            let s3_client = Arc::clone(&s3_client);
                            let inflight_tracker = Arc::clone(&inflight_tracker);
                            let request_semaphore = Arc::clone(&request_semaphore);
                            let inflight_ledger = Arc::clone(&inflight_ledger);
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

                        if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                            eprintln!("proxy connection error: {}", e);
                        }
                    });
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    ProxyServer { addr, shutdown_tx }
}

struct ProxyResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

/// Send a signed GET through the loopback proxy. An optional Range header
/// makes the warmed-cache request reach the response-side buffering site.
async fn proxy_get(addr: SocketAddr, path: &str, range: Option<&str>) -> ProxyResponse {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let uri = format!("http://{}{}", addr, path);
    let mut builder = Request::builder().method("GET").uri(&uri).header(
        "authorization",
        "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=sig",
    );
    if let Some(range) = range {
        builder = builder.header("range", range);
    }

    let response = client
        .request(builder.body(Full::new(Bytes::new())).expect("build GET"))
        .await
        .expect("proxy GET failed");
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.into_body().collect().await.unwrap().to_bytes();

    ProxyResponse {
        status,
        headers,
        body,
    }
}

fn cached_object_body() -> Bytes {
    Bytes::from(
        (0..CACHED_OBJECT_SIZE)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    )
}

fn cached_object_stub() -> StubS3Client {
    StubS3Client::new().with_default(
        StubResponse::ok(cached_object_body())
            .with_header("etag", "\"ledger-test-etag\"")
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .with_header("content-type", "application/octet-stream"),
    )
}

/// Populate the real cache through the normal GET path while the ledger is
/// disabled. Tests then start a separate server over the same cache with their
/// test ledger, so the range request itself is the only ledger participant.
async fn warm_cache(
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
) {
    let server = spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        cached_object_stub().into_trait_object(),
    )
    .await;

    let response = proxy_get(server.addr, CACHE_PATH, None).await;
    assert!(
        response.status.is_success(),
        "warm GET must populate the cache: {}",
        response.status
    );
    assert_eq!(response.body, cached_object_body());
    let _ = server.shutdown_tx.send(());
}

async fn spawn_warmed_response_server(
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
    ledger: Arc<InflightLedger>,
) -> ProxyServer {
    warm_cache(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
    )
    .await;

    spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        cached_object_stub()
            .with_inflight_ledger(ledger)
            .into_trait_object(),
    )
    .await
}

fn expected_range_body() -> Bytes {
    cached_object_body().slice(0..RANGE_SIZE)
}

// ===========================================================================
// Requirement 10.1: concurrent response-side reservations under the ceiling
// are admitted.
// ===========================================================================

/// Five concurrent small range reads from a warm cache all fit under the
/// ceiling. Each request takes the buffered cached-range branch, rather than
/// relying on the retired unsigned request-body reservation site.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_reservations_under_ceiling_all_admitted() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = ledger_test_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let ledger = Arc::new(InflightLedger::new(10_000));
    let server = spawn_warmed_response_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        Arc::clone(&ledger),
    )
    .await;

    let mut handles = Vec::new();
    for _ in 0..5 {
        let addr = server.addr;
        handles.push(tokio::spawn(async move {
            proxy_get(addr, CACHE_PATH, Some(RANGE_HEADER)).await
        }));
    }

    for handle in handles {
        let response = handle.await.expect("range task panicked");
        assert_eq!(
            response.status,
            StatusCode::PARTIAL_CONTENT,
            "each cached range under the ceiling must be admitted"
        );
        assert_eq!(response.body, expected_range_body());
        assert_eq!(response.headers.get("x-cache").unwrap(), "HIT");
    }
    assert_eq!(
        ledger.reserved_bytes(),
        0,
        "all range reservations released"
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// Requirement 10.2/10.3: a breaching response-side reservation is rejected
// with the Shed_Response, leaving the ledger total unchanged.
// ===========================================================================

/// A warm cached range larger than the ceiling must be rejected by the
/// response-side Admission_Check before the cached bytes are loaded.
#[tokio::test(flavor = "multi_thread")]
async fn breaching_reservation_rejected_with_shed_response() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = ledger_test_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let ledger = Arc::new(InflightLedger::new(100));
    let server = spawn_warmed_response_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        Arc::clone(&ledger),
    )
    .await;

    assert_eq!(
        ledger.reserved_bytes(),
        0,
        "nothing reserved before the range GET"
    );
    let rejected_before = ledger.rejected_total();

    let response = proxy_get(server.addr, CACHE_PATH, Some(RANGE_HEADER)).await;
    assert_eq!(
        response.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a ceiling-breaching cached range must get the Shed_Response (503), not 413"
    );
    assert!(
        String::from_utf8_lossy(&response.body).contains("SlowDown"),
        "Shed_Response must carry the S3 error code SlowDown"
    );
    assert_eq!(
        ledger.reserved_bytes(),
        0,
        "a rejected response-side reservation must leave the ledger total unchanged"
    );
    assert!(
        ledger.rejected_total() > rejected_before,
        "rejected_total must increment on a ceiling-breaching response-side Admission_Check"
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// A ledger rejection must remain the shared Shed_Response.
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn ledger_rejection_is_shed_response_with_retry_after_header() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = ledger_test_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let ledger = Arc::new(InflightLedger::new(100));
    let server = spawn_warmed_response_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        ledger,
    )
    .await;

    let response = proxy_get(server.addr, CACHE_PATH, Some(RANGE_HEADER)).await;
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("5"),
        "the response-side ledger Shed_Response must carry Retry-After"
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// Requirement 10.4: the ledger total returns to zero after a range response
// completes.
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn ledger_total_returns_to_zero_after_completion() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = ledger_test_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let ledger = Arc::new(InflightLedger::new(10_000));
    let server = spawn_warmed_response_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        Arc::clone(&ledger),
    )
    .await;

    let response = proxy_get(server.addr, CACHE_PATH, Some(RANGE_HEADER)).await;
    assert_eq!(response.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.body, expected_range_body());
    assert_eq!(
        ledger.reserved_bytes(),
        0,
        "the response-side reservation must release after the cached range is fully delivered"
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// Requirement 10.7: a config omitting the ceiling parses and yields
// Ledger_Disabled.
// ===========================================================================

#[test]
fn config_omitting_ceiling_yields_disabled_default() {
    let yaml = r#"
cache:
  cache_dir: /tmp/s3-proxy-test-cache
logging:
  access_log_dir: /tmp/s3-proxy-test-logs/access
  app_log_dir: /tmp/s3-proxy-test-logs/app
"#;
    let config: Config = serde_yaml_ng::from_str(yaml)
        .expect("a minimal config omitting max_inflight_buffer_bytes must still parse");

    assert_eq!(
        config.server.max_inflight_buffer_bytes, 0,
        "omitting the field must yield the Ledger_Disabled default (0)"
    );

    let ledger = InflightLedger::new(config.server.max_inflight_buffer_bytes);
    assert!(
        ledger.is_disabled(),
        "the ledger constructed from the parsed default must be Ledger_Disabled"
    );
}

// ===========================================================================
// Reservation lifetime: the response-side reservation must span the client
// drain, not just the load. These two tests fail against a single-frame
// (`Full`) buffered body, which hyper exhausts on its first poll — releasing
// the attached Reservation while the payload is still entirely undelivered.
// They pass only with the ChunkedBytes body, whose frames give hyper's write
// watermark something to withhold. (unsigned-write-path-streaming Req 6.2;
// the fleet counterpart is T40g in deployment-verification.sh.)
// ===========================================================================

/// Large enough that the payload cannot be absorbed by hyper's write buffer
/// plus the loopback socket buffers while the client stalls (client recv
/// buffer is pinned small below). 16 MiB gives a wide margin over any
/// plausible autotuned loopback buffering.
const LARGE_OBJECT_SIZE: usize = 16 * 1024 * 1024;
const LARGE_PATH_A: &str = "/mybucket/ledger-large-a.bin";
const LARGE_PATH_B: &str = "/mybucket/ledger-large-b.bin";

fn large_object_body() -> Bytes {
    Bytes::from(
        (0..LARGE_OBJECT_SIZE)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    )
}

fn large_object_stub() -> StubS3Client {
    StubS3Client::new().with_default(
        StubResponse::ok(large_object_body())
            .with_header("etag", "\"ledger-large-etag\"")
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .with_header("content-type", "application/octet-stream"),
    )
}

/// Same shape as `ledger_test_config`, with the disk-streaming threshold
/// raised above the large object so its full-object range still takes
/// `serve_range_from_cache_buffered`. RAM cache stays disabled — the RAM-hit
/// serve path attaches no reservation, so a RAM hit would bypass the site
/// under test.
fn large_ledger_test_config(cache_dir: std::path::PathBuf) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_cache_size = 256 * 1024 * 1024;
    config.cache.read_cache_enabled = true;
    config.cache.disk_streaming_threshold = (LARGE_OBJECT_SIZE + 1) as u64;
    config.server.max_concurrent_requests = 100;
    Arc::new(config)
}

/// Warm both large keys through the normal GET path with the ledger disabled.
async fn warm_large_cache(
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
) {
    let server = spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        large_object_stub().into_trait_object(),
    )
    .await;
    for path in [LARGE_PATH_A, LARGE_PATH_B] {
        let response = proxy_get(server.addr, path, None).await;
        assert!(
            response.status.is_success(),
            "warm GET for {} must populate the cache: {}",
            path,
            response.status
        );
        assert_eq!(response.body.len(), LARGE_OBJECT_SIZE);
    }
    let _ = server.shutdown_tx.send(());
}

/// A raw HTTP/1 client that sends a signed range GET, reads ONLY the response
/// head, and then deliberately stops reading — modeling a stalled client. The
/// receive buffer is pinned small so the kernel cannot absorb the payload on
/// the client side. Returns the open stream (keeping the transfer stalled),
/// any body bytes that arrived in the same reads as the head, and the status.
async fn stalled_range_get(
    addr: SocketAddr,
    path: &str,
    range: &str,
) -> (tokio::net::TcpStream, Vec<u8>, u16) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let socket = tokio::net::TcpSocket::new_v4().expect("client socket");
    socket
        .set_recv_buffer_size(16 * 1024)
        .expect("pin client recv buffer small");
    let mut stream = tokio::time::timeout(Duration::from_secs(10), socket.connect(addr))
        .await
        .expect("connect timed out")
        .expect("connect failed");

    let request = format!(
        "GET {path} HTTP/1.1\r\nhost: {addr}\r\nauthorization: AWS4-HMAC-SHA256 \
         Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=sig\r\nrange: {range}\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");

    // Read until the end of the response head, then stop.
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut chunk))
            .await
            .expect("response head timed out")
            .expect("read response head");
        assert!(n > 0, "connection closed before response head completed");
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status code in response head");
    assert!(
        head.to_lowercase().contains("x-cache: hit"),
        "stalled range GET must be served from cache (head: {head})"
    );
    (stream, buf[head_end..].to_vec(), status)
}

/// Poll the ledger until `predicate` holds, up to `deadline`.
async fn wait_for_ledger(
    ledger: &Arc<InflightLedger>,
    deadline: Duration,
    predicate: impl Fn(u64) -> bool,
    what: &str,
) {
    let start = std::time::Instant::now();
    loop {
        if predicate(ledger.reserved_bytes()) {
            return;
        }
        assert!(
            start.elapsed() < deadline,
            "timed out waiting for {} (reserved_bytes={})",
            what,
            ledger.reserved_bytes()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The buffered-range reservation must stay live while the client stalls
/// mid-body, and release only once the client drains the response.
///
/// Fails against a single-frame body: hyper's first poll exhausts it, the
/// body is dropped, and `reserved_bytes` returns to 0 with the payload still
/// undelivered — exactly the false-zero observed on the fleet.
#[tokio::test(flavor = "multi_thread")]
async fn reservation_held_while_client_stalls_released_on_drain() {
    use tokio::io::AsyncReadExt;

    let temp_dir = TempDir::new().expect("tempdir");
    let config = large_ledger_test_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    warm_large_cache(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
    )
    .await;

    let ledger = Arc::new(InflightLedger::new(2 * LARGE_OBJECT_SIZE as u64));
    let server = spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        large_object_stub()
            .with_inflight_ledger(Arc::clone(&ledger))
            .into_trait_object(),
    )
    .await;

    let range = format!("bytes=0-{}", LARGE_OBJECT_SIZE - 1);
    let (mut stream, leftover, status) = stalled_range_get(server.addr, LARGE_PATH_A, &range).await;
    assert_eq!(status, 206, "stalled cached range must be admitted");

    // The reservation must be observable and SUSTAINED while the client
    // stalls. A load-phase-only reservation (the pre-fix behaviour) is
    // transient and fails the sustain check below.
    wait_for_ledger(
        &ledger,
        Duration::from_secs(5),
        |reserved| reserved == LARGE_OBJECT_SIZE as u64,
        "reservation to appear while client stalls",
    )
    .await;
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            ledger.reserved_bytes(),
            LARGE_OBJECT_SIZE as u64,
            "reservation must remain live while the client has not drained the body"
        );
    }

    // Drain the body and verify byte-exact delivery.
    let mut body = leftover;
    let mut chunk = vec![0u8; 64 * 1024];
    while body.len() < LARGE_OBJECT_SIZE {
        let n = tokio::time::timeout(Duration::from_secs(30), stream.read(&mut chunk))
            .await
            .expect("drain timed out")
            .expect("drain read");
        assert!(n > 0, "connection closed before body completed");
        body.extend_from_slice(&chunk[..n]);
    }
    assert_eq!(body.len(), LARGE_OBJECT_SIZE);
    assert_eq!(Bytes::from(body), large_object_body());

    // Once drained, the reservation must release.
    wait_for_ledger(
        &ledger,
        Duration::from_secs(5),
        |reserved| reserved == 0,
        "reservation release after client drain",
    )
    .await;

    let _ = server.shutdown_tx.send(());
}

/// Two concurrent buffered ranges, each under the ceiling but summing over
/// it, must contend: while the first client stalls mid-body holding its
/// reservation, the second request is shed with 503 SlowDown + Retry-After.
/// After the first client disconnects, the same request is admitted.
///
/// This is the in-process counterpart of fleet group T40g, and the direct
/// contention proof Requirement 10.2's single-request breach cannot give.
#[tokio::test(flavor = "multi_thread")]
async fn second_buffered_range_shed_while_first_reservation_live() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = large_ledger_test_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    warm_large_cache(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
    )
    .await;

    // Each leg is 16 MiB; the ceiling admits one leg but not two.
    let ceiling = (LARGE_OBJECT_SIZE + LARGE_OBJECT_SIZE / 2) as u64;
    let ledger = Arc::new(InflightLedger::new(ceiling));
    let server = spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        large_object_stub()
            .with_inflight_ledger(Arc::clone(&ledger))
            .into_trait_object(),
    )
    .await;

    let range = format!("bytes=0-{}", LARGE_OBJECT_SIZE - 1);
    let (stream_a, _leftover_a, status_a) =
        stalled_range_get(server.addr, LARGE_PATH_A, &range).await;
    assert_eq!(status_a, 206, "first leg must be admitted");
    wait_for_ledger(
        &ledger,
        Duration::from_secs(5),
        |reserved| reserved == LARGE_OBJECT_SIZE as u64,
        "first leg's reservation to be live",
    )
    .await;

    // Second leg while the first reservation is live: must be shed, not 413,
    // not admitted.
    let rejected_before = ledger.rejected_total();
    let response = proxy_get(server.addr, LARGE_PATH_B, Some(&range)).await;
    assert_eq!(
        response.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "second buffered range must be shed while the first reservation is live"
    );
    assert!(
        String::from_utf8_lossy(&response.body).contains("SlowDown"),
        "Shed_Response must carry the S3 error code SlowDown"
    );
    assert_eq!(
        response
            .headers
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("5"),
        "Shed_Response must carry Retry-After"
    );
    assert!(
        ledger.rejected_total() > rejected_before,
        "rejected_total must increment for the shed leg"
    );
    assert_eq!(
        ledger.reserved_bytes(),
        LARGE_OBJECT_SIZE as u64,
        "the shed leg must leave the first leg's reservation untouched"
    );

    // Disconnect the stalled client; its reservation must release, and the
    // previously shed request must now be admitted byte-exactly.
    drop(stream_a);
    wait_for_ledger(
        &ledger,
        Duration::from_secs(5),
        |reserved| reserved == 0,
        "reservation release after client disconnect",
    )
    .await;
    let retry = proxy_get(server.addr, LARGE_PATH_B, Some(&range)).await;
    assert_eq!(
        retry.status,
        StatusCode::PARTIAL_CONTENT,
        "the shed condition must be transient: same request admitted after release"
    );
    assert_eq!(retry.body, large_object_body());

    let _ = server.shutdown_tx.send(());
}
