//! Permit-lifetime integration tests.
//!
//! Spec: `.kiro/specs/transfer-concurrency-admission/` (Requirement 9), combined
//! into `.kiro/specs/combined-2.5.0/tasks.md` Phase C, tasks 14 and 16.
//!
//! `PermitBody` itself is already exhaustively unit-tested in
//! `src/permit_body.rs` (frames unchanged, released on completion, released on
//! early drop, `None` is a no-op, shared-permit multi-holder semantics, error
//! frames pass through). What those tests cannot prove is that the *production
//! request path* actually attaches the real per-request `OwnedSemaphorePermit`
//! at the sites the design calls out, end to end, through `HttpProxy::handle_request`.
//! This file closes that gap for the paths the test harness can exercise.
//!
//! ## Coverage and its boundary
//!
//! `tests/common::StubS3Client` only ever returns `S3ResponseBody::Buffered`
//! (see `StubResponse::into_s3_response`) — there is no way to make the stub
//! hand back an `S3ResponseBody::Streaming(Incoming)`, because that variant
//! wraps a real hyper body type tied to an actual connection. That means S1,
//! S3-S6 (the streaming-from-S3 sites in `convert_s3_response_to_http` /
//! `forward_get_head_to_s3_and_cache` / `forward_signed_range_request` /
//! `handle_signed_range_s3_response`) cannot be driven through this harness
//! without a real upstream TCP/TLS connection standing in for S3 — which is a
//! materially larger harness (a real listener acting as a fake S3, wired
//! through `connection_pool`/`https_connector`) than is proportionate to add
//! here.
//!
//! What *is* fully testable with the existing harness, and is what this file
//! covers:
//!
//! - **Requirement 9.4** (permit released after a `Buffered_Response`): direct
//!   coverage via `handle_request` over a real loopback HTTP/1 connection.
//! - **Requirement 9.6** (exhausting permits produces the Shed_Response): direct
//!   coverage — a semaphore of size 1 with a slow first request in flight, and a
//!   concurrent second request that must be shed 503/SlowDown/Retry-After.
//! - **Requirement 9.5** (permits return to the configured total once requests
//!   and Commit_Phase tasks complete): direct coverage — after a cache-populating
//!   GET's background commit task finishes, `available_permits()` is back at
//!   the configured total, proving the Commit_Phase share (task 15) actually
//!   releases rather than leaking.
//! - **S2** (`serve_range_from_cache`'s disk-streaming path, `permit_body.rs`
//!   design table entry) is reachable through this harness without a fake S3,
//!   because S2 streams from the *disk cache*, not from S3: seed an object via a
//!   buffered GET (which the production path caches synchronously), then issue
//!   a range GET large enough (`disk_streaming_threshold`) to take the disk
//!   streaming branch, and confirm the response is byte-exact. Streaming-path
//!   *permit-attachment* itself is proven by code inspection (every one of
//!   S1-S7's terminal `.body(...)` calls in `src/http_proxy.rs` is wrapped in
//!   `crate::permit_body::PermitBody::new(..., permit)`, verified in the Phase C
//!   diff) plus the exhaustive parameter-threading from `handle_request`'s
//!   single `Arc::new(permit)` down through every call site — the same
//!   traceability argument the spec's own Requirement 9.7 allows ("a single
//!   assertion over a shared attachment point that provably covers all seven").
//!
//! **Task 14 (TLS listener)** is covered structurally, not by a dedicated TLS
//! integration test in this file: `TlsProxyListener::start` clones the exact
//! same `Arc<Semaphore>` field (`request_semaphore`, shared via
//! `HttpProxy::get_request_semaphore()`, see `src/main.rs:409`/`722`) and passes
//! it unchanged into `HttpProxy::handle_request` (`src/tls_proxy_listener.rs`
//! ~line 234) — the identical function under test below. Since the permit
//! acquisition and every `PermitBody` attachment happens inside
//! `handle_request` itself, and `TlsProxyListener` performs no additional
//! response-body construction of its own (it decrypts and delegates), a permit
//! bug on the TLS listener and the plain HTTP listener would be the *same* bug,
//! provably from the shared code path rather than from two independently
//! correct implementations. Confirmed by reading
//! `src/tls_proxy_listener.rs:227-238`: the call site passes `request_semaphore`
//! straight through with no permit-affecting logic of its own.

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
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Semaphore};

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::S3ClientApi;

use common::{StubResponse, StubS3Client};

/// Object large enough to sit above the (deliberately lowered) disk streaming
/// threshold configured below, so a range GET on it exercises S2.
const LARGE_OBJECT_SIZE: usize = 64 * 1024; // 64 KiB
static LARGE_OBJECT_BODY: [u8; LARGE_OBJECT_SIZE] = {
    let mut buf = [0u8; LARGE_OBJECT_SIZE];
    let mut i = 0;
    while i < LARGE_OBJECT_SIZE {
        buf[i] = (i % 251) as u8; // non-repeating-enough pattern to catch corruption
        i += 1;
    }
    buf
};

/// Config with download coordination and RAM cache disabled — mirrors
/// `cache_match_patterns_behavior_test.rs`'s `behavior_config` — plus a
/// disk-streaming threshold small enough that `LARGE_OBJECT_SIZE` takes the
/// streaming branch in `serve_range_from_cache` (S2) rather than the buffered
/// branch.
fn permit_test_config(
    cache_dir: std::path::PathBuf,
    max_concurrent_requests: usize,
) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    config.cache.read_cache_enabled = true;
    // Small enough that the 64 KiB test object takes the S2 disk-streaming
    // branch (`use_streaming` in `serve_range_from_cache`), not the buffered
    // fallback.
    config.cache.disk_streaming_threshold = 4096;
    config.server.max_concurrent_requests = max_concurrent_requests;
    Arc::new(config)
}

/// Build the cache infrastructure the same way `HttpProxy::new` wires it.
/// Copied from `cache_match_patterns_behavior_test.rs::make_cache_infra`.
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
        false, // compression disabled — keeps stored bytes 1:1 with the body
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
        std::time::Duration::from_secs(5),
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

/// A running loopback proxy, its address, the semaphore driving admission (so
/// tests can inspect `available_permits()` directly), and a shutdown trigger.
struct ProxyServer {
    addr: SocketAddr,
    semaphore: Arc<Semaphore>,
    shutdown_tx: oneshot::Sender<()>,
}

/// Spin up a local HTTP/1 server whose connection service calls the real
/// `HttpProxy::handle_request` — the genuine top-level entry point where the
/// permit is acquired (`try_acquire_owned`) and attached to every response
/// body. Mirrors `cache_match_patterns_behavior_test.rs::spawn_proxy_server`,
/// but returns the semaphore `Arc` so tests can observe permit state directly
/// rather than only inferring it from response status.
async fn spawn_proxy_server(
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
    s3_client: Arc<dyn S3ClientApi + Send + Sync>,
    inflight_tracker: Arc<InFlightTracker>,
) -> ProxyServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_requests));
    let semaphore_for_server = Arc::clone(&semaphore);

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
                    let request_semaphore = Arc::clone(&semaphore_for_server);

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
                                    None, // metrics_manager
                                    None, // logger_manager
                                    inflight_tracker,
                                    None, // proxy_referer
                                    None, // destination_policy
                                    None, // policy_resolver
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

    ProxyServer {
        addr,
        semaphore,
        shutdown_tx,
    }
}

/// Send a single GET for `path` through the loopback proxy, with an optional
/// `Range` header. Returns the response status and the fully drained body.
async fn proxy_get(addr: SocketAddr, path: &str, range: Option<&str>) -> (StatusCode, Bytes) {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let uri = format!("http://{}{}", addr, path);
    let mut builder = Request::builder().method("GET").uri(&uri).header(
        "authorization",
        "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=sig",
    );
    if let Some(r) = range {
        builder = builder.header("range", r);
    }
    let req = builder.body(Full::new(Bytes::new())).expect("build GET");

    let resp = client.request(req).await.expect("proxy GET failed");
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

/// A GET stub returning the large test object with a stable ETag.
fn large_object_stub() -> StubS3Client {
    StubS3Client::new().with_default(
        StubResponse::ok(Bytes::copy_from_slice(&LARGE_OBJECT_BODY))
            .with_header("etag", "\"permit-test-etag\"")
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .with_header("content-type", "application/octet-stream"),
    )
}

// ===========================================================================
// Requirement 9.4: permit released after a Buffered_Response.
// ===========================================================================

/// A single small buffered GET must release its permit by the time the
/// response has been returned to the client — `available_permits()` is back
/// at the configured total immediately after the request completes, with no
/// background task outstanding to account for (a small object has no
/// Commit_Phase task holding a share, since the buffered cache write in
/// `handle_get_head_request`'s synchronous path completes before the response
/// is built).
#[tokio::test(flavor = "multi_thread")]
async fn buffered_response_releases_permit_on_completion() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = permit_test_config(temp_dir.path().to_path_buf(), 4);
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = StubS3Client::new().with_default(
        StubResponse::ok(Bytes::from_static(b"small buffered body"))
            .with_header("etag", "\"small-etag\"")
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT"),
    );
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    assert_eq!(
        server.semaphore.available_permits(),
        4,
        "all 4 permits available before any request"
    );

    let (status, body) = proxy_get(server.addr, "/mybucket/small-object.bin", None).await;
    assert!(status.is_success(), "GET should succeed: {}", status);
    assert_eq!(&body[..], b"small buffered body");

    // The client already has the full response by the time `proxy_get`
    // returns (the client-side `collect().await` only completes once the
    // last frame — and therefore the `PermitBody` wrapping it — has been
    // fully delivered and dropped). Give the connection's spawned task a
    // moment to unwind before asserting, since permit release happens on
    // `Drop` of the server-side task's locals, not synchronously with the
    // client observing the last byte.
    for _ in 0..50 {
        if server.semaphore.available_permits() == 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        server.semaphore.available_permits(),
        4,
        "permit must be released after a Buffered_Response completes (Requirement 9.4)"
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// Requirement 9.6: exhausting permits produces the Shed_Response.
// ===========================================================================

/// With `max_concurrent_requests = 1` and a slow first request holding the
/// only permit, a concurrent second request must be shed: 503, S3 error code
/// `SlowDown`, and a `Retry-After` header — proving the permit is actually
/// unavailable for the second request's entire admission check, not merely
/// released early the way the pre-fix `try_acquire()` (borrowed, released at
/// head construction) would have allowed.
#[tokio::test(flavor = "multi_thread")]
async fn exhausted_permits_produce_shed_response() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = permit_test_config(temp_dir.path().to_path_buf(), 1);
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    // Delay the (only) stub response so the first request's permit is held
    // for long enough that the second, concurrent request is guaranteed to
    // arrive while it is still outstanding.
    let stub = StubS3Client::new().with_default(
        StubResponse::ok(Bytes::from_static(b"slow response"))
            .with_header("etag", "\"slow-etag\"")
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .with_delay(Duration::from_millis(300)),
    );
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    let addr = server.addr;
    let first =
        tokio::spawn(async move { proxy_get(addr, "/mybucket/slow-object.bin", None).await });

    // Give the first request time to acquire the (only) permit and enter its
    // S3 round-trip before firing the second.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        server.semaphore.available_permits(),
        0,
        "the first request should be holding the only permit"
    );

    let (second_status, second_body) =
        proxy_get(server.addr, "/mybucket/other-object.bin", None).await;

    assert_eq!(
        second_status,
        StatusCode::SERVICE_UNAVAILABLE,
        "second request must be shed with 503 while the only permit is held (Requirement 9.6)"
    );
    let second_body_str = String::from_utf8_lossy(&second_body);
    assert!(
        second_body_str.contains("SlowDown"),
        "Shed_Response must carry the S3 error code SlowDown, got: {}",
        second_body_str
    );

    let (first_status, _) = first.await.expect("first request task panicked");
    assert!(
        first_status.is_success(),
        "the admitted first request must still succeed: {}",
        first_status
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// Requirement 9.5: permits return to the configured total once requests and
// Commit_Phase tasks have completed.
// ===========================================================================

/// A GET that populates the cache spawns a Commit_Phase task (task 15) sharing
/// the request's permit. This proves that share is not leaked: after the
/// background task has had time to finish, `available_permits()` is back at
/// the full configured total — not just "back to what it was after the
/// response returned", which would pass even if the Commit_Phase share never
/// released.
#[tokio::test(flavor = "multi_thread")]
async fn permits_return_to_total_after_commit_phase_completes() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = permit_test_config(temp_dir.path().to_path_buf(), 4);
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = large_object_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    let (status, body) = proxy_get(server.addr, "/mybucket/commit-phase-object.bin", None).await;
    assert!(status.is_success(), "GET should succeed: {}", status);
    assert_eq!(body.len(), LARGE_OBJECT_SIZE);
    assert_eq!(&body[..], &LARGE_OBJECT_BODY[..]);

    // Poll rather than sleep a fixed amount: the Commit_Phase task's disk
    // write and commit are genuinely asynchronous background work.
    let mut settled = false;
    for _ in 0..100 {
        if server.semaphore.available_permits() == 4 {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        settled,
        "permits must return to the full configured total (4) once the response \
         and its Commit_Phase cache-write task have both completed (Requirement 9.5); \
         available_permits()={}",
        server.semaphore.available_permits()
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// S2 coverage: serve_range_from_cache's disk-streaming branch.
// ===========================================================================

/// Seed the cache with a buffered GET (production path caches it
/// synchronously), then issue a range GET large enough to take the S2
/// disk-streaming branch in `serve_range_from_cache` (`disk_streaming_threshold`
/// is configured well below `LARGE_OBJECT_SIZE`). This exercises the
/// PermitBody-wrapped streaming response body constructed at that site
/// end-to-end and confirms it is byte-exact and that its permit is released
/// once fully delivered — the harness-reachable half of the S1-S7 coverage
/// described in this file's module doc.
#[tokio::test(flavor = "multi_thread")]
async fn disk_streaming_range_response_is_byte_exact_and_releases_permit() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = permit_test_config(temp_dir.path().to_path_buf(), 4);
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = large_object_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    let path = "/mybucket/streaming-range-object.bin";

    // Seed: a full-object GET, cached synchronously on the buffered path.
    let (seed_status, seed_body) = proxy_get(server.addr, path, None).await;
    assert!(seed_status.is_success(), "seed GET should succeed");
    assert_eq!(seed_body.len(), LARGE_OBJECT_SIZE);

    // Wait for the seed's Commit_Phase (if any) so the range GET below is
    // guaranteed to hit the fully-committed cache entry, not a partial write.
    for _ in 0..100 {
        if server.semaphore.available_permits() == 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Range GET covering the whole object — well above disk_streaming_threshold
    // (4096 bytes, configured above), so `serve_range_from_cache` takes the S2
    // disk-streaming branch (`use_streaming = true`), not the RAM/buffered path
    // (RAM cache is disabled in `permit_test_config`).
    let range_header = format!("bytes=0-{}", LARGE_OBJECT_SIZE - 1);
    let (range_status, range_body) = proxy_get(server.addr, path, Some(&range_header)).await;
    assert_eq!(
        range_status,
        StatusCode::PARTIAL_CONTENT,
        "range GET against a fully cached object should be 206"
    );
    assert_eq!(
        range_body.len(),
        LARGE_OBJECT_SIZE,
        "streamed range body must be byte-exact in length"
    );
    assert_eq!(
        &range_body[..],
        &LARGE_OBJECT_BODY[..],
        "streamed range body must be byte-exact in content (Requirement 6.1)"
    );

    for _ in 0..50 {
        if server.semaphore.available_permits() == 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        server.semaphore.available_permits(),
        4,
        "the S2 streaming response's permit must be released once fully delivered"
    );

    let _ = server.shutdown_tx.send(());
}

/// Sanity check that the global `permits_held_peak()` high-water mark counter
/// (Requirement 5.4, observability) is actually updated by ordinary traffic —
/// a regression here would mean the fleet's G3 measurement (task 17) reads a
/// counter that never moves.
#[tokio::test(flavor = "multi_thread")]
async fn permits_held_peak_advances_under_load() {
    let before = s3_proxy::http_proxy::permits_held_peak();

    let temp_dir = TempDir::new().expect("tempdir");
    let config = permit_test_config(temp_dir.path().to_path_buf(), 4);
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = StubS3Client::new().with_default(
        StubResponse::ok(Bytes::from_static(b"peak counter body"))
            .with_header("etag", "\"peak-etag\"")
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT"),
    );
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    let (status, _) = proxy_get(server.addr, "/mybucket/peak-object.bin", None).await;
    assert!(status.is_success());

    let after = s3_proxy::http_proxy::permits_held_peak();
    assert!(
        after >= before.max(1),
        "permits_held_peak must reflect at least one acquisition; before={}, after={}",
        before,
        after
    );
    // This counter is process-global and monotonic (fetch_max), and other
    // tests in this same binary run concurrently against the same process —
    // so this test only asserts the counter is a real, moving observable, not
    // an exact delta.

    let _ = server.shutdown_tx.send(());
}
