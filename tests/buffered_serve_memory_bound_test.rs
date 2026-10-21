//! Buffered-serve memory bound (spec `buffered-serve-memory-bound`, part one).
//!
//! A full-object GET is cached as one extent `0..len-1`. Before this change a
//! sub-range GET, or a HEAD, against that entry reached
//! `serve_range_from_cache_buffered`, whose single-extent contained arm loaded
//! the WHOLE extent into memory and sliced it afterwards. These tests measure
//! the peak heap growth across one request with a counting global allocator,
//! so a whole-extent load is visible as a peak close to the object size.
//!
//! Harness copied from `tests/inflight_ledger_integration_test.rs`: default
//! config (disk-streaming threshold 1 MiB, ledger disabled), RAM tier off,
//! cache warmed by a full GET through `HttpProxy::handle_request`.
//!
//! The allocator counters are process-global, so every test that reads them
//! holds `ALLOC_LOCK` for its whole body.
//!
//! Requirement coverage: R1.1, R1.3, R1.4 (single contained extent), R3.1.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
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
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::S3ClientApi;

use common::{StubResponse, StubS3Client};

// ---------------------------------------------------------------------------
// Counting allocator
// ---------------------------------------------------------------------------

struct CountingAlloc;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn note_alloc(size: usize) {
    let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: every method delegates to `System` with the caller's layout and only
// adjusts two atomics on success.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            note_alloc(layout.size());
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if !p.is_null() {
            note_alloc(layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            if new_size >= layout.size() {
                note_alloc(new_size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        p
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// Set the peak to the current live figure and return that baseline.
fn reset_peak() -> usize {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    live
}

fn peak_delta(baseline: usize) -> usize {
    PEAK.load(Ordering::Relaxed).saturating_sub(baseline)
}

static ALLOC_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const MIB: usize = 1024 * 1024;

/// Allowed heap growth for one streamed GET served from a cached extent.
///
/// Two parts: the 8 MiB Buffered_Operation_Bound itself, plus the LZ4 frame decoder's
/// fixed working set. `lz4_flex::frame::FrameDecoder` holds a compressed-block and a
/// decompressed-block buffer of the block size the frame declares, whatever the request
/// length. Range files are written with 64 KiB blocks (`src/compression.rs`,
/// `range_frame_info`), so that is about 128 KiB per stream; files written by earlier
/// releases declare 4 MiB blocks and carry about 8 MiB, which this allowance still
/// admits because those files remain readable. The discriminator is the red side: the
/// whole-extent load measured 142-157 MB here against a 64 MiB extent, and that figure
/// grows with the extent while this one does not.
const STREAM_ALLOWANCE: usize = 16 * MIB;
const LARGE_OBJECT_SIZE: usize = 64 * MIB;
const SMALL_OBJECT_SIZE: usize = 100 * 1024;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Default config apart from the cache directory, RAM off, and a cache large
/// enough to hold the fixture. `disk_streaming_threshold` and
/// `max_inflight_buffer_bytes` stay at their shipped defaults.
fn test_config(cache_dir: std::path::PathBuf) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_cache_size = 1024 * 1024 * 1024;
    config.cache.read_cache_enabled = true;
    config.server.max_concurrent_requests = 100;
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

    ProxyServer {
        addr,
        _shutdown_tx: shutdown_tx,
    }
}

struct ProxyResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

async fn proxy_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    range: Option<&str>,
) -> ProxyResponse {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let uri = format!("http://{}{}", addr, path);
    let mut builder = Request::builder().method(method).uri(&uri).header(
        "authorization",
        "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=sig",
    );
    if let Some(range) = range {
        builder = builder.header("range", range);
    }

    let response = tokio::time::timeout(
        Duration::from_secs(60),
        client.request(
            builder
                .body(Full::new(Bytes::new()))
                .expect("build request"),
        ),
    )
    .await
    .expect("proxy request timed out")
    .expect("proxy request failed");
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.into_body().collect().await.unwrap().to_bytes();

    ProxyResponse {
        status,
        headers,
        body,
    }
}

/// Highly compressible: LZ4 shrinks it, so the on-disk extent is much smaller
/// than the decompressed load the old path allocated.
fn compressible_body(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<_>>())
}

/// Incompressible: xorshift bytes, stored effectively uncompressed.
fn incompressible_body(len: usize) -> Bytes {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    Bytes::from(out)
}

fn stub_for(body: Bytes) -> StubS3Client {
    StubS3Client::new().with_default(
        StubResponse::ok(body)
            .with_header("etag", "\"scn1-etag\"")
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .with_header("content-type", "application/octet-stream"),
    )
}

struct Fixture {
    _dir: TempDir,
    server: ProxyServer,
    path: &'static str,
    body: Bytes,
}

/// Warm the cache with a full GET, then wait until a range read is answered
/// from the cache (the disk write may land just after the response ends).
async fn warmed_fixture(path: &'static str, body: Bytes) -> Fixture {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path().to_path_buf());
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let server = spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        stub_for(body.clone()).into_trait_object(),
    )
    .await;

    let warm = proxy_request(server.addr, "GET", path, None).await;
    assert!(warm.status.is_success(), "warm GET: {}", warm.status);
    assert_eq!(warm.body.len(), body.len());
    drop(warm);

    for _ in 0..100 {
        let probe = proxy_request(server.addr, "GET", path, Some("bytes=0-0")).await;
        if probe.status == StatusCode::PARTIAL_CONTENT
            && probe.headers.get("x-cache").map(|v| v.as_bytes()) == Some(b"HIT")
        {
            return Fixture {
                _dir: dir,
                server,
                path,
                body,
            };
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("cache never served {} from disk after the warm GET", path);
}

/// Issue one request with the peak reset first, and return the response and the
/// peak heap growth across it.
async fn measured(fixture: &Fixture, method: &str, range: Option<&str>) -> (ProxyResponse, usize) {
    // Let any background work from the warm-up settle before the baseline.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let baseline = reset_peak();
    let response = proxy_request(fixture.server.addr, method, fixture.path, range).await;
    let delta = peak_delta(baseline);
    (response, delta)
}

fn assert_hit_206(resp: &ProxyResponse, expected_len: usize, what: &str) {
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT, "{}: status", what);
    assert_eq!(
        resp.headers.get("x-cache").map(|v| v.to_str().unwrap()),
        Some("HIT"),
        "{}: must be served from cache",
        what
    );
    assert_eq!(
        resp.headers
            .get("content-length")
            .map(|v| v.to_str().unwrap().parse::<usize>().unwrap()),
        Some(expected_len),
        "{}: content-length",
        what
    );
}

// ---------------------------------------------------------------------------
// (a) small sub-range of a large single extent
// ---------------------------------------------------------------------------

async fn small_prefix_bounded(path: &'static str, body: Bytes) {
    let _guard = ALLOC_LOCK.lock().await;
    let fixture = warmed_fixture(path, body).await;
    let (resp, delta) = measured(&fixture, "GET", Some("bytes=0-4095")).await;
    eprintln!("SCN1 (a) {}: peak delta {} bytes", path, delta);
    assert_hit_206(&resp, 4096, "bytes=0-4095");
    assert_eq!(resp.body.len(), 4096, "body length equals content-length");
    assert_eq!(resp.body, fixture.body.slice(0..4096));
    assert!(
        delta < STREAM_ALLOWANCE,
        "GET bytes=0-4095 of a {} byte cached extent grew the heap by {} bytes (allowance {})",
        LARGE_OBJECT_SIZE,
        delta,
        STREAM_ALLOWANCE
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_small_prefix_of_large_extent_compressible_is_bounded() {
    small_prefix_bounded(
        "/mybucket/scn1-a-compressible.txt",
        compressible_body(LARGE_OBJECT_SIZE),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_small_prefix_of_large_extent_incompressible_is_bounded() {
    small_prefix_bounded(
        "/mybucket/scn1-a-random.dat",
        incompressible_body(LARGE_OBJECT_SIZE),
    )
    .await;
}

// ---------------------------------------------------------------------------
// (b) the sub-range fallback: range above the streaming threshold, not the extent
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b_mid_subrange_above_streaming_threshold_is_bounded() {
    let _guard = ALLOC_LOCK.lock().await;
    let fixture = warmed_fixture(
        "/mybucket/scn1-b-random.dat",
        incompressible_body(LARGE_OBJECT_SIZE),
    )
    .await;
    let (resp, delta) = measured(&fixture, "GET", Some("bytes=1048576-3145727")).await;
    eprintln!("SCN1 (b): peak delta {} bytes", delta);
    assert_hit_206(&resp, 2 * MIB, "bytes=1048576-3145727");
    assert_eq!(resp.body.len(), 2 * MIB);
    assert_eq!(resp.body, fixture.body.slice(MIB..3 * MIB));
    // The test client collects the 2 MiB body in this same process, which the counting
    // allocator also sees (growth copies included), so it is added to the allowance.
    let allowance = STREAM_ALLOWANCE + 2 * resp.body.len();
    assert!(
        delta < allowance,
        "sub-range GET grew the heap by {} bytes (allowance {})",
        delta,
        allowance
    );

    // The last byte of the extent, so the adapter's final-chunk slice is exercised.
    let last = LARGE_OBJECT_SIZE - 1;
    let range = format!("bytes={}-{}", last - 1_500_000, last);
    let resp = proxy_request(fixture.server.addr, "GET", fixture.path, Some(&range)).await;
    assert_hit_206(&resp, 1_500_001, "tail sub-range");
    assert_eq!(resp.body, fixture.body.slice(last - 1_500_000..=last));
}

// ---------------------------------------------------------------------------
// (c) HEAD with a Range header loads no body
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c_head_with_range_loads_no_body() {
    let _guard = ALLOC_LOCK.lock().await;
    let fixture = warmed_fixture(
        "/mybucket/scn1-c-compressible.txt",
        compressible_body(LARGE_OBJECT_SIZE),
    )
    .await;
    let (resp, delta) = measured(&fixture, "HEAD", Some("bytes=0-4095")).await;
    eprintln!("SCN1 (c): peak delta {} bytes", delta);
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers
            .get("content-length")
            .map(|v| v.to_str().unwrap()),
        Some("4096")
    );
    assert_eq!(
        resp.headers
            .get("content-range")
            .map(|v| v.to_str().unwrap()),
        Some(format!("bytes 0-4095/{}", LARGE_OBJECT_SIZE).as_str())
    );
    assert_eq!(
        resp.headers.get("etag").map(|v| v.to_str().unwrap()),
        Some("\"scn1-etag\"")
    );
    assert!(resp.body.is_empty(), "HEAD must carry no body");
    assert!(
        delta < MIB,
        "HEAD bytes=0-4095 grew the heap by {} bytes (bound 1 MiB)",
        delta
    );
}

// ---------------------------------------------------------------------------
// (d) exact-match GET of the whole extent still streams (regression)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d_exact_match_whole_extent_still_byte_exact() {
    let _guard = ALLOC_LOCK.lock().await;
    let fixture = warmed_fixture(
        "/mybucket/scn1-d-compressible.txt",
        compressible_body(LARGE_OBJECT_SIZE),
    )
    .await;
    let range = format!("bytes=0-{}", LARGE_OBJECT_SIZE - 1);
    let (resp, delta) = measured(&fixture, "GET", Some(&range)).await;
    // Reported, not asserted: the client collects the whole 64 MiB body in-process.
    eprintln!(
        "SCN1 (d) exact-match stream arm: peak delta {} bytes",
        delta
    );
    assert_hit_206(&resp, LARGE_OBJECT_SIZE, "exact-match whole extent");
    assert_eq!(resp.body, fixture.body);
}

// ---------------------------------------------------------------------------
// (e) small single extent: sub-range served correctly by the buffered path
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e_small_extent_subrange_served_correctly() {
    let _guard = ALLOC_LOCK.lock().await;
    let fixture = warmed_fixture(
        "/mybucket/scn1-e-small.txt",
        compressible_body(SMALL_OBJECT_SIZE),
    )
    .await;
    let resp = proxy_request(
        fixture.server.addr,
        "GET",
        fixture.path,
        Some("bytes=100-1099"),
    )
    .await;
    assert_hit_206(&resp, 1000, "small sub-range");
    assert_eq!(resp.body, fixture.body.slice(100..1100));
}

// ===========================================================================
// Part two: multi-extent merges and over-bound fallbacks.
//
// Requirement coverage: R1.2, R1.4, R1.5, R2.1-R2.4.
//
// Every test below consumes the response body frame by frame and compares it
// against the expected slice as it arrives, so the test client never holds
// the body in memory and the peak figure is the proxy's alone.
// ===========================================================================

const EXTENT_40: usize = 40 * MIB;

struct DrainedResponse {
    status: StatusCode,
    headers: HeaderMap,
    len: usize,
    matched: bool,
}

/// GET with a Range header, draining the body against `expected` without
/// collecting it.
async fn drained_request(
    addr: SocketAddr,
    path: &str,
    range: &str,
    expected: &[u8],
) -> DrainedResponse {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let uri = format!("http://{}{}", addr, path);
    let mut builder = Request::builder().method("GET").uri(&uri).header(
        "authorization",
        "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=sig",
    );
    // An empty `range` sends a plain GET with no Range header.
    if !range.is_empty() {
        builder = builder.header("range", range);
    }
    let request = builder
        .body(Full::new(Bytes::new()))
        .expect("build request");
    let response = tokio::time::timeout(Duration::from_secs(60), client.request(request))
        .await
        .expect("proxy request timed out")
        .expect("proxy request failed");
    let status = response.status();
    let headers = response.headers().clone();
    let mut body = response.into_body();
    let mut len = 0usize;
    let mut matched = true;
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(60), body.frame())
        .await
        .expect("body frame timed out")
    {
        let frame = match frame {
            Ok(f) => f,
            Err(_) => {
                matched = false;
                break;
            }
        };
        if let Ok(data) = frame.into_data() {
            let end = len + data.len();
            if end > expected.len() || expected[len..end] != data[..] {
                matched = false;
            }
            len = end;
        }
    }
    DrainedResponse {
        status,
        headers,
        len,
        matched: matched && len == expected.len(),
    }
}

/// A 206 the stub returns for an exact `Range` header value.
fn stub_range_response(body: &Bytes, start: usize, end: usize) -> StubResponse {
    let mut resp = StubResponse::ok(body.slice(start..=end));
    resp.status = StatusCode::PARTIAL_CONTENT;
    resp.with_header(
        "content-range",
        format!("bytes {}-{}/{}", start, end, body.len()),
    )
    .with_header("etag", "\"scn1-etag\"")
    .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
    .with_header("content-type", "application/octet-stream")
}

fn range_header(start: usize, end: usize) -> String {
    format!("bytes={}-{}", start, end)
}

struct MultiFixture {
    _dir: TempDir,
    server: ProxyServer,
    stub: StubS3Client,
    path: &'static str,
    body: Bytes,
}

impl MultiFixture {
    fn cache_dir(&self) -> &std::path::Path {
        self._dir.path()
    }
}

/// Build a proxy whose stub answers each range in `stub_ranges` with a 206 and
/// the full GET with a 200, then warm the cache with one range GET per extent
/// in `extents` (or one full GET when `extents` is empty), waiting until a
/// one-byte probe inside each extent is a disk HIT.
async fn multi_fixture(
    path: &'static str,
    body: Bytes,
    extents: &[(usize, usize)],
    stub_ranges: &[(usize, usize)],
) -> MultiFixture {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path().to_path_buf());
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let mut stub = stub_for(body.clone());
    for &(s, e) in extents.iter().chain(stub_ranges.iter()) {
        stub = stub.with_response_for_range(range_header(s, e), stub_range_response(&body, s, e));
    }
    for &(s, _) in extents {
        stub = stub.with_response_for_range(range_header(s, s), stub_range_response(&body, s, s));
    }
    // Seed each extent straight into the cache, as a range miss that cached its bytes
    // would. An empty `extents` seeds the whole object as one extent.
    let seeded: Vec<(usize, usize)> = if extents.is_empty() {
        vec![(0, body.len() - 1)]
    } else {
        extents.to_vec()
    };
    let cache_key = CacheManager::generate_cache_key(path, None);
    for &(s, e) in &seeded {
        let object_metadata = s3_proxy::cache_types::ObjectMetadata {
            etag: "\"scn1-etag\"".to_string(),
            last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
            content_length: body.len() as u64,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: s3_proxy::cache_types::UploadState::Complete,
            cumulative_size: (e - s + 1) as u64,
            ..Default::default()
        };
        range_handler
            .get_cache_manager()
            .create_configured_disk_cache_manager()
            .store_range(
                &cache_key,
                s as u64,
                e as u64,
                &body[s..=e],
                object_metadata,
                config.cache.get_ttl,
                true,
            )
            .await
            .expect("seed extent");
    }
    // Fold the seeding journal entries into the `.meta` the serve path reads. A cycle
    // that finds the global consolidation lock held returns Ok having folded nothing, and
    // nothing else runs a cycle in this test process, so run cycles until the `.meta`
    // exists instead of trusting the first one.
    if let Some(consolidator) = cache_manager.get_journal_consolidator().await {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut cycles = 0u32;
        loop {
            consolidator
                .run_consolidation_cycle()
                .await
                .expect("consolidate seeded extents");
            cycles += 1;
            if count_meta_files(dir.path()) > 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no .meta for {} after {} consolidation cycles in 60s: the seeding journal \
                 entries were never folded",
                path,
                cycles
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if cycles > 1 {
            eprintln!(
                "multi_fixture {}: .meta visible after {} cycles",
                path, cycles
            );
        }
    }
    let server = spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        stub.clone().into_trait_object(),
    )
    .await;
    let probes: Vec<usize> = seeded.iter().map(|&(s, _)| s).collect();

    for &p in &probes {
        let mut hit = false;
        for _ in 0..100 {
            let probe = proxy_request(server.addr, "GET", path, Some(&range_header(p, p))).await;
            if probe.status == StatusCode::PARTIAL_CONTENT
                && probe.headers.get("x-cache").map(|v| v.as_bytes()) == Some(b"HIT")
            {
                hit = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if !hit {
            let probe = proxy_request(server.addr, "GET", path, Some(&range_header(p, p))).await;
            fn list(d: &std::path::Path) {
                if let Ok(rd) = std::fs::read_dir(d) {
                    for e in rd.flatten() {
                        let path = e.path();
                        if path.is_dir() {
                            list(&path);
                        } else {
                            eprintln!(
                                "  {:?} {}",
                                path,
                                e.metadata().map(|m| m.len()).unwrap_or(0)
                            );
                        }
                    }
                }
            }
            eprintln!(
                "probe status {} headers {:?} captured {}",
                probe.status,
                probe.headers,
                stub.captured().len()
            );
            list(dir.path());
        }
        assert!(hit, "cache never served byte {} of {} from disk", p, path);
    }

    MultiFixture {
        _dir: dir,
        server,
        stub,
        path,
        body,
    }
}

async fn measured_drain(
    fixture: &MultiFixture,
    start: usize,
    end: usize,
) -> (DrainedResponse, usize) {
    tokio::time::sleep(Duration::from_millis(300)).await;
    let baseline = reset_peak();
    let resp = drained_request(
        fixture.server.addr,
        fixture.path,
        &range_header(start, end),
        &fixture.body[start..=end],
    )
    .await;
    (resp, peak_delta(baseline))
}

fn count_bin_files(dir: &std::path::Path) -> usize {
    fn walk(p: &std::path::Path, n: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, n);
                } else if path.extension().map(|x| x == "bin").unwrap_or(false) {
                    *n += 1;
                }
            }
        }
    }
    let mut n = 0;
    walk(dir, &mut n);
    n
}

fn count_meta_files(dir: &std::path::Path) -> usize {
    fn walk(p: &std::path::Path, n: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, n);
                } else if path.extension().map(|x| x == "meta").unwrap_or(false) {
                    *n += 1;
                }
            }
        }
    }
    let mut n = 0;
    walk(&dir.join("metadata"), &mut n);
    n
}
fn delete_bin_files(dir: &std::path::Path) -> usize {
    fn walk(p: &std::path::Path, n: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, n);
                } else if path.extension().map(|x| x == "bin").unwrap_or(false)
                    && std::fs::remove_file(&path).is_ok()
                {
                    *n += 1;
                }
            }
        }
    }
    let mut n = 0;
    walk(dir, &mut n);
    n
}

fn forwarded_with_range(stub: &StubS3Client, range: &str) -> usize {
    stub.captured()
        .iter()
        .filter(|c| {
            c.headers
                .get("range")
                .or_else(|| c.headers.get("Range"))
                .map(|r| r == range)
                .unwrap_or(false)
        })
        .count()
}

fn assert_bounded(what: &str, delta: usize) {
    assert!(
        delta < STREAM_ALLOWANCE,
        "{}: peak heap growth {} bytes (allowance {})",
        what,
        delta,
        STREAM_ALLOWANCE
    );
}

// (f) two adjacent 40 MiB extents, request spanning both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f_two_adjacent_large_extents_chained_and_bounded() {
    let _guard = ALLOC_LOCK.lock().await;
    let body = incompressible_body(2 * EXTENT_40);
    let fixture = multi_fixture(
        "/mybucket/scn2-f.dat",
        body,
        &[(0, EXTENT_40 - 1), (EXTENT_40, 2 * EXTENT_40 - 1)],
        &[],
    )
    .await;
    let (start, end) = (EXTENT_40 - 4 * MIB, EXTENT_40 + 5 * MIB - 1);
    let (resp, delta) = measured_drain(&fixture, start, end).await;
    eprintln!("SCN2 (f): peak delta {} bytes", delta);
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers.get("x-cache").map(|v| v.to_str().unwrap()),
        Some("HIT")
    );
    assert!(resp.matched, "(f) byte-exact, got {} bytes", resp.len);
    assert_bounded("(f) spanning two 40 MiB extents", delta);
}

// (g) three adjacent extents, the middle one within the bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g_three_adjacent_extents_chained_and_bounded() {
    let _guard = ALLOC_LOCK.lock().await;
    let a = 20 * MIB;
    let b = a + 4 * MIB;
    let total = b + 20 * MIB;
    let body = incompressible_body(total);
    let fixture = multi_fixture(
        "/mybucket/scn2-g.dat",
        body,
        &[(0, a - 1), (a, b - 1), (b, total - 1)],
        &[],
    )
    .await;
    let (start, end) = (a - 3 * MIB, b + 3 * MIB - 1);
    let (resp, delta) = measured_drain(&fixture, start, end).await;
    eprintln!("SCN2 (g): peak delta {} bytes", delta);
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert!(resp.matched, "(g) byte-exact, got {} bytes", resp.len);
    assert_bounded("(g) spanning three extents", delta);
}

// (h) a gap between two extents, request spanning the hole, over the bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h_gap_over_bound_forwarded_uncached_and_bounded() {
    let _guard = ALLOC_LOCK.lock().await;
    let hole_start = EXTENT_40;
    let hole_end = EXTENT_40 + 8 * MIB - 1;
    let second_start = hole_end + 1;
    let total = second_start + EXTENT_40;
    let (start, end) = (EXTENT_40 - 4 * MIB, second_start + 4 * MIB - 1);
    let body = incompressible_body(total);
    let fixture = multi_fixture(
        "/mybucket/scn2-h.dat",
        body,
        &[(0, EXTENT_40 - 1), (second_start, total - 1)],
        // The forwarded request, and the hole fetch the old merge path issued.
        &[(start, end), (hole_start, hole_end)],
    )
    .await;
    let bins_before = count_bin_files(fixture.cache_dir());
    let healing_before = s3_proxy::http_proxy::buffered_serve_healing_refetches_total();
    let (resp, delta) = measured_drain(&fixture, start, end).await;
    eprintln!("SCN2 (h): peak delta {} bytes", delta);
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert!(resp.matched, "(h) byte-exact, got {} bytes", resp.len);
    assert_eq!(
        forwarded_with_range(&fixture.stub, &range_header(start, end)),
        1,
        "(h) the stub must see the request forwarded with its own Range"
    );
    assert_bounded("(h) gap over the bound", delta);
    assert_healing_refetch(healing_before, "(h)");
    eprintln!(
        "SCN3 (h): .bin files {} -> {} (the stub's buffered body is not teed into the cache)",
        bins_before,
        count_bin_files(fixture.cache_dir())
    );
}

/// The over-bound degrade took the streamed cache-miss path
/// (`stream_range_from_s3_with_caching`) rather than the uncached forward. The
/// stub answers with a buffered body, which that path serves without writing a
/// range (only a streamed S3 body is teed into the cache), so the stored range
/// and the repeat hit are not observable in-process; the fleet test superset
/// probe is where they are.
fn assert_healing_refetch(healing_before: u64, what: &str) {
    assert_eq!(
        s3_proxy::http_proxy::buffered_serve_healing_refetches_total(),
        healing_before + 1,
        "{} the degrade must take the healing cache-miss path",
        what
    );
}

// (i) partial containment: request runs 1 MiB past a 40 MiB extent's end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn i_partial_containment_forwarded_and_bounded() {
    let _guard = ALLOC_LOCK.lock().await;
    let total = EXTENT_40 + 8 * MIB;
    let (start, end) = (EXTENT_40 - 8 * MIB, EXTENT_40 + MIB - 1);
    let body = incompressible_body(total);
    let fixture = multi_fixture(
        "/mybucket/scn2-i.dat",
        body,
        &[(0, EXTENT_40 - 1)],
        &[(start, end), (EXTENT_40, EXTENT_40 + MIB - 1)],
    )
    .await;
    let healing_before = s3_proxy::http_proxy::buffered_serve_healing_refetches_total();
    let (resp, delta) = measured_drain(&fixture, start, end).await;
    eprintln!("SCN2 (i): peak delta {} bytes", delta);
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert!(resp.matched, "(i) byte-exact, got {} bytes", resp.len);
    assert_eq!(
        forwarded_with_range(&fixture.stub, &range_header(start, end)),
        1,
        "(i) forwarded with its own Range"
    );
    assert_bounded("(i) partial containment", delta);
    assert_healing_refetch(healing_before, "(i)");
}

// (j) the exact-match stream arm with its .bin deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn j_exact_match_missing_bin_forwarded_and_bounded() {
    let _guard = ALLOC_LOCK.lock().await;
    let body = incompressible_body(EXTENT_40);
    let fixture = multi_fixture("/mybucket/scn2-j.dat", body, &[], &[(0, EXTENT_40 - 1)]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let deleted = delete_bin_files(fixture.cache_dir());
    assert!(deleted >= 1, "(j) fixture must delete the extent's .bin");
    let (resp, delta) = measured_drain(&fixture, 0, EXTENT_40 - 1).await;
    eprintln!("SCN2 (j): peak delta {} bytes", delta);
    assert_ne!(
        resp.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "(j) never 503"
    );
    assert!(resp.status.is_success(), "(j) status {}", resp.status);
    assert!(resp.matched, "(j) byte-exact, got {} bytes", resp.len);
    assert_bounded("(j) exact-match arm with missing .bin", delta);
}

// (k) a within-bound multi-extent merge (two 2 MiB extents) still takes the merge path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k_within_bound_multi_extent_still_merges() {
    use s3_proxy::http_proxy::{
        buffered_serve_degraded_total, buffered_serve_multi_extent_merges_total,
        BufferedServeDegradeReason,
    };
    let _guard = ALLOC_LOCK.lock().await;
    let two = 2 * MIB;
    let body = incompressible_body(2 * two);
    let fixture = multi_fixture(
        "/mybucket/scn2-k.dat",
        body,
        &[(0, two - 1), (two, 2 * two - 1)],
        &[],
    )
    .await;
    let merges_before = buffered_serve_multi_extent_merges_total();
    let chained_before = buffered_serve_degraded_total(BufferedServeDegradeReason::ChainedStream);
    let (start, end) = (MIB, 3 * MIB - 1);
    let resp = drained_request(
        fixture.server.addr,
        fixture.path,
        &range_header(start, end),
        &fixture.body[start..=end],
    )
    .await;
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers.get("x-cache").map(|v| v.to_str().unwrap()),
        Some("HIT")
    );
    assert!(resp.matched, "(k) byte-exact, got {} bytes", resp.len);
    assert_eq!(
        buffered_serve_multi_extent_merges_total(),
        merges_before + 1,
        "(k) a within-bound two-extent range must take the buffered merge path"
    );
    assert_eq!(
        buffered_serve_degraded_total(BufferedServeDegradeReason::ChainedStream),
        chained_before,
        "(k) must not be chained"
    );
}

// ---------------------------------------------------------------------------
// Page widening, HEAD paths, many-extent chain, seek
// ---------------------------------------------------------------------------

const PAGE_16: usize = 16 * MIB;

/// Seed `extents` of `body` into a fresh cache and fold them into the `.meta`,
/// returning the pieces `HttpProxy::handle_range_request` needs directly.
async fn seeded_infra(
    path: &str,
    body: &Bytes,
    extents: &[(usize, usize)],
) -> (
    TempDir,
    Arc<Config>,
    Arc<CacheManager>,
    Arc<RangeHandler>,
    String,
) {
    seeded_infra_ram(path, body, extents, false).await
}

async fn seeded_infra_ram(
    path: &str,
    body: &Bytes,
    extents: &[(usize, usize)],
    ram: bool,
) -> (
    TempDir,
    Arc<Config>,
    Arc<CacheManager>,
    Arc<RangeHandler>,
    String,
) {
    let dir = TempDir::new().unwrap();
    let mut cfg = (*test_config(dir.path().to_path_buf())).clone();
    if ram {
        cfg.cache.ram_cache_enabled = true;
        cfg.cache.max_ram_cache_size = 256 * 1024 * 1024;
    }
    let config = Arc::new(cfg);
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let cache_key = CacheManager::generate_cache_key(path, None);
    for &(s, e) in extents {
        let object_metadata = s3_proxy::cache_types::ObjectMetadata {
            etag: "\"scn1-etag\"".to_string(),
            last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
            content_length: body.len() as u64,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: s3_proxy::cache_types::UploadState::Complete,
            cumulative_size: (e - s + 1) as u64,
            ..Default::default()
        };
        range_handler
            .get_cache_manager()
            .create_configured_disk_cache_manager()
            .store_range(
                &cache_key,
                s as u64,
                e as u64,
                &body[s..=e],
                object_metadata,
                config.cache.get_ttl,
                true,
            )
            .await
            .expect("seed extent");
    }
    if let Some(consolidator) = cache_manager.get_journal_consolidator().await {
        consolidator
            .run_consolidation_cycle()
            .await
            .expect("consolidate seeded extents");
    }
    (dir, config, cache_manager, range_handler, cache_key)
}

/// Drive one widened range GET through `handle_range_request` with page
/// widening on at `page_size`, draining the body frame by frame against the
/// expected bytes, and return (status, x-cache, byte-exact, peak delta).
#[allow(clippy::too_many_arguments)] // test helper: one argument per request dimension
async fn widened_measured(
    config: &Arc<Config>,
    cache_manager: &Arc<CacheManager>,
    range_handler: &Arc<RangeHandler>,
    cache_key: &str,
    body: &Bytes,
    start: usize,
    end: usize,
    page_size: u64,
    stub: StubS3Client,
) -> (StatusCode, Option<String>, bool, usize) {
    let resolved = s3_proxy::bucket_settings::ResolvedSettings {
        page_widening: true,
        page_size,
        ..s3_proxy::bucket_settings::ResolvedSettings::default()
    };
    let inflight_tracker = Arc::new(InFlightTracker::new());
    tokio::time::sleep(Duration::from_millis(300)).await;
    let baseline = reset_peak();
    let response = HttpProxy::handle_range_request(
        hyper::Method::GET,
        cache_key.to_string(),
        &range_header(start, end),
        std::collections::HashMap::new(),
        Arc::clone(cache_manager),
        Arc::clone(range_handler),
        stub.into_trait_object(),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(config),
        &resolved,
        None,
        inflight_tracker,
        None,
        &None,
        false,
        None,
    )
    .await
    .expect("handle_range_request");
    let status = response.status();
    let x_cache = response
        .headers()
        .get("x-cache")
        .map(|v| v.to_str().unwrap().to_string());
    let expected = &body[start..=end];
    let mut b = response.into_body();
    let mut len = 0usize;
    let mut matched = true;
    while let Some(frame) = b.frame().await {
        match frame {
            Ok(f) => {
                if let Ok(data) = f.into_data() {
                    let e = len + data.len();
                    if e > expected.len() || expected[len..e] != data[..] {
                        matched = false;
                    }
                    len = e;
                }
            }
            Err(_) => {
                matched = false;
                break;
            }
        }
    }
    let delta = peak_delta(baseline);
    (status, x_cache, matched && len == expected.len(), delta)
}

// (l) widening on, 16 MiB Page, a small read straddling a Page boundary with
// both Pages cached: at most one Page is resident at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l_widened_read_straddling_two_pages_holds_one_page() {
    let _guard = ALLOC_LOCK.lock().await;
    let body = incompressible_body(4 * PAGE_16);
    let (_dir, config, cm, rh, key) = seeded_infra(
        "/mybucket/scn4-l.dat",
        &body,
        &[(0, PAGE_16 - 1), (PAGE_16, 2 * PAGE_16 - 1)],
    )
    .await;
    let (start, end) = (PAGE_16 - MIB, PAGE_16 + MIB - 1);
    let stub = stub_for(body.clone());
    let (status, x, exact, delta) = widened_measured(
        &config,
        &cm,
        &rh,
        &key,
        &body,
        start,
        end,
        PAGE_16 as u64,
        stub.clone(),
    )
    .await;
    eprintln!(
        "SCN4 (l): peak delta {} bytes x-cache {:?} upstream requests {}",
        delta,
        x,
        stub.captured().len()
    );
    assert_eq!(
        stub.captured().len(),
        0,
        "(l) both Pages must be served from cache"
    );
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(exact, "(l) byte-exact");
    // Allowance is 3 x Page + 16 MiB. With the RAM tier off no promotion work runs, so the
    // peak is one Page's disk load: the 2 MiB slice, the 16 MiB LZ4 decode output and the
    // decoder's two 4 MiB block buffers, 27,283,887 bytes on 10 of 10 runs. The load
    // decodes from the file reader, so the 16 MiB compressed frame is no longer whole in memory
    // beside its output; reading it whole first peaked at 44,052,572 (and 3 of 3 runs
    // with that read restored). The allowance is left as it was: it still separates
    // every breach below, and nothing here argues a tighter bound for the decoder's buffers.
    // A detached promotion task holding Page 1 and its compressed frame while
    // Page 2 loaded read 52,442,178 or 77,607,632 bytes. Unchanged code holding both
    // Pages reads 92,304,918 bytes, so this allowance still fails it.
    assert!(
        delta < 3 * PAGE_16 + 16 * MIB,
        "(l) straddling two 16 MiB Pages: peak heap growth {} bytes (allowance {})",
        delta,
        3 * PAGE_16 + 16 * MIB
    );
}

// (l2) as (l) with the RAM tier ON, so both Pages are promoted. Page 1's
// promotion must finish before Page 2 loads: a detached task holding Page 1
// plus its compressed frame while Page 2 loads is the breach measured with
// the RAM tier off.
//
// Each Page is promoted once, by `promote_page_to_ram`. The peak comes after the
// last load and is three Pages plus the slice: Page 1's frame retained by the RAM
// tier (16 MiB, cache state), Page 2 and its compressed frame in the detached
// promotion (32 MiB, the frame becoming Page 2's cache state), and the 2 MiB
// slice. That is 52,428,800 bytes; 52,444,824-52,444,832 on 10 of 10 runs.
// The allowance is those three Pages plus 8 MiB: the 2 MiB slice and 6 MiB of
// margin, about 6.3 MB above every reading. It fails the single-extent Page
// promoted twice (the load's own disk-hit promotion re-reading the frame under
// the Page's RAM key before `promote_page_to_ram` replaced it), which reads
// 69,223,340 on 10 of 10 runs, and every breach above it. The
// previous allowance, 4 Pages + 16 MiB (83,886,080), passed that defect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l2_widened_read_straddling_two_pages_ram_on_promotes_one_page_at_a_time() {
    let _guard = ALLOC_LOCK.lock().await;
    let body = incompressible_body(4 * PAGE_16);
    let (_dir, config, cm, rh, key) = seeded_infra_ram(
        "/mybucket/scn5-l2.dat",
        &body,
        &[(0, PAGE_16 - 1), (PAGE_16, 2 * PAGE_16 - 1)],
        true,
    )
    .await;
    let (start, end) = (PAGE_16 - MIB, PAGE_16 + MIB - 1);
    let stub = stub_for(body.clone());
    let (status, x, exact, delta) = widened_measured(
        &config,
        &cm,
        &rh,
        &key,
        &body,
        start,
        end,
        PAGE_16 as u64,
        stub.clone(),
    )
    .await;
    eprintln!(
        "SCN5 (l2): peak delta {} bytes x-cache {:?} upstream requests {}",
        delta,
        x,
        stub.captured().len()
    );
    assert_eq!(
        stub.captured().len(),
        0,
        "(l2) both Pages must be served from cache"
    );
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(exact, "(l2) byte-exact");
    let allowance = 3 * PAGE_16 + 8 * MIB;
    assert!(
        delta < allowance,
        "(l2) straddling two 16 MiB Pages, RAM on: peak heap growth {} bytes (allowance {})",
        delta,
        allowance
    );
}

// (m) widening on, 16 MiB Page, a request spanning 4 Pages. A range at or
// above page_size is never widened (it takes the bounded non-widened path), so
// this pins that a widened prefix cannot be used to materialise many Pages.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m_widened_prefix_request_spanning_four_pages_is_bounded() {
    let _guard = ALLOC_LOCK.lock().await;
    let body = incompressible_body(4 * PAGE_16);
    let (_dir, config, cm, rh, key) = seeded_infra(
        "/mybucket/scn4-m.dat",
        &body,
        &[
            (0, PAGE_16 - 1),
            (PAGE_16, 2 * PAGE_16 - 1),
            (2 * PAGE_16, 3 * PAGE_16 - 1),
            (3 * PAGE_16, 4 * PAGE_16 - 1),
        ],
    )
    .await;
    let (start, end) = (PAGE_16 / 2, 4 * PAGE_16 - PAGE_16 / 2 - 1);
    let (status, x, exact, delta) = widened_measured(
        &config,
        &cm,
        &rh,
        &key,
        &body,
        start,
        end,
        PAGE_16 as u64,
        stub_for(body.clone()),
    )
    .await;
    eprintln!("SCN4 (m): peak delta {} bytes x-cache {:?}", delta, x);
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(exact, "(m) byte-exact");
    assert!(
        delta < 2 * PAGE_16 + 16 * MIB,
        "(m) spanning 4 Pages: peak heap growth {} bytes (allowance {})",
        delta,
        2 * PAGE_16 + 16 * MIB
    );
}

// (n) twelve adjacent 4 MiB extents, one request spanning all 48 MiB: the
// chained stream holds one reader at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn n_twelve_adjacent_extents_chained_one_reader_at_a_time() {
    let _guard = ALLOC_LOCK.lock().await;
    let ext = 4 * MIB;
    let body = incompressible_body(12 * ext);
    let extents: Vec<(usize, usize)> = (0..12).map(|i| (i * ext, (i + 1) * ext - 1)).collect();
    let fixture = multi_fixture("/mybucket/scn4-n.dat", body, &extents, &[]).await;
    let (resp, delta) = measured_drain(&fixture, 0, 12 * ext - 1).await;
    eprintln!("SCN4 (n): peak delta {} bytes", delta);
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers.get("x-cache").map(|v| v.to_str().unwrap()),
        Some("HIT")
    );
    assert!(resp.matched, "(n) byte-exact, got {} bytes", resp.len);
    assert_bounded("(n) spanning twelve 4 MiB extents", delta);
}

// (o) a 1 MiB slice at a 60 MiB offset of a 64 MiB incompressible extent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o_deep_offset_slice_of_uncompressed_extent_is_exact() {
    let _guard = ALLOC_LOCK.lock().await;
    let body = incompressible_body(LARGE_OBJECT_SIZE);
    let fixture = multi_fixture("/mybucket/scn4-o.dat", body, &[], &[]).await;
    let (start, end) = (60 * MIB, 61 * MIB - 1);
    let (resp, delta) = measured_drain(&fixture, start, end).await;
    eprintln!("SCN4 (o): peak delta {} bytes", delta);
    assert_eq!(resp.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers.get("x-cache").map(|v| v.to_str().unwrap()),
        Some("HIT")
    );
    assert!(resp.matched, "(o) byte-exact, got {} bytes", resp.len);
    assert_bounded("(o) deep-offset slice", delta);
}

// (p) HEAD with a Range that is resident in the RAM range tier: the HEAD must
// not decompress the RAM entry. RAM on; a 512 KiB compressible range of a
// 64 MiB object (below the disk-streaming threshold, so the buffered path
// promotes it) is promoted by repeat GETs, then HEADed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p_head_on_ram_resident_range_loads_no_body() {
    let _guard = ALLOC_LOCK.lock().await;
    let body = compressible_body(LARGE_OBJECT_SIZE);
    let path = "/mybucket/scn4-p.dat";
    let ext_end = 512 * 1024 - 1;
    let (_dir, config, cache_manager, range_handler, _key) =
        seeded_infra_ram(path, &body, &[(0, ext_end)], true).await;
    let stub = stub_for(body.clone());
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        range_handler,
        stub.into_trait_object(),
    )
    .await;
    let range = range_header(0, ext_end);
    let cache_key = CacheManager::generate_cache_key(path, None);
    // Miss fills the extent; repeat GETs until one is served from the RAM tier.
    let mut in_ram = false;
    let mut last_x = None;
    for _ in 0..100 {
        let r = proxy_request(server.addr, "GET", path, Some(&range)).await;
        assert!(r.status.is_success());
        last_x = r
            .headers
            .get("x-cache")
            .map(|v| v.to_str().unwrap().to_string());
        if cache_manager
            .get_range_from_ram_cache_hit(&cache_key, 0, ext_end as u64, config.cache.get_ttl)
            .is_some()
        {
            in_ram = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        in_ram,
        "(p) precondition: the range never reached the RAM tier (last x-cache {:?})",
        last_x
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    let baseline = reset_peak();
    let resp = proxy_request(server.addr, "HEAD", path, Some(&range)).await;
    let delta = peak_delta(baseline);
    eprintln!("SCN4 (p): peak delta {} bytes", delta);
    assert_hit_206(&resp, ext_end + 1, "(p) HEAD on a RAM-resident range");
    assert!(resp.body.is_empty());
    assert!(
        delta < 256 * 1024,
        "(p) HEAD on a RAM-resident range: peak heap growth {} bytes (allowance {})",
        delta,
        256 * 1024
    );
}

// (q) R6.1 / R6.3: a buffered serve is counted in its size bucket and moves
// the peak-materialised gauge to at least its size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn q_buffered_serve_counted_by_size_and_peak() {
    let _guard = ALLOC_LOCK.lock().await;
    let body = compressible_body(SMALL_OBJECT_SIZE);
    let fixture = warmed_fixture("/mybucket/scn4-q.txt", body).await;
    let before = s3_proxy::http_proxy::buffered_serve_size_buckets();
    let resp = proxy_request(
        fixture.server.addr,
        "GET",
        fixture.path,
        Some("bytes=0-999"),
    )
    .await;
    assert_hit_206(&resp, 1000, "(q) small buffered range");
    assert_eq!(&resp.body[..], &fixture.body[..1000]);
    let after = s3_proxy::http_proxy::buffered_serve_size_buckets();
    eprintln!("SCN4 (q): buckets before {:?} after {:?}", before, after);
    assert!(after[0] > before[0], "(q) <= 64 KiB bucket must move");
    assert_eq!(after[3], before[3], "(q) the > 8 MiB bucket must not move");
    assert!(s3_proxy::http_proxy::buffered_serve_peak_materialised_bytes() >= 1000);
}

// ---------------------------------------------------------------------------
// A no-Range full-object cache hit streams when it cannot be
// materialised within the bound.
// ---------------------------------------------------------------------------

async fn full_object_measured(fixture: &MultiFixture) -> (DrainedResponse, usize) {
    tokio::time::sleep(Duration::from_millis(300)).await;
    let baseline = reset_peak();
    let resp = drained_request(fixture.server.addr, fixture.path, "", &fixture.body[..]).await;
    (resp, peak_delta(baseline))
}

fn assert_full_object_hit(resp: &DrainedResponse, len: usize, what: &str) {
    assert_eq!(resp.status, StatusCode::OK, "{}: status", what);
    assert_eq!(
        resp.headers.get("x-cache").map(|v| v.to_str().unwrap()),
        Some("HIT"),
        "{}: x-cache",
        what
    );
    assert_eq!(
        resp.headers
            .get("content-length")
            .map(|v| v.to_str().unwrap().to_string()),
        Some(len.to_string()),
        "{}: content-length",
        what
    );
    assert!(
        resp.headers.get("content-range").is_none(),
        "{}: a 200 carries no content-range",
        what
    );
    assert!(resp.matched, "{}: byte-exact, got {} bytes", what, resp.len);
}

// (r) a no-Range GET of a 64 MiB object cached as one extent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r_full_object_one_extent_streams_and_is_bounded() {
    let _guard = ALLOC_LOCK.lock().await;
    let body = incompressible_body(LARGE_OBJECT_SIZE);
    let fixture = multi_fixture("/mybucket/scn3-r.dat", body, &[], &[]).await;
    let (resp, delta) = full_object_measured(&fixture).await;
    eprintln!("SCN3 (r): peak delta {} bytes", delta);
    assert_full_object_hit(&resp, LARGE_OBJECT_SIZE, "(r)");
    assert_eq!(
        fixture.stub.captured().len(),
        0,
        "(r) served from cache, nothing forwarded"
    );
    assert_bounded("(r) no-Range GET of a 64 MiB single extent", delta);
}

// (s) the same object cached as three adjacent extents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_full_object_three_extents_streams_and_is_bounded() {
    let _guard = ALLOC_LOCK.lock().await;
    let a = 20 * MIB;
    let b = a + 4 * MIB;
    let body = incompressible_body(LARGE_OBJECT_SIZE);
    let fixture = multi_fixture(
        "/mybucket/scn3-s.dat",
        body,
        &[(0, a - 1), (a, b - 1), (b, LARGE_OBJECT_SIZE - 1)],
        &[],
    )
    .await;
    let forwarded_before = fixture.stub.captured().len();
    let (resp, delta) = full_object_measured(&fixture).await;
    eprintln!("SCN3 (s): peak delta {} bytes", delta);
    assert_full_object_hit(&resp, LARGE_OBJECT_SIZE, "(s)");
    assert_eq!(
        fixture.stub.captured().len(),
        forwarded_before,
        "(s) served from cache, nothing forwarded"
    );
    assert_bounded(
        "(s) no-Range GET of a 64 MiB object in three extents",
        delta,
    );
}

// (t) part 3: widening on, one 16 MiB Page stored as three extents, a small
// read inside it. The Page is assembled by `merge_range_segments`; each extent's
// slice is handed over as a `Bytes` view rather than copied, so the resident set
// is the output buffer plus one loaded extent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t_widened_page_of_three_extents_is_bounded() {
    let _guard = ALLOC_LOCK.lock().await;
    let body = incompressible_body(2 * PAGE_16);
    let (a, b) = (5 * MIB, 11 * MIB);
    let (_dir, config, cm, rh, key) = seeded_infra(
        "/mybucket/scn3-t.dat",
        &body,
        &[(0, a - 1), (a, b - 1), (b, PAGE_16 - 1)],
    )
    .await;
    let (start, end) = (6 * MIB, 6 * MIB + 64 * 1024 - 1);
    let stub = stub_for(body.clone());
    let (status, x, exact, delta) = widened_measured(
        &config,
        &cm,
        &rh,
        &key,
        &body,
        start,
        end,
        PAGE_16 as u64,
        stub.clone(),
    )
    .await;
    eprintln!(
        "SCN3 (t): peak delta {} bytes x-cache {:?} upstream requests {}",
        delta,
        x,
        stub.captured().len()
    );
    assert_eq!(
        stub.captured().len(),
        0,
        "(t) the Page must be served from cache"
    );
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(exact, "(t) byte-exact");
    let allowance = 2 * PAGE_16 + 16 * MIB;
    assert!(
        delta < allowance,
        "(t) a Page of three extents: peak heap growth {} bytes (allowance {})",
        delta,
        allowance
    );
}

// (u) part 1: the no-Range full-object degrade heals.
//
// REACHABILITY, established before the test was written. Every caller of
// `serve_full_object_from_cache` gates on `overlap.has_complete_coverage()`
// (`src/http_proxy.rs:13100`, `:4080`, `:13431`), so the degrade inside it is reached
// only when a COVERED object cannot be streamed: `ForwardUnresolved` (an extent's disk
// `RangeSpec` does not resolve from metadata or the pending journals) or
// `ForwardStreamFailed` (its `.bin` will not open). `ForwardGap` needs coverage claimed
// over non-contiguous extents, which complete coverage otherwise excludes — a gap makes
// the request a PARTIAL cache hit and it never reaches this function at all.
//
// So the fixture is `(j)`'s lever at full-object scale: complete coverage from two
// adjacent extents, both `.bin` files deleted, so the chained stream cannot open its
// first reader and returns `ForwardStreamFailed`.
//
// The DISCRIMINATOR is `buffered_serve_healing_refetches_total`. Both the old uncached
// forward and the new healing refetch answer a no-Range GET with a `200` and the right
// bytes, so status and body cannot tell them apart; the counter can. The
// `content-range`-absent assertion is the guard on the other side: a heal that forwarded
// with a Range header would answer this no-Range request with a `206`.
//
// The repeat-request half of this property (a second identical request is a single-extent
// HIT) is NOT observable in-process: `StubS3Client` answers with a buffered body and only
// a STREAMED S3 body is teed into the cache, the same limitation `(h)`/`(i)` record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn u_full_object_stream_failure_heals_and_is_bounded() {
    let _guard = ALLOC_LOCK.lock().await;
    let half = LARGE_OBJECT_SIZE / 2;
    let body = incompressible_body(LARGE_OBJECT_SIZE);
    let fixture = multi_fixture(
        "/mybucket/scn6-u.dat",
        body,
        &[(0, half - 1), (half, LARGE_OBJECT_SIZE - 1)],
        &[],
    )
    .await;
    let deleted = delete_bin_files(fixture.cache_dir());
    assert!(
        deleted >= 2,
        "(u) fixture: both extents' .bin files must exist to be deleted, deleted {}",
        deleted
    );
    let forwarded_before = fixture.stub.captured().len();
    let healing_before = s3_proxy::http_proxy::buffered_serve_healing_refetches_total();
    let (resp, delta) = full_object_measured(&fixture).await;
    eprintln!(
        "SCN6 (u): peak delta {} bytes status {} upstream requests +{}",
        delta,
        resp.status,
        fixture.stub.captured().len() - forwarded_before
    );
    assert_eq!(
        resp.status,
        StatusCode::OK,
        "(u) a no-Range GET answers 200"
    );
    assert!(
        resp.headers.get("content-range").is_none(),
        "(u) a no-Range GET must carry no content-range"
    );
    assert_eq!(
        resp.headers
            .get("content-length")
            .map(|v| v.to_str().unwrap().to_string()),
        Some(LARGE_OBJECT_SIZE.to_string()),
        "(u) content-length is the object length"
    );
    assert!(resp.matched, "(u) byte-exact, got {} bytes", resp.len);
    assert_eq!(
        fixture.stub.captured().len() - forwarded_before,
        1,
        "(u) exactly one upstream request"
    );
    let ranged = fixture
        .stub
        .captured()
        .iter()
        .skip(forwarded_before)
        .filter(|c| c.headers.contains_key("range") || c.headers.contains_key("Range"))
        .count();
    assert_eq!(
        ranged, 0,
        "(u) the upstream fetch must carry no Range header, or S3 answers a 206"
    );
    assert_healing_refetch(healing_before, "(u)");
    assert_bounded("(u) no-Range full-object degrade heals", delta);
}

// (v) The per-stream decoder floor, measured before and after the change in one test under one
// allocator.
//
// `lz4_flex::frame::FrameDecoder::read_frame_info` takes `max_block_size` from the FRAME
// HEADER (`lz4_flex-0.11.6` `frame/decompress.rs:142`, `frame/header.rs:328-331`) and
// `reserve_exact`s it twice, src and dst (`:159-160`; `dst_size == max_block_size` for
// `BlockMode::Independent`). So a decoder's buffers are set by the block size the frame
// declares, whatever the payload. measured 8,454,144 bytes for a decoder over a 64 KiB
// frame declaring 4 MiB blocks, against 9,270,718 for the whole 4 KiB streamed serve.
//
// Moved every writer to 64 KiB blocks (`src/compression.rs`, `range_frame_info` and
// `encode_store_mode_frame`). This test asserts both writers here declare BD code 4, and
// measures the decoder over the SAME 64 KiB payload framed both ways: the frame the writer
// now produces, and that frame with its descriptor rewritten to declare 4 MiB blocks (the
// frame earlier releases wrote; a single 64 KiB stored block is valid under either). The
// payload and output vector are identical, so the difference is the decoder's buffers.
//
// `lz4_flex` and `twox_hash` are ordinary dependencies of the crate, so this integration
// test can use them directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v_decoder_floor_measured_against_stream_peak() {
    use std::io::Write;

    let _guard = ALLOC_LOCK.lock().await;
    const SMALL: usize = 64 * 1024;

    let handler = s3_proxy::compression::CompressionHandler::new(0, true);
    let small_payload = incompressible_body(SMALL);
    let store_frame =
        s3_proxy::compression::CompressionHandler::encode_store_mode_frame(&small_payload)
            .expect("store-mode frame");
    // BD byte is the 6th byte: magic(4) + FLG(1) + BD(1). Block-size code is bits 4-6.
    let store_bd_code = (store_frame[5] >> 4) & 0x07;

    // The same frame declaring 4 MiB blocks: BD code 7, header checksum recomputed as
    // `xxh32(FLG..BD) >> 8`, as the earlier store-mode writer produced.
    let mut old_store_frame = store_frame.clone();
    old_store_frame[5] = 7 << 4;
    old_store_frame[6] = (twox_hash::XxHash32::oneshot(0, &old_store_frame[4..6]) >> 8) as u8;

    // What the compressed writer declares for a range-sized write.
    let mut enc = s3_proxy::compression::CompressionHandler::new(0, true);
    let compressed = enc
        .compress_with_algorithm(
            &incompressible_body(4 * MIB),
            s3_proxy::compression::CompressionAlgorithm::Lz4,
        )
        .expect("compress")
        .data;
    let compressed_bd_code = (compressed[5] >> 4) & 0x07;
    drop(compressed);

    // (i) the decoder over the old-format frame (4 MiB blocks declared), then over the
    // frame the writer produces now.
    let baseline = reset_peak();
    let out = handler
        .decompress_data(&old_store_frame)
        .expect("decode old");
    let old_decoder_delta = peak_delta(baseline);
    assert!(
        out == small_payload,
        "(v) the 4 MiB-block frame must round-trip"
    );
    drop(out);

    let baseline = reset_peak();
    let out = handler.decompress_data(&store_frame).expect("decode new");
    let decoder_delta = peak_delta(baseline);
    assert!(
        out == small_payload,
        "(v) the 64 KiB-block frame must round-trip"
    );
    drop(out);

    // (ii) a whole streamed serve of a 4 KiB slice of a 64 MiB cached extent, written
    // through the proxy, so its range file carries the frames the writers produce now.
    let fixture = warmed_fixture(
        "/mybucket/scn6-v.dat",
        incompressible_body(LARGE_OBJECT_SIZE),
    )
    .await;
    let (resp, stream_delta) = measured(&fixture, "GET", Some("bytes=0-4095")).await;
    assert_hit_206(&resp, 4096, "(v)");

    eprintln!(
        "SCN7 (v) FLOOR: decoder over a {} B frame declaring 4 MiB blocks {} bytes | same \
         payload declaring 64 KiB blocks {} bytes | whole 4 KiB streamed serve {} bytes | \
         store-mode BD code {} | compressed BD code {} (4 == 64 KiB blocks)",
        SMALL, old_decoder_delta, decoder_delta, stream_delta, store_bd_code, compressed_bd_code
    );

    assert_eq!(
        store_bd_code, 4,
        "(v) the store-mode writer declares 64 KiB blocks"
    );
    assert_eq!(
        compressed_bd_code, 4,
        "(v) the compressed writer declares 64 KiB blocks for a range-sized write"
    );
    // The mechanism: a 4 MiB-block header costs src + dst = 8 MiB of decoder buffers even
    // for 64 KiB of payload.
    assert!(
        old_decoder_delta >= 8 * MIB,
        "(v) a 4 MiB-block decoder allocates src + dst = 8 MiB even for {} B of payload: \
         measured {} bytes",
        SMALL,
        old_decoder_delta
    );
    // The fix: 64 KiB blocks cost src + dst = 128 KiB plus the 64 KiB output vector.
    assert!(
        decoder_delta <= 512 * 1024,
        "(v) a 64 KiB-block decoder must allocate well under 1 MiB for {} B of payload: \
         measured {} bytes",
        SMALL,
        decoder_delta
    );
    assert!(
        decoder_delta <= stream_delta,
        "(v) the decoder cannot cost more than the whole serve containing it: {} vs {}",
        decoder_delta,
        stream_delta
    );

    // THE COST OF 64 KiB BLOCKS, measured rather than asserted to be negligible: with
    // `BlockMode::Independent` no match can span a block, so smaller blocks compress
    // worse. The 4 MiB-block frame is built directly with `lz4_flex`; the 64 KiB-block
    // frame is what the writer produces. Reported for one compressible and one
    // incompressible body.
    for (label, body) in [
        ("compressible", compressible_body(4 * MIB)),
        ("incompressible", incompressible_body(4 * MIB)),
    ] {
        let t0 = std::time::Instant::now();
        let mut info = lz4_flex::frame::FrameInfo::new();
        info.content_checksum = true;
        info.block_mode = lz4_flex::frame::BlockMode::Independent;
        info.block_size = lz4_flex::frame::BlockSize::Max4MB;
        let mut big_enc = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
        big_enc.write_all(&body).expect("compress 4 MiB blocks");
        let big = big_enc.finish().expect("finish 4 MiB blocks");
        let big_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let big_len = big.len();
        let big_bd = (big[5] >> 4) & 0x07;
        drop(big);

        let mut h = s3_proxy::compression::CompressionHandler::new(0, true);
        let t1 = std::time::Instant::now();
        let small = h
            .compress_with_algorithm(&body, s3_proxy::compression::CompressionAlgorithm::Lz4)
            .expect("compress 64 KiB blocks")
            .data;
        let small_ms = t1.elapsed().as_secs_f64() * 1000.0;
        let small_len = small.len();
        let small_bd = (small[5] >> 4) & 0x07;
        drop(small);

        eprintln!(
            "SCN7 (v) COST {}: 4 MiB-block frame {} B (BD {}) in {:.2} ms | 64 KiB-block \
             frame {} B (BD {}) in {:.2} ms | ratio {:.4} -> {:.4} ({:+.2}% size) | \
             encode {:+.1}% time",
            label,
            big_len,
            big_bd,
            big_ms,
            small_len,
            small_bd,
            small_ms,
            big_len as f64 / body.len() as f64,
            small_len as f64 / body.len() as f64,
            (small_len as f64 / big_len as f64 - 1.0) * 100.0,
            (small_ms / big_ms - 1.0) * 100.0,
        );
        assert_eq!(big_bd, 7, "(v) the reference frame declares 4 MiB blocks");
        assert_eq!(
            small_bd, 4,
            "(v) the writer declares 64 KiB blocks for a 4 MiB body"
        );
    }
}

// (w) part 1: a no-Range GET is never answered a 206, not even through the
// partial-cache-merge heal.
//
// REACHABILITY, established before the test was written. A no-Range GET whose coverage is
// PARTIAL never reaches `serve_full_object_from_cache` (every caller of that gates on
// complete coverage). It reaches `src/http_proxy.rs:4753`'s full-object partial-merge arm,
// which SYNTHESIZES `Range: bytes=0-{total-1}` and routes through
// `forward_range_with_coordination` -> `forward_range_request_to_s3`. Four gates, all of
// which this fixture meets: `range` not in SignedHeaders, cached fraction >= 10% of the
// object, total size <= 128 MiB, total size > 0. There, `may_materialise` refuses the
// merge for an over-bound object and the GET takes the healing cache-miss path — which
// passed `full_object: false`, so the upstream refetch carried the synthesized Range, S3
// answered `206`, and that `206` was passed through to a client that sent no Range header.
//
// The DISCRIMINATORS are the status and the absence of `content-range`. The bytes cannot
// discriminate: a `206` covering `0-{total-1}` carries the same body a `200` does, which
// is exactly why this shipped. The upstream-Range count is the third assertion and names
// the cause rather than the symptom.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w_no_range_partial_merge_heal_answers_200() {
    let _guard = ALLOC_LOCK.lock().await;
    let half = LARGE_OBJECT_SIZE / 2;
    let body = incompressible_body(LARGE_OBJECT_SIZE);
    // One cached extent over the first half: 50% coverage, so PARTIAL, over the 10% gate,
    // and the object is over BUFFERED_OPERATION_BOUND. `stub_ranges` carries the
    // synthesized whole-object range so the pre-fix path can be answered at all — without
    // it the defect would surface as an error rather than as the 206 under test.
    let fixture = multi_fixture(
        "/mybucket/scn8-w.dat",
        body,
        &[(0, half - 1)],
        &[(0, LARGE_OBJECT_SIZE - 1)],
    )
    .await;
    let forwarded_before = fixture.stub.captured().len();
    let (resp, delta) = full_object_measured(&fixture).await;
    let ranged = fixture
        .stub
        .captured()
        .iter()
        .skip(forwarded_before)
        .filter(|c| c.headers.contains_key("range") || c.headers.contains_key("Range"))
        .count();
    eprintln!(
        "SCN8 (w): status {} content-range {:?} upstream ranged fetches {} peak delta {}",
        resp.status,
        resp.headers
            .get("content-range")
            .map(|v| v.to_str().unwrap().to_string()),
        ranged,
        delta
    );
    assert_eq!(
        resp.status,
        StatusCode::OK,
        "(w) a GET that carried no Range header must be answered 200, never 206"
    );
    assert!(
        resp.headers.get("content-range").is_none(),
        "(w) a no-Range GET must carry no content-range"
    );
    assert_eq!(
        ranged, 0,
        "(w) the healing upstream fetch must carry no Range header, or S3 answers a 206"
    );
    assert!(resp.matched, "(w) byte-exact, got {} bytes", resp.len);
    assert_eq!(
        resp.headers
            .get("content-length")
            .map(|v| v.to_str().unwrap().to_string()),
        Some(LARGE_OBJECT_SIZE.to_string()),
        "(w) content-length is the object length"
    );
    // The sentinel is internal: it must never reach S3.
    let leaked = fixture
        .stub
        .captured()
        .iter()
        .filter(|c| {
            c.headers
                .keys()
                .any(|k| k.to_lowercase() == "x-proxy-synthesized-full-object-range")
        })
        .count();
    assert_eq!(leaked, 0, "(w) the internal sentinel must not reach S3");
}

// (x) part 1: the IN-BOUND arm of the same route (w) covers. A no-Range GET is never
// answered a 206 when the partial merge FITS within BUFFERED_OPERATION_BOUND either.
//
// REACHABILITY, re-derived by symbol rather than by the earlier line numbers. A no-Range GET with
// PARTIAL coverage is minted a `Range: bytes=0-{total-1}` by the full-object partial-merge
// arm of `handle_get_head_request` and routed through `forward_range_with_coordination` into
// `forward_range_request_to_s3`, which consumes the `x-proxy-synthesized-full-object-range`
// sentinel at its top. (w)'s 64 MiB object is over the 8 MiB bound so `may_materialise`
// refuses and it takes the healing miss path; THIS fixture's 4 MiB object is under it, so the
// merge proceeds and the response is built by the merge-response builder in that same
// function — which unconditionally set `StatusCode::PARTIAL_CONTENT` plus a whole-object
// `content-range`. That builder is the site under test.
//
// DISCRIMINATORS, the same three (w) uses and for the same reason: status, the absence of
// `content-range`, and the count of upstream fetches that carried a whole-object Range. The
// BYTES cannot discriminate — a 206 over `0-{total-1}` carries the identical body — which is
// why this arm survived.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn x_no_range_partial_merge_in_bound_answers_200() {
    let _guard = ALLOC_LOCK.lock().await;
    let total = 4 * MIB;
    let half = total / 2;
    let body = incompressible_body(total);
    // One cached extent over the first half: 50% coverage (PARTIAL, over the 10% gate), the
    // object and every extent under BUFFERED_OPERATION_BOUND so the merge is permitted. The
    // stub carries the missing half (what the merge fetches) and the whole-object range (what
    // a pre-fix heal would have asked for), so a wrong path errors rather than 404s.
    let fixture = multi_fixture(
        "/mybucket/scn9-x.dat",
        body,
        &[(0, half - 1)],
        &[(half, total - 1), (0, total - 1)],
    )
    .await;
    let forwarded_before = fixture.stub.captured().len();
    let (resp, delta) = full_object_measured(&fixture).await;
    let whole_object_ranged = fixture
        .stub
        .captured()
        .iter()
        .skip(forwarded_before)
        .filter(|c| {
            let whole = range_header(0, total - 1);
            c.headers
                .iter()
                .any(|(k, v)| k.to_lowercase() == "range" && v.trim() == whole.as_str())
        })
        .count();
    eprintln!(
        "SCN9 (x): status {} content-range {:?} whole-object upstream ranges {} peak delta {}",
        resp.status,
        resp.headers
            .get("content-range")
            .map(|v| v.to_str().unwrap().to_string()),
        whole_object_ranged,
        delta
    );
    assert_eq!(
        resp.status,
        StatusCode::OK,
        "(x) a GET that carried no Range header must be answered 200, never 206"
    );
    assert!(
        resp.headers.get("content-range").is_none(),
        "(x) a no-Range GET must carry no content-range"
    );
    assert_eq!(
        resp.headers
            .get("content-length")
            .map(|v| v.to_str().unwrap().to_string()),
        Some(total.to_string()),
        "(x) content-length is the object length"
    );
    assert!(resp.matched, "(x) byte-exact, got {} bytes", resp.len);
    assert_eq!(
        whole_object_ranged, 0,
        "(x) the in-bound merge fetches only the missing extent, never the whole object"
    );
    // The sentinel is internal: it must never reach S3.
    let leaked = fixture
        .stub
        .captured()
        .iter()
        .filter(|c| {
            c.headers
                .keys()
                .any(|k| k.to_lowercase() == "x-proxy-synthesized-full-object-range")
        })
        .count();
    assert_eq!(leaked, 0, "(x) the internal sentinel must not reach S3");
}

// ---------------------------------------------------------------------------
// The no-Range whole-object buffered serve is counted by R6.1's
// size buckets, R6.3's peak gauge, and the path discriminator.
//
// Before this change `note_buffered_serve_materialised` had exactly ONE caller,
// inside `serve_range_from_cache_buffered`, so `serve_full_object_from_cache`
// materialised up to BUFFERED_OPERATION_BOUND and took its own ledger
// reservation while being invisible to both. A later fleet run confirmed
// it: a 2 MiB whole-object cached read returned x-cache=HIT byte-exact and the
// <= 8 MiB bucket did not move (36 -> 36).
//
// THE ADJACENT-FIGURE TRAP, and how this avoids it. `multi_fixture` warms the
// cache with one-byte RANGED probes, and every one of those is a ranged
// buffered serve that moves the very buckets this group is about — so a
// fixture judged on `buffered_serve_size_buckets()` alone passes with the
// defect fully present. The predicate asserted here is therefore
// `buffered_serve_full_object_total()`, which is raised by exactly one
// function, `note_buffered_serve_materialised_full_object`, called from exactly
// one site, `HttpProxy::serve_full_object_from_cache`. No ranged read on any
// path can move it. The bucket assertion below is kept as corroboration and
// takes its baseline AFTER warming, immediately before the measured request.
// ---------------------------------------------------------------------------

// (y) R6.1 / R6.3 / path discriminator: a no-Range whole-object cache hit
// within the bound is counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn y_full_object_serve_counted_by_size_peak_and_path() {
    let _guard = ALLOC_LOCK.lock().await;
    // 2 MiB, matching the fleet test's fixture: comfortably inside BUFFERED_OPERATION_BOUND
    // (8 MiB) so the serve materialises rather than streaming, and landing in the
    // `<= 8 MiB` bucket rather than `<= 64 KiB` or `<= 1 MiB`.
    let total = 2 * MIB;
    let body = incompressible_body(total);
    let fixture = multi_fixture("/mybucket/scna-y.dat", body, &[], &[]).await;
    // Baselines AFTER warming, so the fixture's own ranged probes are outside the
    // measurement window.
    let buckets_before = s3_proxy::http_proxy::buffered_serve_size_buckets();
    let full_object_before = s3_proxy::http_proxy::buffered_serve_full_object_total();
    let peak_before = s3_proxy::http_proxy::buffered_serve_peak_materialised_bytes();

    // One no-Range GET, and nothing else, inside the window.
    let resp = drained_request(fixture.server.addr, fixture.path, "", &fixture.body[..]).await;

    let buckets_after = s3_proxy::http_proxy::buffered_serve_size_buckets();
    let full_object_after = s3_proxy::http_proxy::buffered_serve_full_object_total();
    eprintln!(
        "SCNa (y): full_object {} -> {} buckets {:?} -> {:?} peak_before {}",
        full_object_before, full_object_after, buckets_before, buckets_after, peak_before
    );
    assert_full_object_hit(&resp, total, "(y)");
    assert_eq!(
        fixture.stub.captured().len(),
        0,
        "(y) served from cache, nothing forwarded"
    );
    // THE DISCRIMINATOR. Red before the change: nothing raised this counter.
    assert!(
        full_object_after > full_object_before,
        "(y) buffered_serve_full_object_total must move across a whole-object cached serve ({} -> {})",
        full_object_before,
        full_object_after
    );
    // Corroboration: the same serve lands in the `<= 8 MiB` bucket. This is the fleet test's
    // own predicate, which did not move on the fleet.
    assert!(
        buckets_after[2] > buckets_before[2],
        "(y) the <= 8 MiB bucket must move ({:?} -> {:?})",
        buckets_before,
        buckets_after
    );
    assert_eq!(
        buckets_after[3], buckets_before[3],
        "(y) the > 8 MiB invariant-alarm bucket must not move"
    );
    assert!(
        s3_proxy::http_proxy::buffered_serve_peak_materialised_bytes() >= total as u64,
        "(y) the peak gauge must reach at least the whole-object size"
    );
}

// (z) the uncached over-bound forward carries the proxy-identification Referer.
// A range across two adjacent 20 MiB extents is served by a chained stream; with the
// extents' .bin files deleted the chain fails and `forward_over_buffered_bound` takes over. A cache-bypass header makes
// the healing refetch decline, so the request is forwarded uncached. That forward must
// add the configured Referer the way every other forward does, rather than dropping it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn z_uncached_over_bound_forward_carries_proxy_referer() {
    let _guard = ALLOC_LOCK.lock().await;
    const REFERER: &str = "Hybrid Cache for Amazon S3/test (scnb-host)";
    let (start, end) = (MIB, 30 * MIB - 1);
    let body = incompressible_body(EXTENT_40);
    let (dir, config, cache_manager, range_handler, cache_key) = seeded_infra(
        "/mybucket/scnb-z.dat",
        &body,
        &[(0, 20 * MIB - 1), (20 * MIB, EXTENT_40 - 1)],
    )
    .await;
    assert!(
        delete_bin_files(dir.path()) >= 1,
        "(z) fixture must delete the extent's .bin"
    );
    let stub = stub_for(body.clone()).with_response_for_range(
        range_header(start, end),
        stub_range_response(&body, start, end),
    );
    let resolved = s3_proxy::bucket_settings::ResolvedSettings::default();
    let client_headers: std::collections::HashMap<String, String> = [
        ("cache-control".to_string(), "no-cache".to_string()),
        ("range".to_string(), range_header(start, end)),
        (
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
                 SignedHeaders=host;x-amz-date, Signature=sig"
                .to_string(),
        ),
    ]
    .into();
    let healing_before = s3_proxy::http_proxy::buffered_serve_healing_refetches_total();
    let response = tokio::time::timeout(
        Duration::from_secs(60),
        HttpProxy::handle_range_request(
            hyper::Method::GET,
            cache_key.clone(),
            &range_header(start, end),
            client_headers,
            Arc::clone(&cache_manager),
            Arc::clone(&range_handler),
            stub.clone().into_trait_object(),
            "s3.amazonaws.com".to_string(),
            format!("/{}", cache_key).parse().unwrap(),
            Arc::clone(&config),
            &resolved,
            None,
            Arc::new(InFlightTracker::new()),
            None,
            &Some(REFERER.to_string()),
            false,
            None,
        ),
    )
    .await
    .expect("handle_range_request timed out")
    .expect("handle_range_request");
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        s3_proxy::http_proxy::buffered_serve_healing_refetches_total(),
        healing_before,
        "(z) read_cache_enabled: false must take the uncached forward, not the healing path"
    );
    let forwarded: Vec<_> = stub
        .captured()
        .into_iter()
        .filter(|c| {
            c.headers
                .iter()
                .any(|(k, v)| k.eq_ignore_ascii_case("range") && *v == range_header(start, end))
        })
        .collect();
    assert_eq!(
        forwarded.len(),
        1,
        "(z) the stub must see the request forwarded once with its own Range"
    );
    let referer = forwarded[0]
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("referer"))
        .map(|(_, v)| v.clone());
    assert_eq!(
        referer.as_deref(),
        Some(REFERER),
        "(z) the uncached over-bound forward must carry the configured proxy Referer"
    );
}
