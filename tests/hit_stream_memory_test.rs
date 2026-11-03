//! Heap held by a cache hit streamed from disk to a client that is not reading.
//!
//! A hit streamed from disk runs one queue between the disk and the socket: the blocking
//! reader's channel (`DiskCacheManager::stream_range_data_from`), `STREAM_QUEUE_DEPTH` chunks
//! of `STREAMED_HIT_CHUNK_SIZE` (512 KiB) deep. The response body wraps the disk stream
//! directly (`StreamedHitFrames`). Against a client that has stopped reading, a hit holds the
//! queued chunks, the chunk the reader is waiting to send, the LZ4 reader's buffer, and hyper's
//! write queue, which takes frames until it holds more than `DEFAULT_MAX_BUFFER_SIZE` (about
//! 400 KiB, `hyper/src/proto/h1/io.rs`). 3.0.0 also ran a 4-deep relay channel and 1 MiB chunks
//! on this arm, about 12.4 MiB per stalled hit.
//!
//! The test opens `STREAMS` such requests against one proxy per case, waits until the heap
//! stops growing, and reports the live heap per request: a hit at depths 1, 2 and 4 (through
//! the `set_stream_queue_depth_for_tests` seam) and at the build's depth, and a miss whose
//! client stalls. A second test takes the peak heap of one stalled hit on a range smaller
//! than a chunk (R14.1). Harness copied from `tests/buffered_serve_memory_bound_test.rs`. The
//! allocator counters and the depth seam are process-global, so the tests take one lock.
mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
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
/// The highest `LIVE` since a test last stored into it.
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn note_live(live: usize) {
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: every method delegates to `System` with the caller's layout and only
// adjusts one atomic on success.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            note_live(LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size());
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if !p.is_null() {
            note_live(LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size());
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
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            note_live(LIVE.fetch_add(new_size, Ordering::Relaxed) + new_size);
        }
        p
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
/// Stalled requests held open at once in each case.
const STREAMS: usize = 16;
/// Large enough that the socket buffers between proxy and client cannot absorb the object, so
/// every stalled hit fills its queues.
const OBJECT_SIZE: usize = 32 * MIB;
const PATH: &str = "/mybucket/hit-stream-memory.bin";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn test_config(cache_dir: std::path::PathBuf) -> Config {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_cache_size = 1024 * 1024 * 1024;
    config.cache.read_cache_enabled = true;
    config.server.max_concurrent_requests = 100;
    config
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

                        // A stalled client is dropped mid-body; that error is expected.
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

const AUTHORIZATION: &str =
    "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
     SignedHeaders=host;x-amz-date, Signature=sig";

/// One request read to the end: status, `x-cache`, body length.
async fn full_request(
    addr: SocketAddr,
    range: Option<&str>,
) -> (StatusCode, Option<String>, usize) {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let mut builder = Request::builder()
        .method("GET")
        .uri(format!("http://{}{}", addr, PATH))
        .header("authorization", AUTHORIZATION);
    if let Some(range) = range {
        builder = builder.header("range", range);
    }
    let response = tokio::time::timeout(
        Duration::from_secs(60),
        client.request(builder.body(Full::new(Bytes::new())).unwrap()),
    )
    .await
    .expect("proxy request timed out")
    .expect("proxy request failed");
    let status = response.status();
    let x_cache = response
        .headers()
        .get("x-cache")
        .map(|v| v.to_str().unwrap().to_string());
    let len = response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .len();
    (status, x_cache, len)
}

/// A GET of the whole cached extent whose client reads the response head and then stops
/// reading. Its receive buffer is shrunk so the kernel holds little of the body for it.
async fn stalled_get(addr: SocketAddr, start: usize, size: usize, expect_hit: bool) -> TcpStream {
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let mut stream = socket.connect(addr).await.unwrap();
    let request = format!(
        "GET {PATH} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: {AUTHORIZATION}\r\n\
         Range: bytes={start}-{}\r\n\r\n",
        size - 1
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        let n = tokio::time::timeout(Duration::from_secs(30), stream.read(&mut byte))
            .await
            .expect("response head timed out")
            .unwrap();
        assert_eq!(n, 1, "connection closed before the response head ended");
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
    let want = if expect_hit {
        "http/1.1 206"
    } else {
        "http/1.1 200"
    };
    assert!(head.starts_with(want), "stalled GET: {head}");
    assert_eq!(
        head.contains("x-cache: hit"),
        expect_hit,
        "stalled GET must be a {}: {head}",
        if expect_hit { "hit" } else { "miss" }
    );
    stream
}

/// Wait until the live heap has not moved by more than 64 KiB for 500 ms, and return it.
async fn settled_live() -> usize {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = LIVE.load(Ordering::Relaxed);
    let mut stable_since = Instant::now();
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let now = LIVE.load(Ordering::Relaxed);
        if now.abs_diff(last) > 64 * KIB {
            last = now;
            stable_since = Instant::now();
        } else if stable_since.elapsed() >= Duration::from_millis(500) {
            return now;
        }
    }
    LIVE.load(Ordering::Relaxed)
}

/// What the stalled requests of one measurement read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Case {
    /// A hit on the range file the proxy wrote.
    Hit,
    /// A hit on all but the first byte of that range file: the slice arm.
    Slice,
    /// A miss: the cache is cold, every request fetches from the stub and fills the cache.
    Miss,
}

/// Live heap per stalled request for one case, on a proxy of its own.
async fn heap_per_stalled_get(case: Case) -> usize {
    let dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(dir.path().to_path_buf()));
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let body = incompressible_body(OBJECT_SIZE);
    let server = spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        StubS3Client::new()
            .with_default(
                StubResponse::ok(body.clone())
                    .with_header("etag", "\"hit-stream-etag\"")
                    .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                    .with_header("content-type", "application/octet-stream"),
            )
            .into_trait_object(),
    )
    .await;
    if case != Case::Miss {
        let (status, _, len) = full_request(server.addr, None).await;
        assert!(status.is_success(), "warm GET: {status}");
        assert_eq!(len, OBJECT_SIZE);
        // The disk write may land just after the warm response ends.
        let whole = format!("bytes=0-{}", OBJECT_SIZE - 1);
        let mut cached = false;
        for _ in 0..100 {
            if let (StatusCode::PARTIAL_CONTENT, Some(hit), OBJECT_SIZE) =
                full_request(server.addr, Some(&whole)).await
            {
                if hit == "HIT" {
                    cached = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            cached,
            "the cache never served {PATH} from disk after the warm GET"
        );
    }
    let start = if case == Case::Slice { 1 } else { 0 };
    let base = settled_live().await;
    let mut held = Vec::with_capacity(STREAMS);
    for _ in 0..STREAMS {
        held.push(stalled_get(server.addr, start, OBJECT_SIZE, case != Case::Miss).await);
    }
    let growth = settled_live().await.saturating_sub(base);
    drop(held);
    growth / STREAMS
}

/// Incompressible bytes, like the JPEG frames a FruitFox client reads.
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

/// hyper's HTTP/1 write queue takes frames until it holds more than this
/// (`DEFAULT_MAX_BUFFER_SIZE`, 8 KiB + 100 x 4 KiB, `hyper/src/proto/h1/io.rs`), so it holds
/// the fewest whole chunks that exceed it.
const HYPER_WRITE_QUEUE: usize = 408 * KIB;
/// The part of a stalled hit that does not scale with the chunk or the depth (the LZ4 reader's
/// buffer, the frame decoder, the connection and the request): 433 KiB at both 256 KiB and
/// 512 KiB chunks and at depths 1, 2 and 4 on the aarch64 build host, plus 207 KiB of margin,
/// less than the two chunks a relay channel adds.
const FIXED_OVERHEAD: usize = 640 * KIB;

/// Bound on the live heap of one stalled hit at disk queue depth `depth`: the decode buffer,
/// `depth` queued chunks and the one waiting in `blocking_send`, hyper's write queue, and the
/// fixed part. A second queue between the disk and the socket (the relay 3.0.0 ran) adds at
/// least two chunks at any depth and fails it.
fn hit_bound(depth: usize) -> usize {
    use s3_proxy::http_proxy::STREAMED_HIT_CHUNK_SIZE as CHUNK;
    CHUNK * (depth + 2) + (HYPER_WRITE_QUEUE / CHUNK + 1) * CHUNK + FIXED_OVERHEAD
}

/// A stalled hit holds one queue of chunks on the exact and slice arms at depths 1, 2
/// and 4: its live heap stays under `hit_bound(depth)`.
///
/// Also measures, and prints without asserting, a miss whose client stalls.
///
/// # RED SIDE
///
/// With a relay channel of depth 2 restored on the exact arm (a scratch build, 512 KiB chunks),
/// stalled hits held 4,020, 4,532 and 5,556 KiB at depths 1, 2 and 4, over bounds of 2,688,
/// 3,200 and 4,224 KiB: the relay added three chunks at every depth. At 3.0.0's constants
/// (1 MiB chunks, a 4-deep relay after the 4-deep disk channel) a stalled hit held 12,728 KiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_hit_holds_one_queue_of_chunks() {
    use s3_proxy::disk_cache::{set_stream_queue_depth_for_tests, STREAM_QUEUE_DEPTH};
    use s3_proxy::http_proxy::STREAMED_HIT_CHUNK_SIZE;
    let _serial = serial().await;
    let mut figures = Vec::new();
    for depth in [1, 2, 4] {
        set_stream_queue_depth_for_tests(depth);
        for case in [Case::Hit, Case::Slice] {
            figures.push((depth, case, heap_per_stalled_get(case).await));
        }
    }
    set_stream_queue_depth_for_tests(0);
    let miss = heap_per_stalled_get(Case::Miss).await;
    for (depth, case, heap) in &figures {
        println!(
            "HEAP {STREAMS} stalled {case:?} requests of a {} MiB object, chunk {} KiB, depth \
             {depth}: {} KiB per request, bound {} KiB, beyond the chunk and depth terms {} KiB",
            OBJECT_SIZE / MIB,
            STREAMED_HIT_CHUNK_SIZE / KIB,
            heap / KIB,
            hit_bound(*depth) / KIB,
            heap.saturating_sub(hit_bound(*depth) - FIXED_OVERHEAD) / KIB
        );
    }
    println!(
        "HEAP miss with a stalled client at the build's depth {STREAM_QUEUE_DEPTH}: {} KiB",
        miss / KIB
    );
    for (depth, case, heap) in &figures {
        assert!(
            *heap < hit_bound(*depth),
            "a stalled {case:?} hit at depth {depth} held {} KiB, over the {} KiB bound",
            heap / KIB,
            hit_bound(*depth) / KIB
        );
    }
}

/// Serialises this binary's tests: the allocator counters and the depth seam are process-global.
static SERIAL: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

/// A cached range far smaller than one streamed-hit chunk.
const SMALL_SIZE: usize = 16 * KIB;
/// What one streamed hit allocates besides its own bytes, whatever the range's size. With the
/// read buffer capped at the range (R14.1) a 16 KiB hit peaked at 226 KiB over the settled
/// heap (least of 5, aarch64 build host), so about 210 KiB is fixed (measured, not attributed
/// line by line); with a whole 256 KiB read buffer it peaked at 449 KiB.
const SMALL_FIXED_OVERHEAD: usize = 256 * KIB;

/// Peak heap growth while one stalled client holds a streamed hit on `len` bytes from `start`
/// of an `object`-byte cached range: the least of five trials, since anything else the process
/// allocates meanwhile can only raise a trial's peak.
async fn peak_of_one_stalled_hit(object: usize, start: usize, len: usize) -> usize {
    let dir = TempDir::new().unwrap();
    let mut config = test_config(dir.path().to_path_buf());
    // Below the threshold a hit is served buffered; this measures the streamed reader.
    config.cache.disk_streaming_threshold = 1024;
    let config = Arc::new(config);
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let server = spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        StubS3Client::new()
            .with_default(
                StubResponse::ok(incompressible_body(object))
                    .with_header("etag", "\"hit-stream-small-etag\"")
                    .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                    .with_header("content-type", "application/octet-stream"),
            )
            .into_trait_object(),
    )
    .await;
    let (status, _, warm_len) = full_request(server.addr, None).await;
    assert!(status.is_success(), "warm GET: {status}");
    assert_eq!(warm_len, object);
    let whole = format!("bytes=0-{}", object - 1);
    let mut cached = false;
    for _ in 0..100 {
        if let (StatusCode::PARTIAL_CONTENT, Some(hit), n) =
            full_request(server.addr, Some(&whole)).await
        {
            if hit == "HIT" && n == object {
                cached = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        cached,
        "the cache never served the {object}-byte range from disk"
    );
    let mut least = usize::MAX;
    for _ in 0..5 {
        settled_live().await;
        let base = LIVE.load(Ordering::Relaxed);
        PEAK.store(base, Ordering::Relaxed);
        let held = stalled_get(server.addr, start, start + len, true).await;
        settled_live().await;
        least = least.min(PEAK.load(Ordering::Relaxed).saturating_sub(base));
        drop(held);
    }
    least
}

/// A streamed hit on a range smaller than one chunk allocates about its own size plus a fixed
/// overhead, not a whole `STREAMED_HIT_CHUNK_SIZE` read buffer (R14.1).
///
/// # RED SIDE
///
/// With the reader's buffer sized at the chunk rather than the bytes left, the 16 KiB hit
/// peaked at 449 KiB, over the 272 KiB bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_hit_on_a_small_range_allocates_about_its_own_size() {
    let _serial = serial().await;
    let peak = peak_of_one_stalled_hit(SMALL_SIZE, 0, SMALL_SIZE).await;
    println!(
        "SMALL one stalled streamed hit on a {} KiB range: peak {} KiB over the settled heap \
         (least of 5)",
        SMALL_SIZE / KIB,
        peak / KIB
    );
    assert!(
        peak < SMALL_SIZE + SMALL_FIXED_OVERHEAD,
        "a stalled hit on a {} KiB range peaked at {} KiB, over its size plus {} KiB",
        SMALL_SIZE / KIB,
        peak / KIB,
        SMALL_FIXED_OVERHEAD / KIB
    );
}

/// The extent a small slice is cut from: past `BUFFERED_OPERATION_BOUND`, so the slice arm
/// streams it.
const LARGE_EXTENT: usize = 32 * MIB;
/// Where in [`LARGE_EXTENT`] the small slice starts.
const SLICE_AT: usize = 16 * MIB + 100 * KIB;
/// What one streamed slice of [`LARGE_EXTENT`] allocates besides its own bytes. Larger than
/// [`SMALL_FIXED_OVERHEAD`] because the LZ4 reader's buffer and frame decoder are sized by
/// the 32 MiB file (`lz4_range_reader_capacity`), not the slice.
const SLICE_FIXED_OVERHEAD: usize = 768 * KIB;

/// A streamed hit on a 16 KiB slice of a 32 MiB extent allocates about the slice's own size
/// plus a fixed overhead: the reader is given the response's end, discards up to the slice
/// itself, and caps its buffer at the slice (R14.1).
///
/// # RED SIDE
///
/// With the response's end ignored (the buffer capped at the extent's remaining bytes and an
/// LZ4 extent discarded up to the slice by the caller, as before the fix), the slice peaked at
/// 1,456 KiB, over the 784 KiB bound; with it, 464 KiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_small_slice_of_a_large_extent_allocates_about_its_own_size() {
    let _serial = serial().await;
    let peak = peak_of_one_stalled_hit(LARGE_EXTENT, SLICE_AT, SMALL_SIZE).await;
    println!(
        "SLICE one stalled streamed {} KiB slice of a {} MiB extent: peak {} KiB over the \
         settled heap (least of 5)",
        SMALL_SIZE / KIB,
        LARGE_EXTENT / MIB,
        peak / KIB
    );
    assert!(
        peak < SMALL_SIZE + SLICE_FIXED_OVERHEAD,
        "a stalled {} KiB slice of a {} MiB extent peaked at {} KiB, over its size plus {} KiB",
        SMALL_SIZE / KIB,
        LARGE_EXTENT / MIB,
        peak / KIB,
        SLICE_FIXED_OVERHEAD / KIB
    );
}
