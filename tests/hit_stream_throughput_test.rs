//! Cache-hit throughput and process CPU per GB served, for streamed-hit arms, one proxy per arm.
//!
//! A measurement, not a gate: it prints one `RUN` line per arm, load and round, and a summary
//! per arm and load, and asserts nothing about the arms' ratio. The comparison that matters is
//! against 3.0.0 (1 MiB chunks on the exact arm and a relay channel, which this tree no longer
//! has) and is made across builds, so the same file runs on a 3.0.0 build with `apply_arm`'s
//! body replaced by a no-op. Two loads, each on its own warmed proxy per arm:
//!
//! - `small`: `SMALL_CLIENTS` concurrent clients each GET a cached `SMALL_OBJECT` object
//!   `SMALL_REQUESTS` times;
//! - `large`: `LARGE_CLIENTS` clients each GET a cached `LARGE_OBJECT` object `LARGE_REQUESTS`
//!   times, the per-stream case.
//!
//! Arms alternate within each round. `HIT_STREAM_ARMS` lists them as `chunk_kib:depth` pairs
//! separated by commas (default `0:1,0:2,0:4`; a chunk of 0 is the build's
//! `STREAMED_HIT_CHUNK_SIZE`, the only chunk this tree can run), and `HIT_STREAM_ROUNDS` sets
//! the rounds (default 7). The depth seam and the rusage counters are process-global, so this
//! binary holds one test. Run it with `cargo test --release --test hit_stream_throughput_test
//! -- --ignored --nocapture`. CPU is the whole process's, clients included, from `getrusage`.
mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
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

const SMALL_OBJECT: usize = 2_400_000;
const SMALL_CLIENTS: usize = 32;
const SMALL_REQUESTS: usize = 40;
const LARGE_OBJECT: usize = 64 * 1024 * 1024;
const LARGE_CLIENTS: usize = 4;
const LARGE_REQUESTS: usize = 8;
const PATH: &str = "/mybucket/hit-stream-throughput.bin";

// ---------------------------------------------------------------------------
// Arms
// ---------------------------------------------------------------------------

/// One streamed-hit setting: a chunk size (0 for the build's) and a disk queue depth.
#[derive(Clone, Debug)]
struct Arm {
    chunk_kib: usize,
    depth: usize,
}

impl Arm {
    fn label(&self) -> String {
        format!("c{}d{}", self.chunk_kib, self.depth)
    }
}

fn arms() -> Vec<Arm> {
    let spec = std::env::var("HIT_STREAM_ARMS").unwrap_or_else(|_| "0:1,0:2,0:4".to_string());
    spec.split(',')
        .map(|pair| {
            let (chunk, depth) = pair
                .split_once(':')
                .expect("HIT_STREAM_ARMS: chunk_kib:depth pairs");
            Arm {
                chunk_kib: chunk.trim().parse().expect("chunk_kib"),
                depth: depth.trim().parse().expect("depth"),
            }
        })
        .collect()
}

fn rounds() -> usize {
    std::env::var("HIT_STREAM_ROUNDS")
        .ok()
        .map(|v| v.parse().expect("HIT_STREAM_ROUNDS"))
        .unwrap_or(7)
}

/// Sets the process-global streamed-hit knobs for `arm`. This tree can set only the depth.
fn apply_arm(arm: &Arm) {
    assert_eq!(arm.chunk_kib, 0, "this build has no chunk-size seam");
    s3_proxy::disk_cache::set_stream_queue_depth_for_tests(arm.depth);
}

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

type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>;

/// One GET read to the end without collecting the body: status, `x-cache`, body length.
async fn get(
    client: &HttpClient,
    addr: SocketAddr,
    range: Option<&str>,
) -> (StatusCode, Option<String>, usize) {
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
    let mut body = response.into_body();
    let mut len = 0;
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame.unwrap().into_data() {
            len += data.len();
        }
    }
    (status, x_cache, len)
}

/// A proxy with an `object_size` object cached on disk, in a directory of its own.
async fn warmed_proxy(dir: &TempDir, object_size: usize) -> ProxyServer {
    let config = Arc::new(test_config(dir.path().to_path_buf()));
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let body = incompressible_body(object_size);
    let server = spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        StubS3Client::new()
            .with_default(
                StubResponse::ok(body)
                    .with_header("etag", "\"hit-stream-etag\"")
                    .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                    .with_header("content-type", "application/octet-stream"),
            )
            .into_trait_object(),
    )
    .await;
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let (status, _, len) = get(&client, server.addr, None).await;
    assert!(status.is_success(), "warm GET: {status}");
    assert_eq!(len, object_size);
    let range = whole_range(object_size);
    for _ in 0..100 {
        let (status, hit, len) = get(&client, server.addr, Some(&range)).await;
        if status == StatusCode::PARTIAL_CONTENT && hit.as_deref() == Some("HIT") {
            assert_eq!(len, object_size);
            return server;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the cache never served the {object_size}-byte object from disk after the warm GET");
}

fn whole_range(object_size: usize) -> String {
    format!("bytes=0-{}", object_size - 1)
}

/// User plus system CPU seconds this process has used, from `getrusage`.
fn process_cpu_seconds() -> f64 {
    // SAFETY: `getrusage` writes only the struct it is given.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    assert_eq!(rc, 0, "getrusage failed");
    let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    secs(usage.ru_utime) + secs(usage.ru_stime)
}

/// One load: (MB/s in 10^6 bytes, process CPU seconds per 10^9 bytes served).
async fn hit_throughput(
    addr: SocketAddr,
    object_size: usize,
    clients: usize,
    requests: usize,
) -> (f64, f64) {
    let cpu_before = process_cpu_seconds();
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(clients);
    for _ in 0..clients {
        tasks.push(tokio::spawn(async move {
            let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
            let range = whole_range(object_size);
            for _ in 0..requests {
                let (status, x_cache, len) = get(&client, addr, Some(&range)).await;
                assert_eq!(status, StatusCode::PARTIAL_CONTENT);
                assert_eq!(x_cache.as_deref(), Some("HIT"));
                assert_eq!(len, object_size);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let elapsed = started.elapsed().as_secs_f64();
    let cpu = process_cpu_seconds() - cpu_before;
    let bytes = (clients * requests * object_size) as f64;
    (bytes / elapsed / 1e6, cpu / (bytes / 1e9))
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
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

/// Cache-hit MB/s and CPU s/GB per arm and load: one `RUN` line per measurement, then each
/// arm's median, spread and median CPU.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run by hand with --ignored --nocapture"]
async fn hit_throughput_per_arm() {
    let arms = arms();
    let rounds = rounds();
    let loads = [
        ("small", SMALL_OBJECT, SMALL_CLIENTS, SMALL_REQUESTS),
        ("large", LARGE_OBJECT, LARGE_CLIENTS, LARGE_REQUESTS),
    ];
    let mut dirs = Vec::new();
    let mut proxies = Vec::new();
    for _ in &arms {
        let mut per_load = Vec::new();
        for (_, size, _, _) in loads {
            let dir = TempDir::new().unwrap();
            per_load.push(warmed_proxy(&dir, size).await);
            dirs.push(dir);
        }
        proxies.push(per_load);
    }
    // One untimed run per arm and load, so connection setup and first reads are outside the
    // figures.
    for (a, arm) in arms.iter().enumerate() {
        apply_arm(arm);
        for (l, (_, size, clients, requests)) in loads.iter().enumerate() {
            hit_throughput(proxies[a][l].addr, *size, *clients, *requests).await;
        }
    }
    let mut runs = vec![vec![(Vec::new(), Vec::new()); loads.len()]; arms.len()];
    for round in 0..rounds {
        for (a, arm) in arms.iter().enumerate() {
            apply_arm(arm);
            for (l, (load, size, clients, requests)) in loads.iter().enumerate() {
                let (mbps, cpu) =
                    hit_throughput(proxies[a][l].addr, *size, *clients, *requests).await;
                println!(
                    "RUN round={round} arm={} load={load} mbps={mbps:.0} cpu_s_per_gb={cpu:.3}",
                    arm.label()
                );
                runs[a][l].0.push(mbps);
                runs[a][l].1.push(cpu);
            }
        }
    }
    for (a, arm) in arms.iter().enumerate() {
        for (l, (load, size, clients, requests)) in loads.iter().enumerate() {
            let (mbps, cpu) = &runs[a][l];
            let mut sorted = mbps.clone();
            sorted.sort_by(|x, y| x.partial_cmp(y).unwrap());
            println!(
                "THROUGHPUT arm={} load={load} clients={clients} requests={requests} \
                 object={size} median={:.0} MB/s min={:.0} max={:.0} cpu_s_per_gb={:.3}",
                arm.label(),
                median(mbps.clone()),
                sorted[0],
                sorted[sorted.len() - 1],
                median(cpu.clone())
            );
            assert!(median(mbps.clone()) > 0.0);
        }
    }
}
