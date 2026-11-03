//! A cache hit streamed from disk promotes its range to the RAM cache once the client has the
//! whole body.
//!
//! The exact-extent arm's body (`StreamedHitFrames`) runs the promotion when the disk stream
//! ends clean. hyper drops a response body whose `Content-Length` is met without polling it
//! for its end, so the body has to see the end before it hands over the last frame. This
//! drives a real hyper server and client so that hyper behaviour is in the verdict. Harness
//! copied from `tests/hit_stream_memory_test.rs`.
mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, StatusCode};
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

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
/// Above `disk_streaming_threshold` (1 MiB), so a disk hit streams; several 256 KiB chunks.
const OBJECT_SIZE: usize = 2 * MIB;
const PATH: &str = "/mybucket/streamed-hit-promotion.bin";
const CACHE_KEY: &str = "mybucket/streamed-hit-promotion.bin";
/// How long the spawned promotion may take after the body completes.
const PROMOTION_DEADLINE: Duration = Duration::from_secs(10);

fn test_config(cache_dir: std::path::PathBuf) -> Config {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = true;
    config.cache.max_ram_cache_size = 64 * MIB as u64;
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

/// A whole-extent hit streamed from disk, read to the end by its client, leaves the range in
/// the RAM cache.
///
/// # What is asserted, and why
///
/// - **Precondition: the RAM cache is empty before the hit**, so the hit is served from disk
///   (the warm-up's own promotion is invalidated first).
/// - **The hit is a streamed disk hit of the whole object** (`206`, `x-cache: HIT`, full
///   length).
/// - **The RAM cache holds an entry within `PROMOTION_DEADLINE`** after the client has the
///   body.
///
/// # RED SIDE
///
/// A body that runs the promotion only when it is polled past its last frame never runs it:
/// hyper drops the body once `Content-Length` bytes are written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_streamed_disk_hit_read_to_the_end_promotes_to_ram() {
    let dir = TempDir::new().unwrap();
    let config = Arc::new(test_config(dir.path().to_path_buf()));
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let server = spawn_proxy_server(
        config,
        cache_manager.clone(),
        range_handler,
        StubS3Client::new()
            .with_default(
                StubResponse::ok(incompressible_body(OBJECT_SIZE))
                    .with_header("etag", "\"promotion-etag\"")
                    .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                    .with_header("content-type", "application/octet-stream"),
            )
            .into_trait_object(),
    )
    .await;
    let (status, _, len) = full_request(server.addr, None).await;
    assert!(status.is_success(), "warm GET: {status}");
    assert_eq!(len, OBJECT_SIZE);
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
        "PRECONDITION: the cache never served {PATH} after the warm GET"
    );

    // Let any promotion the warm-up or the loop started land, then clear it.
    tokio::time::sleep(Duration::from_millis(500)).await;
    cache_manager
        .invalidate_ram_ranges(CACHE_KEY)
        .await
        .unwrap();
    let before = cache_manager
        .ram_cache_stats()
        .await
        .expect("RAM cache enabled");
    assert_eq!(
        before.entries_count, 0,
        "PRECONDITION: the RAM cache must be empty before the streamed hit"
    );

    let (status, hit, len) = full_request(server.addr, Some(&whole)).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(hit.as_deref(), Some("HIT"));
    assert_eq!(len, OBJECT_SIZE);

    let deadline = Instant::now() + PROMOTION_DEADLINE;
    let mut entries = 0;
    while Instant::now() < deadline {
        entries = cache_manager
            .ram_cache_stats()
            .await
            .expect("RAM cache enabled")
            .entries_count;
        if entries > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    println!("RAM entries after a streamed disk hit: {entries}");
    assert!(
        entries > 0,
        "a streamed disk hit read to the end did not promote to RAM within {PROMOTION_DEADLINE:?}"
    );
}
