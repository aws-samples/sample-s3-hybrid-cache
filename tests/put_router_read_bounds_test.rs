//! The PUT router's two shared-volume reads before a write reaches its handler are bounded at
//! 250 ms, and a write that hits either bound is forwarded to S3 uncached and invalidates the
//! key's cached copy.
//!
//! - The first PUT capacity figure (`CacheManager::put_capacity_usage`), which on a proxy whose
//!   startup read did not finish waits for the first refresh.
//! - The bucket's origin marker (`CacheManager::origin_verdict_within`), read once per bucket
//!   per process and memoised after that.
//!
//! Every test drives real requests through `HttpProxy::handle_request` to a loopback mock S3 over
//! a plaintext upstream override. Two proxies share one cache directory, as a fleet shares a
//! volume: proxy A caches the object, then proxy B takes the write. The stall is a named pipe in
//! place of the file the read opens, as in `put_pre_body_awaits_bounded_test.rs`: opening a FIFO
//! for reading parks the thread in the kernel, which is what a read against a volume in NFS lock
//! recovery looks like. Each test releases its readers afterwards, because a tokio runtime waits
//! for its blocking threads when it is dropped.
//!
//! Red side (the code before the change): each write waits about 5 s, and on the origin-marker stall
//! the write invalidates nothing, so B serves the old object from cache once its claim lands.
#![cfg(unix)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Semaphore};

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::{Config, UpstreamOverrideConfig, UpstreamScheme};
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::s3_client::S3Client;
use s3_proxy::S3ClientApi;

const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000"; // gitleaks:allow
/// The router's bound on each read (`cache::PUT_ROUTER_READ_BOUND`).
const ROUTER_BOUND: Duration = Duration::from_millis(250);
/// Scheduling slack plus the mock's answer and the response-path invalidation. `ROUTER_BOUND +
/// MARGIN` is under 1 s, so a write held by a 1 s bound fails, as one held by the earlier
/// bound of 5 s does.
const MARGIN: Duration = Duration::from_millis(500);
/// Per-request ceiling, far past either bound.
const STEP: Duration = Duration::from_secs(30);

// ── Mock S3 ─────────────────────────────────────────────────────────────────

#[derive(Default)]
struct MockState {
    /// Key (`bucket/object`) -> every version's body, oldest first.
    versions: HashMap<String, Vec<Bytes>>,
    /// Key -> GET/HEAD count.
    gets: HashMap<String, usize>,
}

#[derive(Clone, Default)]
struct MockS3 {
    state: Arc<Mutex<MockState>>,
}

impl MockS3 {
    fn put(&self, key: &str, body: &str) {
        self.state
            .lock()
            .unwrap()
            .versions
            .entry(key.to_string())
            .or_default()
            .push(Bytes::from(body.to_string()));
    }

    fn gets(&self, key: &str) -> usize {
        *self.state.lock().unwrap().gets.get(key).unwrap_or(&0)
    }

    fn latest(&self, key: &str) -> Option<Bytes> {
        self.state
            .lock()
            .unwrap()
            .versions
            .get(key)
            .and_then(|v| v.last().cloned())
    }

    async fn handle(self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let key = req.uri().path().trim_start_matches('/').to_string();
        let query = req.uri().query().unwrap_or("").to_string();
        let body = req.into_body().collect().await?.to_bytes();
        let mut st = self.state.lock().unwrap();
        if method == hyper::Method::POST
            && query.split('&').any(|p| p == "uploads" || p == "uploads=")
        {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult><Key>{}</Key><UploadId>scn1-upload</UploadId></InitiateMultipartUploadResult>",
                key
            );
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .header("content-length", xml.len().to_string())
                .body(Full::new(Bytes::from(xml)))
                .unwrap());
        }
        if method == hyper::Method::PUT {
            let versions = st.versions.entry(key).or_default();
            versions.push(body);
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("etag", format!("\"etag-v{}\"", versions.len()))
                .header("content-length", "0")
                .body(Full::new(Bytes::new()))
                .unwrap());
        }
        if method == hyper::Method::GET || method == hyper::Method::HEAD {
            *st.gets.entry(key.clone()).or_insert(0) += 1;
            let Some(versions) = st.versions.get(&key) else {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("content-length", "0")
                    .body(Full::new(Bytes::new()))
                    .unwrap());
            };
            let data = versions.last().unwrap().clone();
            let body = if method == hyper::Method::HEAD {
                Bytes::new()
            } else {
                data.clone()
            };
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-length", data.len().to_string())
                .header("etag", format!("\"etag-v{}\"", versions.len()))
                .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                .header("content-type", "application/octet-stream")
                .body(Full::new(body))
                .unwrap());
        }
        Ok(Response::builder()
            .status(StatusCode::NOT_IMPLEMENTED)
            .body(Full::new(Bytes::new()))
            .unwrap())
    }

    async fn spawn(self) -> (SocketAddr, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        let (tx, mut rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        let Ok((stream, _)) = accept else { break };
                        let mock = self.clone();
                        tokio::spawn(async move {
                            let service = service_fn(move |req| mock.clone().handle(req));
                            let _ = http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service)
                                .await;
                        });
                    }
                    _ = &mut rx => break,
                }
            }
        });
        (addr, tx)
    }
}

// ── Two proxies on one cache directory ──────────────────────────────────────

struct Proxy {
    addr: SocketAddr,
    _shutdown: oneshot::Sender<()>,
}

struct Harness {
    dir: TempDir,
    _mock_shutdown: oneshot::Sender<()>,
    mock: MockS3,
    mock_host: String,
    /// Caches the object. Runs a consolidation loop until `stop_consolidation`.
    a: Proxy,
    /// Takes the write. No consolidation loop, and no PUT capacity refresher until its first PUT.
    b: Proxy,
    stop_consolidation: Arc<AtomicBool>,
}

struct Answer {
    status: StatusCode,
    body: Bytes,
}

fn config_for(dir: &Path, mock_host: &str) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = dir.to_path_buf();
    config.cache.ram_cache_enabled = false;
    config.cache.write_cache_enabled = true;
    config.cache.read_cache_enabled = true;
    config.cache.download_coordination.enabled = false;
    config.cache.get_ttl = Duration::from_secs(3600);
    config.cache.head_ttl = Duration::from_secs(3600);
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.connection_pool.upstream_overrides.insert(
        mock_host.to_string(),
        UpstreamOverrideConfig {
            scheme: UpstreamScheme::Http,
            validate_tls: true,
        },
    );
    Arc::new(config)
}

async fn start_proxy(
    dir: &Path,
    mock_host: &str,
    consolidate_until: Option<Arc<AtomicBool>>,
) -> Proxy {
    let config = config_for(dir, mock_host);
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        config.cache.cache_dir.clone(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        config.compression.threshold,
        config.compression.enabled,
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
        config.cache.ram_cache_flush_interval,
        config.cache.ram_cache_shard_count,
        config.connection_pool.upstream_first_byte_timeout,
    ));
    let inflight_ledger = Arc::new(s3_proxy::inflight_ledger::InflightLedger::new(
        config.server.max_inflight_buffer_bytes,
    ));
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(
        S3Client::new(&config.connection_pool, None)
            .expect("s3 client")
            .with_inflight_ledger(Arc::clone(&inflight_ledger)),
    );
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));
    cache_manager.initialize().await.expect("cache init");
    disk_cache_manager
        .write()
        .await
        .initialize()
        .await
        .expect("disk init");
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    if let Some(stop) = consolidate_until {
        if let Some(consolidator) = cache_manager.get_journal_consolidator().await {
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(100));
                while !stop.load(Ordering::SeqCst) {
                    interval.tick().await;
                    let _ = consolidator.run_consolidation_cycle().await;
                }
            });
        }
    }
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let request_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_requests));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let addr = listener.local_addr().unwrap();
    let (shutdown, mut rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let Ok((stream, peer)) = accept else { break };
                    let config = Arc::clone(&config);
                    let cache_manager = Arc::clone(&cache_manager);
                    let range_handler = Arc::clone(&range_handler);
                    let s3_client = Arc::clone(&s3_client);
                    let inflight_tracker = Arc::clone(&inflight_tracker);
                    let request_semaphore = Arc::clone(&request_semaphore);
                    tokio::spawn(async move {
                        let service = service_fn(move |req: Request<Incoming>| {
                            let config = Arc::clone(&config);
                            let cache_manager = Arc::clone(&cache_manager);
                            let range_handler = Arc::clone(&range_handler);
                            let s3_client = Arc::clone(&s3_client);
                            let inflight_tracker = Arc::clone(&inflight_tracker);
                            let request_semaphore = Arc::clone(&request_semaphore);
                            let inflight_ledger = s3_client.get_inflight_ledger();
                            async move {
                                HttpProxy::handle_request(
                                    req, peer, config, cache_manager, s3_client, range_handler,
                                    request_semaphore, None, None, inflight_tracker, None, None,
                                    None, inflight_ledger,
                                )
                                .await
                            }
                        });
                        let _ = http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    });
                }
                _ = &mut rx => break,
            }
        }
    });
    Proxy {
        addr,
        _shutdown: shutdown,
    }
}

async fn harness() -> Harness {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mock = MockS3::default();
    let (mock_addr, mock_shutdown) = mock.clone().spawn().await;
    let mock_host = format!("127.0.0.1:{}", mock_addr.port());
    let dir = TempDir::new().unwrap();
    let stop_consolidation = Arc::new(AtomicBool::new(false));
    let a = start_proxy(
        dir.path(),
        &mock_host,
        Some(Arc::clone(&stop_consolidation)),
    )
    .await;
    let b = start_proxy(dir.path(), &mock_host, None).await;
    Harness {
        dir,
        _mock_shutdown: mock_shutdown,
        mock,
        mock_host,
        a,
        b,
        stop_consolidation,
    }
}

impl Harness {
    async fn send(&self, proxy: &Proxy, method: &str, target: &str, body: Bytes) -> Answer {
        self.send_as(proxy, method, target, body, true).await
    }
    /// [`Self::send`], with or without a SigV4 `Authorization` header.
    async fn send_as(
        &self,
        proxy: &Proxy,
        method: &str,
        target: &str,
        body: Bytes,
        signed: bool,
    ) -> Answer {
        let fut = async {
            let stream = TcpStream::connect(proxy.addr).await.expect("connect");
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .expect("handshake");
            tokio::spawn(async move {
                let _ = conn.await;
            });
            let mut req = Request::builder()
                .method(method)
                .uri(target)
                .header("host", self.mock_host.as_str());
            if signed {
                req = req.header("authorization", AUTH);
            }
            let req = req
                .header("content-length", body.len().to_string())
                .body(Full::new(body))
                .unwrap();
            let resp = sender.send_request(req).await.expect("proxy request");
            let status = resp.status();
            let body = resp.into_body().collect().await.expect("body").to_bytes();
            Answer { status, body }
        };
        tokio::time::timeout(STEP, fut)
            .await
            .unwrap_or_else(|_| panic!("{} {} timed out", method, target))
    }

    /// GET `key` through `proxy`, reporting whether it reached the mock.
    async fn probe(&self, proxy: &Proxy, key: &str) -> (bool, Answer) {
        let before = self.mock.gets(key);
        let answer = self
            .send(proxy, "GET", &format!("/{}", key), Bytes::new())
            .await;
        (self.mock.gets(key) > before, answer)
    }

    /// GET `key` through A until a repeat is answered without reaching the mock, then stop A's
    /// consolidation loop so nothing but the write under test touches the volume.
    async fn cache_through_a(&self, key: &str) -> Bytes {
        let (_, first) = self.probe(&self.a, key).await;
        assert_eq!(first.status, StatusCode::OK, "seed GET {}", key);
        for _ in 0..100 {
            let (reached, answer) = self.probe(&self.a, key).await;
            assert_eq!(answer.status, StatusCode::OK);
            assert_eq!(answer.body, first.body);
            if !reached {
                self.stop_consolidation.store(true, Ordering::SeqCst);
                // Let a cycle already running finish.
                tokio::time::sleep(Duration::from_millis(400)).await;
                return first.body;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("{} never became a cache hit on proxy A", key);
    }

    /// `.meta` files under the cache whose name contains `object`.
    fn meta_files(&self, object: &str) -> Vec<PathBuf> {
        fn walk(dir: &Path, needle: &str, out: &mut Vec<PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, needle, out);
                } else if path.extension().is_some_and(|e| e == "meta")
                    && path
                        .file_name()
                        .is_some_and(|n| n.to_string_lossy().contains(needle))
                {
                    out.push(path);
                }
            }
        }
        let mut out = Vec::new();
        walk(&self.dir.path().join("metadata"), object, &mut out);
        out
    }
}

/// Replace `path` with a FIFO, so the next reader blocks in `open`.
fn make_fifo(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let _ = std::fs::remove_file(path);
    let status = std::process::Command::new("mkfifo")
        .arg(path)
        .status()
        .expect("run mkfifo");
    assert!(status.success(), "mkfifo {:?} failed", path);
}

/// Release every reader parked on the FIFO at `path` with `content`, then leave a regular file
/// holding `content` in its place so later readers do not park.
fn release_fifo(path: &Path, content: Vec<u8>) {
    let fifo = path.to_path_buf();
    let (renamed_tx, renamed_rx) = std::sync::mpsc::channel::<()>();
    let for_pipe = content.clone();
    std::thread::spawn(move || {
        if let Ok(mut w) = std::fs::OpenOptions::new().write(true).open(&fifo) {
            let _ = renamed_rx.recv();
            use std::io::Write;
            let _ = w.write_all(&for_pipe);
        }
    });
    std::thread::sleep(Duration::from_millis(300));
    let tmp = path.with_extension("scn1-release");
    std::fs::write(&tmp, &content).unwrap();
    std::fs::rename(&tmp, path).unwrap();
    let _ = renamed_tx.send(());
    std::thread::sleep(Duration::from_millis(300));
}

/// Time one request through B, with the stall still in place.
async fn timed(h: &Harness, method: &str, target: &str, body: &str) -> (Duration, Answer) {
    let start = Instant::now();
    let answer = h
        .send(&h.b, method, target, Bytes::from(body.to_string()))
        .await;
    (start.elapsed(), answer)
}

/// Collects failed checks so a red run reports every one, not only the first.
#[derive(Default)]
struct Checks(Vec<String>);

impl Checks {
    fn check(&mut self, ok: bool, msg: String) {
        if !ok {
            self.0.push(msg);
        }
    }

    fn finish(self) {
        assert!(
            self.0.is_empty(),
            "{} check(s) failed:\n{}",
            self.0.len(),
            self.0.join("\n")
        );
    }
}

/// The checks both reads share: the write and the Create were answered by S3 within the bound,
/// S3 holds the PUT's bytes, the old copy's metadata is gone, and B's GET serves the new object.
#[allow(clippy::too_many_arguments)]
async fn check_write_outcome(
    h: &Harness,
    key: &str,
    what: &str,
    put_elapsed: Duration,
    put: Answer,
    create_elapsed: Duration,
    create: Answer,
    meta_after_put: Vec<PathBuf>,
) {
    eprintln!(
        "SCN1 {}: PUT answered in {:?}, CreateMultipartUpload in {:?}",
        what, put_elapsed, create_elapsed
    );
    let mut c = Checks::default();
    // The stall must have bitten, or the timing checks below pass vacuously.
    c.check(
        put_elapsed >= ROUTER_BOUND && create_elapsed >= ROUTER_BOUND,
        format!(
            "fixture: {} was not stalled (PUT {:?}, Create {:?}, both should be >= {:?})",
            what, put_elapsed, create_elapsed, ROUTER_BOUND
        ),
    );
    c.check(
        put.status == StatusCode::OK,
        format!("the PUT must succeed, got {}", put.status),
    );
    c.check(
        put_elapsed <= ROUTER_BOUND + MARGIN,
        format!(
            "a PUT waited {:?} for {} on a stalled cache volume; the bound is {:?}",
            put_elapsed, what, ROUTER_BOUND
        ),
    );
    c.check(
        create.status == StatusCode::OK
            && String::from_utf8_lossy(&create.body).contains("scn1-upload"),
        format!(
            "the CreateMultipartUpload must be S3's answer, got {} {:?}",
            create.status, create.body
        ),
    );
    c.check(
        create_elapsed <= ROUTER_BOUND + MARGIN,
        format!(
            "a CreateMultipartUpload waited {:?} for {} on a stalled cache volume; the bound is \
             {:?}",
            create_elapsed, what, ROUTER_BOUND
        ),
    );
    let latest = h.mock.latest(key);
    c.check(
        latest == Some(Bytes::from("version two")),
        format!("S3 must hold the PUT's bytes, holds {:?}", latest),
    );
    c.check(
        meta_after_put.is_empty(),
        format!(
            "the old object's cached metadata must be gone once the PUT is answered: {:?}",
            meta_after_put
        ),
    );
    let (reached, after) = h.probe(&h.b, key).await;
    c.check(
        after.status == StatusCode::OK && after.body == "version two",
        format!(
            "the GET after the write must serve the new object, got {} {:?} (reached S3: {})",
            after.status, after.body, reached
        ),
    );
    c.finish();
}

/// Read 1. B has read no capacity figure since it started, and `size_state.json` is stalled.
/// A single PUT and a `CreateMultipartUpload` must each be answered within the bound, the PUT
/// must reach S3, and B's next GET must serve the new object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_first_capacity_read_forwards_the_write_uncached_within_250ms() {
    let h = harness().await;
    let key = "scn1-cap/object.bin";
    h.mock.put(key, "version one");
    assert_eq!(h.cache_through_a(key).await, Bytes::from("version one"));
    // B claims the bucket's marker before the stall, so only the capacity read is stalled.
    let (_, warm) = h.probe(&h.b, "scn1-cap/other.bin").await;
    assert_eq!(
        warm.status,
        StatusCode::NOT_FOUND,
        "fixture: B's warm-up GET"
    );

    let size_state = h.dir.path().join("size_tracking").join("size_state.json");
    let saved = std::fs::read(&size_state).unwrap_or_else(|_| b"{}".to_vec());
    make_fifo(&size_state);

    let (put_elapsed, put) = timed(&h, "PUT", &format!("/{}", key), "version two").await;
    let (create_elapsed, create) = timed(&h, "POST", "/scn1-cap/multipart.bin?uploads", "").await;
    let meta_after_put = h.meta_files("object.bin");
    release_fifo(&size_state, saved);

    check_write_outcome(
        &h,
        key,
        "the first capacity read",
        put_elapsed,
        put,
        create_elapsed,
        create,
        meta_after_put,
    )
    .await;
}

/// Read 2. A claimed the bucket's origin marker and cached the object; B has not read the
/// marker, and it is stalled. A single PUT and a `CreateMultipartUpload` through B must each be
/// answered within the bound, the PUT must reach S3, the old cached copy must be invalidated,
/// and B's GET once its claim lands must serve the new object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_origin_marker_read_forwards_the_write_uncached_and_invalidates() {
    let h = harness().await;
    let key = "scn1-origin/object.bin";
    h.mock.put(key, "version one");
    assert_eq!(h.cache_through_a(key).await, Bytes::from("version one"));

    let marker = h
        .dir
        .path()
        .join("metadata")
        .join("scn1-origin")
        .join("_origin.json");
    let saved = std::fs::read(&marker).expect("fixture: A's GET claimed the bucket's marker");
    make_fifo(&marker);

    let (put_elapsed, put) = timed(&h, "PUT", &format!("/{}", key), "version two").await;
    let (create_elapsed, create) =
        timed(&h, "POST", "/scn1-origin/multipart.bin?uploads", "").await;
    let meta_after_put = h.meta_files("object.bin");
    // B's claim reads the restored marker and memoises `Matches`, so the GET in
    // `check_write_outcome` reads the shared cache: an entry the write left is served from it.
    release_fifo(&marker, saved);

    check_write_outcome(
        &h,
        key,
        "the origin marker",
        put_elapsed,
        put,
        create_elapsed,
        create,
        meta_after_put,
    )
    .await;
}
/// The unsigned arm of read 2. An unsigned PUT through B, whose
/// marker read is stalled, must be answered within the bound, reach S3, invalidate the old
/// cached copy, and leave B's next GET serving the new object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_origin_marker_read_forwards_the_unsigned_write_uncached_and_invalidates() {
    let h = harness().await;
    let key = "scn1-unsigned/object.bin";
    h.mock.put(key, "version one");
    assert_eq!(h.cache_through_a(key).await, Bytes::from("version one"));
    let marker = h
        .dir
        .path()
        .join("metadata")
        .join("scn1-unsigned")
        .join("_origin.json");
    let saved = std::fs::read(&marker).expect("fixture: A's GET claimed the bucket's marker");
    make_fifo(&marker);
    let start = Instant::now();
    let put = h
        .send_as(
            &h.b,
            "PUT",
            &format!("/{}", key),
            Bytes::from("version two"),
            false,
        )
        .await;
    let put_elapsed = start.elapsed();
    let meta_after_put = h.meta_files("object.bin");
    release_fifo(&marker, saved);
    eprintln!("SCN1 unsigned: PUT answered in {:?}", put_elapsed);
    let mut c = Checks::default();
    c.check(
        put_elapsed >= ROUTER_BOUND,
        format!(
            "fixture: the marker read was not stalled (PUT {:?}, should be >= {:?})",
            put_elapsed, ROUTER_BOUND
        ),
    );
    c.check(
        put.status == StatusCode::OK,
        format!("the unsigned PUT must succeed, got {}", put.status),
    );
    c.check(
        put_elapsed <= ROUTER_BOUND + MARGIN,
        format!(
            "an unsigned PUT waited {:?} for the origin marker on a stalled cache volume; the \
             bound is {:?}",
            put_elapsed, ROUTER_BOUND
        ),
    );
    let latest = h.mock.latest(key);
    c.check(
        latest == Some(Bytes::from("version two")),
        format!("S3 must hold the PUT's bytes, holds {:?}", latest),
    );
    c.check(
        meta_after_put.is_empty(),
        format!(
            "the old object's cached metadata must be gone once the PUT is answered: {:?}",
            meta_after_put
        ),
    );
    let (reached, after) = h.probe(&h.b, key).await;
    c.check(
        after.status == StatusCode::OK && after.body == "version two",
        format!(
            "the GET after the write must serve the new object, got {} {:?} (reached S3: {})",
            after.status, after.body, reached
        ),
    );
    c.finish();
}
