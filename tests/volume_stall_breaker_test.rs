//! A sustained stall of the shared cache volume trips this proxy's volume-stall breaker, after
//! which writes add no cache delay and GET and HEAD go to S3; the breaker closes once the volume
//! answers again, and the pending invalidations drain.
//!
//! The stall is each written key's metadata slot lock, held by the test, as in
//! `put_cache_delay_budget_test.rs`, whose harness this copies: real requests through
//! `HttpProxy::handle_request`, a loopback mock S3 over a plaintext upstream override, and two
//! proxies sharing one cache directory as a fleet shares a volume. The breaker's probe is held
//! failing through its test seam while the stall lasts, since a held slot lock does not stall
//! the probe's own write.
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
use s3_proxy::origin_identity::OriginVerdict;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::s3_client::S3Client;
use s3_proxy::S3ClientApi;

const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000"; // gitleaks:allow
/// The most cache work may delay a write's response (`cache::WRITE_CACHE_DELAY_BUDGET`).
const BUDGET: Duration = Duration::from_millis(1000);
/// Scheduling slack plus the mock's answer. A write held for the slot lock's retry exhaustion
/// (about 3.1 s at the default five retries) cannot finish inside `BUDGET + MARGIN`.
const MARGIN: Duration = Duration::from_millis(300);
/// How long the background stamp retry may take to land once the slot is free: one exhausted
/// attempt (about 3.1 s) plus its backoff (at most 5 s), with slack.
const LANDING: Duration = Duration::from_secs(20);
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
    /// While set, PUT responses are held.
    hold_puts: Arc<AtomicBool>,
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
        if method == hyper::Method::PUT {
            while self.hold_puts.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        let mut st = self.state.lock().unwrap();
        if method == hyper::Method::POST
            && query.split('&').any(|p| p == "delete" || p == "delete=")
        {
            // DeleteObjects, quiet: every requested key deleted, none reported.
            let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></DeleteResult>";
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .header("content-length", xml.len().to_string())
                .body(Full::new(Bytes::from(xml)))
                .unwrap());
        }
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
        if method == hyper::Method::DELETE {
            st.versions.remove(&key);
            return Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Full::new(Bytes::new()))
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
    /// This proxy's node-local pending-stamp log directory.
    _local_state: TempDir,
    /// This proxy's cache manager, for its volume-stall breaker.
    cache: Arc<CacheManager>,
}

struct Harness {
    dir: TempDir,
    _mock_shutdown: oneshot::Sender<()>,
    mock: MockS3,
    mock_host: String,
    /// Caches the object. Runs a consolidation loop until `stop_consolidation`.
    a: Proxy,
    /// Takes the writes, and trips its breaker. No consolidation loop.
    b: Proxy,
    stop_consolidation: Arc<AtomicBool>,
}

struct Answer {
    status: StatusCode,
    body: Bytes,
}

fn config_for(dir: &Path, mock_host: &str, write_cache_enabled: bool) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = dir.to_path_buf();
    config.cache.ram_cache_enabled = false;
    config.cache.write_cache_enabled = write_cache_enabled;
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
    write_cache_enabled: bool,
) -> Proxy {
    let config = config_for(dir, mock_host, write_cache_enabled);
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
    // Each proxy opens its own node-local pending-stamp log, as proxies on separate hosts do.
    // The stamp-retry table is process-wide and keyed by cache directory plus log directory, so
    // without this the proxies in this one process would share one pending set, and the other
    // proxy's window could not be observed.
    let local_state = TempDir::new().unwrap();
    cache_manager.open_stamp_backlog(local_state.path());
    cache_manager.initialize().await.expect("cache init");
    cache_manager.start_stamp_backlog_retries();
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

    let kept = Arc::clone(&cache_manager);
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
        _local_state: local_state,
        cache: kept,
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
        true,
    )
    .await;
    let b = start_proxy(dir.path(), &mock_host, None, true).await;
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

    /// A signed request through `proxy` with `extra` headers.
    async fn send_headers(
        &self,
        proxy: &Proxy,
        method: &str,
        target: &str,
        extra: &[(&str, &str)],
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
                .header("host", self.mock_host.as_str())
                .header("authorization", AUTH)
                .header("content-length", "0");
            for (name, value) in extra {
                req = req.header(*name, *value);
            }
            let req = req.body(Full::new(Bytes::new())).unwrap();
            let resp = sender.send_request(req).await.expect("proxy request");
            let status = resp.status();
            let body = resp.into_body().collect().await.expect("body").to_bytes();
            Answer { status, body }
        };
        tokio::time::timeout(STEP, fut)
            .await
            .unwrap_or_else(|_| panic!("{} {} timed out", method, target))
    }

    /// Every file under the cache directory whose name contains `object`.
    fn cache_files(&self, object: &str) -> Vec<PathBuf> {
        fn walk(dir: &Path, needle: &str, out: &mut Vec<PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, needle, out);
                } else if path
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().contains(needle))
                {
                    out.push(path);
                }
            }
        }
        let mut out = Vec::new();
        walk(self.dir.path(), object, &mut out);
        out
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

    /// `object`'s cache key.
    fn cache_key_of(&self, object: &str) -> String {
        CacheManager::generate_cache_key(&format!("/{}", object), Some(&self.mock_host))
    }

    /// Where `object`'s `.meta` lives on the shared volume.
    fn meta_path(&self, object: &str) -> PathBuf {
        self.b
            .cache
            .get_new_metadata_file_path(&self.cache_key_of(object))
    }

    /// The token in `object`'s write-order marker (`{object}.wtok` beside its `.meta`), which an
    /// acknowledged write's invalidation stamps fresh in the same slot-lock hold as the removal
    ///`None` when there is no readable marker.
    fn marker_token(&self, object: &str) -> Option<String> {
        let text = std::fs::read_to_string(self.meta_path(object).with_extension("wtok")).ok()?;
        let doc: serde_json::Value = serde_json::from_str(&text).ok()?;
        doc.get("token")?.as_str().map(str::to_string)
    }

    /// GET every one of `objects` through A until each has a `.meta` on the volume, then stop
    /// A's consolidation loop, as `cache_through_a` does for one key.
    async fn cache_all_through_a(&self, objects: &[String]) {
        use futures::StreamExt;
        for object in objects {
            self.mock.put(object, &format!("version one of {}", object));
        }
        futures::stream::iter(objects.iter())
            .for_each_concurrent(16, |object| async move {
                let (_, answer) = self.probe(&self.a, object).await;
                assert_eq!(answer.status, StatusCode::OK, "seed GET {}", object);
            })
            .await;
        let start = Instant::now();
        loop {
            let missing = objects
                .iter()
                .filter(|o| !self.meta_path(o).exists())
                .count();
            if missing == 0 {
                break;
            }
            assert!(
                start.elapsed() < LANDING,
                "fixture: {} of {} keys never got a .meta on the volume",
                missing,
                objects.len()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.stop_consolidation.store(true, Ordering::SeqCst);
        // Let a cycle already running finish.
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    /// Per key, the two outcomes `invalidate_cache_keys` produces once B has answered a write of
    /// `objects`: invalidated (the key's marker holds a token it did not hold in `before`,
    /// so a stamp landed during the request, and its `.meta` is gone), or pending (handed to the
    /// background stamp retry on this proxy). Pending is read first, before a retry could land.
    fn key_outcomes(&self, objects: &[String], before: &[Option<String>]) -> KeyOutcomes {
        let pending: Vec<bool> = objects
            .iter()
            .map(|o| {
                self.b
                    .cache
                    .write_stamp_pending_for_tests(&self.cache_key_of(o))
            })
            .collect();
        let mut out = KeyOutcomes::default();
        for ((object, was), pending) in objects.iter().zip(before).zip(pending) {
            let now = self.marker_token(object);
            let invalidated = now.is_some() && now != *was && !self.meta_path(object).exists();
            if pending {
                out.pending += 1;
            } else if invalidated {
                out.invalidated += 1;
            } else {
                out.neither.push(object.clone());
            }
        }
        out
    }
}

/// [`Harness::key_outcomes`]: how many keys were invalidated, how many are pending, and which
/// were neither.
#[derive(Default)]
struct KeyOutcomes {
    invalidated: usize,
    pending: usize,
    neither: Vec<String>,
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

/// Hold `key`'s metadata slot lock, as a peer or a stalled volume does, until the guard drops.
fn hold_slot(h: &Harness, key: &str) -> s3_proxy::metadata_lock_manager::MetadataLock {
    let cache_key = CacheManager::generate_cache_key(&format!("/{}", key), Some(&h.mock_host));
    s3_proxy::metadata_lock_manager::try_acquire_slot_lock(h.dir.path(), &cache_key)
        .expect("fixture: hold the key's slot lock")
}
/// Wait until no `.meta` for `object` remains, which only the background stamp retry can do
/// once the slot is free. Returns how long it took, or `None` past `LANDING`.
async fn wait_for_landing(h: &Harness, object: &str) -> Option<Duration> {
    let start = Instant::now();
    while start.elapsed() < LANDING {
        if h.meta_files(object).is_empty() {
            return Some(start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}
/// Wait until a GET of `key` through `proxy` serves `body`. Returns how long it took.
async fn wait_for_body(h: &Harness, proxy: &Proxy, key: &str, body: &str) -> Option<Duration> {
    let start = Instant::now();
    while start.elapsed() < LANDING {
        let (_, answer) = h.probe(proxy, key).await;
        if answer.status == StatusCode::OK && answer.body == body {
            return Some(start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}
/// One write through `proxy` while `key`'s slot is held. Returns how long the write took to be
/// answered, and its answer.
async fn write_while_stalled(
    h: &Harness,
    proxy: &Proxy,
    method: &str,
    key: &str,
    body: &str,
    signed: bool,
) -> (Duration, Answer) {
    let start = Instant::now();
    let answer = h
        .send_as(
            proxy,
            method,
            &format!("/{}", key),
            Bytes::from(body.to_string()),
            signed,
        )
        .await;
    (start.elapsed(), answer)
}
/// A slot-lock hold on each of `keys`, as a stalled volume or a peer does.
fn hold_slots(h: &Harness, keys: &[String]) -> Vec<s3_proxy::metadata_lock_manager::MetadataLock> {
    keys.iter().map(|k| hold_slot(h, k)).collect()
}

/// How long B's breaker may take to close once the volume answers: the probe's backoff has grown
/// to at most 5 s while it failed, then three probes 500 ms apart must succeed.
const CLOSE_WITHIN: Duration = Duration::from_secs(15);
/// A write after the breaker has tripped adds no cache delay: the mock S3's answer plus the
/// node-local fsync of the hand-off, far below the 1 s budget a stalled write otherwise pays.
const NO_DELAY: Duration = Duration::from_millis(300);
/// A write that discovers the stall pays nearly the whole budget.
const PAID_BUDGET: Duration = Duration::from_millis(800);

/// (a)-(d) on one proxy. The volume is stalled for B's writes by holding each written key's slot
/// lock, and B's breaker probe is held failing, as a stalled volume would answer it.
///
/// - (d) The first two stalled writes leave the breaker closed.
/// - (a) The third trips it, and later writes are answered without the cache delay.
/// - (b) While open, B forwards GET to S3 for a key cached on the volume, and after B acknowledges
///   an overwrite of that key, neither its open-state GET nor its GET after recovery serves the
///   old object.
/// - (c) Once the volume answers, the probe closes the breaker and the background retry removes
///   the old entry, after which the other proxy serves the new object too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_volume_trips_the_breaker_and_writes_stop_paying_the_cache_budget() {
    let h = harness().await;
    let seeded = "scn2-stall/seeded.bin";
    h.mock.put(seeded, "version one");
    assert_eq!(h.cache_through_a(seeded).await, Bytes::from("version one"));
    let breaker = Arc::clone(h.b.cache.volume_stall_breaker());
    breaker.set_probe_fails_for_tests(true);
    let trips_before = breaker.trips();
    let stalled: Vec<String> = (1..=5).map(|i| format!("scn2-stall/s{}.bin", i)).collect();
    let mut all = stalled.clone();
    all.push(seeded.to_string());
    let held = hold_slots(&h, &all);

    let mut c = Checks::default();
    // Discovery: three stalled writes, each paying the budget.
    for (i, key) in stalled.iter().take(3).enumerate() {
        let (elapsed, answer) = write_while_stalled(&h, &h.b, "PUT", key, "stalled", true).await;
        let open = breaker.is_open();
        eprintln!(
            "SCN2 discovery write {}: {} in {:?}; breaker open after it: {}",
            i + 1,
            answer.status,
            elapsed,
            open
        );
        c.check(
            answer.status == StatusCode::OK,
            format!(
                "discovery write {} must succeed, got {}",
                i + 1,
                answer.status
            ),
        );
        c.check(
            elapsed >= PAID_BUDGET && elapsed <= BUDGET + MARGIN,
            format!(
                "fixture: discovery write {} took {:?}; a write that discovers the stall pays \
                 about the {:?} budget",
                i + 1,
                elapsed,
                BUDGET
            ),
        );
        if i < 2 {
            c.check(
                !open,
                format!(
                    "(d) {} slow write(s) must not trip the volume-stall breaker",
                    i + 1
                ),
            );
        } else {
            c.check(
                open,
                "(a) three stalled writes in a row must trip the volume-stall breaker".to_string(),
            );
        }
    }
    // (a) After the trip: no cache delay.
    for (i, key) in stalled.iter().skip(3).enumerate() {
        let (elapsed, answer) = write_while_stalled(&h, &h.b, "PUT", key, "stalled", true).await;
        eprintln!(
            "SCN2 write after the trip {}: {} in {:?}",
            i + 1,
            answer.status,
            elapsed
        );
        c.check(
            answer.status == StatusCode::OK,
            format!(
                "write {} after the trip must succeed, got {}",
                i + 1,
                answer.status
            ),
        );
        c.check(
            elapsed <= NO_DELAY,
            format!(
                "(a) a PUT after the breaker tripped waited {:?}; with the volume stalled it must \
                 add no cache delay (at most {:?})",
                elapsed, NO_DELAY
            ),
        );
    }
    // (b) Other reads: a key cached on the volume goes to S3 while the breaker is open.
    let (read_reached, read) = h.probe(&h.b, seeded).await;
    c.check(
        read_reached && read.status == StatusCode::OK && read.body == "version one",
        format!(
            "(b) while the breaker is open a GET must be forwarded to S3, not read from the \
             stalled volume: got {} {:?} (reached S3: {})",
            read.status, read.body, read_reached
        ),
    );
    // (b) Same-key read after an overwrite acknowledged while open.
    let (put_elapsed, put) =
        write_while_stalled(&h, &h.b, "PUT", seeded, "version two", true).await;
    c.check(
        put.status == StatusCode::OK && put_elapsed <= NO_DELAY,
        format!(
            "(a) the overwrite while open must succeed without cache delay: {} in {:?}",
            put.status, put_elapsed
        ),
    );
    let meta_while_open = h.meta_files("seeded.bin");
    c.check(
        !meta_while_open.is_empty(),
        "fixture: the old .meta must still be on the volume while it is stalled".to_string(),
    );
    let (open_reached, open_read) = h.probe(&h.b, seeded).await;
    c.check(
        open_reached && open_read.body == "version two",
        format!(
            "(b) a GET through the proxy that acknowledged the overwrite must not serve the old \
             object while the breaker is open: got {} {:?} (reached S3: {})",
            open_read.status, open_read.body, open_reached
        ),
    );

    // (c) Recovery.
    drop(held);
    breaker.set_probe_fails_for_tests(false);
    let freed = Instant::now();
    let mut closed_after = None;
    while freed.elapsed() < CLOSE_WITHIN {
        if !breaker.is_open() {
            closed_after = Some(freed.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let landed = wait_for_landing(&h, "seeded.bin").await;
    let (after_reached, after) = h.probe(&h.b, seeded).await;
    let a_after = wait_for_body(&h, &h.a, seeded, "version two").await;
    eprintln!(
        "SCN2 recovery: breaker closed {:?} after the volume answered; old .meta removed {:?} \
         after that; B's GET {} {:?} (reached S3: {}); A served the new object {:?} after that; \
         trips {}",
        closed_after,
        landed,
        after.status,
        after.body,
        after_reached,
        a_after,
        breaker.trips() - trips_before
    );
    c.check(
        closed_after.is_some(),
        format!(
            "(c) the probe must close the breaker within {:?} of the volume answering",
            CLOSE_WITHIN
        ),
    );
    c.check(
        landed.is_some(),
        format!(
            "(c) the backlog must drain: the old .meta must be removed within {:?} of recovery",
            LANDING
        ),
    );
    c.check(
        after.status == StatusCode::OK && after.body == "version two",
        format!(
            "(b) after recovery the proxy that acknowledged the overwrite must not serve the old \
             object: got {} {:?} (reached S3: {})",
            after.status, after.body, after_reached
        ),
    );
    c.check(
        a_after.is_some(),
        "(c) the other proxy must serve the new object once the backlog has drained".to_string(),
    );
    c.check(
        breaker.trips() - trips_before == 1,
        format!(
            "the breaker must trip once, tripped {}",
            breaker.trips() - trips_before
        ),
    );
    c.finish();
}

/// Replace `path` with a FIFO, so the next reader blocks in `open` (as in
/// `put_router_read_bounds_test.rs`).
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
    let tmp = path.with_extension("scn2-release");
    std::fs::write(&tmp, &content).unwrap();
    std::fs::rename(&tmp, path).unwrap();
    let _ = renamed_tx.send(());
    std::thread::sleep(Duration::from_millis(300));
}

/// Tighter than `MARGIN`: two stalled steps on one path whose bounds were not summed would take
/// the marker's 250 ms on top of a full second.
const SUM_MARGIN: Duration = Duration::from_millis(150);

/// Follow-up: the budget is a sum across steps. On one PUT through B, the origin-marker read
/// (B has never read it) and the post-S3 invalidation (the key's slot is held) are both stalled.
/// The marker wait takes its 250 ms and the invalidation only what remains, so the response is
/// within the 1 s budget; a path that gave the invalidation a fresh second would take 1.25 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_stalled_steps_on_one_write_share_one_budget() {
    let h = harness().await;
    let key = "scn2-sum/object.bin";
    h.mock.put(key, "version one");
    assert_eq!(h.cache_through_a(key).await, Bytes::from("version one"));
    let marker = h
        .dir
        .path()
        .join("metadata")
        .join("scn2-sum")
        .join("_origin.json");
    let saved = std::fs::read(&marker).expect("fixture: A's GET claimed the bucket's marker");
    make_fifo(&marker);
    let held = hold_slot(&h, key);
    let (elapsed, answer) = write_while_stalled(&h, &h.b, "PUT", key, "version two", true).await;
    let (reached, after) = h.probe(&h.b, key).await;
    drop(held);
    release_fifo(&marker, saved);
    eprintln!(
        "SCN2 cross-step sum: PUT {} in {:?}; same-proxy GET {} {:?} (reached S3: {})",
        answer.status, elapsed, after.status, after.body, reached
    );
    let mut c = Checks::default();
    c.check(
        answer.status == StatusCode::OK,
        format!("the PUT must succeed, got {}", answer.status),
    );
    c.check(
        h.mock.latest(key) == Some(Bytes::from("version two")),
        format!(
            "S3 must hold the PUT's bytes, holds {:?}",
            h.mock.latest(key)
        ),
    );
    c.check(
        elapsed >= PAID_BUDGET,
        format!(
            "fixture: the PUT took {:?}, so the stalled steps did not both bite",
            elapsed
        ),
    );
    c.check(
        elapsed <= BUDGET + SUM_MARGIN,
        format!(
            "a PUT with its origin-marker read and its invalidation both stalled waited {:?}; \
             the steps share one {:?} budget",
            elapsed, BUDGET
        ),
    );
    c.check(
        reached && after.body == "version two",
        format!(
            "a GET through the proxy that acknowledged the PUT must not serve the old object: \
             got {} {:?} (reached S3: {})",
            after.status, after.body, reached
        ),
    );
    c.finish();
}

/// Further apart than the breaker's minimum spacing between counted timeouts, so each write would
/// count on its own.
const STAGGER: Duration = Duration::from_millis(700);

/// A write that starts while the breaker is open has a zero cache budget. When the probe
/// closes the breaker before S3 answers, the write's invalidation is polled once with nothing left
/// and runs out. That is the breaker's own zero budget, not a slow volume, so it must not count:
/// three such writes would re-open the breaker on a healthy volume.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_started_while_open_do_not_re_trip_the_breaker_after_it_closes() {
    let h = harness().await;
    let breaker = Arc::clone(h.b.cache.volume_stall_breaker());
    breaker.set_probe_fails_for_tests(true);
    breaker.open_for_tests();
    let trips_open = breaker.trips();
    h.mock.hold_puts.store(true, Ordering::SeqCst);
    let keys: Vec<String> = (1..=3)
        .map(|i| format!("scn3-straddle/s{}.bin", i))
        .collect();
    let put = |i: usize| {
        let h = &h;
        let key = keys[i].clone();
        async move {
            tokio::time::sleep(STAGGER * i as u32).await;
            let open_at_start = h.b.cache.volume_stalled();
            let (elapsed, answer) =
                write_while_stalled(h, &h.b, "PUT", &key, "straddle", true).await;
            (open_at_start, elapsed, answer.status)
        }
    };
    let recover = async {
        tokio::time::sleep(STAGGER * 3).await;
        let open_before = breaker.is_open();
        breaker.set_probe_fails_for_tests(false);
        let freed = Instant::now();
        let mut closed_after = None;
        while freed.elapsed() < CLOSE_WITHIN {
            if !breaker.is_open() {
                closed_after = Some(freed.elapsed());
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        h.mock.hold_puts.store(false, Ordering::SeqCst);
        (open_before, closed_after)
    };
    let (w0, w1, w2, (open_before, closed_after)) = tokio::join!(put(0), put(1), put(2), recover);
    // The answered writes' invalidations ran on the response path; give a stray report a moment.
    tokio::time::sleep(Duration::from_millis(300)).await;
    eprintln!(
        "SCN3 F1: writes (open at start, elapsed, status) {:?} {:?} {:?}; breaker open before \
         recovery {}, closed {:?} after; trips since the forced open {}; open now {}",
        w0,
        w1,
        w2,
        open_before,
        closed_after,
        breaker.trips() - trips_open,
        breaker.is_open()
    );
    let mut c = Checks::default();
    for (i, w) in [w0, w1, w2].iter().enumerate() {
        c.check(
            w.0,
            format!(
                "fixture: write {} must start while the breaker is open",
                i + 1
            ),
        );
        c.check(
            w.2 == StatusCode::OK,
            format!("write {} must succeed, got {}", i + 1, w.2),
        );
    }
    c.check(
        open_before && closed_after.is_some(),
        format!(
            "fixture: the breaker must be open until the probe is released and close within \
             {:?} after (open before: {}, closed after: {:?})",
            CLOSE_WITHIN, open_before, closed_after
        ),
    );
    c.check(
        breaker.trips() == trips_open && !breaker.is_open(),
        format!(
            "F1: writes that started while the breaker was open and were answered after it \
             closed re-tripped it on a healthy volume: {} trip(s), open now {}",
            breaker.trips() - trips_open,
            breaker.is_open()
        ),
    );
    c.finish();
}

/// One volume pause just over the 1 s budget, holding several writes that started
/// together, is one event. Each write pays the budget, and the breaker stays closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_pause_across_concurrent_writes_does_not_trip_the_breaker() {
    let h = harness().await;
    let breaker = Arc::clone(h.b.cache.volume_stall_breaker());
    breaker.set_probe_fails_for_tests(true);
    let trips_before = breaker.trips();
    let keys: Vec<String> = (1..=6).map(|i| format!("scn3-pause/p{}.bin", i)).collect();
    let held = hold_slots(&h, &keys);
    let put = |i: usize| {
        let h = &h;
        let key = keys[i].clone();
        async move {
            let (elapsed, answer) = write_while_stalled(h, &h.b, "PUT", &key, "pause", true).await;
            (elapsed, answer.status)
        }
    };
    let pause = async move {
        tokio::time::sleep(Duration::from_millis(1100)).await;
        drop(held);
    };
    let (w0, w1, w2, w3, w4, w5, ()) =
        tokio::join!(put(0), put(1), put(2), put(3), put(4), put(5), pause);
    let writes = [w0, w1, w2, w3, w4, w5];
    eprintln!(
        "SCN3 F4: six concurrent writes behind one 1.1 s pause: {:?}; trips {}; open {}",
        writes,
        breaker.trips() - trips_before,
        breaker.is_open()
    );
    let mut c = Checks::default();
    for (i, (elapsed, status)) in writes.iter().enumerate() {
        c.check(
            *status == StatusCode::OK,
            format!("write {} must succeed, got {}", i + 1, status),
        );
        c.check(
            *elapsed >= PAID_BUDGET && *elapsed <= BUDGET + MARGIN,
            format!(
                "fixture: write {} took {:?}; each write behind the pause pays about the {:?} \
                 budget",
                i + 1,
                elapsed,
                BUDGET
            ),
        );
    }
    c.check(
        breaker.trips() == trips_before && !breaker.is_open(),
        format!(
            "F4: one pause across six concurrent writes tripped the volume-stall breaker: {} \
             trip(s), open {}",
            breaker.trips() - trips_before,
            breaker.is_open()
        ),
    );
    c.finish();
}

/// How far apart the writes in the staggered-entry test enter: more than the breaker's 500 ms
/// spacing, so writes counted by when they entered would each count.
const ENTRY_STAGGER: Duration = Duration::from_millis(600);

/// Writes that entered at different times, 600 ms apart, but all reach
/// their cache wait after S3 answers them together, and all stall behind one 1.2 s pause. That
/// is one event, so the breaker stays closed. The mock holds every PUT's answer until all three
/// have entered; the slot holds start only then, so nothing stalls before S3 answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_pause_across_writes_that_entered_apart_does_not_trip_the_breaker() {
    let h = harness().await;
    let breaker = Arc::clone(h.b.cache.volume_stall_breaker());
    breaker.set_probe_fails_for_tests(true);
    let trips_before = breaker.trips();
    h.mock.hold_puts.store(true, Ordering::SeqCst);
    let keys: Vec<String> = (1..=3)
        .map(|i| format!("scn4-staggered/s{}.bin", i))
        .collect();
    let put = |i: usize| {
        let h = &h;
        let key = keys[i].clone();
        async move {
            tokio::time::sleep(ENTRY_STAGGER * i as u32).await;
            let (elapsed, answer) =
                write_while_stalled(h, &h.b, "PUT", &key, "staggered", true).await;
            (elapsed, answer.status)
        }
    };
    let pause = async {
        tokio::time::sleep(ENTRY_STAGGER * 2 + Duration::from_millis(200)).await;
        let held = hold_slots(&h, &keys);
        h.mock.hold_puts.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        drop(held);
    };
    let (w0, w1, w2, ()) = tokio::join!(put(0), put(1), put(2), pause);
    let writes = [w0, w1, w2];
    eprintln!(
        "SCN4 F4: three writes entered {:?} apart, one 1.2 s pause after S3 answered them: \
         {:?}; trips {}; open {}",
        ENTRY_STAGGER,
        writes,
        breaker.trips() - trips_before,
        breaker.is_open()
    );
    let mut c = Checks::default();
    // S3 answers every write at about `2 * ENTRY_STAGGER + 200 ms` after the first entered, so
    // write `i` waited `that - i * ENTRY_STAGGER` for S3, then about the budget behind the pause.
    let answered_at = ENTRY_STAGGER * 2 + Duration::from_millis(200);
    for (i, (elapsed, status)) in writes.iter().enumerate() {
        c.check(
            *status == StatusCode::OK,
            format!("write {} must succeed, got {}", i + 1, status),
        );
        let s3_wait = answered_at - ENTRY_STAGGER * i as u32;
        c.check(
            *elapsed >= s3_wait + PAID_BUDGET,
            format!(
                "fixture: write {} took {:?}; after about {:?} waiting for S3 it must pay about \
                 the {:?} budget behind the pause, or nothing timed out and the breaker verdict \
                 measures nothing",
                i + 1,
                elapsed,
                s3_wait,
                BUDGET
            ),
        );
    }
    c.check(
        breaker.trips() == trips_before && !breaker.is_open(),
        format!(
            "F4: one pause across three writes that entered {:?} apart tripped the volume-stall \
             breaker: {} trip(s), open {}",
            ENTRY_STAGGER,
            breaker.trips() - trips_before,
            breaker.is_open()
        ),
    );
    c.finish();
}

/// A `Cache-Control: no-cache` GET returns before the main path's breaker guard, and
/// its arm reads the origin marker and fills the cache. While the breaker is open it must go to
/// S3 uncached: nothing written to the volume for the key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_no_cache_get_while_open_is_forwarded_without_a_fill() {
    let h = harness().await;
    let key = "scn3-nocache/object.bin";
    h.mock.put(key, "fresh bytes");
    let breaker = Arc::clone(h.b.cache.volume_stall_breaker());
    breaker.set_probe_fails_for_tests(true);
    breaker.open_for_tests();
    let before = h.mock.gets(key);
    let started = Instant::now();
    let answer = h
        .send_headers(
            &h.b,
            "GET",
            &format!("/{}", key),
            &[("cache-control", "no-cache")],
        )
        .await;
    let elapsed = started.elapsed();
    let reached = h.mock.gets(key) > before;
    // A fill is written after the body is streamed; give it time to land.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let written = h.cache_files("object.bin");
    eprintln!(
        "SCN3 F2: no-cache GET while open: {} {:?} in {:?} (reached S3: {}); cache files for \
         the key: {:?}; open {}",
        answer.status,
        answer.body,
        elapsed,
        reached,
        written,
        breaker.is_open()
    );
    let mut c = Checks::default();
    c.check(
        breaker.is_open(),
        "fixture: the breaker must stay open for the whole check".to_string(),
    );
    c.check(
        reached && answer.status == StatusCode::OK && answer.body == "fresh bytes",
        format!(
            "the no-cache GET must be answered by S3: {} {:?} (reached S3: {})",
            answer.status, answer.body, reached
        ),
    );
    c.check(
        written.is_empty(),
        format!(
            "F2: a no-cache GET while the breaker is open wrote to the stalled volume: {:?}",
            written
        ),
    );
    c.finish();
}

/// The metadata RAM tier's default `refresh_interval`, which the harness keeps.
const REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// While the breaker is open, a plain HEAD whose metadata this proxy holds in RAM,
/// loaded within `refresh_interval`, is answered from RAM with no volume read. Once the RAM copy
/// is older than that, the HEAD goes to S3 like every other read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_head_ram_hit_is_served_while_open_until_refresh_interval() {
    let h = harness().await;
    let key = "scn3-head/object.bin";
    h.mock.put(key, "head body");
    let target = format!("/{}", key);
    let cache_key = CacheManager::generate_cache_key(&target, Some(&h.mock_host));
    let (_, first) = h.probe(&h.a, key).await;
    assert_eq!(first.status, StatusCode::OK, "seed GET");
    // Warm B: HEAD until one is answered without S3 from a .meta that carries ranges, which
    // publishes it to B's RAM metadata tier.
    let mut loaded_at = None;
    for _ in 0..100 {
        let before = h.mock.gets(key);
        let answer = h.send(&h.b, "HEAD", &target, Bytes::new()).await;
        assert_eq!(answer.status, StatusCode::OK, "warm-up HEAD");
        if h.mock.gets(key) == before
            && h.b
                .cache
                .get_metadata_cache()
                .get(&cache_key)
                .await
                .is_some()
        {
            loaded_at = Some(Instant::now());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let loaded_at = loaded_at.expect("fixture: B never held the key's metadata in RAM");
    let breaker = Arc::clone(h.b.cache.volume_stall_breaker());
    breaker.set_probe_fails_for_tests(true);
    breaker.open_for_tests();

    let before = h.mock.gets(key);
    let ram_head = h.send(&h.b, "HEAD", &target, Bytes::new()).await;
    let ram_reached = h.mock.gets(key) > before;
    let ram_age = loaded_at.elapsed();

    tokio::time::sleep(
        (REFRESH_INTERVAL + Duration::from_millis(300)).saturating_sub(loaded_at.elapsed()),
    )
    .await;
    let before = h.mock.gets(key);
    let late_head = h.send(&h.b, "HEAD", &target, Bytes::new()).await;
    let late_reached = h.mock.gets(key) > before;
    let late_age = loaded_at.elapsed();
    eprintln!(
        "SCN3 F3: HEAD while open, RAM copy {:?} old: {} (reached S3: {}); RAM copy {:?} old: {} \
         (reached S3: {}); open {}",
        ram_age,
        ram_head.status,
        ram_reached,
        late_age,
        late_head.status,
        late_reached,
        breaker.is_open()
    );
    let mut c = Checks::default();
    c.check(
        breaker.is_open(),
        "fixture: the breaker must stay open for the whole check".to_string(),
    );
    c.check(
        ram_head.status == StatusCode::OK && !ram_reached && ram_age < REFRESH_INTERVAL,
        format!(
            "F3: a plain HEAD whose metadata B holds in RAM ({:?} old) must be answered from RAM \
             while the breaker is open: got {} (reached S3: {})",
            ram_age, ram_head.status, ram_reached
        ),
    );
    c.check(
        late_head.status == StatusCode::OK && late_reached,
        format!(
            "a HEAD whose RAM copy is past refresh_interval ({:?} old) must go to S3 while the \
             breaker is open: got {} (reached S3: {})",
            late_age, late_head.status, late_reached
        ),
    );
    c.finish();
}

/// While the breaker is open every key of a DeleteObjects goes to the
/// background stamp retry, each with a node-local pending record fsynced before the response.
/// One fsync per key put a 1,000-key DeleteObjects past the budget; the keys are now handed over
/// in one append and one fsync, so the response adds no cache delay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_thousand_key_delete_objects_while_open_adds_no_cache_delay() {
    let h = harness().await;
    let breaker = Arc::clone(h.b.cache.volume_stall_breaker());
    breaker.set_probe_fails_for_tests(true);
    breaker.open_for_tests();
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Quiet>true</Quiet>",
    );
    for i in 0..1000 {
        xml.push_str(&format!("<Object><Key>k{:04}.bin</Key></Object>", i));
    }
    xml.push_str("</Delete>");
    let started = Instant::now();
    let answer = h
        .send(&h.b, "POST", "/scn3-bulk?delete", Bytes::from(xml))
        .await;
    let elapsed = started.elapsed();
    let probe_key = CacheManager::generate_cache_key("/scn3-bulk/k0999.bin", Some(&h.mock_host));
    let pending = h.b.cache.write_stamp_pending_for_tests(&probe_key);
    eprintln!(
        "SCN3 DeleteObjects of 1000 keys while open: {} in {:?}; last key pending on B: {}; \
         open {}",
        answer.status,
        elapsed,
        pending,
        breaker.is_open()
    );
    let mut c = Checks::default();
    c.check(
        answer.status == StatusCode::OK,
        format!(
            "the DeleteObjects must succeed, got {} {:?}",
            answer.status, answer.body
        ),
    );
    c.check(
        breaker.is_open(),
        "fixture: the breaker must stay open for the whole request".to_string(),
    );
    c.check(
        pending,
        "every handed-off key must be pending on this proxy once the response returns".to_string(),
    );
    c.check(
        elapsed <= NO_DELAY,
        format!(
            "a 1000-key DeleteObjects while the breaker is open took {:?}; handing its keys to \
             the retry must add no cache delay (at most {:?})",
            elapsed, NO_DELAY
        ),
    );
    c.finish();
}

/// A 1,000-key DeleteObjects on a stalled volume, with the breaker closed so every key
/// starts out paying the cache wait. Every key's slot is held, as a stalled volume holds it. The
/// response must come within the DeleteObjects' 3 s cache-delay budget plus `MARGIN` (the mock's
/// answer and the hand-off's one node-local fsync); every key must be either
/// invalidated or pending-stamped on this proxy once it returns, which are the two outcomes
/// `invalidate_cache_keys` branches on per key (a later change that invalidates some keys
/// inside the budget is correct behaviour, so "all pending" is not the contract); and a GET of a
/// key this proxy had cached must go to S3.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_thousand_key_delete_objects_on_a_stalled_volume_is_answered_within_three_seconds() {
    let h = harness().await;
    let seeded = "scn5-bulk/k0000.bin";
    let objects: Vec<String> = (0..1000)
        .map(|i| format!("scn5-bulk/k{:04}.bin", i))
        .collect();
    // Every key the per-key check covers is cached, so a key the
    // request drops is still cached afterwards with its marker unchanged.
    h.cache_all_through_a(&objects).await;
    // B reads the key from the volume too, so its own GET would be a hit without the guard.
    let (_, warm) = h.probe(&h.b, seeded).await;
    assert_eq!(warm.status, StatusCode::OK, "fixture: B's warm GET");
    let before: Vec<Option<String>> = objects.iter().map(|o| h.marker_token(o)).collect();
    // Keys share slot lock files, so hold each distinct slot once: a key whose slot is already
    // held here is stalled all the same.
    let mut slots = Vec::new();
    for object in &objects {
        let key = CacheManager::generate_cache_key(&format!("/{}", object), Some(&h.mock_host));
        if let Ok(slot) = s3_proxy::metadata_lock_manager::try_acquire_slot_lock(h.dir.path(), &key)
        {
            slots.push(slot);
        }
    }
    let breaker = Arc::clone(h.b.cache.volume_stall_breaker());
    let open_before = breaker.is_open();
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Quiet>true</Quiet>",
    );
    for i in 0..1000 {
        xml.push_str(&format!("<Object><Key>k{:04}.bin</Key></Object>", i));
    }
    xml.push_str("</Delete>");
    let started = Instant::now();
    let answer = h
        .send(&h.b, "POST", "/scn5-bulk?delete", Bytes::from(xml))
        .await;
    let elapsed = started.elapsed();
    // Per key, the two outcomes `invalidate_cache_keys` produces: the invalidation finished
    // inside the batch deadline, so a fresh write-order token is stamped and the `.meta` is gone;
    // or the key was handed to the background stamp retry (`hand_invalidations_to_retry`, or
    // `spawn_write_stamp_retry` on a failure), so a write stamp is pending on this proxy. Either
    // is correct. A key with neither is the state the bound exists to rule out: an older
    // in-flight store of it could still publish.
    let outcome = h.key_outcomes(&objects, &before);
    let (reached, get) = h.probe(&h.b, seeded).await;
    let held = slots.len();
    drop(slots);
    eprintln!(
        "SCN5 DeleteObjects of 1000 keys on a stalled volume ({} distinct slots held): {} in \
         {:?}; pending on B: {}; invalidated: {}; neither: {}; B's GET of a cached key reached \
         S3: {}; breaker open before {} after {}",
        held,
        answer.status,
        elapsed,
        outcome.pending,
        outcome.invalidated,
        outcome.neither.len(),
        reached,
        open_before,
        breaker.is_open()
    );
    let mut c = Checks::default();
    c.check(
        !open_before,
        "fixture: the breaker must be closed when the DeleteObjects starts".to_string(),
    );
    c.check(
        answer.status == StatusCode::OK,
        format!(
            "the DeleteObjects must succeed, got {} {:?}",
            answer.status, answer.body
        ),
    );
    c.check(
        elapsed <= DELETE_OBJECTS_BUDGET + MARGIN,
        format!(
            "a 1000-key DeleteObjects on a stalled volume took {:?}; its cache work after S3's \
             answer may take at most {:?}, plus {:?} for the mock and the hand-off",
            elapsed, DELETE_OBJECTS_BUDGET, MARGIN
        ),
    );
    c.check(
        outcome.neither.is_empty(),
        format!(
            "{} of 1000 keys were neither invalidated (fresh write-order token, no .meta) nor \
             pending on this proxy when the response returned, first {:?} ({} pending, {} \
             invalidated)",
            outcome.neither.len(),
            outcome.neither.first(),
            outcome.pending,
            outcome.invalidated
        ),
    );
    c.check(
        reached && get.status == StatusCode::OK,
        format!(
            "a GET on the proxy that took the DeleteObjects must go to S3 for a key it had \
             cached: got {} (reached S3: {})",
            get.status, reached
        ),
    );
    c.finish();
}

/// `http_proxy::DELETE_OBJECTS_CACHE_DELAY_BUDGET`: the flat budget a DeleteObjects' cache
/// work after S3's answer shares, the origin-marker read included.
const DELETE_OBJECTS_BUDGET: Duration = Duration::from_secs(3);

/// A DeleteObjects body (quiet) naming each of `objects` (`bucket/key`) by its key.
fn delete_objects_body(objects: &[String]) -> Bytes {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Quiet>true</Quiet>",
    );
    for object in objects {
        let key = object.split_once('/').map(|(_, k)| k).unwrap_or(object);
        xml.push_str(&format!("<Object><Key>{}</Key></Object>", key));
    }
    xml.push_str("</Delete>");
    Bytes::from(xml)
}

/// A hold on each distinct slot lock of `objects`. Keys share slot lock files, so a key
/// whose slot is already held here is stalled all the same.
fn hold_distinct_slots(
    h: &Harness,
    objects: &[String],
) -> Vec<s3_proxy::metadata_lock_manager::MetadataLock> {
    objects
        .iter()
        .filter_map(|o| {
            s3_proxy::metadata_lock_manager::try_acquire_slot_lock(h.dir.path(), &h.cache_key_of(o))
                .ok()
        })
        .collect()
}

/// When the held slots are released in [`a_large_delete_objects_on_a_healthy_volume_finishes_every_key_inline`].
/// A key's slot-lock attempts come at about 0, 0.1, 0.3, 0.7 and 1.5 s after its first, each
/// backoff jittered by up to 25 %: the fourth is at most 0.875 s in, the fifth at least 1.125 s.
/// Releasing between the two means each of the first 16 keys takes its lock on its fifth attempt,
/// so the batch cannot finish within 1 s and its first wave finishes by about 1.9 s.
const RELEASE_AT: Duration = Duration::from_millis(1000);

/// (the benefit): a 1,000-key DeleteObjects on a volume that is slow but makes progress.
/// Every key's slot is held when the request arrives and released at [`RELEASE_AT`], so the batch
/// takes longer than the 1 s budget every other write has and less than the DeleteObjects' 3 s.
/// Every key must be invalidated before the response (a fresh write-order token, no `.meta`),
/// none handed to the stamp retry, and the response must come within 3 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_delete_objects_on_a_healthy_volume_finishes_every_key_inline() {
    let h = harness().await;
    let objects: Vec<String> = (0..1000)
        .map(|i| format!("scn6-inline/k{:04}.bin", i))
        .collect();
    h.cache_all_through_a(&objects).await;
    // B reads the bucket's origin marker now, so the DeleteObjects' marker read is in memory.
    let (_, warm) = h.probe(&h.b, &objects[0]).await;
    assert_eq!(warm.status, StatusCode::OK, "fixture: B's warm GET");
    let before: Vec<Option<String>> = objects.iter().map(|o| h.marker_token(o)).collect();
    let slots = hold_distinct_slots(&h, &objects);
    let held = slots.len();
    let breaker = Arc::clone(h.b.cache.volume_stall_breaker());
    let open_before = breaker.is_open();
    let started = Instant::now();
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(RELEASE_AT);
        drop(slots);
        started.elapsed()
    });
    let answer = h
        .send(
            &h.b,
            "POST",
            "/scn6-inline?delete",
            delete_objects_body(&objects),
        )
        .await;
    let elapsed = started.elapsed();
    let outcome = h.key_outcomes(&objects, &before);
    let released = releaser.join().expect("fixture: the releasing thread");
    eprintln!(
        "SCN6 DeleteObjects of 1000 keys, {} distinct slots held until {:?}: {} in {:?}; \
         invalidated: {}; pending on B: {}; neither: {}; breaker open before {} after {}",
        held,
        released,
        answer.status,
        elapsed,
        outcome.invalidated,
        outcome.pending,
        outcome.neither.len(),
        open_before,
        breaker.is_open()
    );
    let mut c = Checks::default();
    c.check(
        !open_before,
        "fixture: the breaker must be closed when the DeleteObjects starts".to_string(),
    );
    c.check(
        answer.status == StatusCode::OK,
        format!(
            "the DeleteObjects must succeed, got {} {:?}",
            answer.status, answer.body
        ),
    );
    c.check(
        elapsed > BUDGET,
        format!(
            "fixture: the DeleteObjects took {:?}, so its batch was not slower than the {:?} \
             every other write has",
            elapsed, BUDGET
        ),
    );
    c.check(
        elapsed <= DELETE_OBJECTS_BUDGET,
        format!(
            "a 1000-key DeleteObjects on a volume that makes progress took {:?}; it must be \
             answered within {:?}",
            elapsed, DELETE_OBJECTS_BUDGET
        ),
    );
    c.check(
        outcome.invalidated == objects.len() && outcome.pending == 0,
        format!(
            "{} of 1000 keys were invalidated before the response and {} were left pending on \
             the stamp retry ({} neither, first {:?}); a batch that finishes within the \
             DeleteObjects' {:?} must invalidate every key inline",
            outcome.invalidated,
            outcome.pending,
            outcome.neither.len(),
            outcome.neither.first(),
            DELETE_OBJECTS_BUDGET
        ),
    );
    c.finish();
}

use s3_proxy::volume_stall::{TRIP_AFTER, TRIP_WINDOW};
/// A DeleteObjects that pays its whole budget on a stalled volume takes at least this long.
const PAID_DELETE_OBJECTS_BUDGET: Duration = Duration::from_millis(2700);

/// The volume-stall breaker still opens on DeleteObjects-only traffic. Three serial
/// DeleteObjects on a stalled volume, each answered at about the 3 s budget, are counted at the
/// end of each batch, so the third count comes `2 x (3 s + overhead)` after the first, inside the
/// 10 s window while the per-request overhead is at most 2 s. A DeleteObjects sent once the
/// breaker is open adds no cache delay. A 5 s budget would put the third count past the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serial_stalled_delete_objects_trip_the_breaker() {
    let h = harness().await;
    let breaker = Arc::clone(h.b.cache.volume_stall_breaker());
    breaker.set_probe_fails_for_tests(true);
    let trips_before = breaker.trips();
    let open_before = breaker.is_open();
    let rounds: Vec<Vec<String>> = (0..TRIP_AFTER)
        .map(|r| {
            (0..20)
                .map(|i| format!("scn6-trip/r{}k{:02}.bin", r, i))
                .collect()
        })
        .collect();
    let all: Vec<String> = rounds.iter().flatten().cloned().collect();
    let held = hold_distinct_slots(&h, &all);
    let mut c = Checks::default();
    c.check(
        !open_before,
        "fixture: the breaker must be closed before the first DeleteObjects".to_string(),
    );
    let mut answered_at: Option<Instant> = None;
    let mut overheads = Vec::new();
    let mut elapsed_all = Vec::new();
    let mut open_after = Vec::new();
    for (r, objects) in rounds.iter().enumerate() {
        let sent = Instant::now();
        let turnaround = answered_at.map(|a| sent.duration_since(a));
        let answer = h
            .send(
                &h.b,
                "POST",
                "/scn6-trip?delete",
                delete_objects_body(objects),
            )
            .await;
        let elapsed = sent.elapsed();
        answered_at = Some(Instant::now());
        let open = breaker.is_open();
        // The time between one count and the next beyond the 3 s window: this request's time
        // outside its batch window plus the client's turnaround since the previous answer.
        if let Some(turnaround) = turnaround {
            overheads.push(turnaround + elapsed.saturating_sub(DELETE_OBJECTS_BUDGET));
        }
        eprintln!(
            "SCN6 serial stalled DeleteObjects {}: {} in {:?}; breaker open after it: {}",
            r + 1,
            answer.status,
            elapsed,
            open
        );
        c.check(
            answer.status == StatusCode::OK,
            format!(
                "DeleteObjects {} must succeed, got {} {:?}",
                r + 1,
                answer.status,
                answer.body
            ),
        );
        c.check(
            elapsed >= PAID_DELETE_OBJECTS_BUDGET,
            format!(
                "fixture: DeleteObjects {} took {:?}; on a stalled volume it pays about its \
                 budget",
                r + 1,
                elapsed
            ),
        );
        elapsed_all.push(elapsed);
        open_after.push(open);
    }
    let largest_overhead = (TRIP_WINDOW / (TRIP_AFTER - 1)).saturating_sub(DELETE_OBJECTS_BUDGET);
    eprintln!(
        "SCN6 serial stalled DeleteObjects: elapsed {:?}; per-request overhead between counts \
         {:?}; the largest at which three still trip a {:?} budget is {:?}; open after each \
         {:?}; trips {}",
        elapsed_all,
        overheads,
        DELETE_OBJECTS_BUDGET,
        largest_overhead,
        open_after,
        breaker.trips() - trips_before
    );
    c.check(
        !open_after[0] && !open_after[1],
        format!(
            "fewer than {} stalled DeleteObjects must not trip the breaker: open after each {:?}",
            TRIP_AFTER, open_after
        ),
    );
    let tripped = open_after[2] && breaker.trips() - trips_before == 1;
    c.check(
        tripped,
        format!(
            "{} serial DeleteObjects on a stalled volume, answered in {:?} with {:?} of \
             overhead between counts, must trip the volume-stall breaker within its {:?} \
             window: open after each {:?}, trips {}",
            TRIP_AFTER,
            elapsed_all,
            overheads,
            TRIP_WINDOW,
            open_after,
            breaker.trips() - trips_before
        ),
    );
    if tripped {
        let objects: Vec<String> = (0..1000)
            .map(|i| format!("scn6-trip/open{:04}.bin", i))
            .collect();
        let sent = Instant::now();
        let answer = h
            .send(
                &h.b,
                "POST",
                "/scn6-trip?delete",
                delete_objects_body(&objects),
            )
            .await;
        let elapsed = sent.elapsed();
        let pending =
            h.b.cache
                .write_stamp_pending_for_tests(&h.cache_key_of(&objects[999]));
        eprintln!(
            "SCN6 DeleteObjects of 1000 keys once the breaker opened: {} in {:?}; last key \
             pending on B: {}",
            answer.status, elapsed, pending
        );
        c.check(
            answer.status == StatusCode::OK && pending,
            format!(
                "a DeleteObjects while the breaker is open must succeed with its keys pending: \
                 {} (last key pending: {})",
                answer.status, pending
            ),
        );
        c.check(
            elapsed <= NO_DELAY,
            format!(
                "a 1000-key DeleteObjects while the breaker is open took {:?}; it must add no \
                 cache delay (at most {:?})",
                elapsed, NO_DELAY
            ),
        );
    }
    drop(held);
    c.finish();
}

/// How long the post-response probe of B's origin marker waits. Far below the 250 ms cap a write
/// waits, and far above a memo lookup.
const MARKER_PROBE_BOUND: Duration = Duration::from_millis(50);

/// (the expensive shape): a DeleteObjects whose origin-marker read is stalled (B has never
/// read the bucket's marker, and the marker is a FIFO no one writes) and whose every key's slot
/// is held. The marker read takes its 250 ms inside the DeleteObjects' 3 s budget and the batch
/// only what remains, so the response is within the budget plus `SUM_MARGIN`; a marker read
/// outside the budget would take 3.25 s.
///
/// Held slots alone also give about 3 s, so the timing cannot show the marker read stalled. The
/// precondition reads the state `origin_verdict_within` branches on: after the response, while
/// the FIFO is still unwritten, a short `origin_verdict_within` on B must be `Unresolved`. B's
/// marker memo is filled only when its claim finishes, so `Unresolved` then means B's claim was
/// still parked on the FIFO, and the DeleteObjects' own read waited its full cap and timed out.
/// A readable marker would have been memoised during the request and answer `Matches`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_objects_with_a_stalled_marker_read_and_held_slots_is_answered_within_three_seconds(
) {
    let h = harness().await;
    let objects: Vec<String> = (0..200)
        .map(|i| format!("scn6-marker/k{:03}.bin", i))
        .collect();
    h.cache_all_through_a(&objects).await;
    let marker = h
        .dir
        .path()
        .join("metadata")
        .join("scn6-marker")
        .join("_origin.json");
    let saved = std::fs::read(&marker).expect("fixture: A's GETs claimed the bucket's marker");
    let before: Vec<Option<String>> = objects.iter().map(|o| h.marker_token(o)).collect();
    make_fifo(&marker);
    let held = hold_distinct_slots(&h, &objects);
    let breaker = Arc::clone(h.b.cache.volume_stall_breaker());
    let open_before = breaker.is_open();
    let started = Instant::now();
    let answer = h
        .send(
            &h.b,
            "POST",
            "/scn6-marker?delete",
            delete_objects_body(&objects),
        )
        .await;
    let elapsed = started.elapsed();
    let marker_after =
        h.b.cache
            .origin_verdict_within(
                &h.cache_key_of(&objects[0]),
                &h.mock_host,
                MARKER_PROBE_BOUND,
            )
            .await;
    let outcome = h.key_outcomes(&objects, &before);
    drop(held);
    release_fifo(&marker, saved);
    eprintln!(
        "SCN6 DeleteObjects of 200 keys with the marker read stalled and every slot held: {} \
         in {:?}; B's marker verdict after the response: {:?}; pending on B: {}; \
         invalidated: {}; neither: {}",
        answer.status,
        elapsed,
        marker_after,
        outcome.pending,
        outcome.invalidated,
        outcome.neither.len()
    );
    let mut c = Checks::default();
    c.check(
        !open_before,
        "fixture: the breaker must be closed when the DeleteObjects starts".to_string(),
    );
    c.check(
        answer.status == StatusCode::OK,
        format!(
            "the DeleteObjects must succeed, got {} {:?}",
            answer.status, answer.body
        ),
    );
    c.check(
        elapsed >= PAID_DELETE_OBJECTS_BUDGET,
        format!(
            "fixture: the DeleteObjects took {:?}, so the stalled steps did not both bite",
            elapsed
        ),
    );
    c.check(
        marker_after == OriginVerdict::Unresolved,
        format!(
            "fixture: B's origin-marker read must still be stalled when the DeleteObjects \
             returns, so its own read timed out inside the budget; a {:?} probe on B answered \
             {:?}, so the marker read did not stall and the timing proves nothing about it",
            MARKER_PROBE_BOUND, marker_after
        ),
    );
    c.check(
        elapsed <= DELETE_OBJECTS_BUDGET + SUM_MARGIN,
        format!(
            "a DeleteObjects with its origin-marker read and its invalidations both stalled \
             waited {:?}; the marker read is inside the {:?} budget",
            elapsed, DELETE_OBJECTS_BUDGET
        ),
    );
    c.check(
        outcome.neither.is_empty(),
        format!(
            "{} of 200 keys were neither invalidated nor pending on this proxy when the \
             response returned, first {:?}",
            outcome.neither.len(),
            outcome.neither.first()
        ),
    );
    c.finish();
}
