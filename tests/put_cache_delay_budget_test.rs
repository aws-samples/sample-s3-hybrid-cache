//! Cache work may delay a signed or unsigned write's response by at most 1,000 ms in total,
//! even with the cache volume stalled, and a write is never failed for cache reasons.
//!
//! The stall here is the key's metadata slot lock, held by the test for the whole write: every
//! response-path invalidation takes that lock, so on the code before the change the response waited for
//! the lock's retry exhaustion (about 3.1 s at the default five retries) and then handed the
//! invalidation to the background stamp retry. The old object's `.meta` stays on the volume
//! until that retry lands.
//!
//! Two properties are asserted, each red on the code before the change:
//!
//! - The same proxy never serves the old object after acknowledging the write: while it holds a
//!   pending write-order stamp for the key, its GET forwards to S3.
//! - The response arrives within the budget, and the background retry later removes the old
//!   entry, after which the other proxy serves the new object too.
//!
//! Harness as in `put_router_read_bounds_test.rs`: real requests through
//! `HttpProxy::handle_request`, a loopback mock S3 over a plaintext upstream override, and
//! proxies sharing one cache directory as a fleet shares a volume.
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
    /// As B, with `write_cache_enabled: false`, so its writes take the uncached-forward arms.
    c: Proxy,
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
    let c = start_proxy(dir.path(), &mock_host, None, false).await;
    Harness {
        dir,
        _mock_shutdown: mock_shutdown,
        mock,
        mock_host,
        a,
        b,
        c,
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
/// Part A. A write whose invalidation cannot take the slot lock is acknowledged with the old
/// `.meta` still on the volume and a pending write-order stamp on this proxy. A GET through the
/// same proxy must not serve the old object from that entry; once the slot is free the retry
/// removes the entry, and the other proxy then serves the new object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pending_stamp_keeps_this_proxy_from_serving_the_old_object() {
    let h = harness().await;
    let key = "scn2-same/object.bin";
    h.mock.put(key, "version one");
    assert_eq!(h.cache_through_a(key).await, Bytes::from("version one"));
    let held = hold_slot(&h, key);
    let (put_elapsed, put) = write_while_stalled(&h, &h.b, "PUT", key, "version two", true).await;
    let meta_after_put = h.meta_files("object.bin");
    let (reached, after) = h.probe(&h.b, key).await;
    // A, the other proxy, before the retry can land: the backlog's window.
    let (_, a_before) = h.probe(&h.a, key).await;
    drop(held);
    let landed = wait_for_landing(&h, "object.bin").await;
    let a_after = wait_for_body(&h, &h.a, key, "version two").await;
    eprintln!(
        "SCN2 same-proxy: PUT answered in {:?}; B's GET {} {:?} (reached S3: {}); A before \
         landing {:?}; old .meta removed {:?} after the slot was freed; A served the new object \
         {:?} after that",
        put_elapsed, after.status, after.body, reached, a_before.body, landed, a_after
    );
    let mut c = Checks::default();
    c.check(
        put.status == StatusCode::OK,
        format!("the PUT must succeed, got {}", put.status),
    );
    c.check(
        h.mock.latest(key) == Some(Bytes::from("version two")),
        format!(
            "S3 must hold the PUT's bytes, holds {:?}",
            h.mock.latest(key)
        ),
    );
    c.check(
        a_before.body == "version one",
        format!(
            "fixture: proxy A must not share B's pending-stamp state (each opens its own \
             node-local log), so before the retry lands it serves the old entry; got {:?}",
            a_before.body
        ),
    );
    c.check(
        !meta_after_put.is_empty(),
        "fixture: the held slot did not keep the old .meta on the volume, so nothing was stalled"
            .to_string(),
    );
    c.check(
        after.status == StatusCode::OK && after.body == "version two" && reached,
        format!(
            "a GET through the proxy that acknowledged the write must not serve the old object: \
             got {} {:?} (reached S3: {})",
            after.status, after.body, reached
        ),
    );
    c.check(
        landed.is_some(),
        format!(
            "the background stamp retry must remove the old .meta within {:?} of the slot being \
             freed",
            LANDING
        ),
    );
    c.check(
        a_after.is_some(),
        format!(
            "the other proxy must serve the new object within {:?} once the retry has landed",
            LANDING
        ),
    );
    c.finish();
}
/// What one stalled write must show (part B): the response within the budget, S3's answer
/// unchanged, a same-proxy GET that is not stale, and the old entry removed by the background
/// retry once the slot is free.
struct Expect {
    /// The status S3 answers the write with.
    status: StatusCode,
    /// What S3 holds for the key after the write; `None` once deleted.
    latest: Option<&'static str>,
}
/// Part B. Write `key` through `proxy` while its slot lock is held, and check the write against
/// the budget. The cache side's reads are not stalled, so the response-path invalidation is the
/// only step that can hold it.
async fn check_stalled_write(
    proxy: fn(&Harness) -> &Proxy,
    what: &str,
    method: &str,
    signed: bool,
    expect: Expect,
) {
    let h = harness().await;
    let key = format!("scn2-budget/{}.bin", what);
    let object = format!("{}.bin", what);
    h.mock.put(&key, "version one");
    assert_eq!(h.cache_through_a(&key).await, Bytes::from("version one"));
    let held = hold_slot(&h, &key);
    let (elapsed, answer) =
        write_while_stalled(&h, proxy(&h), method, &key, "version two", signed).await;
    let meta_after = h.meta_files(&object);
    let (reached, after) = h.probe(proxy(&h), &key).await;
    drop(held);
    let landed = wait_for_landing(&h, &object).await;
    let expected_body = expect.latest.unwrap_or("");
    let a_after = match expect.latest {
        Some(body) => wait_for_body(&h, &h.a, &key, body).await,
        None => {
            let start = Instant::now();
            let mut done = None;
            while start.elapsed() < LANDING {
                let (_, answer) = h.probe(&h.a, &key).await;
                if answer.status == StatusCode::NOT_FOUND {
                    done = Some(start.elapsed());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            done
        }
    };
    eprintln!(
        "SCN2 budget {}: {} answered {} in {:?}; same-proxy GET {} {:?} (reached S3: {}); old \
         .meta removed {:?} after the slot was freed; A caught up {:?} after that",
        what, method, answer.status, elapsed, after.status, after.body, reached, landed, a_after
    );
    let mut c = Checks::default();
    c.check(
        !meta_after.is_empty(),
        format!(
            "fixture: the held slot did not keep the old .meta on the volume for {}",
            what
        ),
    );
    c.check(
        answer.status == expect.status,
        format!(
            "the {} must be S3's answer {}, got {}",
            method, expect.status, answer.status
        ),
    );
    c.check(
        elapsed <= BUDGET + MARGIN,
        format!(
            "a {} ({}) waited {:?} for its invalidation on a stalled cache volume; the budget is \
             {:?}",
            method, what, elapsed, BUDGET
        ),
    );
    let latest = h.mock.latest(&key);
    c.check(
        latest == expect.latest.map(|b| Bytes::from(b.to_string())),
        format!("S3 must hold {:?}, holds {:?}", expect.latest, latest),
    );
    c.check(
        reached
            && match expect.latest {
                Some(_) => after.status == StatusCode::OK && after.body == expected_body,
                None => after.status == StatusCode::NOT_FOUND,
            },
        format!(
            "a GET through the proxy that acknowledged the {} must not serve the old object: got \
             {} {:?} (reached S3: {})",
            method, after.status, after.body, reached
        ),
    );
    c.check(
        landed.is_some(),
        format!(
            "the background stamp retry must remove the old .meta within {:?} of the slot being \
             freed",
            LANDING
        ),
    );
    c.check(
        a_after.is_some(),
        format!(
            "the other proxy must serve S3's state within {:?} once the retry has landed",
            LANDING
        ),
    );
    c.finish();
}
/// A cached signed PUT: the response-path invalidation in the PUT handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cached_put_is_answered_within_the_budget_on_a_stalled_invalidation() {
    check_stalled_write(
        |h| &h.b,
        "cached-put",
        "PUT",
        true,
        Expect {
            status: StatusCode::OK,
            latest: Some("version two"),
        },
    )
    .await;
}
/// A signed PUT the router forwards uncached (`write_cache_enabled: false`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_uncached_signed_put_is_answered_within_the_budget_on_a_stalled_invalidation() {
    check_stalled_write(
        |h| &h.c,
        "uncached-put",
        "PUT",
        true,
        Expect {
            status: StatusCode::OK,
            latest: Some("version two"),
        },
    )
    .await;
}
/// An unsigned PUT the router forwards uncached.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsigned_put_is_answered_within_the_budget_on_a_stalled_invalidation() {
    check_stalled_write(
        |h| &h.c,
        "unsigned-put",
        "PUT",
        false,
        Expect {
            status: StatusCode::OK,
            latest: Some("version two"),
        },
    )
    .await;
}
/// A signed DELETE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signed_delete_is_answered_within_the_budget_on_a_stalled_invalidation() {
    check_stalled_write(
        |h| &h.b,
        "signed-delete",
        "DELETE",
        true,
        Expect {
            status: StatusCode::NO_CONTENT,
            latest: None,
        },
    )
    .await;
}
