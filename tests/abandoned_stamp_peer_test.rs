//! What a PEER proxy serves after another proxy's acknowledged overwrite left its
//! write-order stamp `WriteStampRetry::Abandoned` (every retry task busy, the overflow queue at
//! its bound, and the node-local pending-stamp log unwritable).
//!
//! Made the proxy that acknowledged the write stop serving the replaced object
//! (`abandoned_stamp_retry_test.rs`). A peer sharing the cache volume has no record of the
//! abandoned key, and the invalidation never reached the volume, so its cached `.meta` still
//! names the replaced object. `docs/CACHING.md` (Write-Through Ordering, "When the cache volume
//! is unavailable") states the peer can serve the replaced object until the retry lands, and,
//! for a memory-only proxy, until the object's TTL or the key's next write if that proxy
//! restarts or its memory queue is full. This file measures the first bound through real
//! requests: two proxies over one cache directory, the fault injected through the same
//! existing seams uses (`set_write_stamp_retry_bounds_for_tests`, a refused log
//! directory, held slot locks), no new seam.
//!
//! Both proxies run in this process, so they share one instance id (`{hostname}:{pid}`): one
//! journal file and one write ledger. Only the cache volume's `.meta`, `.bin` and `.wtok` files
//! decide what each serves, and those are shared in production too.
#![cfg(unix)]
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
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
/// How long the abandoned key's stamp may take to land once a retry task is free (the earlier
/// figure), plus the peer's RAM metadata refresh interval (5 s by default), with slack.
const LANDING: Duration = Duration::from_secs(50);
/// Per-request ceiling.
const STEP: Duration = Duration::from_secs(30);
/// Whole-test bound.
const TEST_BOUND: Duration = Duration::from_secs(150);
/// The retry bounds are process-wide, so the tests in this binary run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Restores the default retry bounds when a test ends, pass or fail.
struct Bounds;
impl Bounds {
    fn set(max_tasks: usize, overflow_bytes: usize, untracked_bytes: Option<usize>) -> Self {
        s3_proxy::cache::set_write_stamp_retry_bounds_for_tests(
            Some(max_tasks),
            Some(overflow_bytes),
            untracked_bytes,
        );
        Bounds
    }
}
impl Drop for Bounds {
    fn drop(&mut self) {
        s3_proxy::cache::set_write_stamp_retry_bounds_for_tests(None, None, None);
    }
}

// ── Mock S3 (as in abandoned_stamp_retry_test.rs) ──────────────────────

#[derive(Default)]
struct MockState {
    versions: HashMap<String, Vec<Bytes>>,
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

    async fn handle(self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let key = req.uri().path().trim_start_matches('/').to_string();
        let body = req.into_body().collect().await?.to_bytes();
        let mut st = self.state.lock().unwrap();
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

// ── Proxies over one cache directory ────────────────────────────────────────

struct Proxy {
    _shutdown: oneshot::Sender<()>,
    addr: SocketAddr,
    cache: Arc<CacheManager>,
    /// This proxy's consolidation loop runs while this is set.
    consolidate: Arc<AtomicBool>,
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

/// One proxy over `dir`. Its pending-stamp log directory sits inside the cache directory,
/// which `open_stamp_backlog` refuses, so it runs memory-only.
async fn proxy(dir: &Path, mock_host: &str, state_name: &str) -> Proxy {
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
    let refused_state = dir.join(state_name);
    std::fs::create_dir_all(&refused_state).unwrap();
    cache_manager.open_stamp_backlog(&refused_state);
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
    let consolidate = Arc::new(AtomicBool::new(true));
    if let Some(consolidator) = cache_manager.get_journal_consolidator().await {
        let consolidate = Arc::clone(&consolidate);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                if consolidate.load(Ordering::SeqCst) {
                    let _ = consolidator.run_consolidation_cycle().await;
                }
            }
        });
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
        _shutdown: shutdown,
        addr,
        cache: kept,
        consolidate,
    }
}

struct Fleet {
    dir: TempDir,
    _mock_shutdown: oneshot::Sender<()>,
    mock: MockS3,
    mock_host: String,
    /// The proxy that takes the writes.
    a: Proxy,
    /// A peer sharing the cache directory.
    b: Proxy,
}

async fn fleet() -> Fleet {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mock = MockS3::default();
    let (mock_addr, mock_shutdown) = mock.clone().spawn().await;
    let mock_host = format!("127.0.0.1:{}", mock_addr.port());
    let dir = TempDir::new().unwrap();
    let a = proxy(dir.path(), &mock_host, "scn1-local-state-a").await;
    let b = proxy(dir.path(), &mock_host, "scn1-local-state-b").await;
    Fleet {
        dir,
        _mock_shutdown: mock_shutdown,
        mock,
        mock_host,
        a,
        b,
    }
}

impl Fleet {
    async fn send(&self, to: &Proxy, method: &str, key: &str, body: &str) -> Answer {
        let fut = async {
            let stream = TcpStream::connect(to.addr).await.expect("connect");
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .expect("handshake");
            tokio::spawn(async move {
                let _ = conn.await;
            });
            let req = Request::builder()
                .method(method)
                .uri(format!("/{}", key))
                .header("host", self.mock_host.as_str())
                .header("authorization", AUTH)
                .header("content-length", body.len().to_string())
                .body(Full::new(Bytes::from(body.to_string())))
                .unwrap();
            let resp = sender.send_request(req).await.expect("proxy request");
            let status = resp.status();
            let body = resp.into_body().collect().await.expect("body").to_bytes();
            Answer { status, body }
        };
        tokio::time::timeout(STEP, fut)
            .await
            .unwrap_or_else(|_| panic!("{} {} timed out", method, key))
    }

    /// GET `key` through `to`, reporting whether it reached the mock S3.
    async fn probe(&self, to: &Proxy, key: &str) -> (bool, Answer) {
        let before = self.mock.gets(key);
        let answer = self.send(to, "GET", key, "").await;
        (self.mock.gets(key) > before, answer)
    }

    /// GET `key` through `to` until a repeat is answered from the cache.
    async fn cache(&self, to: &Proxy, key: &str) -> Bytes {
        let (_, first) = self.probe(to, key).await;
        assert_eq!(first.status, StatusCode::OK, "seed GET {}", key);
        for _ in 0..100 {
            let (reached, answer) = self.probe(to, key).await;
            assert_eq!(answer.status, StatusCode::OK);
            if !reached {
                return answer.body;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("{} never became a cache hit", key);
    }

    fn cache_key_of(&self, key: &str) -> String {
        CacheManager::generate_cache_key(&format!("/{}", key), Some(&self.mock_host))
    }

    fn hold_slot(&self, key: &str) -> s3_proxy::metadata_lock_manager::MetadataLock {
        s3_proxy::metadata_lock_manager::try_acquire_slot_lock(
            self.dir.path(),
            &self.cache_key_of(key),
        )
        .expect("fixture: hold the key's slot lock")
    }

    fn pending_on_a(&self, key: &str) -> bool {
        self.a
            .cache
            .write_stamp_pending_for_tests(&self.cache_key_of(key))
    }

    fn consolidate(&self, on: bool) {
        self.a.consolidate.store(on, Ordering::SeqCst);
        self.b.consolidate.store(on, Ordering::SeqCst);
    }
}

/// An acknowledged overwrite through proxy A whose stamp is abandoned. Before the write, the
/// peer B serves the key from the shared cache. While the stamp is abandoned, B is measured
/// through real GETs. Once A's retry task is free the stamp lands, and B must serve the new
/// object within B's RAM metadata refresh interval of that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item5_a_peer_serves_the_replaced_object_only_until_the_abandoned_stamp_lands() {
    tokio::time::timeout(TEST_BOUND, async {
        let _serial = SERIAL.lock().await;
        let _bounds = Bounds::set(1, 0, None);
        let f = fleet().await;
        let busy = "scn1-item5/busy.bin";
        let target = "scn1-item5/target.bin";
        f.mock.put(target, "version one");
        f.mock.put(busy, "busy one");
        assert_eq!(f.cache(&f.a, target).await, Bytes::from("version one"));
        assert_eq!(
            f.cache(&f.b, target).await,
            Bytes::from("version one"),
            "fixture: the peer serves the key from the shared cache before the write"
        );
        f.consolidate(false);
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Keep A's one retry task busy, then overwrite the target with its slot held: no task,
        // no overflow room, no log, so the stamp is abandoned.
        let busy_hold = f.hold_slot(busy);
        let busy_put = f.send(&f.a, "PUT", busy, "busy two").await;
        assert!(
            busy_put.status == StatusCode::OK && f.pending_on_a(busy),
            "fixture: the busy key's stamp holds A's one retry task"
        );
        let target_hold = f.hold_slot(target);
        let put = f.send(&f.a, "PUT", target, "version two").await;
        drop(target_hold);
        assert_eq!(
            put.status,
            StatusCode::OK,
            "fixture: the overwrite is acknowledged"
        );
        assert!(
            !f.a.cache.volume_stalled(),
            "fixture: the stall breaker is closed"
        );
        let mut a_side = Vec::new();
        if !f.pending_on_a(target) {
            a_side.push("A does not track the abandoned key (SCN2's untracked set)".to_string());
        }
        // A, which acknowledged the write, goes to S3.
        let (a_reached, a_after) = f.probe(&f.a, target).await;
        if !(a_reached && a_after.body == "version two") {
            a_side.push(format!(
                "A served {:?} (reached S3: {})",
                a_after.body, a_reached
            ));
        }
        // The peer, measured over a few seconds while the stamp stays abandoned.
        let abandoned_at = Instant::now();
        let mut peer_stale = Vec::new();
        while abandoned_at.elapsed() < Duration::from_secs(3) {
            let (reached, answer) = f.probe(&f.b, target).await;
            peer_stale.push((reached, answer.body == "version one"));
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        eprintln!(
            "SCN1 item5 peer GETs while the stamp is abandoned (reached S3, served version one): \
             {:?}",
            peer_stale
        );

        // Recovery: the busy key's stamp lands, and A's task takes the abandoned key.
        drop(busy_hold);
        f.consolidate(true);
        let freed = Instant::now();
        let mut landed = None;
        while freed.elapsed() < LANDING {
            if !f.pending_on_a(busy) && !f.pending_on_a(target) {
                landed = Some(freed.elapsed());
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            landed.is_some(),
            "recovery: A's abandoned stamp must land once its retry task is free"
        );
        let landed_at = Instant::now();
        let mut peer_after = None;
        while landed_at.elapsed() < LANDING {
            let (reached, answer) = f.probe(&f.b, target).await;
            assert_eq!(answer.status, StatusCode::OK);
            if answer.body == "version two" {
                peer_after = Some((landed_at.elapsed(), reached));
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        eprintln!(
            "SCN1 item5 stamp landed {:?} after the busy slot freed; the peer served version two \
             {:?} after that (elapsed, reached S3)",
            landed, peer_after
        );
        assert!(
            peer_after.is_some(),
            "STALE BYTES (item5): the peer still served the replaced object {:?} after A's \
             stamp landed; A-side: {:?}",
            LANDING,
            a_side
        );
        assert!(a_side.is_empty(), "SCN2 (A-side): {:?}", a_side);
        assert!(
            peer_stale.iter().all(|(_, stale)| *stale) || peer_stale.iter().all(|(_, s)| !*s),
            "the peer's answers while abandoned were mixed: {:?}",
            peer_stale
        );
    })
    .await
    .expect("the test must finish within its bound");
}
