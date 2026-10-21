//! An acknowledged write whose invalidation could not be tracked must not leave the proxy that
//! acknowledged it serving the object it replaced.
//!
//! The untracked case is `WriteStampRetry::Abandoned`: every stamp-retry task is busy, the
//! overflow queue is at its memory bound, and the node-local pending-stamp log cannot be written.
//! Here the bounds are lowered through a process-wide test seam (one task, no overflow room),
//! the one task is kept busy by a write whose key's slot lock the test holds, and the proxy's log
//! directory sits inside the cache directory, which `open_stamp_backlog` refuses, so the proxy
//! runs memory-only and every log append fails, as in production when the log is refused or
//! unwritable. Requests go through `HttpProxy::handle_request` to a loopback mock S3 over a
//! plaintext upstream override, as in `volume_stall_breaker_test.rs`, whose harness this
//! trims to one proxy.
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
/// How long the busy key's background stamp retry, then the drained key's, may take to land once
/// their slots are free: one exhausted attempt (about 3.1 s) plus a backoff of at most 5 s each,
/// with slack.
const LANDING: Duration = Duration::from_secs(40);
/// Per-request ceiling.
const STEP: Duration = Duration::from_secs(30);

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

// ── Mock S3 ─────────────────────────────────────────────────────────────────

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

// ── One proxy ───────────────────────────────────────────────────────────────

struct Harness {
    dir: TempDir,
    _mock_shutdown: oneshot::Sender<()>,
    _shutdown: oneshot::Sender<()>,
    mock: MockS3,
    mock_host: String,
    addr: SocketAddr,
    cache: Arc<CacheManager>,
    /// The proxy's consolidation loop runs while this is set.
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

async fn harness() -> Harness {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mock = MockS3::default();
    let (mock_addr, mock_shutdown) = mock.clone().spawn().await;
    let mock_host = format!("127.0.0.1:{}", mock_addr.port());
    let dir = TempDir::new().unwrap();
    let config = config_for(dir.path(), &mock_host);
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
    // A log directory inside the cache directory is refused: the proxy runs memory-only
    // and every pending-stamp append fails. The scope is still this test's own, so no other
    // test's retries share it.
    let refused_state = dir.path().join("scn1-local-state");
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
    Harness {
        dir,
        _mock_shutdown: mock_shutdown,
        _shutdown: shutdown,
        mock,
        mock_host,
        addr,
        cache: kept,
        consolidate,
    }
}

impl Harness {
    async fn send(&self, method: &str, key: &str, body: &str) -> Answer {
        let fut = async {
            let stream = TcpStream::connect(self.addr).await.expect("connect");
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

    /// GET `key`, reporting whether it reached the mock S3.
    async fn probe(&self, key: &str) -> (bool, Answer) {
        let before = self.mock.gets(key);
        let answer = self.send("GET", key, "").await;
        (self.mock.gets(key) > before, answer)
    }

    /// GET `key` until a repeat is answered from the cache.
    async fn cache(&self, key: &str) -> Bytes {
        let (_, first) = self.probe(key).await;
        assert_eq!(first.status, StatusCode::OK, "seed GET {}", key);
        for _ in 0..100 {
            let (reached, answer) = self.probe(key).await;
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

    /// Hold `key`'s metadata slot lock, as a peer or a stalled volume does, until the guard drops.
    fn hold_slot(&self, key: &str) -> s3_proxy::metadata_lock_manager::MetadataLock {
        s3_proxy::metadata_lock_manager::try_acquire_slot_lock(
            self.dir.path(),
            &self.cache_key_of(key),
        )
        .expect("fixture: hold the key's slot lock")
    }

    fn pending(&self, key: &str) -> bool {
        self.cache
            .write_stamp_pending_for_tests(&self.cache_key_of(key))
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

/// Seed `target` and `control` in the cache, keep the one retry task busy on `busy`, then
/// overwrite `target` while its slot is held so its invalidation fails with no task, no overflow
/// room and no log: `WriteStampRetry::Abandoned`. Returns the busy key's slot hold.
async fn abandon_an_overwrite(
    h: &Harness,
    c: &mut Checks,
    busy: &str,
    target: &str,
    control: &str,
) -> s3_proxy::metadata_lock_manager::MetadataLock {
    h.mock.put(target, "version one");
    h.mock.put(control, "control one");
    h.mock.put(busy, "busy one");
    assert_eq!(h.cache(target).await, Bytes::from("version one"));
    assert_eq!(h.cache(control).await, Bytes::from("control one"));
    // Nothing but the writes under test touches the volume from here.
    h.consolidate.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(400)).await;

    let busy_hold = h.hold_slot(busy);
    let busy_put = h.send("PUT", busy, "busy two").await;
    c.check(
        busy_put.status == StatusCode::OK && h.pending(busy),
        format!(
            "fixture: the busy key's write is acknowledged and its stamp holds the one retry \
             task (status {}, pending {})",
            busy_put.status,
            h.pending(busy)
        ),
    );
    let target_hold = h.hold_slot(target);
    let put = h.send("PUT", target, "version two").await;
    drop(target_hold);
    c.check(
        put.status == StatusCode::OK,
        format!("fixture: the overwrite is acknowledged, got {}", put.status),
    );
    c.check(
        !h.cache.volume_stalled(),
        "fixture: two stalled writes leave the volume-stall breaker closed, so a bypass below \
         is not the breaker's"
            .to_string(),
    );
    busy_hold
}

/// Red side: with the overwrite's stamp abandoned, a GET through the proxy that acknowledged it
/// was served the replaced object from the cache. Now the key's reads go to S3 until a stamp
/// lands, an unrelated key is still served from the cache, and once a retry task is free the
/// abandoned key is stamped and cached again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandoned_overwrite_is_not_served_stale_by_the_proxy_that_acknowledged_it() {
    let _serial = SERIAL.lock().await;
    let _bounds = Bounds::set(1, 0, None);
    let h = harness().await;
    let busy = "scn1-abandoned/busy.bin";
    let target = "scn1-abandoned/target.bin";
    let control = "scn1-abandoned/control.bin";
    let mut c = Checks::default();
    let busy_hold = abandon_an_overwrite(&h, &mut c, busy, target, control).await;

    let (reached, after) = h.probe(target).await;
    eprintln!(
        "SCN1 GET after the abandoned overwrite: {} {:?} (reached S3: {}, pending: {})",
        after.status,
        after.body,
        reached,
        h.pending(target)
    );
    c.check(
        after.status == StatusCode::OK && after.body == "version two" && reached,
        format!(
            "a GET through the proxy that acknowledged the overwrite served the replaced object: \
             got {} {:?} (reached S3: {})",
            after.status, after.body, reached
        ),
    );
    let (again_reached, again) = h.probe(target).await;
    c.check(
        again_reached && again.body == "version two",
        format!(
            "the abandoned key's reads must keep going to S3 until a stamp lands, and its fill \
             must publish nothing: got {:?} (reached S3: {})",
            again.body, again_reached
        ),
    );
    let (control_reached, control_read) = h.probe(control).await;
    c.check(
        !control_reached && control_read.body == "control one",
        format!(
            "control: an unrelated key must still be served from the cache: got {:?} (reached \
             S3: {})",
            control_read.body, control_reached
        ),
    );

    // Recovery: the busy key's stamp lands, and its task takes the abandoned key.
    drop(busy_hold);
    h.consolidate.store(true, Ordering::SeqCst);
    let freed = Instant::now();
    let mut cleared = None;
    while freed.elapsed() < LANDING {
        if !h.pending(busy) && !h.pending(target) {
            cleared = Some(freed.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    c.check(
        cleared.is_some(),
        format!(
            "recovery: once a retry task is free the abandoned key must be stamped within {:?} \
             (busy pending {}, target pending {})",
            LANDING,
            h.pending(busy),
            h.pending(target)
        ),
    );
    let recached = h.cache(target).await;
    eprintln!(
        "SCN1 recovery: pending cleared after {:?}; re-cached {:?}",
        cleared, recached
    );
    c.check(
        recached == "version two",
        format!(
            "recovery: the key must be cached again with the new object, got {:?}",
            recached
        ),
    );
    c.finish();
}

/// With the untracked-key set also at its bound, the proxy cannot name the abandoned key, so it
/// forwards every GET in that cache directory to S3: the overwritten key and the control alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn past_the_untracked_bound_every_read_of_the_cache_directory_goes_to_s3() {
    let _serial = SERIAL.lock().await;
    let _bounds = Bounds::set(1, 0, Some(0));
    let h = harness().await;
    let busy = "scn1-coarse/busy.bin";
    let target = "scn1-coarse/target.bin";
    let control = "scn1-coarse/control.bin";
    let mut c = Checks::default();
    let busy_hold = abandon_an_overwrite(&h, &mut c, busy, target, control).await;

    let (reached, after) = h.probe(target).await;
    c.check(
        after.status == StatusCode::OK && after.body == "version two" && reached,
        format!(
            "a GET through the proxy that acknowledged the overwrite served the replaced object: \
             got {} {:?} (reached S3: {})",
            after.status, after.body, reached
        ),
    );
    let (control_reached, control_read) = h.probe(control).await;
    c.check(
        control_reached && control_read.body == "control one",
        format!(
            "past the untracked bound an unrelated key's GET must go to S3 too: got {:?} \
             (reached S3: {})",
            control_read.body, control_reached
        ),
    );
    drop(busy_hold);
    c.finish();
}
