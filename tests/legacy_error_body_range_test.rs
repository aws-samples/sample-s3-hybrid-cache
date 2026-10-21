//! A range that a release before 3.0.0 cached from an S3 error reply is refetched, never
//! served as object bytes.
//!
//! When a 2.8 proxy found a listed range file missing, it refetched the extent with the
//! client's headers and a rewritten `Range`. For a client that signed `range` the reply is
//! `403 SignatureDoesNotMatch`, which 2.8 stored as the range's bytes, clamped to the error
//! body's length, under the object's real ETag and length. These tests plant that shape (no
//! `file_identity`, an extent starting above 0, the bytes of a real S3 error body) and read it
//! through `HttpProxy::handle_request`.
//!
//! Each fixture asserts its own shape before the request is sent: an extent at offset 0 is
//! already removed by `RangeSpec::is_unverified_prefix`, and an extent with a file identity is
//! a 3.0.0 write the check never inspects, so either would pass without the check.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Semaphore};

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::ObjectMetadata;
use s3_proxy::config::{Config, UpstreamOverrideConfig, UpstreamScheme};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::s3_client::S3Client;
use s3_proxy::S3ClientApi;

const BUCKET: &str = "error-body-bucket";
const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000"; // gitleaks:allow
const AUTH_SIGNS_RANGE: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=0000"; // gitleaks:allow
const STEP: Duration = Duration::from_secs(20);
/// Large enough for a read over `BUFFERED_OPERATION_BOUND` (8 MiB), which only the chained
/// stream serves from the cache.
const TOTAL: usize = 9 * 1024 * 1024;
/// `http_proxy::BUFFERED_OPERATION_BOUND`.
const BUFFERED_BOUND: usize = 8 * 1024 * 1024;
/// The `cache.disk_streaming_threshold` a streaming harness sets: below the error body's length.
const STREAMING_THRESHOLD: u64 = 256;
/// Where the error body is listed: above 0, so the offset-0 rule does not apply.
const OFF: usize = 65_536;
const TTL: Duration = Duration::from_secs(3600);

/// The body S3 returned to a header-signed ranged GET whose `Range` had been rewritten,
/// captured live, with the echoed canonical request and string to sign redacted.
const S3_ERROR_BODY: &str = r#"<?xml version="1.0" encoding="UTF-8"?> <Error><Code>SignatureDoesNotMatch</Code><Message>The request signature we calculated does not match the signature you provided. Check your key and signing method.</Message><StringToSign>xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx</StringToSign><CanonicalRequest>xxxxxxxx"#;

fn error_body() -> &'static [u8] {
    S3_ERROR_BODY.as_bytes()
}

fn error_end() -> usize {
    OFF + error_body().len() - 1
}

/// Position-dependent object bytes, so a window read from the wrong offset cannot equal the
/// window the client asked for.
fn object_bytes(len: usize) -> Bytes {
    Bytes::from(
        (0..len)
            .map(|i| ((i % 251) as u8).wrapping_mul(3))
            .collect::<Vec<u8>>(),
    )
}

// ── Mock S3 ─────────────────────────────────────────────────────────────────

#[derive(Default)]
struct MockState {
    objects: HashMap<String, Bytes>,
    hits: HashMap<String, usize>,
}

#[derive(Clone, Default)]
struct MockS3 {
    state: Arc<Mutex<MockState>>,
}

fn parse_range(spec: &str, total: usize) -> Option<(usize, usize)> {
    let spec = spec.trim().strip_prefix("bytes=")?;
    let (first, last) = spec.split_once('-')?;
    let start = first.trim().parse::<usize>().ok()?;
    let end = if last.trim().is_empty() {
        total - 1
    } else {
        last.trim().parse::<usize>().ok()?.min(total - 1)
    };
    (start < total && start <= end).then_some((start, end))
}

impl MockS3 {
    fn put(&self, key: &str, len: usize) -> Bytes {
        let data = object_bytes(len);
        self.state
            .lock()
            .unwrap()
            .objects
            .insert(key.to_string(), data.clone());
        data
    }

    fn hits(&self, raw_path: &str) -> usize {
        *self.state.lock().unwrap().hits.get(raw_path).unwrap_or(&0)
    }

    async fn handle(self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let range = req
            .headers()
            .get("range")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let _body = req.into_body().collect().await?.to_bytes();
        let key = path
            .strip_prefix(&format!("/{}/", BUCKET))
            .unwrap_or("")
            .to_string();
        if method != Method::GET && method != Method::HEAD {
            return Ok(Response::builder()
                .status(StatusCode::NOT_IMPLEMENTED)
                .body(Full::new(Bytes::new()))
                .unwrap());
        }
        let data = {
            let mut st = self.state.lock().unwrap();
            *st.hits.entry(path.clone()).or_insert(0) += 1;
            st.objects.get(&key).cloned()
        };
        let Some(data) = data else {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::new()))
                .unwrap());
        };
        let total = data.len();
        let (status, body, content_range) =
            match range.as_deref().and_then(|s| parse_range(s, total)) {
                Some((start, end)) => (
                    StatusCode::PARTIAL_CONTENT,
                    data.slice(start..=end),
                    Some(format!("bytes {}-{}/{}", start, end, total)),
                ),
                None => (StatusCode::OK, data.clone(), None),
            };
        let mut builder = Response::builder()
            .status(status)
            .header("content-length", body.len().to_string())
            .header("etag", etag_of(&key))
            .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .header("content-type", "application/octet-stream")
            .header("accept-ranges", "bytes");
        if let Some(cr) = content_range {
            builder = builder.header("content-range", cr);
        }
        let body = if method == Method::HEAD {
            Bytes::new()
        } else {
            body
        };
        Ok(builder.body(Full::new(body)).unwrap())
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

fn etag_of(key: &str) -> String {
    format!("\"etag-{}\"", key.replace('/', "-"))
}

// ── Proxy under test ────────────────────────────────────────────────────────

struct Harness {
    dir: TempDir,
    _mock_shutdown: oneshot::Sender<()>,
    _proxy_shutdown: oneshot::Sender<()>,
    mock: MockS3,
    mock_host: String,
    proxy: SocketAddr,
    disk: Arc<tokio::sync::RwLock<DiskCacheManager>>,
    cache_manager: Arc<CacheManager>,
}

/// `streaming` lowers `cache.disk_streaming_threshold` to `STREAMING_THRESHOLD`, so a read of
/// exactly the error-body extent takes the exact-match streaming arm.
async fn harness(streaming: bool) -> Harness {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mock = MockS3::default();
    let (mock_addr, mock_shutdown) = mock.clone().spawn().await;
    let mock_host = format!("127.0.0.1:{}", mock_addr.port());

    let dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.cache.cache_dir = dir.path().to_path_buf();
    config.cache.ram_cache_enabled = true;
    config.cache.max_ram_cache_size = 64 * 1024 * 1024;
    config.cache.write_cache_enabled = true;
    config.cache.read_cache_enabled = true;
    config.cache.download_coordination.enabled = false;
    config.cache.get_ttl = TTL;
    config.cache.head_ttl = TTL;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    if streaming {
        config.cache.disk_streaming_threshold = STREAMING_THRESHOLD;
    }
    config.connection_pool.upstream_overrides.insert(
        mock_host.clone(),
        UpstreamOverrideConfig {
            scheme: UpstreamScheme::Http,
            validate_tls: true,
        },
    );
    let config = Arc::new(config);

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
    if let Some(consolidator) = cache_manager.get_journal_consolidator().await {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                let _ = consolidator.run_consolidation_cycle().await;
            }
        });
    }
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let request_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_requests));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let proxy = listener.local_addr().unwrap();
    let (proxy_shutdown, mut rx) = oneshot::channel::<()>();
    let serve_disk = Arc::clone(&disk_cache_manager);
    let kept_cache_manager = Arc::clone(&cache_manager);
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
        _proxy_shutdown: proxy_shutdown,
        mock,
        mock_host,
        proxy,
        disk: serve_disk,
        cache_manager: kept_cache_manager,
    }
}

impl Harness {
    /// A ranged GET, and whether it reached the mock (a miss).
    async fn ranged_probe(
        &self,
        key: &str,
        start: usize,
        end: usize,
        auth: &str,
    ) -> (bool, StatusCode, Bytes) {
        let raw = format!("/{}/{}", BUCKET, key);
        let before = self.mock.hits(&raw);
        let fut = async {
            let stream = TcpStream::connect(self.proxy).await.expect("connect");
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .expect("handshake");
            tokio::spawn(async move {
                let _ = conn.await;
            });
            let req = Request::builder()
                .method("GET")
                .uri(raw.as_str())
                .header("host", self.mock_host.as_str())
                .header("content-length", "0")
                .header("authorization", auth)
                .header("range", format!("bytes={}-{}", start, end))
                .body(Full::new(Bytes::new()))
                .unwrap();
            let resp = sender.send_request(req).await.expect("proxy request");
            let status = resp.status();
            let body = resp.into_body().collect().await.expect("body").to_bytes();
            (status, body)
        };
        let (status, body) = tokio::time::timeout(STEP, fut)
            .await
            .unwrap_or_else(|_| panic!("GET {} bytes={}-{} timed out", raw, start, end));
        (self.mock.hits(&raw) > before, status, body)
    }

    /// Store `body` as the extent at `start` through the real store path, as a fill of the
    /// object's real length would.
    async fn plant(&self, key: &str, start: usize, body: &[u8]) {
        let end = start + body.len() - 1;
        let om = ObjectMetadata {
            etag: etag_of(key),
            last_modified: String::from("Wed, 01 Jan 2025 00:00:00 GMT"),
            content_length: TOTAL as u64,
            cumulative_size: TOTAL as u64,
            content_type: Some(String::from("application/octet-stream")),
            response_headers: HashMap::from([
                (
                    String::from("content-range"),
                    format!("bytes {}-{}/{}", start, end, TOTAL),
                ),
                (String::from("content-length"), body.len().to_string()),
                (String::from("etag"), etag_of(key)),
                (
                    String::from("last-modified"),
                    String::from("Wed, 01 Jan 2025 00:00:00 GMT"),
                ),
                (String::from("accept-ranges"), String::from("bytes")),
            ]),
            ..Default::default()
        };
        self.disk
            .write()
            .await
            .store_range(
                &format!("{}/{}", BUCKET, key),
                start as u64,
                end as u64,
                body,
                om,
                TTL,
                false,
            )
            .await
            .expect("plant the extent");
    }

    fn meta_path_now(&self, key: &str) -> Option<std::path::PathBuf> {
        let want = format!("{}/{}", BUCKET, key);
        walkdir::WalkDir::new(self.dir.path().join("metadata"))
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "meta"))
            .find(|e| {
                std::fs::read_to_string(e.path())
                    .ok()
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                    .and_then(|v| {
                        v.get("cache_key")
                            .and_then(|k| k.as_str())
                            .map(|k| k == want)
                    })
                    .unwrap_or(false)
            })
            .map(|e| e.path().to_path_buf())
    }

    fn meta_doc_now(&self, key: &str) -> Option<serde_json::Value> {
        let path = self.meta_path_now(key)?;
        serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
    }

    /// Wait until the key's `.meta` satisfies `pred`, or panic naming `what`.
    async fn wait_for_doc(
        &self,
        key: &str,
        what: &str,
        pred: impl Fn(&serde_json::Value) -> bool,
    ) -> serde_json::Value {
        let until = tokio::time::Instant::now() + STEP;
        loop {
            if let Some(doc) = self.meta_doc_now(key) {
                if pred(&doc) {
                    return doc;
                }
            }
            if tokio::time::Instant::now() >= until {
                panic!(
                    "{}: {:?}",
                    what,
                    self.meta_doc_now(key).map(|d| extents(&d))
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Make the listing at `start` look as a release before 3.0.0 wrote it: no
    /// `file_identity`. Every other listing keeps its identity.
    async fn make_pre_3_0_0_at(&self, key: &str, start: usize) {
        let path = self.meta_path_now(key).expect("planted .meta");
        let mut doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        for range in doc["ranges"].as_array_mut().expect("ranges") {
            if range.get("start").and_then(|v| v.as_u64()) == Some(start as u64) {
                range.as_object_mut().unwrap().remove("file_identity");
            }
        }
        std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
        self.cache_manager
            .invalidate_metadata_cache(&format!("{}/{}", BUCKET, key))
            .await;
    }
}

/// The extents a `.meta` lists, as `(start, end, has_file_identity)`.
fn extents(doc: &serde_json::Value) -> Vec<(u64, u64, bool)> {
    doc.get("ranges")
        .and_then(|r| r.as_array())
        .map(|ranges| {
            ranges
                .iter()
                .map(|r| {
                    (
                        r.get("start").and_then(|v| v.as_u64()).unwrap_or(u64::MAX),
                        r.get("end").and_then(|v| v.as_u64()).unwrap_or(u64::MAX),
                        r.get("file_identity").is_some_and(|v| !v.is_null()),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Plant the 2.8 shape at `OFF` and assert it before any request: listed at `OFF` (not 0)
/// to `error_end()`, no file identity, under the object's real length.
async fn plant_error_extent(h: &Harness, key: &str) {
    h.plant(key, OFF, error_body()).await;
    h.wait_for_doc(key, "fixture: the planted extent is listed", |d| {
        extents(d)
            .iter()
            .any(|(s, e, _)| *s == OFF as u64 && *e == error_end() as u64)
    })
    .await;
    h.make_pre_3_0_0_at(key, OFF).await;
    let doc = h
        .wait_for_doc(
            key,
            "fixture: the planted extent records no file identity",
            |d| extents(d).iter().any(|(s, _, id)| *s == OFF as u64 && !id),
        )
        .await;
    let planted = extents(&doc)
        .into_iter()
        .find(|(s, _, _)| *s == OFF as u64)
        .expect("fixture: the planted extent");
    assert_ne!(
        planted.0, 0,
        "fixture: an extent at 0 is the offset-0 rule's, not this check's"
    );
    assert_eq!(
        doc.pointer("/object_metadata/content_length")
            .and_then(|v| v.as_u64()),
        Some(TOTAL as u64),
        "fixture: the document keeps the object's real length"
    );
}

fn assert_not_error_body(body: &Bytes) {
    assert!(
        !body.windows(30).any(|w| w == &error_body()[40..70]),
        "the client received the stored S3 error body as object bytes: {:?}",
        String::from_utf8_lossy(&body[..body.len().min(120)])
    );
}

// ── The buffered serve ──────────────────────────────────────────────────────

/// A read inside the extent: the client gets the object's bytes, the listing is removed, and
/// once a refill with a file identity is listed the next read is a cache hit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pre_3_0_0_range_holding_an_s3_error_body_is_refetched() {
    let h = harness(false).await;
    let key = "e1/object.bin";
    let object = h.mock.put(key, TOTAL);
    plant_error_extent(&h, key).await;

    let (a, b) = (OFF + 10, OFF + 109);
    let (reached, status, body) = h.ranged_probe(key, a, b, AUTH).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_not_error_body(&body);
    assert_eq!(
        body,
        object.slice(a..=b),
        "the client must receive the object's bytes"
    );
    assert!(reached, "the damaged range is fetched from S3");
    h.wait_for_doc(key, "the error-body listing is removed", |d| {
        extents(d).iter().all(|(s, _, id)| *s != OFF as u64 || *id)
    })
    .await;

    let (_, status, body) = h.ranged_probe(key, a, b, AUTH).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, object.slice(a..=b));
    h.wait_for_doc(key, "a refill with a file identity covers the read", |d| {
        extents(d)
            .iter()
            .any(|(s, e, id)| *id && *s <= a as u64 && *e >= b as u64)
    })
    .await;
    let (reached, status, body) = h.ranged_probe(key, a, b, AUTH).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, object.slice(a..=b));
    assert!(!reached, "the refill is served from the cache");
}

/// The same read from a client that signed `range`, as the AWS SDKs and CLI do: the proxy
/// cannot rewrite its `Range`, so it forwards the client's own request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_signed_read_of_such_a_range_gets_the_object_bytes() {
    let h = harness(false).await;
    let key = "e2/object.bin";
    let object = h.mock.put(key, TOTAL);
    plant_error_extent(&h, key).await;

    let (a, b) = (OFF + 100, OFF + 299);
    let (reached, status, body) = h.ranged_probe(key, a, b, AUTH_SIGNS_RANGE).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_not_error_body(&body);
    assert_eq!(body, object.slice(a..=b));
    assert!(reached, "the damaged range is fetched from S3");
    h.wait_for_doc(key, "the error-body listing is removed", |d| {
        extents(d).iter().all(|(s, _, id)| *s != OFF as u64 || *id)
    })
    .await;
}

/// Scope: a range 3.0.0 wrote records a file identity and is never inspected, even when its
/// bytes are an S3 error document.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_3_0_0_range_holding_an_error_document_is_served_from_cache() {
    let h = harness(false).await;
    let key = "e3/object.bin";
    h.mock.put(key, TOTAL);
    h.plant(key, OFF, error_body()).await;
    h.wait_for_doc(
        key,
        "fixture: the extent is listed with a file identity",
        |d| extents(d).iter().any(|(s, _, id)| *s == OFF as u64 && *id),
    )
    .await;

    let (a, b) = (OFF + 10, OFF + 109);
    let (reached, status, body) = h.ranged_probe(key, a, b, AUTH).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, Bytes::copy_from_slice(&error_body()[10..110]));
    assert!(
        !reached,
        "a range with a file identity is served from the cache"
    );
}

// ── The streaming arms ──────────────────────────────────────────────────────
//
// Both arms refuse the extent through `DiskCacheManager::check_legacy_range` before the
// response head. The buffered serve has its own check in `load_range_data` and never calls
// `check_legacy_range`, so a defect that makes `check_legacy_range` pass everything turns
// these two tests red only if the read took a streaming arm.

/// A read of exactly the extent, at or above `disk_streaming_threshold`, takes the exact-match
/// streaming arm.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_streamed_read_of_such_a_range_is_refetched() {
    let h = harness(true).await;
    let key = "e4/object.bin";
    let object = h.mock.put(key, TOTAL);
    plant_error_extent(&h, key).await;
    assert!(
        error_body().len() as u64 >= STREAMING_THRESHOLD,
        "fixture: an exact read of the extent is at or above the streaming threshold"
    );

    let (reached, status, body) = h.ranged_probe(key, OFF, error_end(), AUTH).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_not_error_body(&body);
    assert_eq!(body, object.slice(OFF..=error_end()));
    assert!(reached, "the damaged range is fetched from S3");
}

/// A read over `BUFFERED_OPERATION_BOUND` spanning a 3.0.0 extent, the error-body extent and a
/// second 3.0.0 extent with no gap is served from the cache only by the chained stream, which
/// checks every extent before the response head, though the error body is not the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chained_stream_spanning_such_a_range_is_refetched() {
    let h = harness(false).await;
    let key = "e5/object.bin";
    let object = h.mock.put(key, TOTAL);
    let first = OFF - 4096;
    let after = error_end() + 1;
    let last = error_end() + BUFFERED_BOUND;
    h.plant(key, first, &object[first..OFF]).await;
    h.plant(key, after, &object[after..=last]).await;
    h.wait_for_doc(key, "fixture: both 3.0.0 extents are listed", |d| {
        let e = extents(d);
        e.iter().any(|(s, _, _)| *s == first as u64) && e.iter().any(|(s, _, _)| *s == after as u64)
    })
    .await;
    plant_error_extent(&h, key).await;
    h.wait_for_doc(
        key,
        "fixture: three contiguous extents, only the middle one without identity",
        |d| {
            let e = extents(d);
            e.iter()
                .any(|(s, x, id)| *s == first as u64 && *x == OFF as u64 - 1 && *id)
                && e.iter()
                    .any(|(s, x, id)| *s == OFF as u64 && *x == error_end() as u64 && !id)
                && e.iter()
                    .any(|(s, x, id)| *s == after as u64 && *x == last as u64 && *id)
        },
    )
    .await;
    assert!(
        last - first + 1 > BUFFERED_BOUND,
        "fixture: the read is over the buffered bound"
    );

    let (reached, status, body) = h.ranged_probe(key, first, last, AUTH).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_not_error_body(&body);
    assert_eq!(body, object.slice(first..=last));
    assert!(reached, "the damaged range is fetched from S3");
}
