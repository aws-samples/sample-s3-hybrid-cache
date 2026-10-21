//! Access-log records under contention, object subresources and unrecognised queries on
//! GET and HEAD, `Content-MD5` on reads, and multi-range bypass counting.
//!
//! The harness: real requests through `HttpProxy::handle_request` to a loopback
//! mock S3 over a plaintext upstream override, optionally with a logger and a metrics
//! manager wired in. A request is only treated as cached once a repeat of it has been
//! served without the mock seeing it, so the "before" state is observed rather than assumed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::HeaderMap;
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
use s3_proxy::logging::LoggerManager;
use s3_proxy::metrics::MetricsManager;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::s3_client::S3Client;
use s3_proxy::S3ClientApi;

const BUCKET: &str = "scn1-bucket";
const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000"; // gitleaks:allow
const STEP: Duration = Duration::from_secs(20);
/// Time for a detached cache store (and the 100 ms consolidation loop) to land, so a
/// defective store has published before the next request looks.
const SETTLE: Duration = Duration::from_millis(1500);
const OBJECT_TYPE: &str = "application/octet-stream";

// ── Mock S3 ─────────────────────────────────────────────────────────────────

#[derive(Default)]
struct MockState {
    /// Key -> every version's body, oldest first. Absent means the object does not exist.
    versions: HashMap<String, Vec<Bytes>>,
    /// Raw request path -> GET/HEAD count.
    gets: HashMap<String, usize>,
}

#[derive(Clone, Default)]
struct MockS3 {
    state: Arc<Mutex<MockState>>,
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&input[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).expect("utf8")
}

/// Query parameters, names percent-decoded and lowercased, values percent-decoded.
fn query_map(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let name = percent_decode(parts.next().unwrap_or("")).to_ascii_lowercase();
            let value = percent_decode(parts.next().unwrap_or(""));
            (name, value)
        })
        .collect()
}

fn etag_of(version: usize) -> String {
    format!("\"etag-v{}\"", version)
}

impl MockS3 {
    /// Write a new version of `key` directly, as another client would.
    fn put(&self, key: &str, body: &str) {
        self.state
            .lock()
            .unwrap()
            .versions
            .entry(key.to_string())
            .or_default()
            .push(Bytes::from(body.to_string()));
    }

    fn gets(&self, raw_path: &str) -> usize {
        *self.state.lock().unwrap().gets.get(raw_path).unwrap_or(&0)
    }

    async fn handle(self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let headers = req.headers().clone();
        let path = req.uri().path().to_string();
        let query = query_map(req.uri().query().unwrap_or(""));
        let body = req.into_body().collect().await?.to_bytes();
        let prefix = format!("/{}/", BUCKET);
        let key = percent_decode(path.strip_prefix(&prefix).unwrap_or(""));
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };

        if method == hyper::Method::PUT {
            let mut st = self.state.lock().unwrap();
            if query.contains_key("partnumber") && query.contains_key("uploadid") {
                // UploadPart or UploadPartCopy. This origin sends the part's ETag as a header
                // on both, so a wrongly staged copy part would carry a matching ETag.
                let copy = header("x-amz-copy-source").is_some();
                let xml = if copy {
                    "<CopyPartResult><ETag>\"part-copy-etag\"</ETag></CopyPartResult>"
                } else {
                    ""
                };
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(
                        "etag",
                        if copy {
                            "\"part-copy-etag\""
                        } else {
                            "\"part-etag\""
                        },
                    )
                    .header("content-length", xml.len().to_string())
                    .body(Full::new(Bytes::from_static(xml.as_bytes())))
                    .unwrap());
            }
            if query.contains_key("tagging") || query.contains_key("acl") {
                // The object is unchanged. This origin echoes the object's current ETag.
                let current = st.versions.get(&key).map_or(0, Vec::len);
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("etag", etag_of(current))
                    .header("content-length", "0")
                    .body(Full::new(Bytes::new()))
                    .unwrap());
            }
            if let Some(source) = header("x-amz-copy-source") {
                // `bucket/key` or `/bucket/key`, optionally `?versionId=vN`.
                let source = percent_decode(source.trim_start_matches('/'));
                let (source_path, version) = match source.split_once("?versionId=v") {
                    Some((p, v)) => (p.to_string(), Some(v.parse::<usize>().unwrap())),
                    None => (source, None),
                };
                let source_key = source_path
                    .strip_prefix(&format!("{}/", BUCKET))
                    .expect("same-bucket copy")
                    .to_string();
                let versions = st.versions.get(&source_key).expect("copy source exists");
                let copied = match version {
                    Some(v) => versions[v - 1].clone(),
                    None => versions.last().unwrap().clone(),
                };
                let target = st.versions.entry(key.clone()).or_default();
                target.push(copied);
                let new_etag = etag_of(target.len());
                let xml = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult><ETag>{}</ETag></CopyObjectResult>",
                    new_etag
                );
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("etag", new_etag)
                    .header("content-type", "application/xml")
                    .header("content-length", xml.len().to_string())
                    .body(Full::new(Bytes::from(xml)))
                    .unwrap());
            }
            let versions = st.versions.entry(key.clone()).or_default();
            versions.push(body);
            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header("content-length", "0");
            if !key.contains("noetag") {
                builder = builder.header("etag", etag_of(versions.len()));
            }
            return Ok(builder.body(Full::new(Bytes::new())).unwrap());
        }

        if method == hyper::Method::GET || method == hyper::Method::HEAD {
            let mut st = self.state.lock().unwrap();
            *st.gets.entry(path.clone()).or_insert(0) += 1;
            if query.contains_key("uploadid") {
                // ListParts: the upload's part list, with no ETag or Last-Modified.
                let xml = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListPartsResult><Key>{}</Key></ListPartsResult>",
                    key
                );
                let body = if method == hyper::Method::HEAD {
                    Bytes::new()
                } else {
                    Bytes::from(xml.clone())
                };
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/xml")
                    .header("content-length", xml.len().to_string())
                    .body(Full::new(body))
                    .unwrap());
            }
            let Some(versions) = st.versions.get(&key) else {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("content-length", "0")
                    .body(Full::new(Bytes::new()))
                    .unwrap());
            };
            let etag = etag_of(versions.len());
            let data = versions.last().unwrap().clone();
            if header("if-none-match").is_some_and(|v| v == etag) {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header("etag", etag)
                    .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                    .body(Full::new(Bytes::new()))
                    .unwrap());
            }
            let content_type = query
                .get("response-content-type")
                .cloned()
                .unwrap_or_else(|| OBJECT_TYPE.to_string());
            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header("content-length", data.len().to_string())
                .header("etag", etag)
                .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                .header("content-type", content_type);
            if let Some(disposition) = query.get("response-content-disposition") {
                builder = builder.header("content-disposition", disposition.as_str());
            }
            let body = if method == hyper::Method::HEAD {
                Bytes::new()
            } else {
                data
            };
            return Ok(builder.body(Full::new(body)).unwrap());
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

// ── Proxy under test ────────────────────────────────────────────────────────

struct Harness {
    dir: TempDir,
    _mock_shutdown: oneshot::Sender<()>,
    _proxy_shutdown: oneshot::Sender<()>,
    mock: MockS3,
    mock_host: String,
    proxy: SocketAddr,
}

struct Answer {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

async fn harness() -> Harness {
    harness_with(None, None).await
}

async fn harness_with(
    logger: Option<Arc<tokio::sync::Mutex<LoggerManager>>>,
    metrics: Option<Arc<tokio::sync::RwLock<MetricsManager>>>,
) -> Harness {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mock = MockS3::default();
    let (mock_addr, mock_shutdown) = mock.clone().spawn().await;
    let mock_host = format!("127.0.0.1:{}", mock_addr.port());

    let dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.cache.cache_dir = dir.path().to_path_buf();
    config.cache.ram_cache_enabled = false;
    config.cache.write_cache_enabled = true;
    config.cache.read_cache_enabled = true;
    config.cache.download_coordination.enabled = false;
    config.cache.get_ttl = Duration::from_secs(3600);
    config.cache.head_ttl = Duration::from_secs(3600);
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
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
                    let logger = logger.clone();
                    let metrics = metrics.clone();
                    tokio::spawn(async move {
                        let service = service_fn(move |req: Request<Incoming>| {
                            let config = Arc::clone(&config);
                            let cache_manager = Arc::clone(&cache_manager);
                            let range_handler = Arc::clone(&range_handler);
                            let s3_client = Arc::clone(&s3_client);
                            let inflight_tracker = Arc::clone(&inflight_tracker);
                            let request_semaphore = Arc::clone(&request_semaphore);
                            let inflight_ledger = s3_client.get_inflight_ledger();
                            let logger = logger.clone();
                            let metrics = metrics.clone();
                            async move {
                                HttpProxy::handle_request(
                                    req, peer, config, cache_manager, s3_client, range_handler,
                                    request_semaphore, metrics, logger, inflight_tracker, None, None,
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
    }
}

impl Harness {
    async fn send(
        &self,
        method: &str,
        path_and_query: &str,
        headers: &[(&str, &str)],
        body: Bytes,
    ) -> Answer {
        self.send_with(method, path_and_query, headers, body, true)
            .await
    }

    /// [`Self::send`], with or without the `Authorization` header (a presigned URL has none).
    async fn send_with(
        &self,
        method: &str,
        path_and_query: &str,
        headers: &[(&str, &str)],
        body: Bytes,
        auth: bool,
    ) -> Answer {
        let fut = async {
            let stream = TcpStream::connect(self.proxy).await.expect("connect");
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .expect("handshake");
            tokio::spawn(async move {
                let _ = conn.await;
            });
            let mut builder = Request::builder()
                .method(method)
                .uri(path_and_query)
                .header("host", self.mock_host.as_str())
                .header("content-length", body.len().to_string());
            if auth {
                builder = builder.header("authorization", AUTH);
            }
            for (k, v) in headers {
                builder = builder.header(*k, *v);
            }
            let resp = sender
                .send_request(builder.body(Full::new(body)).unwrap())
                .await
                .expect("proxy request");
            let status = resp.status();
            let headers = resp.headers().clone();
            let body = resp.into_body().collect().await.expect("body").to_bytes();
            Answer {
                status,
                headers,
                body,
            }
        };
        tokio::time::timeout(STEP, fut)
            .await
            .unwrap_or_else(|_| panic!("{} {} timed out", method, path_and_query))
    }

    fn raw(key: &str) -> String {
        format!("/{}/{}", BUCKET, key)
    }

    /// GET (or HEAD) `key` with `query`, reporting whether it reached the mock.
    async fn probe(&self, method: &str, key: &str, query: &str) -> (bool, Answer) {
        let raw = Self::raw(key);
        let before = self.mock.gets(&raw);
        let target = if query.is_empty() {
            raw.clone()
        } else {
            format!("{}?{}", raw, query)
        };
        let answer = self.send(method, &target, &[], Bytes::new()).await;
        (self.mock.gets(&raw) > before, answer)
    }

    /// GET `key` until a repeat is answered without reaching the mock: the entry is cached.
    async fn cache(&self, key: &str) -> Bytes {
        let (_, first) = self.probe("GET", key, "").await;
        assert_eq!(first.status, StatusCode::OK, "seed GET {}", key);
        for _ in 0..100 {
            let (reached, answer) = self.probe("GET", key, "").await;
            assert_eq!(answer.status, StatusCode::OK);
            assert_eq!(answer.body, first.body);
            if !reached {
                return first.body;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("{} never became a cache hit", key);
    }

    async fn put(&self, key: &str, query: &str, headers: &[(&str, &str)], body: &str) -> Answer {
        let target = if query.is_empty() {
            Self::raw(key)
        } else {
            format!("{}?{}", Self::raw(key), query)
        };
        self.send("PUT", &target, headers, Bytes::from(body.to_string()))
            .await
    }

    /// Files under `cache_dir/<tree>` with extension `ext` whose name contains `key`.
    fn cache_files(&self, tree: &str, key: &str, ext: &str) -> Vec<PathBuf> {
        fn walk(dir: &Path, key: &str, ext: &str, out: &mut Vec<PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, key, ext, out);
                } else if path.extension().is_some_and(|e| e == ext)
                    && path
                        .file_name()
                        .is_some_and(|n| n.to_string_lossy().contains(key))
                {
                    out.push(path);
                }
            }
        }
        let mut out = Vec::new();
        walk(&self.dir.path().join(tree), key, ext, &mut out);
        out
    }

    /// HEAD `key` until a repeat is answered without reaching the mock: the HEAD is cached.
    async fn cache_head(&self, key: &str) {
        let (_, first) = self.probe("HEAD", key, "").await;
        assert_eq!(first.status, StatusCode::OK, "seed HEAD {}", key);
        for _ in 0..100 {
            let (reached, answer) = self.probe("HEAD", key, "").await;
            assert_eq!(answer.status, StatusCode::OK);
            if !reached {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("HEAD {} never became a cache hit", key);
    }
}

/// Every access-log line under `dir/access`, after flushing the buffer.
async fn access_log_lines(
    logger: &Arc<tokio::sync::Mutex<LoggerManager>>,
    dir: &Path,
) -> Vec<String> {
    {
        logger.lock().await.force_flush().await.expect("flush");
        fn walk(dir: &Path, out: &mut Vec<String>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, out);
                } else {
                    let text = std::fs::read_to_string(&path).unwrap_or_default();
                    out.extend(text.lines().map(str::to_string));
                }
            }
        }
        let mut out = Vec::new();
        walk(&dir.join("access"), &mut out);
        out
    }
}

fn logger_in(dir: &Path) -> Arc<tokio::sync::Mutex<LoggerManager>> {
    let mut logger = LoggerManager::new(s3_proxy::logging::LoggingConfig {
        access_log_dir: dir.join("access"),
        app_log_dir: dir.join("app"),
        access_log_enabled: true,
        access_log_mode: s3_proxy::logging::AccessLogMode::All,
        hostname: "scn1-host".to_string(),
        log_level: "info".to_string(),
        // Long enough that only the test's own flush writes the file.
        access_log_flush_interval: Duration::from_secs(3600),
        access_log_buffer_size: 1000,
        access_log_retention_days: 30,
        app_log_retention_days: 30,
        log_cleanup_interval: Duration::from_secs(86400),
        access_log_file_rotation_interval: Duration::from_secs(300),
    });
    logger.initialize().expect("logger init");
    Arc::new(tokio::sync::Mutex::new(logger))
}

// ── Fix 1: an access-log record is written when the logger is busy ─────────

/// Red side: a request that completes while something else holds the logger is still logged.
#[tokio::test]
async fn a_request_finishing_while_the_logger_is_held_is_still_logged() {
    let logs = TempDir::new().unwrap();
    let logger = logger_in(logs.path());
    let h = harness_with(Some(Arc::clone(&logger)), None).await;
    h.mock.put("logged-a", "first logged body");
    h.mock.put("logged-b", "second logged body");

    // Stand-in for a concurrent request holding the logger (its flush, or the log sweep).
    let held = logger.lock().await;
    let (reached_a, a) = h.probe("GET", "logged-a", "").await;
    assert!(reached_a, "fixture: request A is a miss");
    assert_eq!(
        a.status,
        StatusCode::OK,
        "request A is answered while the logger is held"
    );
    drop(held);
    let (_, b) = h.probe("GET", "logged-b", "").await;
    assert_eq!(b.status, StatusCode::OK);

    let a_path = Harness::raw("logged-a");
    let b_path = Harness::raw("logged-b");
    let mut lines = Vec::new();
    for _ in 0..50 {
        lines = access_log_lines(&logger, logs.path()).await;
        if lines.iter().any(|l| l.contains(&a_path)) && lines.iter().any(|l| l.contains(&b_path)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let found = (
        lines.iter().filter(|l| l.contains(&a_path)).count(),
        lines.iter().filter(|l| l.contains(&b_path)).count(),
    );
    assert_eq!(
        found,
        (1, 1),
        "both requests must be logged once, including A, which finished while the logger was \
         held (records for A, B); lines: {:?}",
        lines
    );
}

// ── Fix 2: object subresources and unrecognised queries bypass the cache ────

/// Red side (a): a HEAD `?tagging` of a key whose HEAD is cached must reach S3.
#[tokio::test]
async fn a_head_with_tagging_of_a_cached_head_reaches_s3() {
    let h = harness().await;
    h.mock.put("head-tagged", "head tagged body");
    h.cache_head("head-tagged").await;
    let (reached, got) = h.probe("HEAD", "head-tagged", "tagging").await;
    assert_eq!(got.status, StatusCode::OK);
    assert!(
        reached,
        "a HEAD naming the tagging subresource must be forwarded, not answered from the \
         object's cached HEAD"
    );
}

/// Red side (b): a HEAD `?acl` on a miss must not leave a HEAD entry for the object.
#[tokio::test]
async fn a_head_with_acl_on_a_miss_stores_no_head_entry() {
    let h = harness().await;
    h.mock.put("head-acl", "head acl body");
    let (reached, _) = h.probe("HEAD", "head-acl", "acl").await;
    assert!(reached, "fixture: the HEAD ?acl is a miss");
    tokio::time::sleep(SETTLE).await;
    let (reached, plain) = h.probe("HEAD", "head-acl", "").await;
    assert_eq!(plain.status, StatusCode::OK);
    assert!(
        reached,
        "a plain HEAD after HEAD ?acl must reach S3 (false means the subresource HEAD was \
         cached as the object's)"
    );
}

/// Red side (c): a GET naming a query parameter the proxy does not recognise as part of an
/// object read is forwarded, not answered from the cached object.
#[tokio::test]
async fn a_get_with_an_unrecognised_query_of_a_cached_key_reaches_s3() {
    let h = harness().await;
    h.mock.put("unknown-query", "unknown query body");
    h.cache("unknown-query").await;
    let (reached, got) = h.probe("GET", "unknown-query", "restore-status").await;
    assert_eq!(got.status, StatusCode::OK);
    assert!(
        reached,
        "a GET with an unrecognised query parameter must be forwarded (fail closed)"
    );
}

/// Positive control: a signed GET carrying the SDK's `x-id` is still cached.
#[tokio::test]
async fn a_signed_get_with_x_id_is_still_cached() {
    let h = harness().await;
    h.mock.put("x-id-object", "x-id body");
    let (reached, first) = h.probe("GET", "x-id-object", "x-id=GetObject").await;
    assert!(reached, "fixture: the first GET is a miss");
    assert_eq!(first.status, StatusCode::OK);
    let mut hit = false;
    for _ in 0..100 {
        let (reached, again) = h.probe("GET", "x-id-object", "x-id=GetObject").await;
        assert_eq!(again.body, first.body);
        if !reached {
            hit = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        hit,
        "a signed GET with x-id=GetObject must become a cache hit"
    );
}

/// Positive control: a presigned GET is served from the cache an ordinary signed GET filled.
#[tokio::test]
async fn a_presigned_get_of_a_cached_key_is_a_hit() {
    let h = harness().await;
    h.mock.put("presigned-object", "presigned body");
    let body = h.cache("presigned-object").await;
    let now = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let query = format!(
        "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date={}&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=0000", // gitleaks:allow
        now
    );
    let raw = Harness::raw("presigned-object");
    let before = h.mock.gets(&raw);
    let got = h
        .send_with(
            "GET",
            &format!("{}?{}", raw, query),
            &[],
            Bytes::new(),
            false,
        )
        .await;
    assert_eq!(got.status, StatusCode::OK);
    assert_eq!(
        (h.mock.gets(&raw) > before, got.body),
        (false, body),
        "a presigned GET of a cached key must be served from the cache"
    );
}

// ── Fix 5: Content-MD5 is not served on reads ───────────────────────────────

/// Red side: a write-through PUT that carried `Content-MD5` must not have it served on a hit.
#[tokio::test]
async fn a_write_through_hit_does_not_serve_the_puts_content_md5() {
    let h = harness().await;
    let put = h
        .put(
            "md5-object",
            "",
            &[
                ("content-md5", "Q2hlY2sgSW50ZWdyaXR5IQ=="),
                ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
            ],
            "md5 body",
        )
        .await;
    assert_eq!(put.status, StatusCode::OK, "fixture: S3 accepts the PUT");
    // Observe that the write-through store kept the request's Content-MD5, so a hit could
    // serve it: the mock never sends one.
    let mut stored = String::new();
    for _ in 0..200 {
        stored = h
            .cache_files("metadata", "md5-object", "meta")
            .iter()
            .map(|p| std::fs::read_to_string(p).unwrap_or_default())
            .collect();
        if stored.to_ascii_lowercase().contains("content-md5") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        stored.to_ascii_lowercase().contains("content-md5"),
        "fixture: the PUT's entry does not hold Content-MD5 (apparatus, verdict UNKNOWN); \
         .meta: {:?}",
        stored
    );
    let mut hit = None;
    for _ in 0..100 {
        let (reached, got) = h.probe("GET", "md5-object", "").await;
        assert_eq!(got.status, StatusCode::OK);
        assert_eq!(got.body, Bytes::from_static(b"md5 body"));
        if !reached {
            hit = Some(got);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let hit = hit.expect("the PUT's entry never served a cache hit (apparatus, verdict UNKNOWN)");
    assert_eq!(
        hit.header("content-md5"),
        None,
        "S3 does not return Content-MD5 on GET; a cache hit must not either"
    );
}

// ── Fix 6: multi-range forwards are counted ─────────────────────────────────

/// Red side: a multi-range GET is counted under `cache_bypasses_by_reason`.
#[tokio::test]
async fn a_multi_range_get_is_counted_as_a_bypass() {
    let metrics = Arc::new(tokio::sync::RwLock::new(MetricsManager::new()));
    let h = harness_with(None, Some(Arc::clone(&metrics))).await;
    h.mock.put("multi-range", "0123456789abcdefghij");
    let raw = Harness::raw("multi-range");
    let got = h
        .send("GET", &raw, &[("range", "bytes=0-1,5-6")], Bytes::new())
        .await;
    assert_eq!(
        got.status,
        StatusCode::OK,
        "fixture: forwarded, S3 answers 200"
    );
    let mut count = 0;
    for _ in 0..40 {
        count = metrics
            .read()
            .await
            .get_cache_bypass_count("multi_range")
            .await;
        if count > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        count, 1,
        "one multi-range GET must record one cache_bypasses_by_reason.multi_range"
    );
}

// ── fail-closed GET bypasses are counted; invalid partNumber is not an object read ──

/// Poll `cache_bypasses_by_reason[reason]` until it is non-zero (the count is recorded on a
/// detached task) and return it.
async fn bypass_count(metrics: &Arc<tokio::sync::RwLock<MetricsManager>>, reason: &str) -> u64 {
    let mut count = 0;
    for _ in 0..40 {
        count = metrics.read().await.get_cache_bypass_count(reason).await;
        if count > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    count
}

/// Red side: a GET with `?foo=1` is forwarded and counted under `unrecognised_query`.
#[tokio::test]
async fn a_get_with_an_unrecognised_query_is_counted_as_a_bypass() {
    let metrics = Arc::new(tokio::sync::RwLock::new(MetricsManager::new()));
    let h = harness_with(None, Some(Arc::clone(&metrics))).await;
    h.mock.put("counted-unknown", "counted unknown body");
    let (reached, got) = h.probe("GET", "counted-unknown", "foo=1").await;
    assert_eq!(got.status, StatusCode::OK);
    assert!(reached, "fixture: the GET is forwarded");
    assert_eq!(
        bypass_count(&metrics, "unrecognised_query").await,
        1,
        "one GET with an unrecognised parameter must record one \
         cache_bypasses_by_reason.unrecognised_query"
    );
}

/// A HEAD with an unrecognised parameter uses the same key as the GET.
#[tokio::test]
async fn a_head_with_an_unrecognised_query_uses_the_same_bypass_key() {
    let metrics = Arc::new(tokio::sync::RwLock::new(MetricsManager::new()));
    let h = harness_with(None, Some(Arc::clone(&metrics))).await;
    h.mock
        .put("counted-unknown-head", "counted unknown head body");
    let (reached, got) = h.probe("HEAD", "counted-unknown-head", "foo=1").await;
    assert_eq!(got.status, StatusCode::OK);
    assert!(reached, "fixture: the HEAD is forwarded");
    assert_eq!(
        bypass_count(&metrics, "unrecognised_query").await,
        1,
        "a HEAD with an unrecognised parameter must record unrecognised_query too"
    );
}

/// Control: a plain GET records no `unrecognised_query` count.
#[tokio::test]
async fn a_plain_get_records_no_unrecognised_query_count() {
    let metrics = Arc::new(tokio::sync::RwLock::new(MetricsManager::new()));
    let h = harness_with(None, Some(Arc::clone(&metrics))).await;
    h.mock.put("plain-counted", "plain counted body");
    h.cache("plain-counted").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        metrics
            .read()
            .await
            .get_cache_bypass_count("unrecognised_query")
            .await,
        0
    );
}

/// Red side: `?partNumber=0` and `?partNumber=abc` on a cached key reach S3.
#[tokio::test]
async fn a_get_with_an_invalid_part_number_of_a_cached_key_reaches_s3() {
    let h = harness().await;
    h.mock.put("bad-part", "bad part body");
    h.cache("bad-part").await;
    for query in ["partNumber=0", "partNumber=abc", "partNumber=10001"] {
        let (reached, _) = h.probe("GET", "bad-part", query).await;
        assert!(
            reached,
            "a GET with ?{} must be forwarded to S3, not served from the cached object",
            query
        );
    }
}

/// Red side: an invalid `partNumber` on a miss must not fill the whole-object entry.
#[tokio::test]
async fn a_get_with_an_invalid_part_number_on_a_miss_fills_no_entry() {
    let h = harness().await;
    h.mock.put("bad-part-miss", "bad part miss body");
    let (reached, _) = h.probe("GET", "bad-part-miss", "partNumber=abc").await;
    assert!(reached, "fixture: the GET is a miss");
    tokio::time::sleep(SETTLE).await;
    let (reached, _) = h.probe("GET", "bad-part-miss", "").await;
    assert!(
        reached,
        "a plain GET after ?partNumber=abc must reach S3 (false means the invalid-part GET \
         filled the object's cache entry)"
    );
}
