//! Response-header overrides and non-object PUTs never reach the cache.
//!
//! Every test drives real requests through `HttpProxy::handle_request` to a loopback mock S3
//! reached over a plaintext upstream override. The mock keeps per-key object versions,
//! applies `response-*` query overrides to the headers it returns the way S3 does, answers a
//! conditional GET whose `If-None-Match` names the current ETag with `304` (carrying that
//! ETag), and answers subresource and copy PUTs the way an origin that sends an `ETag` header
//! on them would. That last behaviour is what lets a wrongly stored entry be served: the
//! proxy's revalidation of it gets a `304` with a matching ETag.
//!
//! A GET is only treated as cached once a repeat of it has been served without the mock
//! seeing it, so the "before" state is observed rather than assumed.

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

    fn version_count(&self, key: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .versions
            .get(key)
            .map_or(0, Vec::len)
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
                .header("authorization", AUTH)
                .header("content-length", body.len().to_string());
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

    /// Range files under the cache whose name contains `key`.
    fn range_files(&self, key: &str) -> Vec<PathBuf> {
        self.cache_files("ranges", key, "bin")
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
}

// ── Fix 1: response-header overrides ────────────────────────────────────────

/// Red side (a): an override GET on a miss must not leave its headers for a plain GET.
#[tokio::test]
async fn an_override_get_on_a_miss_does_not_set_a_plain_gets_content_type() {
    let h = harness().await;
    h.mock.put("page", "<p>not html to this client</p>");
    let (reached, over) = h
        .probe("GET", "page", "response-content-type=text%2Fhtml")
        .await;
    assert!(reached, "fixture: the override GET is a miss");
    assert_eq!(over.status, StatusCode::OK);
    assert_eq!(
        over.header("content-type"),
        Some("text/html"),
        "the override GET gets S3's overridden header"
    );
    tokio::time::sleep(SETTLE).await;
    let (reached, plain) = h.probe("GET", "page", "").await;
    assert_eq!(
        (reached, plain.header("content-type")),
        (true, Some(OBJECT_TYPE)),
        "a plain GET after an override GET must reach S3 and carry the object's own \
         Content-Type (false + text/html means the override response was cached)"
    );
}

/// Red side (b): an override GET of a cached key must reach S3 and keep its override.
#[tokio::test]
async fn an_override_get_of_a_cached_key_reaches_s3() {
    let h = harness().await;
    h.mock.put("report", "report body");
    h.cache("report").await;
    let (reached, over) = h
        .probe("GET", "report", "response-content-disposition=attachment")
        .await;
    assert_eq!(over.status, StatusCode::OK);
    assert_eq!(
        (reached, over.header("content-disposition")),
        (true, Some("attachment")),
        "an override GET of a cached key must be forwarded and carry the override \
         (false + None means it was served from cache without it)"
    );
}

/// HEAD takes the same bypass: an override HEAD must not set a plain HEAD's headers.
#[tokio::test]
async fn an_override_head_on_a_miss_does_not_set_a_plain_heads_content_type() {
    let h = harness().await;
    h.mock.put("headed", "headed body");
    let (reached, over) = h
        .probe("HEAD", "headed", "response-content-type=text%2Fhtml")
        .await;
    assert!(reached, "fixture: the override HEAD is a miss");
    assert_eq!(over.header("content-type"), Some("text/html"));
    tokio::time::sleep(SETTLE).await;
    let (reached, plain) = h.probe("HEAD", "headed", "").await;
    assert_eq!(
        (reached, plain.header("content-type")),
        (true, Some(OBJECT_TYPE)),
        "a plain HEAD after an override HEAD must reach S3 and carry the object's own \
         Content-Type"
    );
}

/// The no-cache-directive arm, the one bypass that still writes the cache, never stores an
/// override response.
#[tokio::test]
async fn a_no_cache_override_get_does_not_set_a_plain_gets_content_type() {
    let h = harness().await;
    // Control: a no-cache GET without an override is stored, so this arm writes.
    h.mock.put("nocache-control", "no-cache control body");
    let control = h
        .send(
            "GET",
            &Harness::raw("nocache-control"),
            &[("cache-control", "no-cache")],
            Bytes::new(),
        )
        .await;
    assert_eq!(control.status, StatusCode::OK);
    tokio::time::sleep(SETTLE).await;
    let (reached, _) = h.probe("GET", "nocache-control", "").await;
    assert!(
        !reached,
        "control: the no-cache arm did not store a plain response (apparatus, verdict UNKNOWN)"
    );

    h.mock.put("nocache-page", "<p>no-cache page</p>");
    let over = h
        .send(
            "GET",
            &format!(
                "{}?response-content-type=text%2Fhtml",
                Harness::raw("nocache-page")
            ),
            &[("cache-control", "no-cache")],
            Bytes::new(),
        )
        .await;
    assert_eq!(over.status, StatusCode::OK);
    assert_eq!(over.header("content-type"), Some("text/html"));
    tokio::time::sleep(SETTLE).await;
    let (reached, plain) = h.probe("GET", "nocache-page", "").await;
    assert_eq!(
        (reached, plain.header("content-type")),
        (true, Some(OBJECT_TYPE)),
        "a plain GET after a no-cache override GET must reach S3 and carry the object's own \
         Content-Type (false + text/html means the no-cache arm stored the override response)"
    );
}

/// The parameter name matches after percent-decoding and ignoring case.
#[tokio::test]
async fn an_encoded_mixed_case_override_name_is_recognised() {
    let h = harness().await;
    h.mock.put("mixed", "mixed body");
    let (reached, over) = h
        .probe("GET", "mixed", "Response%2DContent-Type=text%2Fhtml")
        .await;
    assert!(reached);
    assert_eq!(over.header("content-type"), Some("text/html"));
    tokio::time::sleep(SETTLE).await;
    let (reached, plain) = h.probe("GET", "mixed", "").await;
    assert_eq!(
        (reached, plain.header("content-type")),
        (true, Some(OBJECT_TYPE)),
        "`Response%2DContent-Type` is the same override as `response-content-type`"
    );
}

// ── Fix 2: subresource and copy PUTs ────────────────────────────────────────

/// Red side (a): a tagging PUT's XML body must never be served as the object.
#[tokio::test]
async fn a_tagging_put_is_not_served_as_the_object() {
    let h = harness().await;
    h.mock.put("tagged", "the real object");
    let real = h.cache("tagged").await;
    let xml = "<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>";
    let put = h.put("tagged", "tagging", &[], xml).await;
    assert_eq!(
        put.status,
        StatusCode::OK,
        "fixture: S3 accepts the tagging"
    );
    assert_eq!(
        h.mock.version_count("tagged"),
        1,
        "tagging leaves the object"
    );
    tokio::time::sleep(SETTLE).await;
    let (reached, got) = h.probe("GET", "tagged", "").await;
    assert_eq!(got.status, StatusCode::OK);
    assert_eq!(
        (reached, got.body.clone()),
        (true, real),
        "after a tagging PUT the next GET must reach S3 (the key is invalidated) and \
         return the object, never the tagging document"
    );
}

/// Red side (b): a CopyObject onto a cached key must leave a miss that returns the copy.
#[tokio::test]
async fn a_copy_onto_a_cached_key_leaves_a_miss_with_the_copy() {
    let h = harness().await;
    h.mock.put("target", "old target bytes");
    h.mock.put("source", "copied source bytes");
    h.cache("target").await;
    let copy_source = format!("{}/source", BUCKET);
    let put = h
        .put(
            "target",
            "",
            &[("x-amz-copy-source", copy_source.as_str())],
            "",
        )
        .await;
    assert_eq!(put.status, StatusCode::OK, "fixture: S3 accepts the copy");
    tokio::time::sleep(SETTLE).await;
    // The copy request has no body: storing it would record a zero-byte object.
    let metas = h.cache_files("metadata", "target", "meta");
    let stored: Vec<String> = metas
        .iter()
        .map(|p| std::fs::read_to_string(p).unwrap_or_default())
        .collect();
    assert_eq!(
        stored,
        Vec::<String>::new(),
        "after CopyObject the key must hold no cache entry until a GET fetches the copy"
    );
    let (reached, got) = h.probe("GET", "target", "").await;
    assert_eq!(got.status, StatusCode::OK);
    assert_eq!(
        (reached, got.body.clone()),
        (true, Bytes::from_static(b"copied source bytes")),
        "a GET after CopyObject must be a miss returning the copied bytes (false means a \
         cached copy was served: the old object, or zero bytes stored from the copy request)"
    );
}

/// The rollback in docs/CACHING.md: copy an older version of the key onto itself.
#[tokio::test]
async fn a_copy_object_rollback_serves_the_restored_version() {
    let h = harness().await;
    h.mock.put("doc", "version one");
    h.mock.put("doc", "version two");
    assert_eq!(h.cache("doc").await, Bytes::from_static(b"version two"));
    let copy_source = format!("{}/doc?versionId=v1", BUCKET);
    let put = h
        .put(
            "doc",
            "",
            &[("x-amz-copy-source", copy_source.as_str())],
            "",
        )
        .await;
    assert_eq!(put.status, StatusCode::OK);
    tokio::time::sleep(SETTLE).await;
    let (reached, got) = h.probe("GET", "doc", "").await;
    assert_eq!(
        (reached, got.body.clone()),
        (true, Bytes::from_static(b"version one")),
        "after a copy-object rollback the next GET must fetch the restored version"
    );
    assert_eq!(
        h.cache("doc").await,
        Bytes::from_static(b"version one"),
        "and the restored version is what gets cached"
    );
}

/// A single PUT whose S3 response has no ETag is forwarded but not stored.
#[tokio::test]
async fn a_put_answered_without_an_etag_is_not_stored() {
    let h = harness().await;
    let put = h.put("noetag-object", "", &[], "no validator body").await;
    assert_eq!(put.status, StatusCode::OK, "fixture: S3 accepts the PUT");
    // Control: the same shape of PUT with an ETag is stored, so the apparatus sees stores.
    let control = h.put("control-object", "", &[], "validated body").await;
    assert_eq!(control.status, StatusCode::OK);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while h.range_files("control-object").is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "control: a PUT answered with an ETag was never stored, so this test cannot \
             observe a store (apparatus, verdict UNKNOWN)"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        h.range_files("noetag-object"),
        Vec::<PathBuf>::new(),
        "a PUT answered without an ETag must not be write-through cached"
    );
}

// ── ListParts (`?uploadId`) ─────────────────────────────────────────────────

/// A ListParts GET of a cached key must reach S3 and return the part list, not the object.
#[tokio::test]
async fn list_parts_of_a_cached_key_reaches_s3() {
    let h = harness().await;
    h.mock.put("listed", "listed object bytes");
    h.cache("listed").await;
    let (reached, got) = h.probe("GET", "listed", "uploadId=scn1-list-1").await;
    assert_eq!(got.status, StatusCode::OK);
    assert_eq!(
        (
            reached,
            String::from_utf8_lossy(&got.body).contains("ListPartsResult")
        ),
        (true, true),
        "a ListParts GET must be forwarded and return S3's part list; got body {:?}",
        got.body
    );
}

/// A ListParts GET on a miss must store nothing under the object's key.
#[tokio::test]
async fn list_parts_on_a_miss_stores_nothing_for_the_object() {
    let h = harness().await;
    h.mock.put("unlisted", "unlisted object bytes");
    let (reached, got) = h.probe("GET", "unlisted", "uploadId=scn1-list-2").await;
    assert!(reached, "fixture: the ListParts GET is a miss");
    assert!(String::from_utf8_lossy(&got.body).contains("ListPartsResult"));
    tokio::time::sleep(SETTLE).await;
    assert_eq!(
        h.cache_files("metadata", "unlisted", "meta"),
        Vec::<PathBuf>::new(),
        "a ListParts response must not create a cache entry for the object"
    );
    let (_, plain) = h.probe("GET", "unlisted", "").await;
    assert_eq!(
        plain.body,
        Bytes::from_static(b"unlisted object bytes"),
        "a plain GET after ListParts must return the object"
    );
}

/// A HEAD with `uploadId` of a key whose HEAD is cached is forwarded too.
#[tokio::test]
async fn a_head_with_upload_id_of_a_cached_key_reaches_s3() {
    let h = harness().await;
    h.mock.put("headlisted", "headlisted bytes");
    // Cache the HEAD itself, observed: a GET-warmed key leaves a HEAD miss, which would
    // reach S3 whether or not the `uploadId` guard exists.
    let mut head_cached = false;
    for _ in 0..100 {
        let (reached, head) = h.probe("HEAD", "headlisted", "").await;
        assert_eq!(head.status, StatusCode::OK);
        if !reached {
            head_cached = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        head_cached,
        "fixture: a plain HEAD of headlisted never became a cache hit"
    );
    let (reached, got) = h.probe("HEAD", "headlisted", "uploadId=scn1-list-3").await;
    assert_eq!(
        (reached, got.header("content-type")),
        (true, Some("application/xml")),
        "a HEAD naming an uploadId must be forwarded, not answered from the object's entry"
    );
}

// ── UploadPartCopy ──────────────────────────────────────────────────────────

/// An UploadPartCopy carries no part bytes: nothing is staged, and the key is invalidated.
#[tokio::test]
async fn an_upload_part_copy_stages_nothing() {
    let h = harness().await;
    h.mock.put("assembled", "assembled old bytes");
    h.mock.put("part-source", "part source bytes");
    h.cache("assembled").await;
    let cache_dir = h.dir.path().to_path_buf();
    let copy_source = format!("{}/part-source", BUCKET);
    let copy = h
        .put(
            "assembled",
            "partNumber=1&uploadId=scn1-copy-upload",
            &[("x-amz-copy-source", copy_source.as_str())],
            "",
        )
        .await;
    assert_eq!(
        copy.status,
        StatusCode::OK,
        "fixture: S3 accepts the copy part"
    );
    // Control: a plain UploadPart on another upload stages a part, so the apparatus can
    // observe staging.
    let control = h
        .put(
            "control-mpu",
            "partNumber=1&uploadId=scn1-control-upload",
            &[],
            "control part body",
        )
        .await;
    assert_eq!(control.status, StatusCode::OK);
    let control_dir =
        s3_proxy::signed_put_handler::mpu_upload_dir(&cache_dir, "scn1-control-upload");
    let staged = |dir: &Path| -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .map(|entries| entries.flatten().map(|e| e.path()).collect())
            .unwrap_or_default()
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !staged(&control_dir)
        .iter()
        .any(|p| p.extension().is_some_and(|e| e == "json"))
    {
        assert!(
            std::time::Instant::now() < deadline,
            "control: a plain UploadPart never staged a part record, so this test cannot \
             observe staging (apparatus, verdict UNKNOWN); found {:?}",
            staged(&control_dir)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let copy_dir = s3_proxy::signed_put_handler::mpu_upload_dir(&cache_dir, "scn1-copy-upload");
    assert_eq!(
        staged(&copy_dir),
        Vec::<PathBuf>::new(),
        "an UploadPartCopy must stage no part (a zero-byte part with the copy's ETag would \
         be assembled into the object)"
    );
    let (reached, _) = h.probe("GET", "assembled", "").await;
    assert!(
        reached,
        "an UploadPartCopy into a cached key invalidates it, like every non-object PUT"
    );
}
