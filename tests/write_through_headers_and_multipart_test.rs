//! Write-through representation headers and the multipart control path.
//!
//! Every test drives real requests through `HttpProxy::handle_request` to a loopback mock S3
//! reached over a plaintext upstream override. The mock answers GET and HEAD with only
//! `content-length`, `etag` and `last-modified`, and a conditional GET naming the current
//! ETag with `304`. It never sends a representation header (`content-type`,
//! `x-amz-meta-*` and the rest) on a read, so any such header on a proxy answer came from the
//! cache entry the write stored. That is what keeps the header assertions from passing
//! vacuously on a forwarded read.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
const STEP: Duration = Duration::from_secs(30);
/// Time for a detached cache store or finalization (and the 100 ms consolidation loop) to
/// land, so a defective store has published before the next request looks.
const SETTLE: Duration = Duration::from_millis(1500);
/// What S3 sends, byte for byte, when a CompleteMultipartUpload that it already answered
/// `200 OK` fails while it assembles the object.
const ERROR_DOCUMENT: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>InternalError</Code><Message>We encountered an internal error. Please try again.</Message><RequestId>SCN1REQ</RequestId><HostId>SCN1HOST</HostId></Error>";

// ── Mock S3 ─────────────────────────────────────────────────────────────────

#[derive(Default)]
struct MockState {
    /// Key -> (body, quoted ETag).
    objects: HashMap<String, (Bytes, String)>,
    /// Upload id -> part number -> body.
    uploads: HashMap<String, BTreeMap<u32, Bytes>>,
    /// Raw request path -> GET/HEAD count.
    gets: HashMap<String, usize>,
    /// CreateMultipartUpload requests seen.
    creates: usize,
    /// Monotonic counter for ETags and upload ids.
    counter: usize,
}

#[derive(Clone, Default)]
struct MockS3 {
    state: Arc<Mutex<MockState>>,
}

fn query_map(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let name = parts.next().unwrap_or("").to_ascii_lowercase();
            let value = parts.next().unwrap_or("").to_string();
            (name, value)
        })
        .collect()
}

fn xml(status: StatusCode, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/xml")
        .header("content-length", body.len().to_string())
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

impl MockS3 {
    /// Write `key` directly, as another client would.
    fn put(&self, key: &str, body: &str) {
        let mut st = self.state.lock().unwrap();
        st.counter += 1;
        let etag = format!("\"etag-{}\"", st.counter);
        st.objects
            .insert(key.to_string(), (Bytes::from(body.to_string()), etag));
    }

    fn gets(&self, raw_path: &str) -> usize {
        *self.state.lock().unwrap().gets.get(raw_path).unwrap_or(&0)
    }

    fn creates(&self) -> usize {
        self.state.lock().unwrap().creates
    }

    async fn handle(self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let headers = req.headers().clone();
        let path = req.uri().path().to_string();
        let query = query_map(req.uri().query().unwrap_or(""));
        let body = req.into_body().collect().await?.to_bytes();
        let key = path
            .strip_prefix(&format!("/{}/", BUCKET))
            .unwrap_or("")
            .to_string();
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };

        if method == hyper::Method::PUT {
            let mut st = self.state.lock().unwrap();
            if let (Some(upload), Some(part)) = (query.get("uploadid"), query.get("partnumber")) {
                let part: u32 = part.parse().unwrap();
                st.uploads
                    .entry(upload.clone())
                    .or_default()
                    .insert(part, body);
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("etag", format!("\"part-etag-{}\"", part))
                    .header("content-length", "0")
                    .body(Full::new(Bytes::new()))
                    .unwrap());
            }
            st.counter += 1;
            let n = st.counter;
            let etag = format!("\"etag-{}\"", n);
            st.objects.insert(key, (body, etag.clone()));
            // What S3 sends on a PutObject: no representation headers.
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("etag", etag)
                .header("x-amz-server-side-encryption", "AES256")
                .header("x-amz-version-id", format!("v{}", n))
                .header("content-length", "0")
                .body(Full::new(Bytes::new()))
                .unwrap());
        }

        if method == hyper::Method::POST {
            let mut st = self.state.lock().unwrap();
            if query.contains_key("uploads") {
                st.creates += 1;
                st.counter += 1;
                let upload = format!("SCN1UPLOAD{}", st.counter);
                st.uploads.insert(upload.clone(), BTreeMap::new());
                return Ok(xml(
                    StatusCode::OK,
                    format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
                        BUCKET, key, upload
                    ),
                ));
            }
            if let Some(upload) = query.get("uploadid") {
                if key.contains("errdoc") {
                    return Ok(xml(StatusCode::OK, ERROR_DOCUMENT.to_string()));
                }
                let parts = st.uploads.remove(upload).unwrap_or_default();
                let count = body
                    .windows(b"<PartNumber>".len())
                    .filter(|w| *w == b"<PartNumber>")
                    .count();
                let data: Vec<u8> = parts.values().flat_map(|b| b.to_vec()).collect();
                let etag = format!("\"0123456789abcdef0123456789abcdef-{}\"", count);
                st.objects
                    .insert(key.clone(), (Bytes::from(data), etag.clone()));
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/xml")
                    .header("x-amz-server-side-encryption", "AES256")
                    .body(Full::new(Bytes::from(format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
                        BUCKET,
                        key,
                        etag.replace('"', "&quot;")
                    ))))
                    .unwrap());
            }
        }

        if method == hyper::Method::GET || method == hyper::Method::HEAD {
            let mut st = self.state.lock().unwrap();
            *st.gets.entry(path.clone()).or_insert(0) += 1;
            let Some((data, etag)) = st.objects.get(&key).cloned() else {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("content-length", "0")
                    .body(Full::new(Bytes::new()))
                    .unwrap());
            };
            if header("if-none-match").is_some_and(|v| v == etag) {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header("etag", etag)
                    .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                    .body(Full::new(Bytes::new()))
                    .unwrap());
            }
            // A multi-range `Range` is answered the way S3 answers it (measured): `200` with
            // the whole object. A single range is not used by these tests.
            let body = if method == hyper::Method::HEAD {
                Bytes::new()
            } else {
                data.clone()
            };
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-length", data.len().to_string())
                .header("etag", etag)
                .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
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
}

impl Harness {
    /// The upload directories under `mpus_in_progress/`.
    fn staging_dirs(&self) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(self.dir.path().join("mpus_in_progress")) else {
            return Vec::new();
        };
        entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect()
    }

    /// Wait, bounded, for the object `.meta` of `key` to appear (a finalization runs after
    /// the client's response).
    async fn await_meta(&self, key: &str) -> bool {
        for _ in 0..100 {
            if !self.cache_files("metadata", key, "meta").is_empty() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    /// GET `key` until a repeat is answered without reaching the mock: a cache hit.
    async fn hit(&self, key: &str) -> Answer {
        for _ in 0..50 {
            let (reached, answer) = self.probe("GET", key, "").await;
            if !reached {
                return answer;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("{} never became a cache hit", key);
    }

    /// CreateMultipartUpload through the proxy; returns the upload id from the XML.
    async fn create(&self, key: &str, headers: &[(&str, &str)]) -> String {
        let answer = self
            .send(
                "POST",
                &format!("{}?uploads", Self::raw(key)),
                headers,
                Bytes::new(),
            )
            .await;
        assert_eq!(answer.status, StatusCode::OK, "CreateMultipartUpload");
        let body = String::from_utf8(answer.body.to_vec()).unwrap();
        let start = body.find("<UploadId>").expect("UploadId") + "<UploadId>".len();
        let end = body.find("</UploadId>").expect("/UploadId");
        body[start..end].to_string()
    }

    async fn upload_part(&self, key: &str, upload: &str, part: u32, body: &str) {
        let answer = self
            .put(
                key,
                &format!("partNumber={}&uploadId={}", part, upload),
                &[],
                body,
            )
            .await;
        assert_eq!(answer.status, StatusCode::OK, "UploadPart {}", part);
    }

    /// CompleteMultipartUpload naming `parts`, timed.
    async fn complete(&self, key: &str, upload: &str, parts: &[u32]) -> (Answer, Duration) {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for part in parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>\"part-etag-{}\"</ETag></Part>",
                part, part
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let started = Instant::now();
        let answer = self
            .send(
                "POST",
                &format!("{}?uploadId={}", Self::raw(key), upload),
                &[],
                Bytes::from(xml),
            )
            .await;
        (answer, started.elapsed())
    }
}

/// Every representation header a PUT sets, and the value S3 would return on a GET.
const REPRESENTATION: &[(&str, &str)] = &[
    ("content-type", "application/json"),
    ("content-encoding", "gzip"),
    ("content-disposition", "attachment; filename=\"scn1.json\""),
    ("content-language", "en-GB"),
    ("cache-control", "max-age=3600"),
    ("expires", "Thu, 01 Jan 2032 00:00:00 GMT"),
    ("x-amz-meta-a", "b"),
    ("x-amz-storage-class", "STANDARD_IA"),
    ("x-amz-website-redirect-location", "/elsewhere.html"),
];

/// The headers an answer is missing or carries with a different value, of `expected`.
fn differences(answer: &Answer, expected: &[(&str, &str)]) -> Vec<String> {
    expected
        .iter()
        .filter(|(name, value)| answer.header(name) != Some(*value))
        .map(|(name, _)| format!("{}={:?}", name, answer.header(name)))
        .collect()
}

// ── Fix 1: write-through entries carry the object's representation headers ─────

/// Red side: a single-part PUT's cached entry is served without the headers the PUT set.
#[tokio::test]
async fn a_put_entry_is_served_with_its_representation_headers() {
    let h = harness().await;
    let mut headers: Vec<(&str, &str)> = REPRESENTATION.to_vec();
    headers.push(("x-amz-tagging", "t1=v1&t2=v2"));
    headers.push(("x-amz-content-sha256", "UNSIGNED-PAYLOAD"));
    headers.push(("content-md5", "q1Vd3PGQUDmg5NYuJ8Xcuw=="));
    let put = h.put("repr", "", &headers, "{\"scn1\":true}").await;
    assert_eq!(put.status, StatusCode::OK);
    assert!(
        h.await_meta("repr").await,
        "fixture: the PUT was write-through cached"
    );

    let mut expected: Vec<(&str, &str)> = REPRESENTATION.to_vec();
    expected.push(("x-amz-tagging-count", "2"));
    expected.push(("x-amz-server-side-encryption", "AES256"));
    let (_, get) = h.probe("GET", "repr", "").await;
    assert_eq!(get.status, StatusCode::OK);
    assert_eq!(get.body, Bytes::from_static(b"{\"scn1\":true}"));
    // The first GET revalidates (a write-through entry has no Last-Modified) and the `304`
    // carries no representation header; the next is a plain hit. A HEAD of a write-through
    // entry is forwarded to S3 by design (the entry has no HEAD freshness), so S3 supplies its
    // headers and it is not asserted here.
    let hit = h.hit("repr").await;
    assert_eq!(
        (differences(&get, &expected), differences(&hit, &expected)),
        (Vec::<String>::new(), Vec::<String>::new()),
        "a revalidated GET and a cache-hit GET of a write-through entry must carry the PUT's \
         representation headers (the mock never sends them on a read, so they can only come \
         from the entry)"
    );
    assert_eq!(
        (
            get.header("x-amz-content-sha256"),
            get.header("x-amz-tagging")
        ),
        (None, None),
        "request-only headers must not be served back on a read"
    );
}

/// `x-amz-storage-class: STANDARD` is not returned by S3 on a read, so it is not stored.
#[tokio::test]
async fn a_standard_storage_class_is_not_served() {
    let h = harness().await;
    let put = h
        .put(
            "standard",
            "",
            &[
                ("x-amz-storage-class", "STANDARD"),
                ("content-type", "text/plain"),
            ],
            "plain",
        )
        .await;
    assert_eq!(put.status, StatusCode::OK);
    assert!(h.await_meta("standard").await);
    let (_, get) = h.probe("GET", "standard", "").await;
    assert_eq!(
        (
            get.header("content-type"),
            get.header("x-amz-storage-class")
        ),
        (Some("text/plain"), None)
    );
}

// ── Fixes 1 and 2: the multipart write-through entry, via CreateMultipartUpload ──

/// Red side (fix 2): a signed CreateMultipartUpload must reach the signed handler, which
/// records the upload before any part arrives.
#[tokio::test]
async fn a_create_multipart_upload_records_the_upload() {
    let h = harness().await;
    let upload = h
        .create("mpu-routed", &[("content-type", "text/csv")])
        .await;
    assert_eq!(h.mock.creates(), 1, "the Create reached S3 unchanged");
    let dirs = h.staging_dirs();
    let recorded: Vec<bool> = dirs
        .iter()
        .map(|d| d.join("upload.meta").exists())
        .collect();
    assert_eq!(
        recorded,
        vec![true],
        "a CreateMultipartUpload must write its upload tracker (upload {})",
        upload
    );
}

/// Red side (fix 1, multipart): the completed object is served with the headers its
/// CreateMultipartUpload set.
#[tokio::test]
async fn a_multipart_entry_is_served_with_its_create_headers() {
    let h = harness().await;
    let upload = h.create("mpu-repr", REPRESENTATION).await;
    h.upload_part("mpu-repr", &upload, 1, "multipart body")
        .await;
    let (complete, _) = h.complete("mpu-repr", &upload, &[1]).await;
    assert_eq!(complete.status, StatusCode::OK);
    assert!(
        h.await_meta("mpu-repr").await,
        "fixture: the multipart upload was write-through cached"
    );
    let (_, get) = h.probe("GET", "mpu-repr", "").await;
    assert_eq!(get.status, StatusCode::OK);
    assert_eq!(get.body, Bytes::from_static(b"multipart body"));
    let hit = h.hit("mpu-repr").await;
    assert_eq!(
        (
            differences(&get, REPRESENTATION),
            differences(&hit, REPRESENTATION)
        ),
        (Vec::<String>::new(), Vec::<String>::new()),
        "a revalidated GET and a cache-hit GET of a multipart write-through entry must carry \
         its CreateMultipartUpload's representation headers"
    );
}

// ── Fix 3: Complete answers when S3 answers ──────────────────────────────────

/// Red side: a Complete naming a part this proxy never staged waits for it before
/// answering.
#[tokio::test]
async fn a_complete_with_an_unstaged_part_answers_at_once() {
    let h = harness().await;
    let upload = h.create("mpu-slow", &[]).await;
    h.upload_part("mpu-slow", &upload, 1, "first part").await;
    // Part 2 is listed but never sent through this proxy.
    let (complete, elapsed) = h.complete("mpu-slow", &upload, &[1, 2]).await;
    assert_eq!(complete.status, StatusCode::OK);
    assert!(
        elapsed < Duration::from_secs(1),
        "CompleteMultipartUpload must be answered when S3 answers, not after the cache's \
         wait for unstaged parts: took {:?}",
        elapsed
    );
}

// ── Fix 4: a 200 Complete carrying an <Error> document ──────────────────────

/// Red side: the error document is treated as a success and published.
#[tokio::test]
async fn a_complete_answered_with_an_error_document_is_not_cached() {
    let h = harness().await;
    let upload = h.create("mpu-errdoc", &[]).await;
    h.upload_part("mpu-errdoc", &upload, 1, "never completed")
        .await;
    let (complete, _) = h.complete("mpu-errdoc", &upload, &[1]).await;
    assert_eq!(complete.status, StatusCode::OK);
    assert_eq!(
        complete.body,
        Bytes::from_static(ERROR_DOCUMENT.as_bytes()),
        "the error document must reach the client unchanged"
    );
    tokio::time::sleep(SETTLE).await;
    assert_eq!(
        (
            h.cache_files("metadata", "mpu-errdoc", "meta"),
            h.staging_dirs()
        ),
        (Vec::<PathBuf>::new(), Vec::<PathBuf>::new()),
        "a Complete whose 200 carries an <Error> document must publish nothing and remove \
         the staged parts"
    );
}

// ── Fix 6: a multi-range GET is forwarded ───────────────────────────────────

/// Red side: a multi-range GET is answered 501 without contacting S3.
#[tokio::test]
async fn a_multi_range_get_is_answered_the_way_s3_answers_it() {
    let h = harness().await;
    h.mock.put("ranged", "0123456789abcdefghij");
    let raw = Harness::raw("ranged");
    for cached in [false, true] {
        if cached {
            // Cache the object, then ask again: a hit must not change the answer.
            for _ in 0..3 {
                let _ = h.probe("GET", "ranged", "").await;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        let before = h.mock.gets(&raw);
        let answer = h
            .send("GET", &raw, &[("range", "bytes=0-1,5-6")], Bytes::new())
            .await;
        assert_eq!(
            (
                answer.status,
                answer.body.clone(),
                h.mock.gets(&raw) > before
            ),
            (
                StatusCode::OK,
                Bytes::from_static(b"0123456789abcdefghij"),
                true
            ),
            "a multi-range GET (object cached: {}) must be forwarded and answered with S3's \
             200 and the whole object",
            cached
        );
    }
}

// ── the empty-object store, and the default Content-Type ───────────────

/// An empty-object PUT takes the buffered store (`spawn_cache_write_task`), not the
/// streamed one, and its entry must record the same representation headers. A GET of a
/// zero-length entry is forwarded to S3 (the cache reports no cached ranges for a zero-length
/// object), so its answer carries the headers S3 stored with the object, as it does for any
/// zero-byte PUT (measured). The entry is therefore asserted where it is written, and a GET
/// answered without reaching S3 must carry the headers too.
#[tokio::test]
async fn an_empty_put_entry_records_its_representation_headers() {
    let h = harness().await;
    let put = h.put("empty-repr", "", REPRESENTATION, "").await;
    assert_eq!(put.status, StatusCode::OK);
    assert!(
        h.await_meta("empty-repr").await,
        "fixture: the empty PUT was write-through cached"
    );
    let metas = h.cache_files("metadata", "empty-repr", "meta");
    assert_eq!(metas.len(), 1, "fixture: one .meta for the key");
    let stored: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&metas[0]).unwrap()).unwrap();
    let recorded = &stored["object_metadata"]["response_headers"];
    let not_recorded: Vec<String> = REPRESENTATION
        .iter()
        .filter(|(name, value)| recorded[*name].as_str() != Some(*value))
        .map(|(name, _)| format!("{}={}", name, recorded[*name]))
        .collect();
    let (reached, get) = h.probe("GET", "empty-repr", "").await;
    assert_eq!((get.status, get.body.len()), (StatusCode::OK, 0));
    let served_without = if reached {
        Vec::new()
    } else {
        differences(&get, REPRESENTATION)
    };
    assert_eq!(
        (not_recorded, served_without),
        (Vec::<String>::new(), Vec::<String>::new()),
        "(headers the empty entry did not record, headers a GET answered from the cache lacked): \
         an empty write-through entry must record the PUT's representation headers"
    );
}

/// S3 stores `binary/octet-stream` for an object written without `Content-Type` and returns
/// it on GET and HEAD (measured on PutObject and on a multipart upload), so a write-through
/// hit of such an object carries it too.
#[tokio::test]
async fn an_entry_written_without_content_type_is_served_as_binary_octet_stream() {
    let h = harness().await;
    let put = h.put("no-ct", "", &[], "untyped").await;
    assert_eq!(put.status, StatusCode::OK);
    assert!(h.await_meta("no-ct").await, "fixture: the PUT was cached");
    let upload = h.create("mpu-no-ct", &[]).await;
    h.upload_part("mpu-no-ct", &upload, 1, "untyped multipart")
        .await;
    let (complete, _) = h.complete("mpu-no-ct", &upload, &[1]).await;
    assert_eq!(complete.status, StatusCode::OK);
    assert!(
        h.await_meta("mpu-no-ct").await,
        "fixture: the multipart upload was cached"
    );
    let put_hit = h.hit("no-ct").await;
    let mpu_hit = h.hit("mpu-no-ct").await;
    assert_eq!(
        (
            put_hit.header("content-type"),
            mpu_hit.header("content-type")
        ),
        (Some("binary/octet-stream"), Some("binary/octet-stream")),
        "(single PUT, multipart) cache hits of objects written without Content-Type must carry \
         S3's default"
    );
}
