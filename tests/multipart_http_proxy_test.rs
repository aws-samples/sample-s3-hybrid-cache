//! Multipart operations through a real `HttpProxy` against a loopback mock S3.
//!
//! Covers what no other test reaches at request level: AbortMultipartUpload, the body cap and
//! in-flight reservation of CompleteMultipartUpload, the origin guard over all four multipart
//! operations, a presigned UploadPart, and one chain (Create, UploadPart, Complete, then whole
//! and part-number reads) that checks the bytes, `Content-Range` and ETag a client sees.
//!
//! The mock implements Create, UploadPart, Complete, Abort and part-number reads, and records
//! every request it receives as `METHOD path?query`, so "forwarded verbatim" and "never
//! reached S3" are observations of the mock, not of the proxy.
use std::collections::{BTreeMap, HashMap};
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Semaphore};

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::{Config, UpstreamOverrideConfig, UpstreamScheme};
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_ledger::InflightLedger;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::s3_client::S3Client;
use s3_proxy::S3ClientApi;

const BUCKET: &str = "mpu-bucket";
const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000"; // gitleaks:allow
const STEP: Duration = Duration::from_secs(30);
/// How long a raw exchange waits for the proxy's status line before reporting none.
const RAW_WAIT: Duration = Duration::from_secs(5);
/// Time for a detached finalization or cleanup to land before a negative check looks.
const SETTLE: Duration = Duration::from_millis(1500);
const COMPLETE_CAP: u64 = 400;
const LEDGER_CEILING: u64 = 300;
const MARKER_OTHER_ORIGIN: &str = r#"{"schema":1,"origin":"minio-a.example.com"}"#;

// ── Mock S3 ─────────────────────────────────────────────────────────────────

struct StoredObject {
    data: Bytes,
    etag: String,
    part_lens: Vec<usize>,
}

#[derive(Default)]
struct MockState {
    objects: HashMap<String, StoredObject>,
    uploads: HashMap<String, BTreeMap<u32, Bytes>>,
    log: Vec<String>,
    counter: usize,
    /// When set, DeleteMultipartUpload answers this status instead of `204`.
    abort_status: Option<u16>,
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
            (name, parts.next().unwrap_or("").to_string())
        })
        .collect()
}

fn empty(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-length", "0")
        .body(Full::new(Bytes::new()))
        .unwrap()
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
    fn log(&self) -> Vec<String> {
        self.state.lock().unwrap().log.clone()
    }

    /// Requests recorded so far that start with `prefix`.
    fn count(&self, prefix: &str) -> usize {
        self.log().iter().filter(|l| l.starts_with(prefix)).count()
    }

    fn fail_aborts_with(&self, status: u16) {
        self.state.lock().unwrap().abort_status = Some(status);
    }

    async fn handle(self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let headers = req.headers().clone();
        let path = req.uri().path().to_string();
        let raw_query = req.uri().query().unwrap_or("").to_string();
        let query = query_map(&raw_query);
        let body = req.into_body().collect().await?.to_bytes();
        let key = path.trim_start_matches('/').to_string();
        let mut st = self.state.lock().unwrap();
        st.log.push(if raw_query.is_empty() {
            format!("{} {}", method, path)
        } else {
            format!("{} {}?{}", method, path, raw_query)
        });

        if method == hyper::Method::PUT {
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
            return Ok(empty(StatusCode::OK));
        }
        if method == hyper::Method::POST {
            if query.contains_key("uploads") {
                st.counter += 1;
                let upload = format!("MPUUPLOAD{}", st.counter);
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
                let parts = st.uploads.remove(upload).unwrap_or_default();
                let count = body
                    .windows(b"<PartNumber>".len())
                    .filter(|w| *w == b"<PartNumber>")
                    .count();
                let part_lens: Vec<usize> = parts.values().map(|b| b.len()).collect();
                let data: Vec<u8> = parts.values().flat_map(|b| b.to_vec()).collect();
                let etag = format!("\"0123456789abcdef0123456789abcdef-{}\"", count);
                st.objects.insert(
                    key.clone(),
                    StoredObject {
                        data: Bytes::from(data),
                        etag: etag.clone(),
                        part_lens,
                    },
                );
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/xml")
                    .body(Full::new(Bytes::from(format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
                        BUCKET,
                        key,
                        etag.replace('"', "&quot;")
                    ))))
                    .unwrap());
            }
        }
        if method == hyper::Method::DELETE && query.contains_key("uploadid") {
            if let Some(status) = st.abort_status {
                return Ok(xml(
                    StatusCode::from_u16(status).unwrap(),
                    "<Error><Code>NoSuchUpload</Code><Message>The specified upload does not exist.</Message></Error>".to_string(),
                ));
            }
            if let Some(upload) = query.get("uploadid") {
                st.uploads.remove(upload);
            }
            return Ok(empty(StatusCode::NO_CONTENT));
        }
        if method == hyper::Method::GET || method == hyper::Method::HEAD {
            let Some(object) = st.objects.get(&key) else {
                return Ok(empty(StatusCode::NOT_FOUND));
            };
            let header = |name: &str| {
                headers
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string)
            };
            if header("if-none-match").is_some_and(|v| v == object.etag) {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header("etag", object.etag.clone())
                    .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                    .body(Full::new(Bytes::new()))
                    .unwrap());
            }
            let total = object.data.len();
            let (status, start, end) = match query.get("partnumber").map(|p| p.parse::<usize>()) {
                Some(Ok(n)) if n >= 1 && n <= object.part_lens.len() => {
                    let start: usize = object.part_lens[..n - 1].iter().sum();
                    (
                        StatusCode::PARTIAL_CONTENT,
                        start,
                        start + object.part_lens[n - 1],
                    )
                }
                _ => (StatusCode::OK, 0, total),
            };
            let mut builder = Response::builder()
                .status(status)
                .header("content-length", (end - start).to_string())
                .header("etag", object.etag.clone())
                .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT");
            if status == StatusCode::PARTIAL_CONTENT {
                builder = builder
                    .header(
                        "content-range",
                        format!("bytes {}-{}/{}", start, end - 1, total),
                    )
                    .header("x-amz-mp-parts-count", object.part_lens.len().to_string());
            }
            let body = if method == hyper::Method::HEAD {
                Bytes::new()
            } else {
                object.data.slice(start..end)
            };
            return Ok(builder.body(Full::new(body)).unwrap());
        }
        Ok(empty(StatusCode::NOT_IMPLEMENTED))
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
    ledger: Arc<InflightLedger>,
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
    harness_with(10 * 1024 * 1024, 64 * 1024 * 1024, |_| {}).await
}

/// A proxy whose Complete body cap is `complete_cap` bytes and whose in-flight ledger holds
/// `ledger_ceiling` bytes. `before_start` runs on the cache directory before the cache starts.
async fn harness_with(
    complete_cap: u64,
    ledger_ceiling: u64,
    before_start: impl FnOnce(&Path),
) -> Harness {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mock = MockS3::default();
    let (mock_addr, mock_shutdown) = mock.clone().spawn().await;
    let mock_host = format!("127.0.0.1:{}", mock_addr.port());
    let dir = TempDir::new().unwrap();
    before_start(dir.path());
    let mut config = Config::default();
    config.cache.cache_dir = dir.path().to_path_buf();
    config.cache.ram_cache_enabled = false;
    config.cache.write_cache_enabled = true;
    config.cache.read_cache_enabled = true;
    config.cache.download_coordination.enabled = false;
    config.cache.get_ttl = Duration::from_secs(3600);
    config.cache.head_ttl = Duration::from_secs(3600);
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_complete_body_bytes = complete_cap;
    config.server.max_inflight_buffer_bytes = ledger_ceiling;
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
    let ledger = Arc::new(InflightLedger::new(config.server.max_inflight_buffer_bytes));
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(
        S3Client::new(&config.connection_pool, None)
            .expect("s3 client")
            .with_inflight_ledger(Arc::clone(&ledger)),
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
        ledger,
    }
}

fn raw(key: &str) -> String {
    format!("/{}/{}", BUCKET, key)
}

/// `path` with a presigned query appended; the request carries no `Authorization` header.
fn presigned(path: &str, extra: &str) -> String {
    let now = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    format!(
        "{path}?{extra}&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date={now}&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=0000" // gitleaks:allow
    )
}

/// Bytes that differ by part and by offset, so a shifted or swapped slice cannot match.
fn part_bytes(part: u32, len: usize) -> Bytes {
    Bytes::from(
        (0..len)
            .map(|i| ((part as usize * 37 + i * 7) % 251) as u8)
            .collect::<Vec<u8>>(),
    )
}

impl Harness {
    async fn send_with(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: Bytes,
        signed: bool,
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
                .uri(target)
                .header("host", self.mock_host.as_str())
                .header("content-length", body.len().to_string());
            if signed {
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
            .unwrap_or_else(|_| panic!("{} {} timed out", method, target))
    }

    async fn send(&self, method: &str, target: &str, body: Bytes) -> Answer {
        self.send_with(method, target, &[], body, true).await
    }

    /// Write `head` and `body` on a fresh connection without finishing the request, and return
    /// the proxy's status line, or `None` when it sent no response head within `RAW_WAIT`.
    async fn raw_exchange(&self, head: String, body: Vec<u8>) -> Option<String> {
        let mut stream = TcpStream::connect(self.proxy).await.expect("connect");
        stream.write_all(head.as_bytes()).await.expect("write head");
        stream.write_all(&body).await.expect("write body");
        let mut received = Vec::new();
        let mut buf = [0u8; 1024];
        let read = async {
            loop {
                let n = stream.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                received.extend_from_slice(&buf[..n]);
                if received.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
        };
        tokio::time::timeout(RAW_WAIT, read).await.ok()?;
        Some(
            String::from_utf8_lossy(&received)
                .lines()
                .next()
                .unwrap_or("")
                .to_string(),
        )
    }

    fn complete_head(&self, key: &str, upload: &str, framing: &str) -> String {
        format!(
            "POST {}?uploadId={} HTTP/1.1\r\nhost: {}\r\nauthorization: {}\r\n{}\r\n\r\n",
            raw(key),
            upload,
            self.mock_host,
            AUTH,
            framing
        )
    }

    async fn create(&self, key: &str) -> String {
        let answer = self
            .send("POST", &format!("{}?uploads", raw(key)), Bytes::new())
            .await;
        assert_eq!(answer.status, StatusCode::OK, "CreateMultipartUpload");
        let body = String::from_utf8(answer.body.to_vec()).unwrap();
        let start = body.find("<UploadId>").expect("UploadId") + "<UploadId>".len();
        let end = body.find("</UploadId>").expect("/UploadId");
        body[start..end].to_string()
    }

    async fn upload_part(&self, key: &str, upload: &str, part: u32, body: Bytes) {
        let answer = self
            .send(
                "PUT",
                &format!("{}?partNumber={}&uploadId={}", raw(key), part, upload),
                body,
            )
            .await;
        assert_eq!(answer.status, StatusCode::OK, "UploadPart {}", part);
    }

    fn complete_xml(parts: &[u32]) -> String {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for part in parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>\"part-etag-{}\"</ETag></Part>",
                part, part
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        xml
    }

    async fn complete(&self, key: &str, upload: &str, parts: &[u32]) -> Answer {
        self.send(
            "POST",
            &format!("{}?uploadId={}", raw(key), upload),
            Bytes::from(Self::complete_xml(parts)),
        )
        .await
    }

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

    /// File names under every upload directory, sorted.
    fn staged_files(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .staging_dirs()
            .iter()
            .flat_map(|d| std::fs::read_dir(d).into_iter().flatten().flatten())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        names.sort();
        names
    }

    /// Wait, bounded, until `staging_dirs()` has `want` directories.
    async fn await_staging_dirs(&self, want: usize) -> usize {
        for _ in 0..100 {
            if self.staging_dirs().len() == want {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.staging_dirs().len()
    }

    fn meta_files(&self, key: &str) -> Vec<PathBuf> {
        fn walk(dir: &Path, key: &str, out: &mut Vec<PathBuf>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, key, out);
                } else if path.extension().is_some_and(|e| e == "meta")
                    && path
                        .file_name()
                        .is_some_and(|n| n.to_string_lossy().contains(key))
                {
                    out.push(path);
                }
            }
        }
        let mut out = Vec::new();
        walk(&self.dir.path().join("metadata"), key, &mut out);
        out
    }

    async fn await_meta(&self, key: &str) -> bool {
        for _ in 0..100 {
            if !self.meta_files(key).is_empty() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    /// Wait, bounded, for the in-flight ledger to hold nothing.
    async fn await_ledger_empty(&self) -> u64 {
        for _ in 0..100 {
            if self.ledger.reserved_bytes() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.ledger.reserved_bytes()
    }
}

// ── AbortMultipartUpload ────────────────────────────────────────────────────

/// Abort of one of two uploads is forwarded verbatim, answered with S3's `204`, and removes
/// that upload's staging while the other upload's staging stays.
///
/// # RED SIDE
///
/// With the cleanup after the forward removed, or the `DELETE ?uploadId` route sent to the
/// generic object delete, the staging directory of the aborted upload remains.
#[tokio::test]
async fn an_abort_is_forwarded_and_removes_the_aborted_uploads_staging() {
    let h = harness().await;
    let aborted = h.create("abort-me").await;
    h.upload_part("abort-me", &aborted, 1, part_bytes(1, 600))
        .await;
    let kept = h.create("keep-me").await;
    h.upload_part("keep-me", &kept, 1, part_bytes(2, 600)).await;
    assert_eq!(
        h.staging_dirs().len(),
        2,
        "fixture: both uploads are staged"
    );

    let answer = h
        .send(
            "DELETE",
            &format!("{}?uploadId={}", raw("abort-me"), aborted),
            Bytes::new(),
        )
        .await;
    let remaining = h.await_staging_dirs(1).await;
    let delete = format!("DELETE {}?uploadId={}", raw("abort-me"), aborted);
    assert_eq!(
        (
            answer.status,
            h.mock.count(&delete),
            remaining,
            h.staged_files()
                .iter()
                .any(|n| n.starts_with("upload.meta"))
        ),
        (StatusCode::NO_CONTENT, 1, 1, true),
        "(client status, DELETEs the mock received for `{}`, staging directories left, whether \
         the other upload kept its tracker)",
        delete
    );
}

/// Staging is removed whatever S3 answered: an Abort S3 refuses with `404 NoSuchUpload` still
/// reaches the client unchanged, and the cleanup still runs.
///
/// # RED SIDE
///
/// The same levers as the test above leave the staging directory in place.
#[tokio::test]
async fn an_abort_s3_refuses_still_removes_the_staging() {
    let h = harness().await;
    let upload = h.create("refused").await;
    h.upload_part("refused", &upload, 1, part_bytes(1, 600))
        .await;
    assert_eq!(h.staging_dirs().len(), 1, "fixture: the upload is staged");
    h.mock.fail_aborts_with(404);

    let answer = h
        .send(
            "DELETE",
            &format!("{}?uploadId={}", raw("refused"), upload),
            Bytes::new(),
        )
        .await;
    let remaining = h.await_staging_dirs(0).await;
    assert_eq!(
        (
            answer.status,
            String::from_utf8_lossy(&answer.body).contains("NoSuchUpload"),
            remaining
        ),
        (StatusCode::NOT_FOUND, true, 0),
        "(client status, S3's NoSuchUpload body reached the client, staging directories left)"
    );
}

// ── CompleteMultipartUpload: body cap and in-flight reservation ───────────────

/// The refused Complete reached no origin and left no bytes reserved, and a Complete inside
/// both limits is still forwarded (the limits, not the route, refused the first).
async fn assert_refusal_then_control(h: &Harness, refused: Option<String>, status_line: &str) {
    assert_eq!(
        refused.as_deref().map(|l| l.starts_with(status_line)),
        Some(true),
        "the proxy's status line for the refused Complete (`None`: no response within {:?}, \
         the request was left waiting for its body): {:?}",
        RAW_WAIT,
        refused
    );
    assert_eq!(
        h.mock.log(),
        Vec::<String>::new(),
        "a refused Complete must not reach S3"
    );
    assert_eq!(
        h.await_ledger_empty().await,
        0,
        "a refused Complete must release its in-flight reservation"
    );
    let control = h.complete("control", "UCONTROL", &[1]).await;
    assert_eq!(
        (control.status, h.mock.count("POST")),
        (StatusCode::OK, 1),
        "control: a Complete inside both limits is forwarded"
    );
}

/// A declared `Content-Length` over `max_complete_body_bytes` is refused with `413` before the
/// body is read.
///
/// # RED SIDE
///
/// With the `Content-Length` check removed, the handler waits for the rest of the body, so the
/// exchange gets no response head.
#[tokio::test]
async fn a_complete_declaring_more_than_the_cap_gets_413_before_its_body() {
    let h = harness_with(COMPLETE_CAP, 64 * 1024 * 1024, |_| {}).await;
    let head = h.complete_head("capped", "UCAP", "content-length: 5000");
    let line = h.raw_exchange(head, vec![b'x'; 10]).await;
    assert_refusal_then_control(&h, line, "HTTP/1.1 413").await;
}

/// A chunked body that grows past `max_complete_body_bytes` is refused with `413` as soon as a
/// chunk crosses the cap.
///
/// # RED SIDE
///
/// With the mid-stream check removed, the handler keeps waiting for more chunks, so the exchange
/// gets no response head. With the refusal leaking the 300 bytes the first chunk reserved, the
/// ledger never empties.
#[tokio::test]
async fn a_complete_streamed_past_the_cap_gets_413() {
    let h = harness_with(COMPLETE_CAP, 64 * 1024 * 1024, |_| {}).await;
    let head = h.complete_head("capped", "UCAP", "transfer-encoding: chunked");
    // The first chunk fits under the cap (400) and grows the reservation to 300 bytes; the
    // second takes the body past the cap while that reservation is held.
    let mut body = Vec::new();
    for _ in 0..2 {
        body.extend_from_slice(format!("{:x}\r\n", 300).as_bytes());
        body.extend_from_slice(&[b'x'; 300]);
        body.extend_from_slice(b"\r\n");
    }
    let line = h.raw_exchange(head, body).await;
    assert_refusal_then_control(&h, line, "HTTP/1.1 413").await;
}

/// A Complete whose declared length the in-flight ledger cannot hold is refused with `503
/// SlowDown` before its body is read.
///
/// # RED SIDE
///
/// With the reservation taken for no declared bytes, the handler waits for the rest of the body,
/// so the exchange gets no response head.
#[tokio::test]
async fn a_complete_the_ledger_cannot_hold_is_shed_before_its_body() {
    let h = harness_with(COMPLETE_CAP, LEDGER_CEILING, |_| {}).await;
    let head = h.complete_head("shed", "USHED", "content-length: 350");
    let line = h.raw_exchange(head, vec![b'x'; 10]).await;
    assert_refusal_then_control(&h, line, "HTTP/1.1 503").await;
}

/// A chunked Complete that outgrows the ledger part-way is shed with `503 SlowDown`, and what
/// it had reserved is released.
///
/// # RED SIDE
///
/// With the growth check removed, the handler accepts the chunk and waits for more, so the
/// exchange gets no response head. With the refusal leaking the 200 bytes the first chunk
/// reserved, the ledger never empties.
#[tokio::test]
async fn a_complete_that_outgrows_the_ledger_mid_stream_is_shed() {
    let h = harness_with(COMPLETE_CAP, LEDGER_CEILING, |_| {}).await;
    let head = h.complete_head("shed", "USHED", "transfer-encoding: chunked");
    // The first chunk fits the ledger (300) and grows the reservation to 200 bytes; the
    // second needs 400 in total, so the growth fails while that reservation is held.
    let mut body = Vec::new();
    for _ in 0..2 {
        body.extend_from_slice(format!("{:x}\r\n", 200).as_bytes());
        body.extend_from_slice(&[b'x'; 200]);
        body.extend_from_slice(b"\r\n");
    }
    let line = h.raw_exchange(head, body).await;
    assert_refusal_then_control(&h, line, "HTTP/1.1 503").await;
}

// ── The origin guard over the four multipart operations ──────────────────────

/// With the bucket bound to another origin, Create, UploadPart, Complete and Abort are
/// forwarded verbatim and leave no tracker, staged part or cache entry. An unbound bucket
/// stages the same operations, so the harness can see staging.
///
/// # RED SIDE
///
/// With the guard removed, the signed-write handler runs for the bound bucket: its Create and
/// UploadPart stage under `mpus_in_progress/`, which the mid-upload check reads (the upload
/// directories and trackers come to 2 against 1), and its Complete leaves a cache entry.
#[tokio::test]
async fn multipart_operations_on_a_bucket_bound_to_another_origin_cache_nothing() {
    let h = harness_with(10 * 1024 * 1024, 64 * 1024 * 1024, |dir| {
        let bound = dir.join("metadata").join(BUCKET);
        std::fs::create_dir_all(&bound).unwrap();
        std::fs::write(bound.join("_origin.json"), MARKER_OTHER_ORIGIN).unwrap();
    })
    .await;
    // The unbound bucket is the control: the same Create and UploadPart stage there.
    let control_key = "free-bucket/object";
    let create = h
        .send("POST", &format!("/{}?uploads", control_key), Bytes::new())
        .await;
    assert_eq!(create.status, StatusCode::OK, "control Create");
    let body = String::from_utf8(create.body.to_vec()).unwrap();
    let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
    let control_upload = body[start..body.find("</UploadId>").unwrap()].to_string();
    let part = h
        .send(
            "PUT",
            &format!("/{}?partNumber=1&uploadId={}", control_key, control_upload),
            part_bytes(9, 500),
        )
        .await;
    assert_eq!(part.status, StatusCode::OK, "control UploadPart");
    assert_eq!(
        h.staging_dirs().len(),
        1,
        "fixture: an unbound bucket's Create and UploadPart stage, so the checks below can see staging"
    );

    // The bound bucket: a full upload, and a second upload that is aborted.
    let upload = h.create("bound-object").await;
    h.upload_part("bound-object", &upload, 1, part_bytes(1, 600))
        .await;
    // Mid-upload, after Create and UploadPart and before Complete: the guard keeps the bound
    // bucket's upload out of staging and out of the in-progress trackers, so only the
    // control's directory and tracker exist. Complete and Abort remove staging whether or not
    // the guard ran, so this is the only point where its absence shows.
    tokio::time::sleep(SETTLE).await;
    assert_eq!(
        (
            h.staging_dirs().len(),
            h.staged_files()
                .iter()
                .filter(|n| n.starts_with("upload.meta"))
                .count(),
        ),
        (1, 1),
        "mid-upload (upload directories, upload trackers): only the control's, not the bound \
         bucket's; staged files: {:?}",
        h.staged_files()
    );
    let complete = h.complete("bound-object", &upload, &[1]).await;
    let second = h.create("bound-second").await;
    h.upload_part("bound-second", &second, 1, part_bytes(2, 600))
        .await;
    let abort = h
        .send(
            "DELETE",
            &format!("{}?uploadId={}", raw("bound-second"), second),
            Bytes::new(),
        )
        .await;
    tokio::time::sleep(SETTLE).await;

    let log = h.mock.log();
    let forwarded = |needle: &str| log.iter().filter(|l| l.contains(needle)).count();
    assert_eq!(
        (
            complete.status,
            abort.status,
            forwarded("POST /mpu-bucket/bound-object?uploads"),
            forwarded("PUT /mpu-bucket/bound-object?partNumber=1"),
            forwarded("POST /mpu-bucket/bound-object?uploadId"),
            forwarded("DELETE /mpu-bucket/bound-second?uploadId"),
        ),
        (StatusCode::OK, StatusCode::NO_CONTENT, 1, 1, 1, 1),
        "(Complete status, Abort status, and the mock's count of the forwarded Create, \
         UploadPart, Complete and Abort); full log: {:?}",
        log
    );
    assert_eq!(
        (
            h.staging_dirs().len(),
            h.meta_files("bound-object").len(),
            std::fs::read_to_string(
                h.dir
                    .path()
                    .join("metadata")
                    .join(BUCKET)
                    .join("_origin.json")
            )
            .unwrap(),
        ),
        (1, 0, MARKER_OTHER_ORIGIN.to_string()),
        "(upload directories: only the control's; cache entries for the bound object; the \
         bound bucket's marker, which must be unchanged)"
    );
}

// ── A presigned UploadPart ───────────────────────────────────────────────────

/// A presigned UploadPart (no `Authorization`, signature in the query) is forwarded to S3 and
/// stages its part the way a signed UploadPart of the same upload does: the staging directory
/// holds bytes for both parts.
///
/// # RED SIDE
///
/// If a presigned UploadPart were forwarded without staging, the staging directory would hold
/// no bytes for part 1 while part 2, sent signed, has its bytes.
#[tokio::test]
async fn a_presigned_upload_part_is_forwarded_and_staged() {
    let h = harness().await;
    let upload = h.create("presigned-part").await;
    let target = presigned(
        &raw("presigned-part"),
        &format!("partNumber=1&uploadId={}", upload),
    );
    let answer = h
        .send_with("PUT", &target, &[], part_bytes(3, 700), false)
        .await;
    h.upload_part("presigned-part", &upload, 2, part_bytes(4, 700))
        .await;
    // A staged part is a `<part>.<token>.tmp` file while the tee writes and the part's own
    // file once it commits, so count every non-lock file of the part, waiting, bounded, for
    // both parts to hold bytes.
    let staged_bytes = |part: &str| -> u64 {
        h.staging_dirs()
            .iter()
            .flat_map(|d| std::fs::read_dir(d).into_iter().flatten().flatten())
            .filter(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.starts_with(part) && !name.ends_with(".lock")
            })
            .map(|e| e.metadata().unwrap().len())
            .sum()
    };
    for _ in 0..100 {
        if staged_bytes("part1") > 0 && staged_bytes("part2") > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        (
            answer.status,
            h.mock.count("PUT /mpu-bucket/presigned-part?partNumber=1"),
            staged_bytes("part1") > 0,
            staged_bytes("part2") > 0,
        ),
        (StatusCode::OK, 1, true, true),
        "(client status, UploadPart requests the mock received, whether the presigned part 1 \
         has staged bytes, whether the signed part 2 has); all staged files: {:?}",
        h.staged_files()
    );
}

// ── One chain ────────────────────────────────────────────────────────────────

/// Create, three UploadParts of different sizes (uploaded out of order), Complete, then a whole
/// GET, a part GET and a part HEAD: the client sees the right bytes, `Content-Range` and the
/// multipart ETag, and the repeat whole GET is a cache hit.
///
/// # RED SIDE
///
/// With the part layout the finalize records shifted by one byte, the finalize refuses the
/// layout; with `part_layout_verified` recorded as `false` the cache will not serve it. Either
/// way the part GET still returns the right bytes, but from S3, so the count of part requests
/// that reach the mock is 1 instead of 0.
#[tokio::test]
async fn an_uploaded_object_reads_back_whole_and_by_part_number() {
    let h = harness().await;
    let key = "chain";
    let sizes = [3000usize, 1500, 700];
    let parts: Vec<Bytes> = sizes
        .iter()
        .enumerate()
        .map(|(i, len)| part_bytes(i as u32 + 1, *len))
        .collect();
    let whole: Vec<u8> = parts.iter().flat_map(|p| p.to_vec()).collect();
    let total = whole.len();

    let upload = h.create(key).await;
    for number in [2u32, 1, 3] {
        h.upload_part(key, &upload, number, parts[number as usize - 1].clone())
            .await;
    }
    let complete = h.complete(key, &upload, &[1, 2, 3]).await;
    assert_eq!(complete.status, StatusCode::OK);
    assert!(
        h.await_meta(key).await,
        "fixture: the completed upload was cached"
    );

    let path = raw(key);
    let whole_get = format!("GET {}", path);
    let first = h.send("GET", &path, Bytes::new()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before_repeat = h.mock.count(&whole_get);
    let repeat = h.send("GET", &path, Bytes::new()).await;
    assert_eq!(
        (
            first.status,
            first.body.as_ref() == whole.as_slice(),
            first.header("etag"),
            repeat.body.as_ref() == whole.as_slice(),
            h.mock.count(&whole_get) - before_repeat,
        ),
        (
            StatusCode::OK,
            true,
            Some("\"0123456789abcdef0123456789abcdef-3\""),
            true,
            0
        ),
        "(first GET status, its bytes equal the three parts in order, its ETag, the repeat GET's \
         bytes equal them, requests the repeat GET sent to S3)"
    );

    let part2_start = sizes[0];
    let part2_end = sizes[0] + sizes[1] - 1;
    let part_get = format!("GET {}?partNumber=2", path);
    let part = h
        .send("GET", &format!("{}?partNumber=2", path), Bytes::new())
        .await;
    assert_eq!(
        (
            part.status,
            part.header("content-range").map(str::to_string),
            part.body.as_ref() == parts[1].as_ref(),
            part.header("etag"),
            h.mock.count(&part_get),
        ),
        (
            StatusCode::PARTIAL_CONTENT,
            Some(format!("bytes {}-{}/{}", part2_start, part2_end, total)),
            true,
            Some("\"0123456789abcdef0123456789abcdef-3\""),
            0
        ),
        "(part 2 status, Content-Range, bytes equal the uploaded part 2, ETag, requests for it \
         that reached S3: the cache serves a completed upload's parts)"
    );

    let part3_start = sizes[0] + sizes[1];
    let head = h
        .send("HEAD", &format!("{}?partNumber=3", path), Bytes::new())
        .await;
    assert_eq!(
        (
            head.status,
            head.header("content-range").map(str::to_string),
            head.header("content-length").map(str::to_string),
            head.header("etag"),
        ),
        (
            StatusCode::PARTIAL_CONTENT,
            Some(format!("bytes {}-{}/{}", part3_start, total - 1, total)),
            Some(sizes[2].to_string()),
            Some("\"0123456789abcdef0123456789abcdef-3\""),
        ),
        "(part 3 HEAD status, Content-Range, Content-Length, ETag)"
    );
}

// ── The part record ─────────────────────────────────────────────────────────

/// The part record an UploadPart commit publishes carries the part file's inode, which
/// Complete checks after its renames to tell the file it read the record for from a same-length
/// file renamed over it. The record is read from disk after a real UploadPart through the proxy,
/// and the upload then completes.
///
/// # RED SIDE
///
/// With the streamed commit leaving `file_inode` out of the record it writes, the record has no
/// `file_inode` and the guard is off for every production upload.
#[tokio::test]
async fn a_streamed_part_record_carries_the_part_files_inode() {
    use std::os::unix::fs::MetadataExt;
    let h = harness().await;
    let key = "inode-part";
    let upload = h.create(key).await;
    h.upload_part(key, &upload, 1, part_bytes(1, 600)).await;
    let mut published = None;
    for _ in 0..100 {
        if let Some(dir) = h.staging_dirs().first() {
            if let Ok(text) = std::fs::read_to_string(dir.join("part1.json")) {
                published = Some((dir.clone(), text));
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (dir, record) = published.expect("fixture: the part record was published");
    let json: serde_json::Value = serde_json::from_str(&record).expect("the record is JSON");
    let actual = std::fs::metadata(dir.join("part1.bin"))
        .expect("the part file")
        .ino();
    assert_eq!(
        json.get("file_inode").and_then(|v| v.as_u64()),
        Some(actual),
        "the published part record does not carry the part file's inode: {record}"
    );
    let complete = h.complete(key, &upload, &[1]).await;
    assert_eq!(complete.status, StatusCode::OK);
    assert!(
        h.await_meta(key).await,
        "fixture: the completed upload was cached"
    );
}
