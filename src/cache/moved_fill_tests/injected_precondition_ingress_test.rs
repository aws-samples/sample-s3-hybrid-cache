//! The proxy's own conditional headers never decide a client's answer.
//!
//! Three defects, each driven through the real `HttpProxy::handle_request` on a
//! loopback port against an origin mock that evaluates preconditions the way S3
//! does and can be overwritten between requests:
//!
//! - **Forged sentinels.** The proxy marks a precondition it added with an internal
//!   header (`x-proxy-injected-if-match` and two siblings). A client that sent one
//!   itself, alongside its own failing `If-Match`, had S3's `412` read as the
//!   proxy's: the cache key's ranges were invalidated and the client received a
//!   `206`. Any client could repeat it.
//! - **The `If-Range` pin.** In Mode B a client `If-Range` naming the cached ETag was
//!   rewritten, in the request's own header map, into `If-Match`. Every later
//!   "forward the client's request" path then sent that `If-Match`, so an object
//!   overwritten in S3 earned the client a `412` where S3's answer is `200`.
//! - **The retry.** The `412` retry for a proxy-pinned range fetch stripped the
//!   `If-Match` but could not restore the `If-Range` the pin had removed, so the
//!   retry was unconditional and the client received a `206` of the new version
//!   where S3 answers `200` with the whole object.
//!
//! Requests are anonymous (no `Authorization`): a header-signed SDK request signs
//! `range` and takes `forward_signed_range_request`, a different branch. The
//! anonymous / presigned form is the one that reaches the unsigned paths under
//! test.

use super::support::StoreRangeExt as _;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Semaphore};

use crate::cache::{CacheEvictionAlgorithm, CacheManager};
use crate::cache_types::{CacheMetadata, ObjectMetadata, UploadState};
use crate::compression::CompressionAlgorithm;
use crate::config::Config;
use crate::connection_pool::ConnectionPoolManager;
use crate::disk_cache::DiskCacheManager;
use crate::http_proxy::HttpProxy;
use crate::inflight_tracker::InFlightTracker;
use crate::range_handler::RangeHandler;
use crate::{Result, S3ClientApi, S3RequestContext, S3Response, S3ResponseBody};

use super::common::StubS3Client;

const V1_ETAG: &str = "\"scn1-v1\"";
const V2_ETAG: &str = "\"scn1-v2\"";
const V1_LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";
const V2_LAST_MODIFIED: &str = "Thu, 02 Jan 2025 00:00:00 GMT";
const V1_BODY: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
/// A different length from v1 so "the whole new object" is identifiable by length.
const V2_BODY: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuv";

const SENTINELS: [&str; 3] = [
    "x-proxy-injected-if-match",
    "x-proxy-injected-if-unmodified-since",
    "x-proxy-synthesized-full-object-range",
];

#[derive(Clone)]
struct Version {
    etag: &'static str,
    last_modified: &'static str,
    body: &'static [u8],
}

const V1: Version = Version {
    etag: V1_ETAG,
    last_modified: V1_LAST_MODIFIED,
    body: V1_BODY,
};
const V2: Version = Version {
    etag: V2_ETAG,
    last_modified: V2_LAST_MODIFIED,
    body: V2_BODY,
};

#[derive(Clone, Debug)]
struct Seen {
    method: Method,
    uri: String,
    headers: HashMap<String, String>,
}

impl Seen {
    fn get(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }
}

/// An origin that answers preconditions as S3 does: `If-Match` failure is `412`,
/// a matching `If-None-Match` is `304`, and a `Range` is honoured only when
/// `If-Range` is absent or names the current ETag, otherwise `200` with the whole
/// object. `If-Unmodified-Since` / `If-Modified-Since` are not evaluated: the
/// proxy only ever sends them beside an ETag precondition that decides first.
///
/// With `streaming`, a body is returned as `S3ResponseBody::Streaming`, a real
/// `hyper::body::Incoming` fetched from a loopback body server, as the production
/// client returns it when `allow_streaming` is set. Only that variant reaches the
/// tee-to-cache write in `convert_s3_response_to_http_with_caching`; a buffered body
/// is passed through uncached.
#[derive(Clone)]
struct OriginS3 {
    current: Arc<Mutex<Version>>,
    seen: Arc<Mutex<Vec<Seen>>>,
    delegate: StubS3Client,
    body_server: Option<SocketAddr>,
    /// `Range` values whose next GET is answered `500`, once each.
    fail_once: Arc<Mutex<Vec<String>>>,
    /// The object is deleted: every request is answered `404`.
    deleted: Arc<std::sync::atomic::AtomicBool>,
}

/// Serve `GET /<hex>` with the bytes the hex encodes, so the mock can turn any body
/// into a real `Incoming` without a shared table.
async fn spawn_body_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind body server");
    let addr = listener.local_addr().expect("body server address");
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let service = service_fn(|request: Request<hyper::body::Incoming>| async move {
                    let hex = request.uri().path().trim_start_matches('/').to_string();
                    let bytes: Vec<u8> = (0..hex.len() / 2)
                        .map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).expect("hex"))
                        .collect();
                    Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(Bytes::from(
                        bytes,
                    ))))
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    addr
}

impl OriginS3 {
    fn new(version: Version) -> Self {
        Self {
            current: Arc::new(Mutex::new(version)),
            seen: Arc::new(Mutex::new(Vec::new())),
            delegate: StubS3Client::new(),
            body_server: None,
            fail_once: Arc::new(Mutex::new(Vec::new())),
            deleted: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    async fn streaming(version: Version) -> Self {
        Self {
            body_server: Some(spawn_body_server().await),
            ..Self::new(version)
        }
    }

    async fn body(&self, bytes: Bytes, allow_streaming: bool) -> Option<S3ResponseBody> {
        let (Some(addr), true, false) = (self.body_server, allow_streaming, bytes.is_empty())
        else {
            return Some(S3ResponseBody::Buffered(bytes));
        };
        let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
        let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
            .build_http::<Full<Bytes>>();
        let response = client
            .get(
                format!("http://{}/{}", addr, hex)
                    .parse()
                    .expect("body uri"),
            )
            .await
            .expect("body server");
        Some(S3ResponseBody::Streaming(response.into_body()))
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("seen").clone()
    }

    fn assert_no_sentinel_reached_origin(&self) {
        for request in self.seen() {
            for name in SENTINELS {
                assert!(
                    request.get(name).is_none(),
                    "the proxy's internal header {name} reached the origin: {request:?}"
                );
            }
        }
    }
}

/// The signed-header list from the `Authorization` header's `SignedHeaders=` or a
/// presigned URL's `X-Amz-SignedHeaders`, lowercased.
fn signed_header_list(headers: &HashMap<String, String>, uri: &str) -> Vec<String> {
    let mut list = Vec::new();
    if let Some(auth) = headers.get("authorization") {
        if let Some((_, rest)) = auth.split_once("SignedHeaders=") {
            let end = rest.find([',', ' ']).unwrap_or(rest.len());
            list.extend(rest[..end].split(';').map(str::to_ascii_lowercase));
        }
    }
    if let Some((_, query)) = uri.split_once('?') {
        for pair in query.split('&') {
            if let Some((name, value)) = pair.split_once('=') {
                if name.eq_ignore_ascii_case("x-amz-signedheaders") {
                    let value = value.replace("%3B", ";").replace("%3b", ";");
                    list.extend(value.split(';').map(str::to_ascii_lowercase));
                }
            }
        }
    }
    list
}
fn etag_matches(list: &str, etag: &str) -> bool {
    list.split(',').any(|v| {
        let v = v.trim();
        v == "*" || v == etag
    })
}

fn parse_range(range: &str, len: u64) -> Option<(u64, u64)> {
    let spec = range.trim().strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    let start: u64 = start.parse().ok()?;
    let end: u64 = if end.is_empty() {
        len - 1
    } else {
        end.parse::<u64>().ok()?.min(len - 1)
    };
    (start <= end && start < len).then_some((start, end))
}

fn response(status: StatusCode, headers: HashMap<String, String>, body: Bytes) -> S3Response {
    S3Response {
        status,
        headers,
        body: Some(S3ResponseBody::Buffered(body)),
        request_duration: Duration::ZERO,
    }
}

impl OriginS3 {
    async fn respond(
        &self,
        status: StatusCode,
        headers: HashMap<String, String>,
        body: Bytes,
        allow_streaming: bool,
    ) -> S3Response {
        S3Response {
            status,
            headers,
            body: self.body(body, allow_streaming).await,
            request_duration: Duration::ZERO,
        }
    }
}

#[async_trait]
impl S3ClientApi for OriginS3 {
    async fn forward_request(&self, context: S3RequestContext) -> Result<S3Response> {
        let headers: HashMap<String, String> = context
            .headers
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
            .collect();
        let uri = context.uri.to_string();
        self.seen.lock().expect("seen").push(Seen {
            method: context.method.clone(),
            uri: uri.clone(),
            headers: headers.clone(),
        });
        if let Some(range) = headers.get("range") {
            let mut fail_once = self.fail_once.lock().expect("fail_once");
            if let Some(i) = fail_once.iter().position(|r| r == range) {
                fail_once.remove(i);
                return Ok(response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    HashMap::new(),
                    Bytes::new(),
                ));
            }
        }
        if self.deleted.load(std::sync::atomic::Ordering::SeqCst) {
            let body = Bytes::from_static(b"<Error><Code>NoSuchKey</Code></Error>");
            let mut h = HashMap::new();
            h.insert("content-length".to_string(), body.len().to_string());
            return Ok(response(StatusCode::NOT_FOUND, h, body));
        }
        // S3 recomputes the canonical request from what arrives, so a signed
        // `cache-control` or `pragma` that the proxy removed fails the signature.
        let signed = signed_header_list(&headers, &uri);
        for name in ["cache-control", "pragma"] {
            if signed.iter().any(|h| h == name) && !headers.contains_key(name) {
                let body = Bytes::from_static(b"<Error><Code>SignatureDoesNotMatch</Code></Error>");
                let mut h = HashMap::new();
                h.insert("content-length".to_string(), body.len().to_string());
                return Ok(response(StatusCode::FORBIDDEN, h, body));
            }
        }
        let v = self.current.lock().expect("current").clone();
        let len = v.body.len() as u64;
        let mut out = HashMap::new();
        out.insert("etag".to_string(), v.etag.to_string());
        out.insert("last-modified".to_string(), v.last_modified.to_string());
        out.insert(
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        );
        out.insert("accept-ranges".to_string(), "bytes".to_string());

        if let Some(if_match) = headers.get("if-match") {
            if !etag_matches(if_match, v.etag) {
                let body = Bytes::from_static(b"<Error><Code>PreconditionFailed</Code></Error>");
                let mut h = HashMap::new();
                h.insert("content-length".to_string(), body.len().to_string());
                return Ok(response(StatusCode::PRECONDITION_FAILED, h, body));
            }
        }
        if let Some(if_none_match) = headers.get("if-none-match") {
            if etag_matches(if_none_match, v.etag) {
                return Ok(response(StatusCode::NOT_MODIFIED, out, Bytes::new()));
            }
        }
        let range_applies = headers
            .get("if-range")
            .map(|ir| ir == v.etag || ir == v.last_modified)
            .unwrap_or(true);
        let head = context.method == Method::HEAD;
        if let (Some(range), true) = (headers.get("range"), range_applies) {
            if let Some((start, end)) = parse_range(range, len) {
                out.insert(
                    "content-range".to_string(),
                    format!("bytes {}-{}/{}", start, end, len),
                );
                out.insert("content-length".to_string(), (end - start + 1).to_string());
                let body = if head {
                    Bytes::new()
                } else {
                    Bytes::copy_from_slice(&v.body[start as usize..=end as usize])
                };
                return Ok(self
                    .respond(
                        StatusCode::PARTIAL_CONTENT,
                        out,
                        body,
                        context.allow_streaming,
                    )
                    .await);
            }
        }
        out.insert("content-length".to_string(), len.to_string());
        let body = if head {
            Bytes::new()
        } else {
            Bytes::from_static(v.body)
        };
        Ok(self
            .respond(StatusCode::OK, out, body, context.allow_streaming)
            .await)
    }

    fn extract_metadata_from_response(&self, headers: &HashMap<String, String>) -> CacheMetadata {
        self.delegate.extract_metadata_from_response(headers)
    }

    fn extract_object_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> ObjectMetadata {
        self.delegate.extract_object_metadata_from_response(headers)
    }

    fn get_connection_pool(&self) -> Arc<tokio::sync::RwLock<ConnectionPoolManager>> {
        self.delegate.get_connection_pool()
    }

    fn has_endpoint_overrides(&self) -> bool {
        false
    }

    async fn set_metrics_manager(
        &self,
        _metrics_manager: Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>,
    ) {
    }

    async fn register_endpoint(&self, _endpoint: &str) {}

    async fn refresh_dns(&self) -> Result<()> {
        Ok(())
    }

    fn get_inflight_ledger(&self) -> Arc<crate::inflight_ledger::InflightLedger> {
        self.delegate.get_inflight_ledger()
    }
}

struct Fixture {
    _temp_dir: TempDir,
    cache_dir: std::path::PathBuf,
    disk_cache: Arc<tokio::sync::RwLock<DiskCacheManager>>,
    origin: OriginS3,
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

fn base_config(cache_dir: std::path::PathBuf) -> Config {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    // One request, one path: no fetcher/waiter layer, no RAM tier answering first.
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    config
}

fn object_metadata(v: &Version) -> ObjectMetadata {
    let mut response_headers = HashMap::new();
    response_headers.insert("etag".to_string(), v.etag.to_string());
    response_headers.insert("last-modified".to_string(), v.last_modified.to_string());
    response_headers.insert("content-length".to_string(), v.body.len().to_string());
    ObjectMetadata {
        etag: v.etag.to_string(),
        last_modified: v.last_modified.to_string(),
        content_length: v.body.len() as u64,
        content_type: Some("application/octet-stream".to_string()),
        response_headers,
        upload_state: UploadState::Complete,
        cumulative_size: v.body.len() as u64,
        compression_algorithm: CompressionAlgorithm::None,
        ..ObjectMetadata::default()
    }
}

/// Build the cache, seed v1 extents for `path`, and serve the real
/// `handle_request` against an origin currently holding `origin_version`.
async fn fixture(
    path: &str,
    seed: &[(usize, usize)],
    origin_version: Version,
    tweak: impl FnOnce(&mut Config),
) -> Fixture {
    fixture_with(path, seed, OriginS3::new(origin_version), tweak).await
}

async fn fixture_with(
    path: &str,
    seed: &[(usize, usize)],
    origin: OriginS3,
    tweak: impl FnOnce(&mut Config),
) -> Fixture {
    let temp_dir = TempDir::new().expect("tempdir");
    let cache_dir = temp_dir.path().to_path_buf();
    let mut config = base_config(cache_dir.clone());
    tweak(&mut config);
    let config = Arc::new(config);
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        config.cache.cache_dir.clone(),
        false,
        0,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        false,
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
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    ));
    let _configured = cache_manager.create_configured_disk_cache_manager();
    cache_manager.initialize().await.expect("cache init");
    let disk_cache = Arc::new(tokio::sync::RwLock::new(DiskCacheManager::new(
        config.cache.cache_dir.clone(),
        false,
        0,
        config.cache.write_cache_enabled,
        config.cache.compression_batch_size,
    )));
    disk_cache
        .write()
        .await
        .initialize()
        .await
        .expect("disk init");
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache),
    ));

    let cache_key = CacheManager::generate_cache_key(path, None);
    for &(start, end) in seed {
        disk_cache
            .write()
            .await
            .store_range(
                &cache_key,
                start as u64,
                end as u64,
                &V1_BODY[start..=end],
                object_metadata(&V1),
                Duration::from_secs(3600),
                false,
            )
            .await
            .expect("seed range");
    }

    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(origin.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let addr = listener.local_addr().expect("proxy address");
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let request_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_requests));
    tokio::spawn(async move {
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let (stream, peer) = match accept {
                        Ok(connection) => connection,
                        Err(_) => break,
                    };
                    let config = Arc::clone(&config);
                    let cache_manager = Arc::clone(&cache_manager);
                    let range_handler = Arc::clone(&range_handler);
                    let s3_client = Arc::clone(&s3_client);
                    let request_semaphore = Arc::clone(&request_semaphore);
                    tokio::spawn(async move {
                        let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                            let config = Arc::clone(&config);
                            let cache_manager = Arc::clone(&cache_manager);
                            let range_handler = Arc::clone(&range_handler);
                            let s3_client = Arc::clone(&s3_client);
                            let request_semaphore = Arc::clone(&request_semaphore);
                            let ledger = s3_client.get_inflight_ledger();
                            async move {
                                HttpProxy::handle_request(
                                    request,
                                    peer,
                                    config,
                                    cache_manager,
                                    s3_client,
                                    range_handler,
                                    request_semaphore,
                                    None,
                                    None,
                                    Arc::new(InFlightTracker::new()),
                                    None,
                                    None,
                                    None,
                                    ledger,
                                ).await
                            }
                        });
                        let _ = http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    });
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    Fixture {
        _temp_dir: temp_dir,
        cache_dir,
        disk_cache,
        origin,
        addr,
        shutdown_tx: Some(shutdown_tx),
    }
}

struct Answer {
    status: StatusCode,
    headers: hyper::HeaderMap,
    body: Bytes,
}

async fn send(addr: SocketAddr, method: Method, path: &str, headers: &[(&str, &str)]) -> Answer {
    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .build_http::<Full<Bytes>>();
    let mut builder = Request::builder()
        .method(method)
        .uri(format!("http://{}{}", addr, path));
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let request = builder.body(Full::new(Bytes::new())).expect("request");
    let response = tokio::time::timeout(Duration::from_secs(20), client.request(request))
        .await
        .expect("proxy answered within 20s")
        .expect("proxy request");
    let status = response.status();
    let headers = response.headers().clone();
    let body = tokio::time::timeout(Duration::from_secs(20), response.into_body().collect())
        .await
        .expect("body within 20s")
        .expect("response body")
        .to_bytes();
    Answer {
        status,
        headers,
        body,
    }
}

async fn cached_extents(fx: &Fixture, path: &str) -> Vec<(u64, u64)> {
    let cache_key = CacheManager::generate_cache_key(path, None);
    let mut extents: Vec<(u64, u64)> = fx
        .disk_cache
        .read()
        .await
        .get_metadata(&cache_key)
        .await
        .ok()
        .flatten()
        .map(|m| m.ranges.iter().map(|r| (r.start, r.end)).collect())
        .unwrap_or_default();
    extents.sort();
    extents
}

/// Remove the seeded `.bin` for one extent, leaving its metadata entry claiming it.
fn orphan_bin(cache_dir: &std::path::Path, needle: &str) {
    let mut removed = 0;
    let mut stack = vec![cache_dir.join("ranges")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.to_string_lossy().contains(needle) {
                std::fs::remove_file(&p).expect("remove .bin");
                removed += 1;
            }
        }
    }
    assert_eq!(removed, 1, "the fixture must orphan exactly one .bin");
}

fn assert_whole_v2(answer: &Answer, origin: &OriginS3, what: &str) {
    assert_eq!(
        answer.status,
        StatusCode::OK,
        "{what}: S3 answers a stale If-Range with 200 and the whole object; body={:?} origin saw {:?}",
        String::from_utf8_lossy(&answer.body),
        origin.seen()
    );
    assert!(
        answer.headers.get("content-range").is_none(),
        "{what}: a 200 carries no Content-Range: {:?}",
        answer.headers
    );
    assert_eq!(
        answer.body.as_ref(),
        V2_BODY,
        "{what}: the body must be the whole new object"
    );
}

/// The request whose answer reached the client carried the client's `If-Range`
/// and no `If-Match` the client never sent.
fn assert_last_forward_is_clients(origin: &OriginS3, what: &str) {
    let seen = origin.seen();
    let last = seen.last().expect("the origin was asked");
    assert_eq!(
        last.get("if-range"),
        Some(V1_ETAG),
        "{what}: the client's If-Range must reach S3 unchanged: {seen:?}"
    );
    assert_eq!(
        last.get("if-match"),
        None,
        "{what}: S3 must not see an If-Match the client did not send: {seen:?}"
    );
}

// ---------------------------------------------------------------------------
// L5: forged sentinels
// ---------------------------------------------------------------------------

#[tokio::test]
async fn forged_sentinel_with_failing_client_if_match_gets_s3_412_and_keeps_cache() {
    let path = "/bucket/scn1-forged.bin";
    let fx = fixture(path, &[(20, 29)], V1, |_| {}).await;

    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[
            ("range", "bytes=0-9"),
            ("if-match", "\"not-the-etag\""),
            ("x-proxy-injected-if-match", "1"),
            ("x-proxy-injected-if-unmodified-since", "1"),
        ],
    )
    .await;

    assert_eq!(
        answer.status,
        StatusCode::PRECONDITION_FAILED,
        "the client's own failing If-Match must get S3's 412; origin saw {:?}",
        fx.origin.seen()
    );
    assert!(
        fx.origin
            .seen()
            .iter()
            .any(|r| r.get("if-match") == Some("\"not-the-etag\"")),
        "the client's If-Match must have reached S3: {:?}",
        fx.origin.seen()
    );
    fx.origin.assert_no_sentinel_reached_origin();
    assert_eq!(
        cached_extents(&fx, path).await,
        vec![(20, 29)],
        "a client's own 412 must not invalidate the cache key"
    );
}

#[tokio::test]
async fn forged_sentinels_on_plain_range_miss_never_reach_origin() {
    let path = "/bucket/scn1-forged-plain.bin";
    let fx = fixture(path, &[(20, 29)], V1, |_| {}).await;

    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[
            ("range", "bytes=0-9"),
            ("x-proxy-injected-if-match", "1"),
            ("x-proxy-injected-if-unmodified-since", "1"),
            ("x-proxy-synthesized-full-object-range", "1"),
        ],
    )
    .await;

    assert_eq!(answer.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(answer.body.as_ref(), &V1_BODY[0..10]);
    assert!(
        !fx.origin.seen().is_empty(),
        "the miss must reach the origin"
    );
    fx.origin.assert_no_sentinel_reached_origin();
}

#[tokio::test]
async fn forged_sentinels_on_head_bypass_never_reach_origin() {
    // HEAD with Range past head_ttl forwards the client's header map through
    // `forward_get_head_to_s3_without_caching`, which no sentinel consumer sees.
    let path = "/bucket/scn1-forged-head.bin";
    let fx = fixture(path, &[(0, 9)], V1, |c| c.cache.head_ttl = Duration::ZERO).await;

    let answer = send(
        fx.addr,
        Method::HEAD,
        path,
        &[
            ("range", "bytes=0-9"),
            ("x-proxy-injected-if-match", "1"),
            ("x-proxy-synthesized-full-object-range", "1"),
        ],
    )
    .await;

    assert_eq!(answer.status, StatusCode::PARTIAL_CONTENT);
    assert!(
        !fx.origin.seen().is_empty(),
        "the HEAD must reach the origin"
    );
    fx.origin.assert_no_sentinel_reached_origin();
}

#[tokio::test]
async fn control_client_failing_if_match_without_sentinel_gets_412_and_keeps_cache() {
    let path = "/bucket/scn1-control-if-match.bin";
    let fx = fixture(path, &[(20, 29)], V1, |_| {}).await;

    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[("range", "bytes=0-9"), ("if-match", "\"not-the-etag\"")],
    )
    .await;

    assert_eq!(answer.status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(cached_extents(&fx, path).await, vec![(20, 29)]);
}

// ---------------------------------------------------------------------------
// L3: the If-Range pin must not leak into a forward of the client's request
// ---------------------------------------------------------------------------

#[tokio::test]
async fn head_range_if_range_past_head_ttl_on_overwritten_object_gets_200() {
    let path = "/bucket/scn1-head.bin";
    let fx = fixture(path, &[(0, 9)], V2, |c| c.cache.head_ttl = Duration::ZERO).await;

    let answer = send(
        fx.addr,
        Method::HEAD,
        path,
        &[("range", "bytes=0-9"), ("if-range", V1_ETAG)],
    )
    .await;

    assert_eq!(
        answer.status,
        StatusCode::OK,
        "S3 answers a stale If-Range with 200; origin saw {:?}",
        fx.origin.seen()
    );
    assert_eq!(
        fx.origin.seen().last().map(|r| r.method.clone()),
        Some(Method::HEAD)
    );
    assert_last_forward_is_clients(&fx.origin, "HEAD+Range past head_ttl");
}

#[tokio::test]
async fn expired_get_without_range_with_if_range_on_overwritten_object_gets_200() {
    let path = "/bucket/scn1-expired.bin";
    let len = V1_BODY.len() - 1;
    let fx = fixture(path, &[(0, len)], V2, |c| c.cache.get_ttl = Duration::ZERO).await;

    let answer = send(fx.addr, Method::GET, path, &[("if-range", V1_ETAG)]).await;

    assert_whole_v2(&answer, &fx.origin, "expired no-Range GET");
    assert!(
        fx.origin.seen().iter().all(|r| r.get("if-match").is_none()),
        "no request for this client may carry an If-Match: {:?}",
        fx.origin.seen()
    );
}

#[tokio::test]
async fn missing_bin_with_if_range_on_overwritten_object_gets_200_whole_object() {
    let path = "/bucket/scn1-missing-bin.bin";
    let fx = fixture(path, &[(0, 9)], V2, |_| {}).await;
    orphan_bin(&fx.cache_dir, "_0-9");

    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[("range", "bytes=0-9"), ("if-range", V1_ETAG)],
    )
    .await;

    assert_whole_v2(&answer, &fx.origin, "missing .bin");
    assert_last_forward_is_clients(&fx.origin, "missing .bin");
}

// ---------------------------------------------------------------------------
// L4: the 412 retry must keep the client's If-Range
// ---------------------------------------------------------------------------

#[tokio::test]
async fn range_miss_with_if_range_on_overwritten_object_gets_200_whole_object() {
    let path = "/bucket/scn1-retry.bin";
    let fx = fixture_with(path, &[(20, 29)], OriginS3::streaming(V2).await, |_| {}).await;

    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[("range", "bytes=0-9"), ("if-range", V1_ETAG)],
    )
    .await;

    assert_whole_v2(&answer, &fx.origin, "range miss");
    assert_last_forward_is_clients(&fx.origin, "range miss");
}

#[tokio::test]
async fn partial_range_with_if_range_on_overwritten_object_gets_200_whole_object() {
    let path = "/bucket/scn1-partial.bin";
    let fx = fixture(path, &[(0, 9)], V2, |_| {}).await;

    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[("range", "bytes=0-19"), ("if-range", V1_ETAG)],
    )
    .await;

    assert_whole_v2(&answer, &fx.origin, "partial coverage");
    assert_last_forward_is_clients(&fx.origin, "partial coverage");
}

/// A `200` to a ranged request is the whole object. It must not be cached as
/// the requested range: here the request starts at byte 10, so a commit that
/// salvaged the received prefix would label bytes 0.. of v2 as bytes 10.. .
#[tokio::test]
async fn whole_object_200_to_offset_range_is_not_cached_as_the_range() {
    let path = "/bucket/scn1-offset.bin";
    let fx = fixture_with(path, &[(30, 35)], OriginS3::streaming(V2).await, |_| {}).await;

    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[("range", "bytes=10-59"), ("if-range", V1_ETAG)],
    )
    .await;
    assert_whole_v2(&answer, &fx.origin, "offset range");

    // Give any spawned cache write time to land, then check no extent starting at
    // 10 claims v2's leading bytes.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let cache_key = CacheManager::generate_cache_key(path, None);
    let meta = fx
        .disk_cache
        .read()
        .await
        .get_metadata(&cache_key)
        .await
        .ok()
        .flatten();
    if let Some(meta) = meta {
        assert!(
            meta.ranges.iter().all(|r| r.start != 10),
            "a whole-object 200 was cached as the requested range: {:?}",
            meta.ranges
                .iter()
                .map(|r| (r.start, r.end))
                .collect::<Vec<_>>()
        );
    }
}

/// A cold key (no cached size, so `Range` is not clamped) asked for `bytes=10-57`,
/// exactly the 48-byte object's length but offset by 10, with a stale `If-Range`.
/// S3's `200` body is the same length as the requested range, so a cache commit as
/// `range_spec` would succeed and file bytes 0..47 under offsets 10..57. A later
/// plain read of that range would then serve the wrong bytes.
#[tokio::test]
async fn whole_object_200_with_range_length_is_not_cached_at_the_offset() {
    let path = "/bucket/scn1-cold-offset.bin";
    let fx = fixture_with(path, &[], OriginS3::streaming(V2).await, |_| {}).await;
    let requested = format!("bytes=10-{}", 10 + V2_BODY.len() - 1);

    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[("range", requested.as_str()), ("if-range", V1_ETAG)],
    )
    .await;
    assert_whole_v2(&answer, &fx.origin, "cold offset range");

    tokio::time::sleep(Duration::from_millis(500)).await;
    let extents = cached_extents(&fx, path).await;
    assert!(
        extents.iter().all(|(start, _)| *start != 10),
        "a whole-object 200 was cached as the requested range: {extents:?}"
    );
    let reread = send(fx.addr, Method::GET, path, &[("range", "bytes=10-20")]).await;
    assert_eq!(reread.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        reread.body.as_ref(),
        &V2_BODY[10..=20],
        "bytes 10-20 must be the object's bytes 10-20"
    );
}

// ---------------------------------------------------------------------------
// Controls: no version change still serves from cache with the pinned sub-fetch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn control_if_range_match_partial_range_served_with_pinned_subfetch() {
    let path = "/bucket/scn1-control-partial.bin";
    let fx = fixture(path, &[(0, 9)], V1, |_| {}).await;

    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[("range", "bytes=0-19"), ("if-range", V1_ETAG)],
    )
    .await;

    assert_eq!(answer.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(answer.body.as_ref(), &V1_BODY[0..20]);
    let seen = fx.origin.seen();
    assert_eq!(seen.len(), 1, "one gap sub-fetch: {seen:?}");
    assert_eq!(seen[0].get("range"), Some("bytes=10-19"));
    assert_eq!(
        seen[0].get("if-match"),
        Some(V1_ETAG),
        "the gap sub-fetch pins the cached version: {seen:?}"
    );
    assert_eq!(
        seen[0].get("if-range"),
        None,
        "the gap sub-fetch carries the pin instead of If-Range, so a change costs a bodyless 412: {seen:?}"
    );
    fx.origin.assert_no_sentinel_reached_origin();
}

#[tokio::test]
async fn control_if_range_match_fully_cached_range_served_from_cache() {
    let path = "/bucket/scn1-control-hit.bin";
    let fx = fixture(path, &[(0, 9)], V1, |_| {}).await;

    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[("range", "bytes=0-9"), ("if-range", V1_ETAG)],
    )
    .await;

    assert_eq!(answer.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(answer.body.as_ref(), &V1_BODY[0..10]);
    assert!(
        fx.origin.seen().is_empty(),
        "a matching If-Range on a fully cached range is answered from cache: {:?}",
        fx.origin.seen()
    );
}

#[tokio::test]
async fn control_client_if_range_mismatch_is_forwarded_unchanged() {
    // The client's If-Range names a version the cache does not hold: Mode B
    // forwards and S3 decides.
    let path = "/bucket/scn1-control-mismatch.bin";
    let fx = fixture(path, &[(0, 9)], V2, |_| {}).await;

    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[("range", "bytes=0-9"), ("if-range", V2_ETAG)],
    )
    .await;

    assert_eq!(answer.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(answer.body.as_ref(), &V2_BODY[0..10]);
    let seen = fx.origin.seen();
    let last = seen.last().expect("forwarded");
    assert_eq!(last.get("if-range"), Some(V2_ETAG), "{seen:?}");
}
// ---------------------------------------------------------------------------
// Cache bypass headers the client signed
//
// `Cache-Control: no-cache` and `Pragma: no-cache` make the proxy bypass its cache,
// and it removes both headers before forwarding. A client that listed either in
// its signed set must reach S3 with it, or S3 answers `403 SignatureDoesNotMatch`.
// The cache is seeded with v1 and the origin holds v2, so a `200` with v2's bytes
// shows the request still bypassed the cache.
// ---------------------------------------------------------------------------
fn presigned_path(path: &str, signed_headers: &str) -> String {
    let date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    format!(
        "{path}?X-Amz-Algorithm=AWS4-HMAC-SHA256\
         &X-Amz-Credential=TESTACCESSKEY%2F20250101%2Fus-east-1%2Fs3%2Faws4_request\
         &X-Amz-Date={date}&X-Amz-Expires=3600&X-Amz-SignedHeaders={signed_headers}\
         &X-Amz-Signature=sig01"
    )
}
fn assert_bypassed_to_v2(answer: &Answer, origin: &OriginS3, what: &str) {
    assert_eq!(
        answer.status,
        StatusCode::OK,
        "{what}: body={:?} origin saw {:?}",
        String::from_utf8_lossy(&answer.body),
        origin.seen()
    );
    assert_eq!(
        answer.body.as_ref(),
        V2_BODY,
        "{what}: a bypass request must be answered by the origin, not the cache"
    );
}
#[tokio::test]
async fn presigned_signed_cache_control_no_cache_reaches_s3_and_bypasses() {
    let path = "/bucket/scn2-signed-cache-control.bin";
    let fx = fixture(path, &[(0, V1_BODY.len() - 1)], V2, |_| {}).await;
    let answer = send(
        fx.addr,
        Method::GET,
        &presigned_path(path, "cache-control%3Bhost"),
        &[("cache-control", "no-cache")],
    )
    .await;
    assert_bypassed_to_v2(&answer, &fx.origin, "signed cache-control");
    let seen = fx.origin.seen();
    assert!(
        seen.iter()
            .all(|r| r.get("cache-control") == Some("no-cache")),
        "a signed Cache-Control must reach S3 unchanged: {seen:?}"
    );
    assert!(
        seen.iter().all(|r| r.uri.contains("X-Amz-SignedHeaders=")),
        "the presigned query must reach S3: {seen:?}"
    );
}
#[tokio::test]
async fn header_signed_pragma_no_cache_reaches_s3_and_bypasses() {
    let path = "/bucket/scn2-signed-pragma.bin";
    let fx = fixture(path, &[(0, V1_BODY.len() - 1)], V2, |_| {}).await;
    let authorization = "AWS4-HMAC-SHA256 \
        Credential=TESTACCESSKEY/20250101/us-east-1/s3/aws4_request, \
        SignedHeaders=host;pragma;x-amz-date, Signature=sig01";
    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[("authorization", authorization), ("pragma", "no-cache")],
    )
    .await;
    assert_bypassed_to_v2(&answer, &fx.origin, "signed pragma");
    let seen = fx.origin.seen();
    assert!(
        seen.iter().all(|r| r.get("pragma") == Some("no-cache")),
        "a signed Pragma must reach S3 unchanged: {seen:?}"
    );
}
#[tokio::test]
async fn control_unsigned_cache_control_no_cache_is_stripped_and_bypasses() {
    let path = "/bucket/scn2-unsigned-cache-control.bin";
    let fx = fixture(path, &[(0, V1_BODY.len() - 1)], V2, |_| {}).await;
    let answer = send(
        fx.addr,
        Method::GET,
        &presigned_path(path, "host"),
        &[("cache-control", "no-cache"), ("pragma", "no-cache")],
    )
    .await;
    assert_bypassed_to_v2(&answer, &fx.origin, "unsigned cache-control");
    let seen = fx.origin.seen();
    assert!(!seen.is_empty(), "the bypass must reach the origin");
    assert!(
        seen.iter()
            .all(|r| r.get("cache-control").is_none() && r.get("pragma").is_none()),
        "unsigned bypass headers are removed before forwarding: {seen:?}"
    );
}
// ---------------------------------------------------------------------------
// A 412 on the proxy's precondition with a failed range-file unlink
// ---------------------------------------------------------------------------
/// The RAM metadata copy outlives each test's reads, so a stale one would serve.
fn long_metadata_refresh(config: &mut Config) {
    config.cache.metadata_cache.refresh_interval = Duration::from_secs(60);
}
/// Drive `fx` (v1 cached at 0-9 and 20-29) into the state a `412` on the proxy's own `If-Match`
/// leaves when the unlink of the 0-9 extent's `.bin` fails: S3 now holds v2, the hole fetch for
/// 10-19 fails with a `500`, so the fallback fetch of the whole range carries the pin, S3
/// answers it `412`, and the proxy invalidates and retries. Returns the kept `.bin`.
async fn after_a_412_with_a_failed_unlink(fx: &Fixture, path: &str) -> std::path::PathBuf {
    let cache_key = CacheManager::generate_cache_key(path, None);
    let listed = fx
        .disk_cache
        .read()
        .await
        .get_metadata(&cache_key)
        .await
        .expect("fixture: the .meta reads")
        .expect("fixture: v1 is cached");
    let kept = fx.cache_dir.join("ranges").join(
        &listed
            .ranges
            .iter()
            .find(|r| r.start == 0)
            .expect("fixture: the 0-9 extent is listed")
            .file_path,
    );
    *fx.origin.current.lock().expect("current") = V2;
    fx.origin
        .fail_once
        .lock()
        .expect("fail_once")
        .push("bytes=10-19".to_string());
    crate::disk_cache::unlink_fault::arm(&kept);
    let answer = send(fx.addr, Method::GET, path, &[("range", "bytes=0-29")]).await;
    let seen = fx.origin.seen();
    assert!(
        seen.iter()
            .any(|r| r.get("range") == Some("bytes=0-29") && r.get("if-match") == Some(V1_ETAG))
            && seen
                .iter()
                .any(|r| r.get("range") == Some("bytes=0-29") && r.get("if-match").is_none()),
        "FIXTURE: the pinned fallback fetch and its retry must reach the origin: {seen:?}"
    );
    assert_eq!(
        answer.body.as_ref(),
        &V2_BODY[0..30],
        "FIXTURE: the retried fetch is answered with v2; origin saw {seen:?}"
    );
    assert!(
        kept.exists(),
        "FIXTURE: the injected failure must leave the 0-9 .bin"
    );
    kept
}
/// After the `412`, the document on disk lists the kept extent unservable. A GET on the same
/// proxy, inside `metadata_cache.refresh_interval`, must not be served it from the RAM
/// metadata copy loaded before the invalidation.
#[tokio::test]
async fn a_get_after_a_412_with_a_failed_unlink_is_not_served_the_stale_extent_from_ram() {
    let path = "/bucket/scn-412-ram-snapshot.bin";
    let fx = fixture(path, &[(0, 9), (20, 29)], V1, long_metadata_refresh).await;
    after_a_412_with_a_failed_unlink(&fx, path).await;
    let before = fx.origin.seen().len();
    let answer = send(fx.addr, Method::GET, path, &[("range", "bytes=0-9")]).await;
    let asked = fx.origin.seen().len() - before;
    assert_eq!(
        answer.body.as_ref(),
        &V2_BODY[0..10],
        "A GET INSIDE metadata_cache.refresh_interval AFTER A 412 WAS SERVED THE STALE EXTENT \
         whose unlink failed: body={:?}, origin requests for it={asked}",
        String::from_utf8_lossy(&answer.body)
    );
    let whole = send(fx.addr, Method::GET, path, &[]).await;
    assert_eq!(
        whole.body.as_ref(),
        V2_BODY,
        "A WHOLE-OBJECT GET INSIDE metadata_cache.refresh_interval AFTER A 412 WAS NOT THE NEW \
         VERSION: body={:?}",
        String::from_utf8_lossy(&whole.body)
    );
}
/// After the `412`, a HEAD must ask S3 and return the new version's headers. Before, the kept
/// document's `head_expires_at` was set to the time of the invalidation, which HEAD freshness
/// does not compare with the clock, so the HEAD was answered from cache for up to `head_ttl`.
#[tokio::test]
async fn a_head_after_a_412_with_a_failed_unlink_asks_s3() {
    let path = "/bucket/scn-412-head.bin";
    let fx = fixture(path, &[(0, 9), (20, 29)], V1, long_metadata_refresh).await;
    let cached_head = send(fx.addr, Method::HEAD, path, &[]).await;
    assert_eq!(cached_head.status, StatusCode::OK, "FIXTURE: the v1 HEAD");
    let cache_key = CacheManager::generate_cache_key(path, None);
    let head_cached = fx
        .disk_cache
        .read()
        .await
        .get_metadata(&cache_key)
        .await
        .ok()
        .flatten()
        .is_some_and(|m| m.head_expires_at.is_some());
    assert!(
        head_cached,
        "FIXTURE: the v1 HEAD must be cached in the .meta"
    );
    after_a_412_with_a_failed_unlink(&fx, path).await;
    let before = fx.origin.seen().len();
    let answer = send(fx.addr, Method::HEAD, path, &[]).await;
    let asked_head = fx.origin.seen()[before..]
        .iter()
        .any(|r| r.method == Method::HEAD);
    let header = |name: &str| {
        answer
            .headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string()
    };
    let v2_length = V2_BODY.len().to_string();
    assert!(
        asked_head
            && header("etag") == V2_ETAG
            && header("content-length") == v2_length
            && header("last-modified") == V2_LAST_MODIFIED,
        "A HEAD AFTER A 412 WAS ANSWERED WITH THE PREVIOUS VERSION'S HEADERS: asked S3={asked_head}, \
         etag={}, content-length={}, last-modified={}",
        header("etag"),
        header("content-length"),
        header("last-modified")
    );
}

// ---------------------------------------------------------------------------
// Pending journal `Add`s an invalidation leaves behind. The fixture serves through the
// production-shaped disk cache manager (shared storage, so the journal fallback is live). v1
// has 0-9 folded into the `.meta` and 20-29 as a range fill whose `Add` no consolidation has
// folded yet. S3 then holds v2, an invalidation runs, and nothing this proxy answers may carry
// v1's bytes or headers.
// ---------------------------------------------------------------------------

/// The extent the `.meta` lists, and the extent only a pending journal `Add` names.
const LISTED: (usize, usize) = (0, 9);
const PENDING: (usize, usize) = (20, 29);
/// Unlinks of one path a persistent fault refuses: more than any test below makes.
const PERSISTENT_FAULT: usize = 50;

struct PendingFixture {
    fx: Fixture,
    cache_manager: Arc<CacheManager>,
    key: String,
    path: String,
    /// The download-coordination tracker every request of this fixture shares.
    inflight: &'static Arc<InFlightTracker>,
    /// The peer proxy that ran the invalidation, when one did.
    _peer: Option<Arc<CacheManager>>,
}

fn shared_manager(config: &Config) -> Arc<CacheManager> {
    Arc::new(CacheManager::new_with_shared_storage(
        config.cache.cache_dir.clone(),
        false,
        0,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        false,
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
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    ))
}

fn pending_config(cache_dir: std::path::PathBuf) -> Config {
    let mut config = base_config(cache_dir);
    // No RAM metadata copy: every read takes the document from disk, so what a test observes
    // is the disk state the invalidation left.
    config.cache.metadata_cache.enabled = false;
    config
}

async fn seed_v1_fill(
    disk_cache: &Arc<tokio::sync::RwLock<DiskCacheManager>>,
    key: &str,
    (start, end): (usize, usize),
) {
    disk_cache
        .write()
        .await
        .store_range_witnessed(
            key,
            start as u64,
            end as u64,
            &V1_BODY[start..=end],
            object_metadata(&V1),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("FIXTURE: seed a v1 range fill");
}

/// v1 with `LISTED` folded into the `.meta` and, with `pending`, `PENDING` as an unfolded
/// journal `Add`, served by the real `handle_request`. No background consolidation runs.
async fn pending_fixture(path: &str, pending: bool) -> PendingFixture {
    pending_fixture_with(path, true, pending, OriginS3::new(V1)).await
}

/// [`pending_fixture`], with `LISTED` seeded and folded only when `listed`, against `origin`.
async fn pending_fixture_with(
    path: &str,
    listed: bool,
    pending: bool,
    origin: OriginS3,
) -> PendingFixture {
    pending_fixture_configured(path, listed, pending, origin, |_| {}).await
}
/// [`pending_fixture_with`], with `tweak` applied to the configuration.
async fn pending_fixture_configured(
    path: &str,
    listed: bool,
    pending: bool,
    origin: OriginS3,
    tweak: impl FnOnce(&mut Config),
) -> PendingFixture {
    let temp_dir = TempDir::new().expect("tempdir");
    let cache_dir = temp_dir.path().to_path_buf();
    let mut config = pending_config(cache_dir.clone());
    tweak(&mut config);
    let config = Arc::new(config);
    // Leaked so every connection's service can copy the reference: one per fixture.
    let inflight: &'static Arc<InFlightTracker> =
        Box::leak(Box::new(Arc::new(InFlightTracker::new())));
    let cache_manager = shared_manager(&config);
    let disk_cache = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));
    cache_manager.initialize().await.expect("cache init");
    disk_cache
        .write()
        .await
        .initialize()
        .await
        .expect("disk init");
    assert!(
        disk_cache.read().await.is_shared_storage_mode(),
        "FIXTURE: the journal fallback must be live"
    );
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache),
    ));
    let key = CacheManager::generate_cache_key(path, None);
    let consolidator = cache_manager
        .get_journal_consolidator()
        .await
        .expect("FIXTURE: journal consolidator");
    if listed {
        seed_v1_fill(&disk_cache, &key, LISTED).await;
        consolidator
            .consolidate_object(&key)
            .await
            .expect("FIXTURE: fold the listed range");
        // The cycle's journal cleanup after that fold.
        for journal in std::fs::read_dir(cache_dir.join("metadata/_journals")).expect("journals") {
            let p = journal.expect("entry").path();
            if p.extension().and_then(|e| e.to_str()) == Some("journal") {
                std::fs::remove_file(&p).expect("FIXTURE: clean the folded journal lines");
            }
        }
    }
    if pending {
        seed_v1_fill(&disk_cache, &key, PENDING).await;
        assert_pending_v1_add(&disk_cache, &key).await;
    }

    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(origin.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let addr = listener.local_addr().expect("proxy address");
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let request_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_requests));
    let served_manager = Arc::clone(&cache_manager);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let (stream, peer) = match accept {
                        Ok(connection) => connection,
                        Err(_) => break,
                    };
                    let config = Arc::clone(&config);
                    let cache_manager = Arc::clone(&served_manager);
                    let range_handler = Arc::clone(&range_handler);
                    let s3_client = Arc::clone(&s3_client);
                    let request_semaphore = Arc::clone(&request_semaphore);
                    tokio::spawn(async move {
                        let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                            let config = Arc::clone(&config);
                            let cache_manager = Arc::clone(&cache_manager);
                            let range_handler = Arc::clone(&range_handler);
                            let s3_client = Arc::clone(&s3_client);
                            let request_semaphore = Arc::clone(&request_semaphore);
                            let ledger = s3_client.get_inflight_ledger();
                            async move {
                                HttpProxy::handle_request(
                                    request,
                                    peer,
                                    config,
                                    cache_manager,
                                    s3_client,
                                    range_handler,
                                    request_semaphore,
                                    None,
                                    None,
                                    Arc::clone(inflight),
                                    None,
                                    None,
                                    None,
                                    ledger,
                                ).await
                            }
                        });
                        let _ = http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    });
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });
    PendingFixture {
        fx: Fixture {
            _temp_dir: temp_dir,
            cache_dir,
            disk_cache,
            origin,
            addr,
            shutdown_tx: Some(shutdown_tx),
        },
        cache_manager,
        key,
        path: path.to_string(),
        inflight,
        _peer: None,
    }
}

/// The `.meta` does not list `PENDING`, and the journal holds v1's `Add` for it.
async fn assert_pending_v1_add(disk_cache: &Arc<tokio::sync::RwLock<DiskCacheManager>>, key: &str) {
    let meta = disk_cache
        .read()
        .await
        .get_metadata(key)
        .await
        .expect("meta read")
        .expect("FIXTURE: v1 .meta");
    assert!(
        meta.ranges.iter().all(|r| r.start != PENDING.0 as u64),
        "FIXTURE: the .meta must not list the pending extent"
    );
    let found = disk_cache
        .read()
        .await
        .find_pending_journal_ranges(key, PENDING.0 as u64, PENDING.1 as u64)
        .await
        .expect("pending lookup");
    assert_eq!(
        found.len(),
        1,
        "FIXTURE: v1's Add for the pending extent is pending"
    );
}

/// A peer proxy on the same volume, as `invalidate_for_write`'s caller.
async fn peer_manager(pf: &PendingFixture) -> Arc<CacheManager> {
    let peer = shared_manager(&pending_config(pf.fx.cache_dir.clone()));
    let _peer_disk = peer.create_configured_disk_cache_manager();
    peer.initialize().await.expect("peer init");
    peer
}

/// How S3's newer version came to be seen, and what failed while it was.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Invalidation {
    /// A peer's invalidation of an acknowledged write or DELETE (`invalidate_for_write`, the
    /// stamping invalidation `invalidate_acknowledged_write` runs, without the background stamp
    /// retry it starts on `Err`), with the `.meta` unlink failing (EACCES, persistently). The
    /// retry table is keyed by cache directory, which two managers in one test process share
    /// and two proxies do not, so the peer is called directly.
    AcknowledgedWriteMetaUnlinkFails,
    /// The same, with the fill's `Add` published after the invalidation enumerated the journals
    /// and before its slot-lock hold: its S3 request began before the write, so its witness is
    /// the token the stamp replaces.
    AcknowledgedWriteFillAfterEnumeration,
    /// The 412 and recovery-refetch invalidation (`invalidate_all_ranges`, as
    /// `invalidate_after_recovery_version_change` runs it) where the key's range directory
    /// refuses unlinks: the listed and the pending `.bin` unlinks both fail, persistently.
    VersionChangeBinUnlinksFail,
    /// A GET-path version change (`invalidate_cache_hierarchy`, marker mode `Ignore`), with no
    /// fault. The `.meta` is unlinked.
    IgnoreModeVersionChange,
    /// The same, with the pending `.bin` unlink failing (persistently) and the `Remove`-marker
    /// append for the key failing too.
    IgnoreModeBinUnlinkAndMarkerFail,
    /// `invalidate_all_ranges` with the pending `.bin` unlink failing (persistently) and the
    /// `Remove`-marker append failing too. The listed unlink succeeds, so the `.meta` is unlinked.
    VersionChangeBinUnlinkAndMarkerFail,
}

/// The pending extent's `.bin`, as its journal `Add` names it.
async fn pending_bin(pf: &PendingFixture) -> std::path::PathBuf {
    let pending = pf
        .fx
        .disk_cache
        .read()
        .await
        .find_pending_journal_ranges(&pf.key, PENDING.0 as u64, PENDING.1 as u64)
        .await
        .expect("pending lookup");
    pf.fx.cache_dir.join("ranges").join(&pending[0].file_path)
}

/// The fixture, after S3 moved to v2 and `how` ran.
async fn invalidated(path: &str, how: Invalidation) -> PendingFixture {
    let mut pf = pending_fixture(
        path,
        how != Invalidation::AcknowledgedWriteFillAfterEnumeration,
    )
    .await;
    *pf.fx.origin.current.lock().expect("current") = V2;
    match how {
        Invalidation::AcknowledgedWriteMetaUnlinkFails
        | Invalidation::AcknowledgedWriteFillAfterEnumeration => {
            let peer = peer_manager(&pf).await;
            let meta_path = pf.cache_manager.get_new_metadata_file_path(&pf.key);
            for _ in 0..PERSISTENT_FAULT {
                crate::disk_cache::unlink_fault::arm(&meta_path);
            }
            let result = if how == Invalidation::AcknowledgedWriteFillAfterEnumeration {
                let fill = pf
                    .fx
                    .disk_cache
                    .write()
                    .await
                    .prepare_range_store_witnessed(
                        &pf.key,
                        PENDING.0 as u64,
                        PENDING.1 as u64,
                        &V1_BODY[PENDING.0..=PENDING.1],
                        object_metadata(&V1),
                        Duration::from_secs(3600),
                        false,
                    )
                    .await
                    .expect("FIXTURE: prepare the fill")
                    .expect("FIXTURE: the fill is admitted");
                let gate = crate::cache::stamp_pre_hold_gate::arm(&pf.key);
                let task = tokio::spawn({
                    let peer = Arc::clone(&peer);
                    let key = pf.key.clone();
                    async move { peer.invalidate_for_write(&key).await.map(|_| ()) }
                });
                tokio::time::timeout(Duration::from_secs(10), gate.reached.notified())
                    .await
                    .expect("FIXTURE: the invalidation reaches its pre-hold gate");
                fill.finish()
                    .await
                    .expect("FIXTURE: the fill publishes before the stamp");
                assert_pending_v1_add(&pf.fx.disk_cache, &pf.key).await;
                gate.resume.notify_one();
                tokio::time::timeout(Duration::from_secs(30), task)
                    .await
                    .expect("FIXTURE: the invalidation finishes")
                    .expect("FIXTURE: join")
            } else {
                peer.invalidate_for_write(&pf.key).await.map(|_| ())
            };
            assert!(
                result.is_err(),
                "FIXTURE: the .meta unlink failure is reported as Err"
            );
            assert!(
                !pf.cache_manager.write_stamp_pending(&pf.key),
                "FIXTURE: the serving proxy holds no pending stamp for the key"
            );
            assert!(meta_path.exists(), "FIXTURE: the .meta stays");
            pf._peer = Some(peer);
        }
        Invalidation::VersionChangeBinUnlinksFail => {
            let ranges = pf.fx.cache_dir.join("ranges");
            let meta = pf
                .fx
                .disk_cache
                .read()
                .await
                .get_metadata(&pf.key)
                .await
                .expect("meta read")
                .expect("FIXTURE: .meta");
            let listed_bin = ranges.join(&meta.ranges[0].file_path);
            let pending = pf
                .fx
                .disk_cache
                .read()
                .await
                .find_pending_journal_ranges(&pf.key, PENDING.0 as u64, PENDING.1 as u64)
                .await
                .expect("pending lookup");
            let pending_bin = ranges.join(&pending[0].file_path);
            for _ in 0..PERSISTENT_FAULT {
                crate::disk_cache::unlink_fault::arm(&listed_bin);
                crate::disk_cache::unlink_fault::arm(&pending_bin);
            }
            pf.fx
                .disk_cache
                .read()
                .await
                .invalidate_all_ranges(&pf.key)
                .await
                .expect("FIXTURE: invalidate_all_ranges");
            pf.cache_manager.invalidate_metadata_cache(&pf.key).await;
            assert!(
                listed_bin.exists() && pending_bin.exists(),
                "FIXTURE: both injected failures leave their .bin"
            );
        }
        Invalidation::IgnoreModeVersionChange => {
            pf.cache_manager
                .invalidate_cache_hierarchy(&pf.key)
                .await
                .expect("FIXTURE: invalidate_cache_hierarchy");
            assert!(
                !pf.cache_manager
                    .get_new_metadata_file_path(&pf.key)
                    .exists(),
                "FIXTURE: the .meta is gone"
            );
        }
        Invalidation::IgnoreModeBinUnlinkAndMarkerFail
        | Invalidation::VersionChangeBinUnlinkAndMarkerFail => {
            let bin = pending_bin(&pf).await;
            for _ in 0..PERSISTENT_FAULT {
                crate::disk_cache::unlink_fault::arm(&bin);
                crate::journal_consolidator::remove_marker_append_fault::arm(&pf.key);
            }
            if how == Invalidation::IgnoreModeBinUnlinkAndMarkerFail {
                pf.cache_manager
                    .invalidate_cache_hierarchy(&pf.key)
                    .await
                    .expect("FIXTURE: invalidate_cache_hierarchy");
            } else {
                pf.fx
                    .disk_cache
                    .read()
                    .await
                    .invalidate_all_ranges(&pf.key)
                    .await
                    .expect("FIXTURE: invalidate_all_ranges");
                pf.cache_manager.invalidate_metadata_cache(&pf.key).await;
            }
            assert!(
                bin.exists(),
                "FIXTURE: the injected unlink failure leaves the .bin"
            );
            assert!(
                !pf.cache_manager
                    .get_new_metadata_file_path(&pf.key)
                    .exists(),
                "FIXTURE: the .meta is gone"
            );
        }
    }
    pf
}

/// One consolidation cycle for the key: the fold, then the cycle's journal cleanup of the
/// lines the fold consumed or retired.
async fn fold_cycle(pf: &PendingFixture) {
    let consolidator = pf
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator");
    let result = consolidator
        .consolidate_object(&pf.key)
        .await
        .expect("consolidation");
    consolidator
        .cleanup_consolidated_entries(&result.consolidated_entries, &HashMap::new())
        .await
        .expect("journal cleanup");
}

/// The key's `.meta` as a test failure prints it.
async fn describe_meta(pf: &PendingFixture) -> String {
    match pf
        .fx
        .disk_cache
        .read()
        .await
        .get_metadata(&pf.key)
        .await
        .ok()
        .flatten()
    {
        None => "no .meta".to_string(),
        Some(m) => format!(
            "etag={} expires_in_past={} ranges(start,end,unservable)={:?}",
            m.object_metadata.etag,
            m.expires_at <= std::time::SystemTime::now(),
            m.ranges
                .iter()
                .map(|r| (r.start, r.end, r.unservable))
                .collect::<Vec<_>>()
        ),
    }
}

/// What one request got, with the origin requests it caused and the `.meta` after it.
struct Observed {
    answer: Answer,
    origin_requests: usize,
    meta: String,
}

impl std::fmt::Display for Observed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "status={} etag={:?} content-length={:?} body={:?} origin_requests={} .meta: {}",
            self.answer.status,
            self.answer.headers.get("etag"),
            self.answer.headers.get("content-length"),
            String::from_utf8_lossy(&self.answer.body),
            self.origin_requests,
            self.meta
        )
    }
}

async fn observe(
    pf: &PendingFixture,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
) -> Observed {
    let before = pf.fx.origin.seen().len();
    let answer = send(pf.fx.addr, method, path, headers).await;
    let origin_requests = pf.fx.origin.seen().len() - before;
    Observed {
        answer,
        origin_requests,
        meta: describe_meta(pf).await,
    }
}

/// The range GET of the pending extent, before and after one consolidation cycle.
async fn pending_extent_before_and_after_a_fold(pf: &PendingFixture) -> (Observed, Observed) {
    let range = format!("bytes={}-{}", PENDING.0, PENDING.1);
    let before = observe(pf, Method::GET, &pf.path, &[("range", range.as_str())]).await;
    fold_cycle(pf).await;
    let after = observe(pf, Method::GET, &pf.path, &[("range", range.as_str())]).await;
    (before, after)
}

fn is_v2_pending_extent(o: &Observed) -> bool {
    o.answer.status == StatusCode::PARTIAL_CONTENT
        && o.answer.body.as_ref() == &V2_BODY[PENDING.0..=PENDING.1]
}

#[derive(Clone, Copy, Debug)]
enum ControlRequest {
    WholeGet,
    Head,
    PartGet,
}

/// Controls for one invalidation shape: a whole-object GET, a HEAD and a part GET, each on a
/// fresh fixture, before and after a consolidation cycle, are S3's v2.
async fn controls(stem: &str, how: Invalidation) {
    let mut wrong = Vec::new();
    for fold in [false, true] {
        for request in [
            ControlRequest::WholeGet,
            ControlRequest::Head,
            ControlRequest::PartGet,
        ] {
            let path = format!("/bucket/{stem}-{request:?}-fold-{fold}.bin").to_lowercase();
            let pf = invalidated(&path, how).await;
            if fold {
                fold_cycle(&pf).await;
            }
            let (observed, v2) = match request {
                ControlRequest::WholeGet => {
                    let o = observe(&pf, Method::GET, &path, &[]).await;
                    let v2 = o.answer.status == StatusCode::OK && o.answer.body.as_ref() == V2_BODY;
                    (o, v2)
                }
                ControlRequest::Head => {
                    let o = observe(&pf, Method::HEAD, &path, &[]).await;
                    let header = |name: &str| {
                        o.answer
                            .headers
                            .get(name)
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string)
                    };
                    let v2 = o.answer.status == StatusCode::OK
                        && header("etag").as_deref() == Some(V2_ETAG)
                        && header("content-length") == Some(V2_BODY.len().to_string());
                    (o, v2)
                }
                ControlRequest::PartGet => {
                    let part = format!("{path}?partNumber=1");
                    let o = observe(&pf, Method::GET, &part, &[]).await;
                    // The origin answers a part request with the whole object, as S3 does for
                    // an object uploaded in one part.
                    let v2 = o.answer.status.is_success() && o.answer.body.as_ref() == V2_BODY;
                    (o, v2)
                }
            };
            if !v2 {
                wrong.push(format!("{request:?} (after a fold: {fold}): {observed}"));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{how:?}: a control request did not get S3's current version:\n{}",
        wrong.join("\n")
    );
}

/// A peer acknowledged an overwrite of the key, and its `.meta` unlink failed, so the document
/// stays (rewritten to list nothing servable). This proxy filled 20-29 inside the last
/// consolidation cycle. Its range GET of 20-29 must not be served v1's bytes from that pending
/// `Add`, either before the next fold or after it.
#[tokio::test]
async fn a_peers_acknowledged_write_with_a_failed_meta_unlink_does_not_serve_a_pending_fill() {
    let pf = invalidated(
        "/bucket/pending-add-ack-meta-unlink-fails.bin",
        Invalidation::AcknowledgedWriteMetaUnlinkFails,
    )
    .await;
    let (before, after) = pending_extent_before_and_after_a_fold(&pf).await;
    assert!(
        is_v2_pending_extent(&before) && is_v2_pending_extent(&after),
        "AFTER S3 ACKNOWLEDGED AN OVERWRITE, A PEER SERVED THE PREVIOUS VERSION'S BYTES from a \
         pending journal Add.\n before the fold: {before}\n after the fold: {after}"
    );
}

/// The fill's `Add` lands between the acknowledging invalidation's journal enumeration and its
/// slot-lock hold, so the invalidation cannot unlink it. Only the stamp it then writes shows the
/// fill is superseded.
#[tokio::test]
async fn a_fill_published_inside_an_acknowledged_writes_invalidation_is_not_served() {
    let pf = invalidated(
        "/bucket/pending-add-after-enumeration.bin",
        Invalidation::AcknowledgedWriteFillAfterEnumeration,
    )
    .await;
    let (before, after) = pending_extent_before_and_after_a_fold(&pf).await;
    assert!(
        is_v2_pending_extent(&before) && is_v2_pending_extent(&after),
        "A FILL SUPERSEDED BY AN ACKNOWLEDGED WRITE WAS SERVED from its pending journal Add.\n \
         before the fold: {before}\n after the fold: {after}"
    );
}

/// S3 showed this proxy another version (a 412 on a pinned fetch, or a recovery refetch), and
/// the invalidation could unlink neither the listed nor the pending `.bin`. The range GET of the
/// pending extent must not be served v1's bytes, and the next fold must not list them.
#[tokio::test]
async fn a_version_change_with_failed_bin_unlinks_does_not_serve_a_pending_fill() {
    let pf = invalidated(
        "/bucket/pending-add-version-change-unlinks-fail.bin",
        Invalidation::VersionChangeBinUnlinksFail,
    )
    .await;
    let (before, after) = pending_extent_before_and_after_a_fold(&pf).await;
    assert!(
        is_v2_pending_extent(&before) && is_v2_pending_extent(&after),
        "AFTER S3 SHOWED ANOTHER VERSION, THE PREVIOUS VERSION'S BYTES WERE SERVED from a pending \
         journal Add whose unlink failed.\n before the fold: {before}\n after the fold: {after}"
    );
}

/// A version change seen on the GET path removes the key's entry with no I/O failure. The
/// pending `Add` of the removed version must not stay for the next fold to rebuild a document
/// from: the range GET of its extent must be S3's v2 before that fold and after it.
#[tokio::test]
async fn an_ignore_mode_version_change_leaves_no_pending_fill_for_the_fold() {
    let pf = invalidated(
        "/bucket/pending-add-ignore-mode-version-change.bin",
        Invalidation::IgnoreModeVersionChange,
    )
    .await;
    let (before, after) = pending_extent_before_and_after_a_fold(&pf).await;
    assert!(
        is_v2_pending_extent(&before) && is_v2_pending_extent(&after),
        "AFTER A VERSION CHANGE, THE NEXT FOLD REBUILT THE PREVIOUS VERSION'S DOCUMENT from a \
         pending journal Add and it was served.\n before the fold: {before}\n after the fold: \
         {after}"
    );
}

#[tokio::test]
async fn an_ignore_mode_version_change_controls() {
    controls(
        "pending-add-ignore-mode-version-change-control",
        Invalidation::IgnoreModeVersionChange,
    )
    .await;
}

#[tokio::test]
async fn a_version_change_with_failed_bin_unlinks_controls() {
    controls(
        "pending-add-version-change-unlinks-fail-control",
        Invalidation::VersionChangeBinUnlinksFail,
    )
    .await;
}

#[tokio::test]
async fn a_peers_acknowledged_write_with_a_failed_meta_unlink_controls() {
    controls(
        "pending-add-ack-meta-unlink-fails-control",
        Invalidation::AcknowledgedWriteMetaUnlinkFails,
    )
    .await;
}

#[tokio::test]
async fn a_fill_published_inside_an_acknowledged_writes_invalidation_controls() {
    controls(
        "pending-add-after-enumeration-control",
        Invalidation::AcknowledgedWriteFillAfterEnumeration,
    )
    .await;
}

/// The request whose revalidation S3 answers `404`.
#[derive(Clone, Copy, Debug)]
enum NotFoundProbe {
    /// A HEAD of the write-expired entry (`revalidate_write_expired_head`).
    Head,
    /// A range GET of the listed extent (the range path's conditional validation).
    RangeGet,
}

/// v1 with `LISTED` folded and the entry expired by a write S3 answered `5xx`, so the next GET
/// or HEAD asks S3 first. A v1 fill of `PENDING` is prepared: its S3 request is in flight. S3
/// then deletes the object, `probe` learns it from S3's `404`, and only then does the fill try
/// to publish. Returns the probe, and the range GET of `PENDING` before and after one
/// consolidation cycle.
async fn fill_straddling_a_not_found(
    path: &str,
    probe: NotFoundProbe,
) -> (Observed, Observed, Observed) {
    let pf = pending_fixture(path, false).await;
    pf.cache_manager
        .expire_for_write(&pf.key, "PUT")
        .await
        .expect("FIXTURE: expire the entry as a 5xx write does");
    let fill = pf
        .fx
        .disk_cache
        .write()
        .await
        .prepare_range_store_witnessed(
            &pf.key,
            PENDING.0 as u64,
            PENDING.1 as u64,
            &V1_BODY[PENDING.0..=PENDING.1],
            object_metadata(&V1),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("FIXTURE: prepare the fill")
        .expect("FIXTURE: the fill is admitted");
    pf.fx
        .origin
        .deleted
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let listed = format!("bytes={}-{}", LISTED.0, LISTED.1);
    let probed = match probe {
        NotFoundProbe::Head => observe(&pf, Method::HEAD, &pf.path, &[]).await,
        NotFoundProbe::RangeGet => {
            observe(&pf, Method::GET, &pf.path, &[("range", listed.as_str())]).await
        }
    };
    assert!(
        probed.answer.status == StatusCode::NOT_FOUND && probed.origin_requests == 1,
        "FIXTURE: {probe:?} is answered S3's 404 after one origin request: {probed}"
    );
    fill.finish()
        .await
        .expect("FIXTURE: the fill's publish runs");
    let (before, after) = pending_extent_before_and_after_a_fold(&pf).await;
    (probed, before, after)
}

async fn assert_deleted_range_not_served(stem: &str, probe: NotFoundProbe) {
    let path = format!("/bucket/{stem}.bin");
    let (probed, before, after) = fill_straddling_a_not_found(&path, probe).await;
    let not_found = |o: &Observed| o.answer.status == StatusCode::NOT_FOUND;
    assert!(
        not_found(&before) && not_found(&after),
        "AFTER S3 ANSWERED 404 ({probe:?}), A FILL OF THE DELETED OBJECT THAT WAS IN FLIGHT WAS \
         SERVED.\n probe: {probed}\n before the fold: {before}\n after the fold: {after}"
    );
}

/// A HEAD learns from S3's `404` that the object is gone while a range fill of it is in flight.
/// No later range GET may serve that fill, before the next fold or after it.
#[tokio::test]
async fn a_fill_in_flight_across_a_head_404_is_not_served() {
    assert_deleted_range_not_served("not-found-head-fill-in-flight", NotFoundProbe::Head).await;
}

/// The same, learnt by a range GET's revalidation.
#[tokio::test]
async fn a_fill_in_flight_across_a_range_get_404_is_not_served() {
    assert_deleted_range_not_served(
        "not-found-range-get-fill-in-flight",
        NotFoundProbe::RangeGet,
    )
    .await;
}

/// A GET-path version change could unlink neither the pending `.bin` nor append its `Remove`
/// marker. The pending `Add` must not be served, and the next fold must not rebuild the removed
/// version's document from it.
#[tokio::test]
async fn an_ignore_mode_version_change_with_a_failed_unlink_and_marker_does_not_serve_a_pending_fill(
) {
    let pf = invalidated(
        "/bucket/pending-add-ignore-mode-unlink-and-marker-fail.bin",
        Invalidation::IgnoreModeBinUnlinkAndMarkerFail,
    )
    .await;
    let (before, after) = pending_extent_before_and_after_a_fold(&pf).await;
    assert!(
        is_v2_pending_extent(&before) && is_v2_pending_extent(&after),
        "A VERSION CHANGE WHOSE UNLINK AND REMOVE MARKER BOTH FAILED LEFT THE PREVIOUS VERSION \
         SERVABLE from a pending journal Add.\n before the fold: {before}\n after the fold: \
         {after}"
    );
}

/// The same for the 412 and recovery-refetch invalidation (`invalidate_all_ranges`).
#[tokio::test]
async fn a_version_change_with_a_failed_unlink_and_marker_does_not_serve_a_pending_fill() {
    let pf = invalidated(
        "/bucket/pending-add-version-change-unlink-and-marker-fail.bin",
        Invalidation::VersionChangeBinUnlinkAndMarkerFail,
    )
    .await;
    let (before, after) = pending_extent_before_and_after_a_fold(&pf).await;
    assert!(
        is_v2_pending_extent(&before) && is_v2_pending_extent(&after),
        "A 412 INVALIDATION WHOSE UNLINK AND REMOVE MARKER BOTH FAILED LEFT THE PREVIOUS VERSION \
         SERVABLE from a pending journal Add.\n before the fold: {before}\n after the fold: \
         {after}"
    );
}

/// A streamed whole-object GET miss on this proxy filled v1 before any `.meta` existed, so its
/// fill-visibility entry serves it until the fold. S3 then moved to v2, and a peer proxy's
/// version-change invalidation removed the key but could neither unlink the fill's `.bin` nor
/// append its `Remove` marker. This proxy's next GET must be S3's v2.
///
/// With the marker appended, the GET is v2 without the fresh stamp too: the tracker still holds
/// the entry, but the serve takes its coverage from the journal, which the marker retires.
#[tokio::test]
async fn a_peers_version_change_with_a_failed_unlink_and_marker_ends_this_proxys_tracked_fill() {
    let path = "/bucket/tracked-fill-peer-version-change-unlink-fails.bin";
    let pf = pending_fixture_with(path, false, false, OriginS3::streaming(V1).await).await;
    let filled = observe(&pf, Method::GET, path, &[]).await;
    assert_eq!(
        filled.answer.body.as_ref(),
        V1_BODY,
        "FIXTURE: the miss is v1: {filled}"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !pf.cache_manager.fill_visibility_tracker().holds(&pf.key) {
        assert!(
            std::time::Instant::now() < deadline,
            "FIXTURE: the streamed fill was not recorded in the fill-visibility tracker: {filled}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let served = observe(&pf, Method::GET, path, &[]).await;
    assert!(
        served.origin_requests == 0 && served.answer.body.as_ref() == V1_BODY,
        "FIXTURE: the tracker serves the fill: {served}"
    );
    *pf.fx.origin.current.lock().expect("current") = V2;
    let pending = pf
        .fx
        .disk_cache
        .read()
        .await
        .find_pending_journal_ranges(&pf.key, 0, V1_BODY.len() as u64 - 1)
        .await
        .expect("pending lookup");
    let bin = pf.fx.cache_dir.join("ranges").join(&pending[0].file_path);
    for _ in 0..PERSISTENT_FAULT {
        crate::disk_cache::unlink_fault::arm(&bin);
        crate::journal_consolidator::remove_marker_append_fault::arm(&pf.key);
    }
    let peer = peer_manager(&pf).await;
    peer.invalidate_cache_hierarchy(&pf.key)
        .await
        .expect("FIXTURE: the peer's invalidation");
    assert!(
        bin.exists(),
        "FIXTURE: the injected unlink failure leaves the .bin"
    );
    let after = observe(&pf, Method::GET, path, &[]).await;
    assert!(
        after.answer.status == StatusCode::OK && after.answer.body.as_ref() == V2_BODY,
        "AFTER A PEER'S VERSION CHANGE, THIS PROXY'S FILL-VISIBILITY ENTRY SERVED THE PREVIOUS \
         VERSION.\n filled: {filled}\n served before: {served}\n after: {after}"
    );
}

/// Download coordination on, so a request that misses while a fetch of the key is in flight
/// waits for it and then validates with S3 itself.
fn coordinated(config: &mut Config) {
    config.cache.download_coordination.enabled = true;
    config.cache.download_coordination.wait_timeout_secs = 10;
}
/// A v1 fill of `PENDING` whose S3 request is in flight: its witness is the key's marker now,
/// and it publishes only in [`after_the_straddling_fill`].
async fn straddling_v1_fill(pf: &PendingFixture) -> crate::disk_cache::PreparedRangeFill {
    pf.fx
        .disk_cache
        .write()
        .await
        .prepare_range_store_witnessed(
            &pf.key,
            PENDING.0 as u64,
            PENDING.1 as u64,
            &V1_BODY[PENDING.0..=PENDING.1],
            object_metadata(&V1),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("FIXTURE: prepare the fill")
        .expect("FIXTURE: the fill is admitted")
}
/// The straddling fill tries to publish; then the range GET of `PENDING` before and after one
/// consolidation cycle.
async fn after_the_straddling_fill(
    pf: &PendingFixture,
    fill: crate::disk_cache::PreparedRangeFill,
) -> (Observed, Observed) {
    fill.finish()
        .await
        .expect("FIXTURE: the fill's publish runs");
    pending_extent_before_and_after_a_fold(pf).await
}
fn is_not_found(o: &Observed) -> bool {
    o.answer.status == StatusCode::NOT_FOUND
}
fn assert_straddling_fill_not_served(
    what: &str,
    probe: &Observed,
    before: &Observed,
    after: &Observed,
    s3s_answer: fn(&Observed) -> bool,
) {
    assert!(
        s3s_answer(before) && s3s_answer(after),
        "AFTER {what}, A FILL OF THE PREVIOUS VERSION THAT WAS IN FLIGHT WAS SERVED.\n probe: \
         {probe}\n before the fold: {before}\n after the fold: {after}"
    );
}
/// The `.meta` of `pf`'s key, once `edit` has changed it. Written without the slot lock: no
/// other writer runs in these fixtures.
async fn edit_meta(
    pf: &PendingFixture,
    edit: impl FnOnce(&mut crate::cache_types::NewCacheMetadata),
) {
    let mut meta = pf
        .fx
        .disk_cache
        .read()
        .await
        .get_metadata(&pf.key)
        .await
        .expect("FIXTURE: the .meta reads")
        .expect("FIXTURE: the .meta exists");
    edit(&mut meta);
    std::fs::write(
        pf.cache_manager.get_new_metadata_file_path(&pf.key),
        serde_json::to_string(&meta).expect("FIXTURE: serialize the .meta"),
    )
    .expect("FIXTURE: write the .meta");
}
/// v1 cached whole and folded, then expired as a write S3 answered `5xx` expires it, so the
/// next GET asks S3 first.
async fn whole_v1_expired(path: &str) -> PendingFixture {
    let pf = pending_fixture_with(path, false, false, OriginS3::new(V1)).await;
    seed_v1_fill(&pf.fx.disk_cache, &pf.key, (0, V1_BODY.len() - 1)).await;
    fold_cycle(&pf).await;
    pf.cache_manager
        .expire_for_write(&pf.key, "PUT")
        .await
        .expect("FIXTURE: expire the entry as a 5xx write does");
    pf
}
/// A `412` on the proxy's own `If-Match` pin, and the recovery invalidation that follows it,
/// while a v1 fill of another extent is in flight. S3 now holds v2; the hole fetch for 10-19
/// fails, so the fallback fetch of 0-19 carries the pin.
#[tokio::test]
async fn a_fill_in_flight_across_a_412_recovery_is_not_served() {
    let pf = pending_fixture("/bucket/recovery-412-fill-in-flight.bin", false).await;
    let fill = straddling_v1_fill(&pf).await;
    *pf.fx.origin.current.lock().expect("current") = V2;
    pf.fx
        .origin
        .fail_once
        .lock()
        .expect("fail_once")
        .push("bytes=10-19".to_string());
    let probe = observe(&pf, Method::GET, &pf.path, &[("range", "bytes=0-19")]).await;
    let seen = pf.fx.origin.seen();
    assert!(
        seen.iter()
            .any(|r| r.get("range") == Some("bytes=0-19") && r.get("if-match") == Some(V1_ETAG))
            && probe.answer.body.as_ref() == &V2_BODY[0..20],
        "FIXTURE: the pinned fallback fetch must reach S3, and the retry be answered v2: \
         {probe}\n{seen:?}"
    );
    let (before, after) = after_the_straddling_fill(&pf, fill).await;
    assert_straddling_fill_not_served(
        "A 412 ON THE PROXY'S OWN PIN",
        &probe,
        &before,
        &after,
        is_v2_pending_extent,
    );
}
/// A whole-object GET's revalidation, which S3 answers `200` with v2 (marker mode `Ignore`
/// before this change), while a v1 fill of `PENDING` is in flight.
#[tokio::test]
async fn a_fill_in_flight_across_a_changed_full_object_is_not_served() {
    let pf = whole_v1_expired("/bucket/changed-full-object-fill-in-flight.bin").await;
    let fill = straddling_v1_fill(&pf).await;
    *pf.fx.origin.current.lock().expect("current") = V2;
    let probe = observe(&pf, Method::GET, &pf.path, &[]).await;
    let validated = pf
        .fx
        .origin
        .seen()
        .iter()
        .any(|r| r.get("if-none-match") == Some(V1_ETAG));
    assert!(
        validated && probe.answer.status == StatusCode::OK && probe.answer.body.as_ref() == V2_BODY,
        "FIXTURE: the whole-object GET revalidates and is answered v2: {probe}"
    );
    let (before, after) = after_the_straddling_fill(&pf, fill).await;
    assert_straddling_fill_not_served(
        "A WHOLE-OBJECT REVALIDATION ANSWERED WITH ANOTHER VERSION",
        &probe,
        &before,
        &after,
        is_v2_pending_extent,
    );
}
/// A range GET's revalidation of the listed extent, which S3 answers with v2, while a v1 fill
/// of `PENDING` is in flight.
#[tokio::test]
async fn a_fill_in_flight_across_a_changed_range_is_not_served() {
    let pf = pending_fixture_with(
        "/bucket/changed-range-fill-in-flight.bin",
        true,
        false,
        OriginS3::new(V1),
    )
    .await;
    pf.cache_manager
        .expire_for_write(&pf.key, "PUT")
        .await
        .expect("FIXTURE: expire the entry as a 5xx write does");
    let fill = straddling_v1_fill(&pf).await;
    *pf.fx.origin.current.lock().expect("current") = V2;
    let listed = format!("bytes={}-{}", LISTED.0, LISTED.1);
    let probe = observe(&pf, Method::GET, &pf.path, &[("range", listed.as_str())]).await;
    assert!(
        probe.answer.status == StatusCode::PARTIAL_CONTENT
            && probe.answer.body.as_ref() == &V2_BODY[LISTED.0..=LISTED.1],
        "FIXTURE: the range GET revalidates and is answered v2: {probe}"
    );
    let (before, after) = after_the_straddling_fill(&pf, fill).await;
    assert_straddling_fill_not_served(
        "A RANGE REVALIDATION ANSWERED WITH ANOTHER VERSION",
        &probe,
        &before,
        &after,
        is_v2_pending_extent,
    );
}
/// A whole-object GET waits on a fetch in flight, which commits v1. S3 then moves to v2, or
/// deletes the object (`deleted`), and a v1 fill of `PENDING` is in flight when the waiter
/// validates with S3. Returns the waiter's answer and the range GET of `PENDING` before and
/// after one consolidation cycle.
async fn waiter_validates_across_a_fill(
    stem: &str,
    deleted: bool,
) -> (Observed, Observed, Observed) {
    let path = format!("/bucket/{stem}.bin");
    let pf = pending_fixture_configured(&path, false, false, OriginS3::new(V1), coordinated).await;
    let fetch = match pf
        .inflight
        .try_register(&InFlightTracker::make_full_key(&pf.key))
    {
        crate::inflight_tracker::FetchRole::Fetcher(guard) => guard,
        crate::inflight_tracker::FetchRole::Waiter(_) => panic!("FIXTURE: no fetch is in flight"),
    };
    let waiter = {
        let addr = pf.fx.addr;
        let path = pf.path.clone();
        tokio::spawn(async move { send(addr, Method::GET, &path, &[]).await })
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while fetch.waiter_count() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "FIXTURE: the whole-object GET must wait on the fetch in flight"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The fetch commits v1.
    seed_v1_fill(&pf.fx.disk_cache, &pf.key, LISTED).await;
    fold_cycle(&pf).await;
    let fill = straddling_v1_fill(&pf).await;
    if deleted {
        pf.fx
            .origin
            .deleted
            .store(true, std::sync::atomic::Ordering::SeqCst);
    } else {
        *pf.fx.origin.current.lock().expect("current") = V2;
    }
    let seen_before = pf.fx.origin.seen().len();
    fetch.complete_success();
    let answer = tokio::time::timeout(Duration::from_secs(30), waiter)
        .await
        .expect("FIXTURE: the waiter answers")
        .expect("FIXTURE: the waiter's task");
    let seen = pf.fx.origin.seen();
    let probe = Observed {
        answer,
        origin_requests: seen.len() - seen_before,
        meta: describe_meta(&pf).await,
    };
    assert!(
        probe.origin_requests == 1
            && seen.last().and_then(|r| r.get("if-none-match")) == Some(V1_ETAG),
        "FIXTURE: the waiter validates the committed v1 with S3: {probe}\n{seen:?}"
    );
    let (before, after) = after_the_straddling_fill(&pf, fill).await;
    (probe, before, after)
}
/// The waiter's validation is answered `200` with v2.
#[tokio::test]
async fn a_fill_in_flight_across_a_waiter_shown_another_etag_is_not_served() {
    let (probe, before, after) =
        waiter_validates_across_a_fill("waiter-other-etag-fill-in-flight", false).await;
    assert!(
        probe.answer.status == StatusCode::OK && probe.answer.body.as_ref() == V2_BODY,
        "FIXTURE: the waiter is answered v2: {probe}"
    );
    assert_straddling_fill_not_served(
        "A COALESCING WAITER WAS SHOWN ANOTHER ETAG",
        &probe,
        &before,
        &after,
        is_v2_pending_extent,
    );
}
/// The waiter's validation is answered `404`.
#[tokio::test]
async fn a_fill_in_flight_across_a_waiter_404_is_not_served() {
    let (probe, before, after) =
        waiter_validates_across_a_fill("waiter-404-fill-in-flight", true).await;
    assert!(
        is_not_found(&probe),
        "FIXTURE: the waiter is answered S3's 404: {probe}"
    );
    assert_straddling_fill_not_served(
        "A COALESCING WAITER WAS ANSWERED 404",
        &probe,
        &before,
        &after,
        is_not_found,
    );
}
/// The coalescing waiters that validate a cached extent rather than the whole object.
#[derive(Clone, Copy, Debug)]
enum ExtentWaiter {
    /// A range GET of `WAITED`.
    Range,
    /// A `?partNumber=1` GET of a one-part object.
    Part,
}
/// The extent the range waiter asks for: neither `LISTED` nor `PENDING`.
const WAITED: (usize, usize) = (10, 19);
/// One part covering the whole object, as S3 lays out an object uploaded in one part.
fn one_part(meta: &mut crate::cache_types::NewCacheMetadata) {
    let o = &mut meta.object_metadata;
    o.parts_count = Some(1);
    o.part_layout_verified = true;
    o.part_ranges.insert(1, (0, V1_BODY.len() as u64 - 1));
    o.part_layout_marked.insert(1);
}
/// `kind`'s request waits on a fetch in flight, which commits v1. S3 then moves to v2, and a v1
/// fill of `PENDING` is in flight when the waiter validates the cached v1 with S3. S3 answers v2:
/// `206` to the range, `200` to the part (this origin ignores `partNumber`). Returns the waiter's
/// answer, the range GET of `PENDING` before and after one consolidation cycle, and the
/// waiter's request sent again after them.
async fn extent_waiter_shown_another_version(
    stem: &str,
    kind: ExtentWaiter,
) -> (Observed, Observed, Observed, Observed) {
    let path = format!("/bucket/{stem}.bin");
    let pf = pending_fixture_configured(&path, true, false, OriginS3::new(V1), coordinated).await;
    let waited = format!("bytes={}-{}", WAITED.0, WAITED.1);
    let (target, headers, flight_key) = match kind {
        ExtentWaiter::Range => (
            pf.path.clone(),
            vec![("range".to_string(), waited.clone())],
            InFlightTracker::make_range_key(&pf.key, WAITED.0 as u64, WAITED.1 as u64),
        ),
        ExtentWaiter::Part => (
            format!("{}?partNumber=1", pf.path),
            Vec::new(),
            InFlightTracker::make_part_key(&pf.key, 1),
        ),
    };
    let fetch = match pf.inflight.try_register(&flight_key) {
        crate::inflight_tracker::FetchRole::Fetcher(guard) => guard,
        crate::inflight_tracker::FetchRole::Waiter(_) => panic!("FIXTURE: no fetch is in flight"),
    };
    let waiter = {
        let addr = pf.fx.addr;
        let target = target.clone();
        let headers = headers.clone();
        tokio::spawn(async move {
            let headers: Vec<(&str, &str)> = headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            send(addr, Method::GET, &target, &headers).await
        })
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while fetch.waiter_count() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "FIXTURE: the {kind:?} GET must wait on the fetch in flight"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The fetch commits v1.
    match kind {
        ExtentWaiter::Range => {
            seed_v1_fill(&pf.fx.disk_cache, &pf.key, WAITED).await;
            fold_cycle(&pf).await;
        }
        ExtentWaiter::Part => {
            seed_v1_fill(&pf.fx.disk_cache, &pf.key, (0, V1_BODY.len() - 1)).await;
            fold_cycle(&pf).await;
            edit_meta(&pf, one_part).await;
        }
    }
    let fill = straddling_v1_fill(&pf).await;
    *pf.fx.origin.current.lock().expect("current") = V2;
    let seen_before = pf.fx.origin.seen().len();
    fetch.complete_success();
    let answer = tokio::time::timeout(Duration::from_secs(30), waiter)
        .await
        .expect("FIXTURE: the waiter answers")
        .expect("FIXTURE: the waiter's task");
    let seen = pf.fx.origin.seen();
    let probe = Observed {
        answer,
        origin_requests: seen.len() - seen_before,
        meta: describe_meta(&pf).await,
    };
    assert!(
        probe.origin_requests == 1
            && seen.last().and_then(|r| r.get("if-none-match")) == Some(V1_ETAG)
            && is_v2_waited(kind, &probe),
        "FIXTURE: the {kind:?} waiter validates the committed v1 with S3 and is answered v2: \
         {probe}\n{seen:?}"
    );
    let (before, after) = after_the_straddling_fill(&pf, fill).await;
    let headers: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let again = observe(&pf, Method::GET, &target, &headers).await;
    (probe, before, after, again)
}
/// Whether `o` is S3's v2 answer to `kind`'s request.
fn is_v2_waited(kind: ExtentWaiter, o: &Observed) -> bool {
    match kind {
        ExtentWaiter::Range => {
            o.answer.status == StatusCode::PARTIAL_CONTENT
                && o.answer.body.as_ref() == &V2_BODY[WAITED.0..=WAITED.1]
        }
        ExtentWaiter::Part => {
            o.answer.status == StatusCode::OK && o.answer.body.as_ref() == V2_BODY
        }
    }
}
async fn assert_extent_waiter_supersedes(stem: &str, kind: ExtentWaiter) {
    let (probe, before, after, again) = extent_waiter_shown_another_version(stem, kind).await;
    assert!(
        is_v2_pending_extent(&before) && is_v2_pending_extent(&after) && is_v2_waited(kind, &again),
        "AFTER A COALESCING {kind:?} WAITER WAS SHOWN ANOTHER VERSION, THE SUPERSEDED ENTRY WAS \
         SERVED.\n probe: {probe}\n before the fold: {before}\n after the fold: {after}\n the \
         waiter's request again: {again}"
    );
}
/// A range waiter's validation is answered `206` with v2: no later read serves v1, the cached
/// extent or a fill of it that was in flight.
#[tokio::test]
async fn a_range_waiter_shown_another_version_leaves_nothing_superseded_served() {
    assert_extent_waiter_supersedes("range-waiter-other-version", ExtentWaiter::Range).await;
}
/// The same for a part waiter's validation, answered `200` with v2.
#[tokio::test]
async fn a_part_waiter_shown_another_version_leaves_nothing_superseded_served() {
    assert_extent_waiter_supersedes("part-waiter-other-version", ExtentWaiter::Part).await;
}
/// An expired cached part's revalidation is answered `404` while a v1 fill of `PENDING` is in
/// flight.
#[tokio::test]
async fn a_fill_in_flight_across_a_part_404_is_not_served() {
    let pf = whole_v1_expired("/bucket/part-404-fill-in-flight.bin").await;
    edit_meta(&pf, one_part).await;
    let fill = straddling_v1_fill(&pf).await;
    pf.fx
        .origin
        .deleted
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let part = format!("{}?partNumber=1", pf.path);
    let probe = observe(&pf, Method::GET, &part, &[]).await;
    let seen = pf.fx.origin.seen();
    assert!(
        is_not_found(&probe)
            && probe.origin_requests == 1
            && seen.last().and_then(|r| r.get("if-none-match")) == Some(V1_ETAG),
        "FIXTURE: the part GET revalidates and is answered S3's 404: {probe}\n{seen:?}"
    );
    let (before, after) = after_the_straddling_fill(&pf, fill).await;
    assert_straddling_fill_not_served(
        "A PART REVALIDATION ANSWERED 404",
        &probe,
        &before,
        &after,
        is_not_found,
    );
}
