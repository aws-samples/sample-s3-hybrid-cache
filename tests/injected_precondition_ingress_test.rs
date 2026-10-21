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

mod common;

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

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{CacheMetadata, ObjectMetadata, UploadState};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::Config;
use s3_proxy::connection_pool::ConnectionPoolManager;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::{Result, S3ClientApi, S3RequestContext, S3Response, S3ResponseBody};

use common::StubS3Client;

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
        _metrics_manager: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
    ) {
    }

    async fn register_endpoint(&self, _endpoint: &str) {}

    async fn refresh_dns(&self) -> Result<()> {
        Ok(())
    }

    fn get_inflight_ledger(&self) -> Arc<s3_proxy::inflight_ledger::InflightLedger> {
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
