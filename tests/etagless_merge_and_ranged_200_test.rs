//! One response is one version, also where the cache cannot name the version.
//!
//! - **An extent with no recorded ETag.** An origin that sends no `ETag` leaves cached
//!   extents with an empty one. A partially cached range read then had nothing to pin its
//!   hole fetch to and nothing to compare the fetched segments with, so after an overwrite
//!   it merged old cached bytes with new fetched bytes into one `206`.
//! - **A `200` to a ranged miss.** S3 answers a stale `If-Range` with `200` and the whole
//!   object. The streamed miss path must not commit that body as the requested range: with
//!   the production `partial_range_commit_ratio` (0.5) a body shorter than the range is
//!   salvaged as a prefix, which would file the object's leading bytes at the requested
//!   offset.
//!
//! Both are driven through the real `HttpProxy::handle_request` on a loopback port, on the
//! harness of `injected_precondition_ingress_test.rs` with the disk cache built the way
//! production builds it.

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

use s3_proxy::cache::CacheManager;
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

fn etag_matches(list: &str, etag: &str) -> bool {
    list.split(',').any(|v| {
        let v = v.trim();
        v == "*" || v == etag
    })
}

fn parse_range(range: &str, len: u64) -> Option<(u64, u64)> {
    let spec = range.trim().strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    if start.is_empty() {
        // Suffix form `bytes=-N`: the last N bytes of the object as it is now.
        let suffix: u64 = end.parse().ok()?;
        return (suffix > 0).then(|| (len.saturating_sub(suffix), len - 1));
    }
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
        self.seen.lock().expect("seen").push(Seen {
            method: context.method.clone(),
            headers: headers.clone(),
        });
        let v = self.current.lock().expect("current").clone();
        let len = v.body.len() as u64;
        let mut out = HashMap::new();
        if !v.etag.is_empty() {
            out.insert("etag".to_string(), v.etag.to_string());
        }
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
    cache_manager: Arc<CacheManager>,
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
    if !v.etag.is_empty() {
        response_headers.insert("etag".to_string(), v.etag.to_string());
    }
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
    fixture_seeded(path, seed, V1, origin, tweak).await
}

async fn fixture_seeded(
    path: &str,
    seed: &[(usize, usize)],
    seed_version: Version,
    origin: OriginS3,
    tweak: impl FnOnce(&mut Config),
) -> Fixture {
    let temp_dir = TempDir::new().expect("tempdir");
    let cache_dir = temp_dir.path().to_path_buf();
    let mut config = base_config(cache_dir.clone());
    tweak(&mut config);
    let config = Arc::new(config);
    // `HttpProxy::new` is the production builder: it sets the configured
    // `partial_range_commit_ratio` on the cache manager, which every configured disk
    // cache then carries. The test constructors default it to 1.0 (exact only).
    let proxy = HttpProxy::new(SocketAddr::from(([127, 0, 0, 1], 0)), Arc::clone(&config))
        .expect("HttpProxy::new");
    let cache_manager = proxy.get_cache_manager();
    cache_manager.initialize().await.expect("cache init");
    let disk_cache = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));
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
    let fixture_cache_manager = Arc::clone(&cache_manager);
    // Seeds are written straight to `.meta` by an unjournaled manager, so they are
    // visible at once; the configured manager's own writes go through the journals.
    let seed_cache = Arc::new(tokio::sync::RwLock::new(DiskCacheManager::new(
        config.cache.cache_dir.clone(),
        false,
        0,
        config.cache.write_cache_enabled,
        config.cache.compression_batch_size,
    )));
    seed_cache
        .write()
        .await
        .initialize()
        .await
        .expect("seed disk init");

    let cache_key = CacheManager::generate_cache_key(path, None);
    for &(start, end) in seed {
        seed_cache
            .write()
            .await
            .store_range(
                &cache_key,
                start as u64,
                end as u64,
                &seed_version.body[start..=end],
                object_metadata(&seed_version),
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
        cache_manager: fixture_cache_manager,
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

// ---------------------------------------------------------------------------
// An extent with no recorded ETag is never merged with fetched bytes
// ---------------------------------------------------------------------------

/// v1 and v2 of an object on an origin that sends no `ETag`, only `Last-Modified`.
const V1_NO_ETAG: Version = Version {
    etag: "",
    last_modified: V1_LAST_MODIFIED,
    body: V1_BODY,
};
const V2_NO_ETAG: Version = Version {
    etag: "",
    last_modified: V2_LAST_MODIFIED,
    body: V2_BODY,
};

#[tokio::test]
async fn etagless_partial_range_after_overwrite_is_one_version() {
    let path = "/bucket/scn2-etagless-partial.bin";
    let fx = fixture_seeded(
        path,
        &[(0, 9)],
        V1_NO_ETAG,
        OriginS3::streaming(V2_NO_ETAG).await,
        |_| {},
    )
    .await;

    let answer = send(fx.addr, Method::GET, path, &[("range", "bytes=0-19")]).await;

    assert_eq!(
        answer.status,
        StatusCode::PARTIAL_CONTENT,
        "origin saw {:?}",
        fx.origin.seen()
    );
    assert_eq!(
        String::from_utf8_lossy(&answer.body),
        String::from_utf8_lossy(&V2_BODY[0..20]),
        "an extent with no recorded ETag was merged with bytes of another version; origin saw {:?}",
        fx.origin.seen()
    );
    let seen = fx.origin.seen();
    let last = seen.last().expect("the origin was asked");
    assert_eq!(last.method, Method::GET);
    assert_eq!(
        last.get("range"),
        Some("bytes=0-19"),
        "the whole requested range is fetched, not the hole: {seen:?}"
    );
    assert_eq!(
        last.get("if-unmodified-since"),
        None,
        "nothing cached is combined with this fetch, so it carries no proxy precondition: {seen:?}"
    );
    fx.origin.assert_no_sentinel_reached_origin();
}

/// Control: a range the ETag-less extent fully covers is still served from cache.
#[tokio::test]
async fn control_etagless_fully_cached_range_is_still_a_hit() {
    let path = "/bucket/scn2-etagless-hit.bin";
    let fx = fixture_seeded(
        path,
        &[(0, 19)],
        V1_NO_ETAG,
        OriginS3::new(V2_NO_ETAG),
        |_| {},
    )
    .await;

    let answer = send(fx.addr, Method::GET, path, &[("range", "bytes=0-9")]).await;

    assert_eq!(answer.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(answer.body.as_ref(), &V1_BODY[0..10]);
    assert!(
        fx.origin.seen().is_empty(),
        "a fully cached range must not reach the origin: {:?}",
        fx.origin.seen()
    );
}

/// Control: with a recorded ETag the hole fetch is still pinned and merged.
#[tokio::test]
async fn control_partial_range_with_etag_still_merges_one_hole_fetch() {
    let path = "/bucket/scn2-etag-partial.bin";
    let fx = fixture(path, &[(0, 9)], V1, |_| {}).await;

    let answer = send(fx.addr, Method::GET, path, &[("range", "bytes=0-19")]).await;

    assert_eq!(answer.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(answer.body.as_ref(), &V1_BODY[0..20]);
    let seen = fx.origin.seen();
    assert_eq!(seen.len(), 1, "one hole fetch: {seen:?}");
    assert_eq!(seen[0].get("range"), Some("bytes=10-19"), "{seen:?}");
    assert_eq!(seen[0].get("if-match"), Some(V1_ETAG), "{seen:?}");
    fx.origin.assert_no_sentinel_reached_origin();
}

// ---------------------------------------------------------------------------
// A 200 to a ranged miss is not cached as the requested range
// ---------------------------------------------------------------------------

/// Control for the test below: a streamed `206` miss is committed and, after one
/// consolidation cycle, visible in the metadata, so "no extent at 10" there is a
/// finding and not an unpublished write.
#[tokio::test]
async fn control_streamed_206_miss_is_committed_and_visible() {
    let path = "/bucket/scn2-ranged-206.bin";
    let fx = fixture_with(path, &[], OriginS3::streaming(V2).await, |_| {}).await;

    let answer = send(fx.addr, Method::GET, path, &[("range", "bytes=10-19")]).await;
    assert_eq!(answer.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(answer.body.as_ref(), &V2_BODY[10..20]);

    tokio::time::sleep(Duration::from_millis(500)).await;
    let consolidator = fx
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("a journal consolidator");
    consolidator
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
    assert_eq!(cached_extents(&fx, path).await, vec![(10, 19)]);
}

/// A cold key asked for `bytes=10-69` (60 bytes) with a stale `If-Range`. S3 answers
/// `200` with the whole 48-byte v2. 48 of 60 is above the default 0.5 salvage ratio, so
/// a commit of that body as `10-69` keeps the received prefix and files v2's bytes 0..47
/// at offsets 10..57. A later plain read of `bytes=10-19` would then be served
/// `ABCDEFGHIJ` from cache where v2 holds `KLMNOPQRST`.
#[tokio::test]
async fn ranged_200_to_a_cold_offset_range_is_not_salvaged_as_the_range() {
    let path = "/bucket/scn2-ranged-200.bin";
    let fx = fixture_with(path, &[], OriginS3::streaming(V2).await, |_| {}).await;

    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[("range", "bytes=10-69"), ("if-range", V1_ETAG)],
    )
    .await;
    assert_whole_v2(&answer, &fx.origin, "cold offset range");

    // Let the spawned cache write commit, then fold the journals so a committed extent
    // is visible in the metadata whichever way it was published.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let consolidator = fx
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("a journal consolidator, or a committed extent would stay invisible");
    consolidator
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
    let extents = cached_extents(&fx, path).await;
    assert!(
        extents.iter().all(|(start, _)| *start != 10),
        "a whole-object 200 was cached at the requested offset: {extents:?}"
    );

    let before = fx.origin.seen().len();
    let plain = send(fx.addr, Method::GET, path, &[("range", "bytes=10-19")]).await;
    assert_eq!(
        String::from_utf8_lossy(&plain.body),
        String::from_utf8_lossy(&V2_BODY[10..20]),
        "a later plain read was served the object's leading bytes from cache (origin asked {} more times, extents {extents:?})",
        fx.origin.seen().len() - before
    );
}

// ---------------------------------------------------------------------------
// A 206 is filed under the range the proxy computed, so its Content-Range must
// agree with that range, or end earlier only where the object ends.
// ---------------------------------------------------------------------------

/// A cold key asked for `bytes=0-95`, twice the 48-byte object. S3 clamps the range to
/// the object and answers `206 bytes 0-47/48`. The proxy keeps those 48 bytes as `0-47`,
/// and a repeat of the same request is answered from cache.
#[tokio::test]
async fn range_check_cold_overshooting_range_caches_the_clamped_object() {
    let path = "/bucket/range-check-overshoot.bin";
    let fx = fixture_with(path, &[], OriginS3::streaming(V2).await, |_| {}).await;

    let first = send(fx.addr, Method::GET, path, &[("range", "bytes=0-95")]).await;
    assert_eq!(first.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(first.body.as_ref(), V2_BODY);

    tokio::time::sleep(Duration::from_millis(500)).await;
    let consolidator = fx
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("a journal consolidator");
    consolidator
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
    let extents = cached_extents(&fx, path).await;
    assert_eq!(
        extents,
        vec![(0, 47)],
        "a 206 clamped at the end of the object was not cached as 0-47"
    );

    let before = fx.origin.seen().len();
    let repeat = send(fx.addr, Method::GET, path, &[("range", "bytes=0-95")]).await;
    assert_eq!(repeat.body.as_ref(), V2_BODY);
    assert_eq!(
        fx.origin.seen().len(),
        before,
        "the repeat was forwarded to S3 instead of answered from cache (extents {extents:?})"
    );
}

/// v2's ETag beside v1's 36-byte length: the cached entry already names the new version
/// but still records the old length.
const V2_STALE_LENGTH: Version = Version {
    body: b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij",
    ..V2
};

/// The cached entry records 36 bytes where the object now has 48. A signed `bytes=-10`
/// is forwarded unchanged, so S3 answers `206 bytes 38-47/48` while the proxy computed
/// the range as `26-35` from the cached length. Those bytes must not be filed at
/// `26-35`, or a later read of `bytes=26-35` is answered with the object's bytes 38-47.
#[tokio::test]
async fn range_check_stale_length_signed_suffix_is_not_filed_at_stale_offsets() {
    let path = "/bucket/range-check-stale-suffix.bin";
    let fx = fixture_seeded(
        path,
        &[(0, 4)],
        V2_STALE_LENGTH,
        OriginS3::streaming(V2).await,
        |_| {},
    )
    .await;

    let authorization = common::expired_fixture::signed_range_authorization();
    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[
            ("authorization", authorization.as_str()),
            ("range", "bytes=-10"),
        ],
    )
    .await;
    assert_eq!(answer.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        answer.body.as_ref(),
        &V2_BODY[38..48],
        "the client gets S3's answer unchanged"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    let consolidator = fx
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("a journal consolidator");
    consolidator
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
    let extents = cached_extents(&fx, path).await;
    assert!(
        !extents.contains(&(26, 35)),
        "S3's bytes 38-47 were filed at the stale offsets 26-35: {extents:?}"
    );

    let plain = send(fx.addr, Method::GET, path, &[("range", "bytes=26-35")]).await;
    assert_eq!(
        String::from_utf8_lossy(&plain.body),
        String::from_utf8_lossy(&V2_BODY[26..36]),
        "a later read of bytes=26-35 was answered with the wrong bytes (extents {extents:?})"
    );
}

/// The buffered-origin twin of the test above: the same stale-length signed suffix
/// through `handle_signed_range_s3_response`'s buffered arm.
#[tokio::test]
async fn range_check_buffered_stale_length_signed_suffix_is_not_filed_at_stale_offsets() {
    let path = "/bucket/range-check-stale-suffix-buffered.bin";
    let fx = fixture_seeded(path, &[(0, 4)], V2_STALE_LENGTH, OriginS3::new(V2), |_| {}).await;

    let authorization = common::expired_fixture::signed_range_authorization();
    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[
            ("authorization", authorization.as_str()),
            ("range", "bytes=-10"),
        ],
    )
    .await;
    assert_eq!(answer.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(answer.body.as_ref(), &V2_BODY[38..48]);

    let extents = consolidated_extents(&fx, path).await;
    assert!(
        !extents.contains(&(26, 35)),
        "buffered arm: S3's bytes 38-47 were filed at the stale offsets 26-35: {extents:?}"
    );
}

/// A cold key asked for a signed `bytes=0-95`, twice the 48-byte object. S3 clamps it to
/// `206 bytes 0-47/48`, and the signed path keeps those 48 bytes as `0-47`.
#[tokio::test]
async fn range_check_signed_cold_overshooting_range_caches_the_clamped_object() {
    let path = "/bucket/range-check-signed-overshoot.bin";
    let fx = fixture_with(path, &[], OriginS3::streaming(V2).await, |_| {}).await;

    let authorization = common::expired_fixture::signed_range_authorization();
    let first = send(
        fx.addr,
        Method::GET,
        path,
        &[
            ("authorization", authorization.as_str()),
            ("range", "bytes=0-95"),
        ],
    )
    .await;
    assert_eq!(first.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(first.body.as_ref(), V2_BODY);

    let extents = consolidated_extents(&fx, path).await;
    assert_eq!(
        extents,
        vec![(0, 47)],
        "a signed 206 clamped at the end of the object was not cached as 0-47"
    );
}

/// A cold key asked for a signed `bytes=10-69` with a stale `If-Range`. S3 answers `200`
/// with the whole 48-byte v2, which the client gets unchanged. Filed as `10-69`, the
/// salvaged prefix would put v2's bytes 0..47 at offsets 10..57.
async fn signed_ranged_200_to_a_cold_offset_range(
    path: &str,
    origin: OriginS3,
    what: &str,
    range: &str,
) {
    let fx = fixture_with(path, &[], origin, |_| {}).await;

    let authorization = common::expired_fixture::signed_range_authorization();
    let answer = send(
        fx.addr,
        Method::GET,
        path,
        &[
            ("authorization", authorization.as_str()),
            ("range", range),
            ("if-range", V1_ETAG),
        ],
    )
    .await;
    assert_whole_v2(&answer, &fx.origin, what);

    let extents = consolidated_extents(&fx, path).await;
    assert!(
        extents.iter().all(|(start, _)| *start != 10),
        "{what}: a whole-object 200 to a signed range was cached at the requested offset: {extents:?}"
    );

    let plain = send(fx.addr, Method::GET, path, &[("range", "bytes=10-19")]).await;
    assert_eq!(
        String::from_utf8_lossy(&plain.body),
        String::from_utf8_lossy(&V2_BODY[10..20]),
        "{what}: a later plain read was served the object's leading bytes (extents {extents:?})"
    );
}

#[tokio::test]
async fn ranged_200_to_a_signed_cold_offset_range_is_not_cached_as_the_range_streamed() {
    signed_ranged_200_to_a_cold_offset_range(
        "/bucket/scn2-signed-ranged-200-streamed.bin",
        OriginS3::streaming(V2).await,
        "streamed",
        "bytes=10-69",
    )
    .await;
}

#[tokio::test]
async fn ranged_200_to_a_signed_cold_offset_range_is_not_cached_as_the_range_buffered() {
    signed_ranged_200_to_a_cold_offset_range(
        "/bucket/scn2-signed-ranged-200-buffered.bin",
        OriginS3::new(V2),
        "buffered",
        "bytes=10-69",
    )
    .await;
}

/// The buffered twin with a range whose length equals the object's: `bytes=10-57` on the
/// 48-byte v2. The whole-object `200` body passes the buffered writer's length check, so
/// only the arm's own `200` check keeps it from being filed at offsets 10..57.
#[tokio::test]
async fn ranged_200_to_a_signed_cold_offset_range_of_the_objects_length_is_not_cached_buffered() {
    signed_ranged_200_to_a_cold_offset_range(
        "/bucket/scn2-signed-ranged-200-buffered-same-length.bin",
        OriginS3::new(V2),
        "buffered same length",
        "bytes=10-57",
    )
    .await;
}

/// The buffered-origin twin of `range_check_signed_cold_overshooting_range_caches_the_clamped_object`.
#[tokio::test]
async fn range_check_buffered_signed_cold_overshooting_range_caches_the_clamped_object() {
    let path = "/bucket/range-check-signed-overshoot-buffered.bin";
    let fx = fixture_with(path, &[], OriginS3::new(V2), |_| {}).await;
    let authorization = common::expired_fixture::signed_range_authorization();
    let first = send(
        fx.addr,
        Method::GET,
        path,
        &[
            ("authorization", authorization.as_str()),
            ("range", "bytes=0-95"),
        ],
    )
    .await;
    assert_eq!(first.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(first.body.as_ref(), V2_BODY);
    let extents = consolidated_extents(&fx, path).await;
    assert_eq!(
        extents,
        vec![(0, 47)],
        "buffered: a signed 206 clamped at the end of the object was not cached as 0-47"
    );
}

/// Let the spawned cache write commit, then fold the journals, so a committed extent is
/// visible in the metadata whichever way it was published.
async fn consolidated_extents(fx: &Fixture, path: &str) -> Vec<(u64, u64)> {
    tokio::time::sleep(Duration::from_millis(500)).await;
    fx.cache_manager
        .get_journal_consolidator()
        .await
        .expect("a journal consolidator")
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
    cached_extents(fx, path).await
}
