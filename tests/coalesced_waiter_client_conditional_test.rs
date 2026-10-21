//! A coalesced waiter never answers with the fetcher's pin, and a client's own
//! conditional gets S3's answer on every path.
//!
//! Four paths answered a request differently from S3:
//!
//! - L1. A range waiter coalesced behind a partial-cache fetch carried the proxy's
//!   `If-Match: <cached ETag>` pin into its own validation. Once the fetcher had
//!   replaced the cached version, that validation failed the pin and the waiter's
//!   client, which sent no precondition, received `412`.
//! - L6. A waiter whose client sent its own `If-None-Match` forwarded it unchanged,
//!   then answered S3's `304` with the cached body (`200` full-object, `206` range).
//! - L7. A range GET on an expired entry ran the proxy's revalidation with the
//!   client's `If-None-Match` and answered S3's `304` with a cached `206`.
//! - L8. Page widening served a fresh cached page to a request carrying
//!   `If-Modified-Since` or `If-Unmodified-Since`, which `fill_page`'s
//!   range-conditional check does not name. (`If-None-Match`, `If-Match` and
//!   `If-Range` already bypassed the cached page; those tests pin that.)
//!
//! Plus the `412` body: a client's own failing precondition got a proxy-built `412`
//! instead of S3's headers and body.
//!
//! The S3 model below holds two versions and can switch after a given request
//! count, so a fetcher and a waiter can see different versions.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{Method, StatusCode};
use tempfile::TempDir;

use s3_proxy::bucket_settings::ResolvedSettings;
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;

const OBJECT_SIZE: u64 = 4096;
const PAGE_SIZE: u64 = 1024;
/// Over the 8 MiB buffered-operation bound, so a partially cached read of it takes
/// the streamed path with the proxy's pin.
const LARGE_OBJECT: u64 = 10 * 1024 * 1024;
const V1_ETAG: &str = "\"waiter-v1\"";
const V2_ETAG: &str = "\"waiter-v2\"";
const V1_FILL: u8 = b'A';
const V2_FILL: u8 = b'B';
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";
const S3_412_REQUEST_ID: &str = "S3-MODEL-412";
const S3_412_BODY: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>PreconditionFailed</Code><Message>At least one of the pre-conditions you specified did not hold</Message><Condition>If-Match</Condition><RequestId>S3-MODEL-412</RequestId></Error>";

fn unquote(s: &str) -> &str {
    s.trim().trim_matches('"')
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct Seen {
    range: Option<String>,
    if_match: Option<String>,
    if_none_match: Option<String>,
    status: StatusCode,
}

/// Requests numbered below `flip_after` see version 1, the rest version 2.
/// `If-Match` that does not name the current version (or `*`) answers `412` with
/// an S3-shaped body and request id; `If-None-Match` naming it (or `*`) answers
/// `304` carrying the current ETag; otherwise a `206`/`200` of version bytes.
/// Request 0 is held for `delay_first` so a second request can coalesce behind it.
///
/// A request with `allow_streaming` (the streamed miss and fill paths) gets its
/// body as `S3ResponseBody::Streaming`, as the real client returns it: only that
/// shape is cached by the streamed fill and completes a coalescing fetcher's guard
/// on commit. `Incoming` has no public constructor, so the body is served by a
/// loopback origin and read back over a real HTTP/1 connection.
struct VersionedS3 {
    flip_after: usize,
    object_size: u64,
    delay_first: Duration,
    seen: Mutex<Vec<Seen>>,
    origin: std::net::SocketAddr,
    bodies: Arc<Mutex<HashMap<usize, Bytes>>>,
}

impl VersionedS3 {
    async fn new(flip_after: usize, object_size: u64, delay_first: Duration) -> Arc<Self> {
        use hyper::server::conn::http1;
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;

        let bodies: Arc<Mutex<HashMap<usize, Bytes>>> = Arc::new(Mutex::new(HashMap::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind origin");
        let origin = listener.local_addr().expect("origin addr");
        let served = Arc::clone(&bodies);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let served = Arc::clone(&served);
                tokio::spawn(async move {
                    let service = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let id: usize = req.uri().path().trim_start_matches('/').parse().unwrap();
                        let data = served.lock().unwrap().remove(&id).unwrap_or_default();
                        async move {
                            Ok::<_, std::convert::Infallible>(
                                hyper::Response::builder()
                                    .status(200)
                                    .header("content-length", data.len().to_string())
                                    .body(http_body_util::Full::new(data))
                                    .expect("origin response"),
                            )
                        }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Arc::new(Self {
            flip_after,
            object_size,
            delay_first,
            seen: Mutex::new(Vec::new()),
            origin,
            bodies,
        })
    }
    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
    /// `data` as a real `Incoming` body.
    async fn streamed(&self, id: usize, data: Bytes) -> s3_proxy::S3ResponseBody {
        self.bodies.lock().unwrap().insert(id, data);
        let stream = tokio::net::TcpStream::connect(self.origin)
            .await
            .expect("connect origin");
        let (mut sender, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .expect("origin handshake");
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let req = hyper::Request::builder()
            .method("GET")
            .uri(format!("/{}", id))
            .header("host", "origin")
            .body(http_body_util::Empty::<Bytes>::new())
            .expect("origin request");
        let resp = sender.send_request(req).await.expect("origin response");
        s3_proxy::S3ResponseBody::Streaming(resp.into_body())
    }
}

fn header<'a>(headers: &'a HashMap<String, String>, name: &str) -> Option<&'a String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v)
}

#[async_trait::async_trait]
impl s3_proxy::S3ClientApi for VersionedS3 {
    async fn forward_request(
        &self,
        context: s3_proxy::S3RequestContext,
    ) -> s3_proxy::Result<s3_proxy::S3Response> {
        let range = header(&context.headers, "range").cloned();
        let if_match = header(&context.headers, "if-match").cloned();
        let if_none_match = header(&context.headers, "if-none-match").cloned();
        let index = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(Seen {
                range: range.clone(),
                if_match: if_match.clone(),
                if_none_match: if_none_match.clone(),
                status: StatusCode::CONTINUE,
            });
            seen.len() - 1
        };
        let (etag, fill) = if index < self.flip_after {
            (V1_ETAG, V1_FILL)
        } else {
            (V2_ETAG, V2_FILL)
        };
        let mut headers = HashMap::new();
        headers.insert("etag".to_string(), etag.to_string());
        headers.insert("last-modified".to_string(), LAST_MODIFIED.to_string());

        let mut outcome: Option<(StatusCode, HashMap<String, String>, Option<Bytes>)> = None;
        if let Some(im) = &if_match {
            if unquote(im) != "*" && unquote(im) != unquote(etag) {
                let mut h = HashMap::new();
                h.insert(
                    "x-amz-request-id".to_string(),
                    S3_412_REQUEST_ID.to_string(),
                );
                h.insert("content-type".to_string(), "application/xml".to_string());
                h.insert("content-length".to_string(), S3_412_BODY.len().to_string());
                outcome = Some((
                    StatusCode::PRECONDITION_FAILED,
                    h,
                    Some(Bytes::from_static(S3_412_BODY.as_bytes())),
                ));
            }
        }
        if outcome.is_none() && if_match.is_none() {
            if let Some(ius) = header(&context.headers, "if-unmodified-since") {
                if ius != LAST_MODIFIED {
                    let mut h = HashMap::new();
                    h.insert(
                        "x-amz-request-id".to_string(),
                        S3_412_REQUEST_ID.to_string(),
                    );
                    h.insert("content-type".to_string(), "application/xml".to_string());
                    h.insert("content-length".to_string(), S3_412_BODY.len().to_string());
                    outcome = Some((
                        StatusCode::PRECONDITION_FAILED,
                        h,
                        Some(Bytes::from_static(S3_412_BODY.as_bytes())),
                    ));
                }
            }
        }
        if outcome.is_none() {
            if let Some(inm) = &if_none_match {
                if unquote(inm) == "*" || unquote(inm) == unquote(etag) {
                    outcome = Some((StatusCode::NOT_MODIFIED, headers.clone(), None));
                }
            } else if header(&context.headers, "if-modified-since").map(String::as_str)
                == Some(LAST_MODIFIED)
            {
                outcome = Some((StatusCode::NOT_MODIFIED, headers.clone(), None));
            }
        }
        let outcome = match outcome {
            Some(o) => o,
            None => {
                let (status, start, end) = match &range {
                    Some(r) => {
                        let r = r.trim_start_matches("bytes=");
                        let mut parts = r.split('-');
                        let s: u64 = parts.next().unwrap().parse().unwrap();
                        let e: u64 = parts.next().unwrap().parse().unwrap();
                        (StatusCode::PARTIAL_CONTENT, s, e.min(self.object_size - 1))
                    }
                    None => (StatusCode::OK, 0, self.object_size - 1),
                };
                let data = vec![fill; (end - start + 1) as usize];
                if status == StatusCode::PARTIAL_CONTENT {
                    headers.insert(
                        "content-range".to_string(),
                        format!("bytes {}-{}/{}", start, end, self.object_size),
                    );
                }
                headers.insert("content-length".to_string(), data.len().to_string());
                let body = if context.method == Method::HEAD {
                    None
                } else {
                    Some(Bytes::from(data))
                };
                (status, headers, body)
            }
        };
        self.seen.lock().unwrap()[index].status = outcome.0;
        if index == 0 && !self.delay_first.is_zero() {
            tokio::time::sleep(self.delay_first).await;
        }
        let is_data = outcome.0 == StatusCode::OK || outcome.0 == StatusCode::PARTIAL_CONTENT;
        let body = match outcome.2 {
            Some(data) if is_data && context.allow_streaming => {
                Some(self.streamed(index, data).await)
            }
            other => other.map(s3_proxy::S3ResponseBody::Buffered),
        };
        Ok(s3_proxy::S3Response {
            status: outcome.0,
            headers: outcome.1,
            body,
            request_duration: Duration::from_millis(0),
        })
    }

    fn extract_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> s3_proxy::cache_types::CacheMetadata {
        s3_proxy::cache_types::CacheMetadata {
            etag: headers.get("etag").cloned().unwrap_or_default(),
            last_modified: headers.get("last-modified").cloned().unwrap_or_default(),
            content_length: self.object_size,
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: std::time::SystemTime::now(),
        }
    }

    fn extract_object_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> s3_proxy::cache_types::ObjectMetadata {
        s3_proxy::cache_types::ObjectMetadata::new_with_headers(
            headers.get("etag").cloned().unwrap_or_default(),
            headers.get("last-modified").cloned().unwrap_or_default(),
            self.object_size,
            None,
            headers.clone(),
        )
    }

    fn get_connection_pool(
        &self,
    ) -> Arc<tokio::sync::RwLock<s3_proxy::connection_pool::ConnectionPoolManager>> {
        Arc::new(tokio::sync::RwLock::new(
            s3_proxy::connection_pool::ConnectionPoolManager::new_with_config(Default::default())
                .expect("pool"),
        ))
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

    async fn refresh_dns(&self) -> s3_proxy::Result<()> {
        Ok(())
    }
}

fn config(coordination: bool) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.download_coordination.enabled = coordination;
    config.cache.download_coordination.wait_timeout_secs = 10;
    config.cache.ram_cache_enabled = false;
    // Skip the range path's early full-object shortcut so the range lookup runs.
    config.cache.full_object_check_threshold = 512;
    Arc::new(config)
}

async fn make_infra(config: &Arc<Config>) -> (TempDir, Arc<CacheManager>, Arc<RangeHandler>) {
    let temp_dir = TempDir::new().expect("tempdir");
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
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
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    ));
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));
    cache_manager.initialize().await.expect("cache init");
    disk_cache_manager
        .write()
        .await
        .initialize()
        .await
        .expect("disk cache init");
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    (temp_dir, cache_manager, range_handler)
}

/// Lay down one version-1 extent (`.bin` plus a stored-fresh `.meta`).
async fn seed_v1_extent(
    cache_key: &str,
    cache_manager: &Arc<CacheManager>,
    start: u64,
    end: u64,
    object_size: u64,
) {
    seed_v1_extent_with_etag(cache_key, cache_manager, start, end, object_size, V1_ETAG).await
}

/// [`seed_v1_extent`] with the entry's recorded ETag given; `""` is an entry from an
/// origin that sent none.
async fn seed_v1_extent_with_etag(
    cache_key: &str,
    cache_manager: &Arc<CacheManager>,
    start: u64,
    end: u64,
    object_size: u64,
    etag: &str,
) {
    use s3_proxy::cache_types::{
        CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState,
    };
    use s3_proxy::compression::CompressionAlgorithm;

    let now = std::time::SystemTime::now();
    let body = vec![V1_FILL; (end - start + 1) as usize];
    let bin_relative = format!(
        "bucket/00/000/{}_{}-{}.bin",
        cache_key.replace(['/', ':'], "_"),
        start,
        end
    );
    let bin_path = cache_manager
        .get_cache_dir()
        .join("ranges")
        .join(&bin_relative);
    std::fs::create_dir_all(bin_path.parent().unwrap()).unwrap();
    std::fs::write(&bin_path, &body).unwrap();
    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata {
            etag: etag.to_string(),
            last_modified: LAST_MODIFIED.to_string(),
            content_length: object_size,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: UploadState::Complete,
            cumulative_size: object_size,
            parts: Vec::new(),
            response_headers: HashMap::new(),
            compression_algorithm: CompressionAlgorithm::None,
            compressed_size: body.len() as u64,
            parts_count: None,
            part_ranges: HashMap::new(),
            upload_id: None,
            is_write_cached: false,
            write_cache_expires_at: None,
            write_cache_created_at: None,
            write_cache_last_accessed: None,
            graduation_accounted: false,
        },
        ranges: vec![RangeSpec {
            start,
            end,
            file_path: bin_relative,
            compression_algorithm: CompressionAlgorithm::None,
            compressed_size: body.len() as u64,
            uncompressed_size: body.len() as u64,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            // As a 3.0.0 writer records it (`RangeSpec::is_unverified_prefix`).
            file_identity: s3_proxy::cache_types::RangeFileIdentity::of(
                &std::fs::metadata(&bin_path).expect("fixture: stat the planted .bin"),
            ),
        }],
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        head_expires_at: None,
        head_last_accessed: None,
        head_access_count: 0,
        head_cached_at: None,
        last_validated_at: None,
    };
    let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
    std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
    std::fs::write(
        &metadata_path,
        serde_json::to_string_pretty(&metadata).unwrap(),
    )
    .unwrap();
    cache_manager.invalidate_metadata_cache(cache_key).await;
}

#[derive(Debug)]
#[allow(dead_code)] // `etag` and `content_range` are read through `{:?}` only
struct Served {
    status: StatusCode,
    etag: Option<String>,
    content_range: Option<String>,
    request_id: Option<String>,
    body: Vec<u8>,
}

async fn collect(
    response: hyper::Response<http_body_util::combinators::BoxBody<Bytes, hyper::Error>>,
) -> Served {
    let status = response.status();
    let get = |name: &str| {
        response
            .headers()
            .get(name)
            .map(|v| v.to_str().unwrap().to_string())
    };
    let etag = get("etag");
    let content_range = get("content-range");
    let request_id = get("x-amz-request-id");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes()
        .to_vec();
    Served {
        status,
        etag,
        content_range,
        request_id,
        body,
    }
}

/// The version every byte of `body` came from, `None` when empty or mixed.
fn body_version(body: &[u8]) -> Option<&'static str> {
    if body.is_empty() {
        return None;
    }
    if body.iter().all(|&b| b == V1_FILL) {
        Some(V1_ETAG)
    } else if body.iter().all(|&b| b == V2_FILL) {
        Some(V2_ETAG)
    } else {
        None
    }
}

struct Harness {
    _t: TempDir,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
    config: Arc<Config>,
    tracker: Arc<InFlightTracker>,
}

impl Harness {
    async fn new(coordination: bool) -> Self {
        let config = config(coordination);
        let (t, cache_manager, range_handler) = make_infra(&config).await;
        Self {
            _t: t,
            cache_manager,
            range_handler,
            config,
            tracker: Arc::new(InFlightTracker::new()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn range_get(
        &self,
        key: &str,
        range: &str,
        client: HashMap<String, String>,
        resolved: ResolvedSettings,
        forward_to_s3: bool,
        s3: Arc<VersionedS3>,
    ) -> Served {
        let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = s3;
        let response = HttpProxy::handle_range_request(
            Method::GET,
            key.to_string(),
            range,
            client,
            Arc::clone(&self.cache_manager),
            Arc::clone(&self.range_handler),
            s3_client,
            "s3.us-west-2.amazonaws.com".to_string(),
            format!("/{}", key).parse().unwrap(),
            Arc::clone(&self.config),
            &resolved,
            None,
            Arc::clone(&self.tracker),
            None,
            &None,
            forward_to_s3,
            None,
        )
        .await
        .expect("Infallible");
        collect(response).await
    }

    async fn full_get(
        &self,
        key: &str,
        client: HashMap<String, String>,
        s3: Arc<VersionedS3>,
    ) -> Served {
        let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = s3;
        let response = HttpProxy::forward_get_head_with_coordination(
            Method::GET,
            format!("/{}", key).parse().unwrap(),
            "s3.us-west-2.amazonaws.com".to_string(),
            client,
            key.to_string(),
            Arc::clone(&self.cache_manager),
            s3_client,
            Arc::clone(&self.tracker),
            Arc::clone(&self.range_handler),
            Arc::clone(&self.config),
            self.config.cache.download_coordination.enabled,
            self.config.cache.download_coordination.wait_timeout(),
            None,
            &ResolvedSettings::default(),
            &None,
            None,
        )
        .await
        .expect("Infallible");
        collect(response).await
    }
}

fn with(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Run `fetcher` then, once its first S3 request is in flight, `waiter` against
/// the same harness and tracker. Returns both responses.
async fn pair<F1, F2>(fetcher: F1, waiter: F2) -> (Served, Served)
where
    F1: std::future::Future<Output = Served> + Send + 'static,
    F2: std::future::Future<Output = Served> + Send + 'static,
{
    let f = tokio::spawn(fetcher);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let w = tokio::spawn(waiter);
    let f = tokio::time::timeout(Duration::from_secs(30), f)
        .await
        .expect("fetcher timed out")
        .unwrap();
    let w = tokio::time::timeout(Duration::from_secs(30), w)
        .await
        .expect("waiter timed out")
        .unwrap();
    (f, w)
}

// ---------------------------------------------------------------------------
// L1: the range waiter's pinned If-Match
// ---------------------------------------------------------------------------

/// A partially cached, over-bound range: the cached extent is version 1, S3 holds
/// version 2. Two identical GETs that sent no precondition coalesce. The fetcher's
/// pinned fetch fails, it retries unpinned and caches version 2; the waiter must
/// then get version 2's bytes, not a `412` to the fetcher's pin.
#[tokio::test]
async fn l1_range_waiter_that_sent_nothing_never_gets_the_fetchers_pin_412() {
    let h = Arc::new(Harness::new(true).await);
    let key = "bucket/scn1-l1.bin";
    seed_v1_extent(key, &h.cache_manager, 0, 1024 * 1024 - 1, LARGE_OBJECT).await;
    let s3 = VersionedS3::new(0, LARGE_OBJECT, Duration::from_millis(500)).await;
    let range = format!("bytes=0-{}", 9 * 1024 * 1024 - 1);

    let (h1, s1, r1) = (h.clone(), s3.clone(), range.clone());
    let (h2, s2, r2) = (h.clone(), s3.clone(), range.clone());
    let (fetcher, waiter) = pair(
        async move {
            h1.range_get(
                key,
                &r1,
                HashMap::new(),
                ResolvedSettings::default(),
                false,
                s1,
            )
            .await
        },
        async move {
            h2.range_get(
                key,
                &r2,
                HashMap::new(),
                ResolvedSettings::default(),
                false,
                s2,
            )
            .await
        },
    )
    .await;

    let seen = s3.seen();
    assert_eq!(
        seen.first().and_then(|r| r.if_match.as_deref()),
        Some(V1_ETAG),
        "PRECONDITION MISSING: the fetcher's first request must be pinned to the cached version: {:?}",
        seen
    );
    assert_eq!(
        fetcher.status,
        StatusCode::PARTIAL_CONTENT,
        "fetcher: {:?}",
        seen
    );
    assert_eq!(
        waiter.status,
        StatusCode::PARTIAL_CONTENT,
        "L1: a waiter that sent no precondition got {} (the fetcher's pin leaked): {:?}",
        waiter.status,
        seen
    );
    assert_eq!(body_version(&waiter.body), Some(V2_ETAG));
    assert_eq!(waiter.body.len() as u64, 9 * 1024 * 1024);
    assert!(
        seen.iter()
            .all(|r| r.if_match.as_deref() != Some(V1_ETAG) || r.if_none_match.is_none()),
        "L1: the waiter's validation carried the fetcher's pin: {:?}",
        seen
    );
}

// ---------------------------------------------------------------------------
// L6: coalesced waiters and the client's own If-None-Match
// ---------------------------------------------------------------------------

/// Full object. A plain GET is fetching; a second GET with `If-None-Match` naming
/// the current ETag coalesces behind it. S3 answers that `304`; so must the proxy.
#[tokio::test]
async fn l6_full_object_waiter_with_matching_if_none_match_gets_304() {
    let h = Arc::new(Harness::new(true).await);
    let key = "bucket/scn1-l6-full.bin";
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::from_millis(500)).await;
    let (h1, s1) = (h.clone(), s3.clone());
    let (h2, s2) = (h.clone(), s3.clone());
    let (fetcher, waiter) = pair(
        async move { h1.full_get(key, HashMap::new(), s1).await },
        async move {
            h2.full_get(key, with(&[("if-none-match", V1_ETAG)]), s2)
                .await
        },
    )
    .await;
    let seen = s3.seen();
    assert_eq!(fetcher.status, StatusCode::OK, "{:?}", seen);
    assert_eq!(
        seen.len(),
        2,
        "PRECONDITION MISSING: one fetch plus the waiter's own request: {:?}",
        seen
    );
    assert_eq!(
        seen[1].if_none_match.as_deref(),
        Some(V1_ETAG),
        "{:?}",
        seen
    );
    assert_eq!(
        waiter.status,
        StatusCode::NOT_MODIFIED,
        "L6: S3 answered 304 to the client's own If-None-Match, the proxy answered {} with {} bytes",
        waiter.status,
        waiter.body.len()
    );
    assert!(waiter.body.is_empty());
}

/// Full object, `If-None-Match: *`. The fetcher cached version 1, then the object
/// changed. S3 answers `*` with `304`; the waiter must not serve the superseded
/// cached body.
#[tokio::test]
async fn l6_full_object_waiter_with_wildcard_never_gets_a_superseded_body() {
    let h = Arc::new(Harness::new(true).await);
    let key = "bucket/scn1-l6-full-star.bin";
    let s3 = VersionedS3::new(1, OBJECT_SIZE, Duration::from_millis(500)).await;
    let (h1, s1) = (h.clone(), s3.clone());
    let (h2, s2) = (h.clone(), s3.clone());
    let (_fetcher, waiter) = pair(
        async move { h1.full_get(key, HashMap::new(), s1).await },
        async move { h2.full_get(key, with(&[("if-none-match", "*")]), s2).await },
    )
    .await;
    let seen = s3.seen();
    assert_eq!(seen.len(), 2, "PRECONDITION MISSING: {:?}", seen);
    assert_ne!(
        body_version(&waiter.body),
        Some(V1_ETAG),
        "L6: a superseded cached body was served to If-None-Match: * ({}): {:?}",
        waiter.status,
        seen
    );
    assert_eq!(waiter.status, StatusCode::NOT_MODIFIED, "{:?}", seen);
}

/// Range. The range waiter's own validation, once the fetcher's fill is visible
/// (complete coverage): the client's `If-None-Match` names the current ETag, S3
/// answers `304`, and the waiter must not turn that into a cached `206`. Driven at
/// the waiter directly because in this harness a streamed fill stays
/// journal-pending, so a coalesced waiter finds incomplete coverage and forwards.
#[tokio::test]
async fn l6_range_waiter_with_matching_if_none_match_gets_304() {
    let h = Harness::new(true).await;
    let key = "bucket/scn1-l6-range.bin";
    seed_v1_extent(key, &h.cache_manager, 0, 1023, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = s3.clone();
    let response = HttpProxy::serve_range_from_cache_validated(
        Method::GET,
        format!("/{}", key).parse().unwrap(),
        "s3.us-west-2.amazonaws.com".to_string(),
        with(&[("if-none-match", V1_ETAG), ("range", "bytes=0-1023")]),
        key.to_string(),
        s3_proxy::range_handler::RangeSpec {
            start: 0,
            end: 1023,
        },
        Arc::clone(&h.cache_manager),
        Arc::clone(&h.range_handler),
        s3_client,
        Arc::clone(&h.config),
        false,
        None,
        &ResolvedSettings::default(),
        &None,
        None,
    )
    .await
    .expect("Infallible");
    let waiter = collect(response).await;
    let seen = s3.seen();
    assert_eq!(seen.len(), 1, "{:?}", seen);
    assert_eq!(
        seen[0].if_none_match.as_deref(),
        Some(V1_ETAG),
        "{:?}",
        seen
    );
    assert_eq!(
        waiter.status,
        StatusCode::NOT_MODIFIED,
        "L6: S3 answered 304 to the client's own If-None-Match, the proxy answered {} with {} bytes",
        waiter.status,
        waiter.body.len()
    );
}

/// L1, the full-object form. A no-Range GET of a partially cached object is
/// routed through the range merge with a Range the proxy synthesized, its sentinel,
/// and the proxy's pin. A waiter on that flight answers the client's request: a
/// `200` with no `Content-Range`, never S3's `206` to the synthesized Range, a cached
/// `206`, or a `412` to the pin.
#[tokio::test]
async fn l1_synthesized_full_object_waiter_answers_a_200_without_content_range() {
    let h = Harness::new(true).await;
    let key = "bucket/scn1-l1-synth.bin";
    seed_v1_extent(key, &h.cache_manager, 0, OBJECT_SIZE - 1, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = s3.clone();
    let range = format!("bytes=0-{}", OBJECT_SIZE - 1);
    let response = HttpProxy::serve_range_from_cache_validated(
        Method::GET,
        format!("/{}", key).parse().unwrap(),
        "s3.us-west-2.amazonaws.com".to_string(),
        with(&[
            ("Range", range.as_str()),
            ("x-proxy-synthesized-full-object-range", "1"),
            ("if-match", "\"waiter-v0\""),
            ("x-proxy-injected-if-match", "1"),
        ]),
        key.to_string(),
        s3_proxy::range_handler::RangeSpec {
            start: 0,
            end: OBJECT_SIZE - 1,
        },
        Arc::clone(&h.cache_manager),
        Arc::clone(&h.range_handler),
        s3_client,
        Arc::clone(&h.config),
        false,
        None,
        &ResolvedSettings::default(),
        &None,
        None,
    )
    .await
    .expect("Infallible");
    let served = collect(response).await;
    let seen = s3.seen();
    assert_eq!(seen.len(), 1, "{:?}", seen);
    assert_eq!(
        seen[0].if_match, None,
        "the validation carried the proxy's pin: {:?}",
        seen
    );
    assert_eq!(
        seen[0].range, None,
        "the validation carried the synthesized Range: {:?}",
        seen
    );
    assert_eq!(
        served.status,
        StatusCode::OK,
        "a no-Range GET's waiter answered {} (content-range {:?}): {:?}",
        served.status,
        served.content_range,
        seen
    );
    assert_eq!(served.content_range, None);
    assert_eq!(served.body.len() as u64, OBJECT_SIZE);
    assert_eq!(body_version(&served.body), Some(V1_ETAG));
}

/// Reachability for the range waiter: a range GET with the client's own
/// `If-None-Match` coalesces behind a plain fetch and its validation carries the
/// client's validator. Whatever the waiter then does, the client gets S3's `304`.
#[tokio::test]
async fn l6_range_waiter_behind_a_plain_fetch_gets_304() {
    let h = Arc::new(Harness::new(true).await);
    let key = "bucket/scn1-l6-range-pair.bin";
    seed_v1_extent(key, &h.cache_manager, 0, 1024 * 1024 - 1, LARGE_OBJECT).await;
    let s3 = VersionedS3::new(usize::MAX, LARGE_OBJECT, Duration::from_millis(500)).await;
    let range = format!("bytes=0-{}", 9 * 1024 * 1024 - 1);
    let (h1, s1, r1) = (h.clone(), s3.clone(), range.clone());
    let (h2, s2, r2) = (h.clone(), s3.clone(), range.clone());
    let (fetcher, waiter) = pair(
        async move {
            h1.range_get(
                key,
                &r1,
                HashMap::new(),
                ResolvedSettings::default(),
                false,
                s1,
            )
            .await
        },
        async move {
            h2.range_get(
                key,
                &r2,
                with(&[("if-none-match", V1_ETAG)]),
                ResolvedSettings::default(),
                true,
                s2,
            )
            .await
        },
    )
    .await;
    let seen = s3.seen();
    assert_eq!(fetcher.status, StatusCode::PARTIAL_CONTENT, "{:?}", seen);
    assert_eq!(
        seen[1].if_none_match.as_deref(),
        Some(V1_ETAG),
        "{:?}",
        seen
    );
    assert_eq!(waiter.status, StatusCode::NOT_MODIFIED, "{:?}", seen);
}

/// Control, full object: waiters that sent nothing still get the cached body, each
/// with one `304` validation and no data fetch of its own, so N identical requests
/// make one data fetch.
#[tokio::test]
async fn control_plain_full_object_waiters_still_coalesce_onto_one_fetch() {
    let h = Arc::new(Harness::new(true).await);
    let key = "bucket/scn1-control-full.bin";
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::from_millis(500)).await;
    let mut tasks = Vec::new();
    for i in 0..4 {
        let (hh, ss) = (h.clone(), s3.clone());
        tasks.push(tokio::spawn(async move {
            hh.full_get(key, HashMap::new(), ss).await
        }));
        if i == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    for t in tasks {
        let served = t.await.unwrap();
        assert_eq!(served.status, StatusCode::OK);
        assert_eq!(body_version(&served.body), Some(V1_ETAG));
        assert_eq!(served.body.len() as u64, OBJECT_SIZE);
    }
    let seen = s3.seen();
    let data_fetches = seen.iter().filter(|r| r.status == StatusCode::OK).count();
    assert_eq!(
        data_fetches, 1,
        "N identical requests, one data fetch: {:?}",
        seen
    );
    assert_eq!(seen.len(), 4, "{:?}", seen);
    assert!(
        seen.iter()
            .skip(1)
            .all(|r| r.status == StatusCode::NOT_MODIFIED
                && r.if_none_match.as_deref() == Some(V1_ETAG)),
        "every waiter validates with the proxy's validator and gets 304: {:?}",
        seen
    );
}

/// Control, range: plain waiters behind a pinned over-bound fetch of an unchanged
/// object get the fetcher's version, never a `412`, and send no pin of their own.
#[tokio::test]
async fn control_plain_range_waiters_behind_a_pinned_fetch() {
    let h = Arc::new(Harness::new(true).await);
    let key = "bucket/scn1-control-range.bin";
    seed_v1_extent(key, &h.cache_manager, 0, 1024 * 1024 - 1, LARGE_OBJECT).await;
    let s3 = VersionedS3::new(usize::MAX, LARGE_OBJECT, Duration::from_millis(500)).await;
    let range = format!("bytes=0-{}", 9 * 1024 * 1024 - 1);
    let mut tasks = Vec::new();
    for i in 0..4 {
        let (hh, ss, rr) = (h.clone(), s3.clone(), range.clone());
        tasks.push(tokio::spawn(async move {
            hh.range_get(
                key,
                &rr,
                HashMap::new(),
                ResolvedSettings::default(),
                false,
                ss,
            )
            .await
        }));
        if i == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    for t in tasks {
        let served = t.await.unwrap();
        assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(body_version(&served.body), Some(V1_ETAG));
        assert_eq!(served.body.len() as u64, 9 * 1024 * 1024);
    }
    // In this harness the fetcher's streamed fill stays journal-pending, so each
    // waiter's `304` finds incomplete coverage and forwards (pinned) for the bytes:
    // a harness property, the same before and after this change. What is asserted is
    // the answer and the validation's shape.
    let seen = s3.seen();
    assert_eq!(
        seen[0].if_match.as_deref(),
        Some(V1_ETAG),
        "the fetcher's fetch is pinned: {:?}",
        seen
    );
    let waiter_requests: Vec<_> = seen.iter().filter(|r| r.if_none_match.is_some()).collect();
    assert_eq!(
        waiter_requests.len(),
        3,
        "each waiter validates once: {:?}",
        seen
    );
    assert!(
        waiter_requests.iter().all(|r| r.if_match.is_none()),
        "a waiter's validation carries no pin: {:?}",
        seen
    );
    assert!(
        seen.iter()
            .all(|r| r.status != StatusCode::PRECONDITION_FAILED),
        "{:?}",
        seen
    );
}

// ---------------------------------------------------------------------------
// L7: a range GET on an expired entry with the client's own If-None-Match
// ---------------------------------------------------------------------------

fn expired() -> ResolvedSettings {
    ResolvedSettings {
        get_ttl: Duration::ZERO,
        ..ResolvedSettings::default()
    }
}

#[tokio::test]
async fn l7_expired_range_with_matching_if_none_match_gets_304() {
    let h = Harness::new(false).await;
    let key = "bucket/scn1-l7.bin";
    seed_v1_extent(key, &h.cache_manager, 0, 1023, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let served = h
        .range_get(
            key,
            "bytes=0-1023",
            with(&[("if-none-match", V1_ETAG)]),
            expired(),
            true,
            s3.clone(),
        )
        .await;
    let seen = s3.seen();
    assert_eq!(seen.len(), 1, "{:?}", seen);
    assert_eq!(seen[0].if_none_match.as_deref(), Some(V1_ETAG));
    assert_eq!(
        served.status,
        StatusCode::NOT_MODIFIED,
        "L7: S3 answered 304 to the client's own If-None-Match, the proxy answered {} with {} bytes",
        served.status,
        served.body.len()
    );
}

/// Control: with no conditional, the expired entry revalidates with the proxy's
/// own validator and is served from cache.
#[tokio::test]
async fn l7_control_expired_range_without_conditional_revalidates_and_serves_cache() {
    let h = Harness::new(false).await;
    let key = "bucket/scn1-l7-control.bin";
    seed_v1_extent(key, &h.cache_manager, 0, 1023, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let served = h
        .range_get(
            key,
            "bytes=0-1023",
            HashMap::new(),
            expired(),
            false,
            s3.clone(),
        )
        .await;
    let seen = s3.seen();
    assert_eq!(served.status, StatusCode::PARTIAL_CONTENT, "{:?}", seen);
    assert_eq!(body_version(&served.body), Some(V1_ETAG));
    assert_eq!(seen.len(), 1, "{:?}", seen);
    assert_eq!(seen[0].status, StatusCode::NOT_MODIFIED);
}

// ---------------------------------------------------------------------------
// L8: page widening and the client's own conditional
// ---------------------------------------------------------------------------

fn widened() -> ResolvedSettings {
    ResolvedSettings {
        page_widening: true,
        page_size: PAGE_SIZE,
        ..ResolvedSettings::default()
    }
}

/// `If-None-Match` and `If-Match` already bypass the cached page:
/// `fill_page` treats the page as missing when `has_range_conditional_headers`
/// holds, and forwards the client's header. Pinned here.
#[tokio::test]
async fn l8_widened_cached_page_with_matching_if_none_match_gets_304() {
    let h = Harness::new(false).await;
    let key = "bucket/scn1-l8-inm.bin";
    seed_v1_extent(key, &h.cache_manager, 0, PAGE_SIZE - 1, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let served = h
        .range_get(
            key,
            "bytes=10-20",
            with(&[("if-none-match", V1_ETAG)]),
            widened(),
            true,
            s3.clone(),
        )
        .await;
    let seen = s3.seen();
    assert_eq!(
        served.status,
        StatusCode::NOT_MODIFIED,
        "L8: a matching If-None-Match on a widened key got {} with {} bytes: {:?}",
        served.status,
        served.body.len(),
        seen
    );
}

#[tokio::test]
async fn l8_widened_cached_page_with_failing_if_match_gets_412() {
    let h = Harness::new(false).await;
    let key = "bucket/scn1-l8-im.bin";
    seed_v1_extent(key, &h.cache_manager, 0, PAGE_SIZE - 1, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let served = h
        .range_get(
            key,
            "bytes=10-20",
            with(&[("if-match", "\"someone-elses\"")]),
            widened(),
            true,
            s3.clone(),
        )
        .await;
    let seen = s3.seen();
    assert_eq!(
        served.status,
        StatusCode::PRECONDITION_FAILED,
        "L8: a failing If-Match on a widened key got {} with {} bytes: {:?}",
        served.status,
        served.body.len(),
        seen
    );
}

/// `If-Modified-Since` is not in `has_range_conditional_headers`, so a fresh cached
/// page answered it: S3 says `304`, the proxy said `206`.
#[tokio::test]
async fn l8_widened_cached_page_with_if_modified_since_gets_304() {
    let h = Harness::new(false).await;
    let key = "bucket/scn1-l8-ims.bin";
    seed_v1_extent(key, &h.cache_manager, 0, PAGE_SIZE - 1, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let served = h
        .range_get(
            key,
            "bytes=10-20",
            with(&[("if-modified-since", LAST_MODIFIED)]),
            widened(),
            true,
            s3.clone(),
        )
        .await;
    let seen = s3.seen();
    assert_eq!(
        served.status,
        StatusCode::NOT_MODIFIED,
        "L8: If-Modified-Since on a widened key got {} with {} bytes: {:?}",
        served.status,
        served.body.len(),
        seen
    );
}

/// `If-Unmodified-Since` likewise: S3 says `412`, the proxy said `206`.
#[tokio::test]
async fn l8_widened_cached_page_with_failing_if_unmodified_since_gets_412() {
    let h = Harness::new(false).await;
    let key = "bucket/scn1-l8-ius.bin";
    seed_v1_extent(key, &h.cache_manager, 0, PAGE_SIZE - 1, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let served = h
        .range_get(
            key,
            "bytes=10-20",
            with(&[("if-unmodified-since", "Mon, 01 Jan 2024 00:00:00 GMT")]),
            widened(),
            true,
            s3.clone(),
        )
        .await;
    let seen = s3.seen();
    assert_eq!(
        served.status,
        StatusCode::PRECONDITION_FAILED,
        "L8: a failing If-Unmodified-Since on a widened key got {} with {} bytes: {:?}",
        served.status,
        served.body.len(),
        seen
    );
}

/// Control: widening still serves a cached page with no S3 request when the
/// client sent no conditional.
#[tokio::test]
async fn l8_control_widened_cached_page_without_conditional_is_a_hit() {
    let h = Harness::new(false).await;
    let key = "bucket/scn1-l8-control.bin";
    seed_v1_extent(key, &h.cache_manager, 0, PAGE_SIZE - 1, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let served = h
        .range_get(
            key,
            "bytes=10-20",
            HashMap::new(),
            widened(),
            false,
            s3.clone(),
        )
        .await;
    assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(served.body.len(), 11);
    assert!(s3.seen().is_empty(), "{:?}", s3.seen());
}

// ---------------------------------------------------------------------------
// The 412 body: a client's own failing precondition gets S3's 412
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_if_match_412_on_a_range_miss_carries_s3s_headers_and_body() {
    let h = Harness::new(false).await;
    let key = "bucket/scn1-412-body.bin";
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let served = h
        .range_get(
            key,
            "bytes=0-1023",
            with(&[("if-match", "\"someone-elses\"")]),
            ResolvedSettings::default(),
            true,
            s3.clone(),
        )
        .await;
    let seen = s3.seen();
    assert_eq!(seen.len(), 1, "{:?}", seen);
    assert_eq!(served.status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        served.request_id.as_deref(),
        Some(S3_412_REQUEST_ID),
        "the client's 412 must carry S3's headers, got body {:?}",
        String::from_utf8_lossy(&served.body)
    );
    assert_eq!(String::from_utf8_lossy(&served.body), S3_412_BODY);
}

/// Control: the client's own matching `If-Match` on a range miss still passes
/// through to S3 and gets the bytes.
#[tokio::test]
async fn control_client_if_match_that_holds_still_passes_through() {
    let h = Harness::new(false).await;
    let key = "bucket/scn1-412-control.bin";
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let served = h
        .range_get(
            key,
            "bytes=0-1023",
            with(&[("if-match", V1_ETAG)]),
            ResolvedSettings::default(),
            true,
            s3.clone(),
        )
        .await;
    let seen = s3.seen();
    assert_eq!(seen.len(), 1, "{:?}", seen);
    assert_eq!(seen[0].if_match.as_deref(), Some(V1_ETAG));
    assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body_version(&served.body), Some(V1_ETAG));
}

// ---------------------------------------------------------------------------
// The paths that had no test of their own
// ---------------------------------------------------------------------------

/// The complete-range fallback. A partly cached range whose client `If-Match` names
/// the cached ETag (a cache-evaluated `If-Match`, so not forwarded) on an object since
/// overwritten: the hole fetch carries the client's `If-Match` and gets S3's `412`,
/// which is not the proxy's pin, so the merge falls back to
/// `fetch_complete_range_from_s3`. Its `412` is the client's answer, with S3's headers
/// and body.
#[tokio::test]
async fn client_if_match_412_on_the_complete_range_fallback_carries_s3s_headers_and_body() {
    let h = Harness::new(false).await;
    let key = "bucket/scn2-412-complete.bin";
    seed_v1_extent(key, &h.cache_manager, 0, 1023, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(0, OBJECT_SIZE, Duration::ZERO).await;
    let served = h
        .range_get(
            key,
            "bytes=0-2047",
            with(&[("if-match", V1_ETAG)]),
            ResolvedSettings::default(),
            false,
            s3.clone(),
        )
        .await;
    let seen = s3.seen();
    assert!(
        seen.iter()
            .any(|s| s.range.as_deref() == Some("bytes=0-2047")
                && s.status == StatusCode::PRECONDITION_FAILED),
        "PRECONDITION MISSING: the complete-range fallback must have run: {:?}",
        seen
    );
    assert_eq!(served.status, StatusCode::PRECONDITION_FAILED, "{:?}", seen);
    assert_eq!(
        served.request_id.as_deref(),
        Some(S3_412_REQUEST_ID),
        "the client's 412 must carry S3's headers, got body {:?}",
        String::from_utf8_lossy(&served.body)
    );
    assert_eq!(String::from_utf8_lossy(&served.body), S3_412_BODY);
}

/// The synthesized-Range delegation for a HEAD: the waiter answers a no-Range HEAD
/// with a `200`, no `Content-Range` and no body, and validates with neither the
/// synthesized Range nor the proxy's pin.
#[tokio::test]
async fn l1_synthesized_full_object_head_waiter_answers_a_200_without_content_range() {
    let h = Harness::new(true).await;
    let key = "bucket/scn2-l1-synth-head.bin";
    seed_v1_extent(key, &h.cache_manager, 0, OBJECT_SIZE - 1, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = s3.clone();
    let range = format!("bytes=0-{}", OBJECT_SIZE - 1);
    let response = HttpProxy::serve_range_from_cache_validated(
        Method::HEAD,
        format!("/{}", key).parse().unwrap(),
        "s3.us-west-2.amazonaws.com".to_string(),
        with(&[
            ("Range", range.as_str()),
            ("x-proxy-synthesized-full-object-range", "1"),
            ("if-match", "\"waiter-v0\""),
            ("x-proxy-injected-if-match", "1"),
        ]),
        key.to_string(),
        s3_proxy::range_handler::RangeSpec {
            start: 0,
            end: OBJECT_SIZE - 1,
        },
        Arc::clone(&h.cache_manager),
        Arc::clone(&h.range_handler),
        s3_client,
        Arc::clone(&h.config),
        false,
        None,
        &ResolvedSettings::default(),
        &None,
        None,
    )
    .await
    .expect("Infallible");
    let served = collect(response).await;
    let seen = s3.seen();
    assert_eq!(seen.len(), 1, "{:?}", seen);
    assert_eq!(seen[0].if_match, None, "the pin reached S3: {:?}", seen);
    assert_eq!(
        seen[0].range, None,
        "the synthesized Range reached S3: {:?}",
        seen
    );
    assert_eq!(
        served.status,
        StatusCode::OK,
        "a no-Range HEAD's waiter answered {} (content-range {:?}): {:?}",
        served.status,
        served.content_range,
        seen
    );
    assert_eq!(served.content_range, None);
    assert!(served.body.is_empty());
}

/// The realistic synthesized waiter: the pin names the cached ETag and S3 still holds
/// that version. The waiter answers the client's no-Range GET with a `200` of the
/// cached object, never a `206` to the Range the proxy synthesized.
#[tokio::test]
async fn l1_synthesized_waiter_with_a_holding_pin_answers_a_200_of_the_cached_object() {
    let h = Harness::new(true).await;
    let key = "bucket/scn2-l1-synth-holding.bin";
    seed_v1_extent(key, &h.cache_manager, 0, OBJECT_SIZE - 1, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = s3.clone();
    let range = format!("bytes=0-{}", OBJECT_SIZE - 1);
    let response = HttpProxy::serve_range_from_cache_validated(
        Method::GET,
        format!("/{}", key).parse().unwrap(),
        "s3.us-west-2.amazonaws.com".to_string(),
        with(&[
            ("Range", range.as_str()),
            ("x-proxy-synthesized-full-object-range", "1"),
            ("if-match", V1_ETAG),
            ("x-proxy-injected-if-match", "1"),
        ]),
        key.to_string(),
        s3_proxy::range_handler::RangeSpec {
            start: 0,
            end: OBJECT_SIZE - 1,
        },
        Arc::clone(&h.cache_manager),
        Arc::clone(&h.range_handler),
        s3_client,
        Arc::clone(&h.config),
        false,
        None,
        &ResolvedSettings::default(),
        &None,
        None,
    )
    .await
    .expect("Infallible");
    let served = collect(response).await;
    let seen = s3.seen();
    assert_eq!(seen.len(), 1, "{:?}", seen);
    assert_eq!(
        served.status,
        StatusCode::OK,
        "a no-Range GET's waiter answered {} (content-range {:?}): {:?}",
        served.status,
        served.content_range,
        seen
    );
    assert_eq!(served.content_range, None);
    assert_eq!(served.body.len() as u64, OBJECT_SIZE);
    assert_eq!(body_version(&served.body), Some(V1_ETAG));
}

/// L6 with `If-Modified-Since` only: S3's `304` to the client's own validator is the
/// waiter's answer.
#[tokio::test]
async fn l6_full_object_waiter_with_if_modified_since_only_gets_304() {
    let h = Arc::new(Harness::new(true).await);
    let key = "bucket/scn2-l6-full-ims.bin";
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::from_millis(500)).await;
    let (h1, s1) = (h.clone(), s3.clone());
    let (h2, s2) = (h.clone(), s3.clone());
    let (fetcher, waiter) = pair(
        async move { h1.full_get(key, HashMap::new(), s1).await },
        async move {
            h2.full_get(key, with(&[("if-modified-since", LAST_MODIFIED)]), s2)
                .await
        },
    )
    .await;
    let seen = s3.seen();
    assert_eq!(fetcher.status, StatusCode::OK, "{:?}", seen);
    assert_eq!(
        seen.len(),
        2,
        "PRECONDITION MISSING: one fetch plus the waiter's own request: {:?}",
        seen
    );
    assert_eq!(
        seen[1].if_none_match, None,
        "the proxy added its own validator: {:?}",
        seen
    );
    assert_eq!(
        waiter.status,
        StatusCode::NOT_MODIFIED,
        "L6: S3 answered 304 to the client's own If-Modified-Since, the proxy answered {} with {} bytes",
        waiter.status,
        waiter.body.len()
    );
    assert!(waiter.body.is_empty());
}

/// L7 with `If-Modified-Since` only: an expired range entry is not revalidated with
/// the client's validator; the request is forwarded and S3's `304` returned.
#[tokio::test]
async fn l7_expired_range_with_if_modified_since_only_gets_304() {
    let h = Harness::new(false).await;
    let key = "bucket/scn2-l7-ims.bin";
    seed_v1_extent(key, &h.cache_manager, 0, 1023, OBJECT_SIZE).await;
    let s3 = VersionedS3::new(usize::MAX, OBJECT_SIZE, Duration::ZERO).await;
    let served = h
        .range_get(
            key,
            "bytes=0-1023",
            with(&[("if-modified-since", LAST_MODIFIED)]),
            expired(),
            true,
            s3.clone(),
        )
        .await;
    let seen = s3.seen();
    assert_eq!(seen.len(), 1, "{:?}", seen);
    assert_eq!(seen[0].if_none_match, None, "{:?}", seen);
    assert_eq!(
        served.status,
        StatusCode::NOT_MODIFIED,
        "L7: S3 answered 304 to the client's own If-Modified-Since, the proxy answered {} with {} bytes",
        served.status,
        served.body.len()
    );
}

/// Page widening, an ETag-less cached extent: the Page's cached half has no recorded
/// ETag and the object has been overwritten. Filling the Page's hole and merging it
/// with the cached half would answer one range with two versions; the Page is not
/// merged and the client's own range is forwarded.
#[tokio::test]
async fn widened_page_with_an_etagless_extent_is_not_merged_with_another_version() {
    let h = Harness::new(false).await;
    let key = "bucket/scn2-widen-etagless.bin";
    seed_v1_extent_with_etag(key, &h.cache_manager, 0, 511, OBJECT_SIZE, "").await;
    let s3 = VersionedS3::new(0, OBJECT_SIZE, Duration::ZERO).await;
    let served = h
        .range_get(
            key,
            "bytes=500-600",
            HashMap::new(),
            widened(),
            false,
            s3.clone(),
        )
        .await;
    let seen = s3.seen();
    assert_eq!(served.status, StatusCode::PARTIAL_CONTENT, "{:?}", seen);
    assert_eq!(served.body.len(), 101, "{:?}", seen);
    assert!(
        served.body.iter().all(|b| *b == V2_FILL),
        "a widened page merged an ETag-less cached extent with another version: {} bytes of v1, {} of v2: {:?}",
        served.body.iter().filter(|b| **b == V1_FILL).count(),
        served.body.iter().filter(|b| **b == V2_FILL).count(),
        seen
    );
}
