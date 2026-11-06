//! A real `HttpProxy` in front of a mock origin on loopback, for tests that need cached
//! ranges on disk.
//!
//! Cached state is made the way production makes it: a ranged or part-numbered GET that misses
//! is forwarded to the origin, and the range fill the request path starts publishes the `.bin`,
//! journals it and credits it. Nothing here writes a range file, a `.meta` or a journal line
//! itself.
//!
//! The origin keys objects by the raw request path (`/bucket/key`, percent-encoding as the
//! client sent it), answers `Range` and `partNumber` (a two-part object), and counts the GET and
//! HEAD requests that reach it per path.
// Each test file includes this module and uses part of it.
#![allow(dead_code)]

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
use s3_proxy::cache_types::RangeLookupPurpose;
use s3_proxy::config::{Config, UpstreamOverrideConfig, UpstreamScheme};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::s3_client::S3Client;
use s3_proxy::S3ClientApi;

pub const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000"; // gitleaks:allow

/// Bound on one request or one wait for a fill to land.
pub const STEP: Duration = Duration::from_secs(20);

/// Position-dependent bytes: `* 3` is injective modulo 256, so no two offsets in one period
/// carry the same byte and a window read from the wrong offset cannot equal the window asked
/// for.
pub fn object_bytes(len: usize) -> Bytes {
    Bytes::from(
        (0..len)
            .map(|i| ((i % 251) as u8).wrapping_mul(3))
            .collect::<Vec<u8>>(),
    )
}

/// The ETag the origin answers for `raw_path`.
pub fn etag_of(raw_path: &str) -> String {
    format!("\"etag-{}\"", raw_path.replace('/', "-"))
}

#[derive(Default)]
struct MockState {
    objects: HashMap<String, Bytes>,
    hits: HashMap<String, usize>,
    /// The ETag every response carries, when set; otherwise [`etag_of`] the request path.
    etag: Option<String>,
}

/// The origin. Clone shares state.
#[derive(Clone, Default)]
pub struct MockOrigin {
    state: Arc<Mutex<MockState>>,
}

/// `bytes=A-B`, `bytes=A-` and `bytes=-N` against a known length.
fn parse_range(spec: &str, total: usize) -> Option<(usize, usize)> {
    let spec = spec.trim().strip_prefix("bytes=")?;
    let (first, last) = spec.split_once('-')?;
    let (start, end) = match (first.trim().is_empty(), last.trim().is_empty()) {
        (false, false) => (
            first.trim().parse::<usize>().ok()?,
            last.trim().parse::<usize>().ok()?.min(total - 1),
        ),
        (false, true) => (first.trim().parse::<usize>().ok()?, total - 1),
        (true, false) => (
            total.saturating_sub(last.trim().parse::<usize>().ok()?),
            total - 1,
        ),
        (true, true) => return None,
    };
    (start < total && start <= end).then_some((start, end))
}

impl MockOrigin {
    /// Store `data` under `raw_path` (for example `/bucket/key`).
    pub fn put_bytes(&self, raw_path: &str, data: Bytes) {
        self.state
            .lock()
            .unwrap()
            .objects
            .insert(raw_path.to_string(), data);
    }

    /// Store [`object_bytes`] of `len` under `raw_path` and return them.
    pub fn put(&self, raw_path: &str, len: usize) -> Bytes {
        let data = object_bytes(len);
        self.put_bytes(raw_path, data.clone());
        data
    }

    /// Answer every request with `etag`, for a test that describes the same object version to
    /// another origin.
    pub fn set_etag(&self, etag: &str) {
        self.state.lock().unwrap().etag = Some(etag.to_string());
    }

    /// GET and HEAD requests that reached the origin for `raw_path`.
    pub fn hits(&self, raw_path: &str) -> usize {
        *self.state.lock().unwrap().hits.get(raw_path).unwrap_or(&0)
    }

    async fn handle(self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let part_number = req.uri().query().and_then(|q| {
            q.split('&')
                .find_map(|p| p.strip_prefix("partNumber=")?.parse::<usize>().ok())
        });
        let range_header = req
            .headers()
            .get("range")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let _body = req.into_body().collect().await?.to_bytes();
        if method != Method::GET && method != Method::HEAD {
            return Ok(Response::builder()
                .status(StatusCode::NOT_IMPLEMENTED)
                .body(Full::new(Bytes::new()))
                .unwrap());
        }
        let (data, etag) = {
            let mut st = self.state.lock().unwrap();
            *st.hits.entry(path.clone()).or_insert(0) += 1;
            (
                st.objects.get(&path).cloned(),
                st.etag.clone().unwrap_or_else(|| etag_of(&path)),
            )
        };
        let Some(data) = data else {
            let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code><Message>gone</Message></Error>";
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("content-type", "application/xml")
                .header("content-length", xml.len().to_string())
                .body(Full::new(Bytes::from_static(xml.as_bytes())))
                .unwrap());
        };
        let total = data.len();
        let half = total / 2;
        let range = match part_number {
            Some(1) => Some(format!("bytes=0-{}", half - 1)),
            Some(2) => Some(format!("bytes={}-{}", half, total - 1)),
            _ => range_header,
        };
        let (status, body, content_range) = match range.as_deref() {
            Some(spec) => match parse_range(spec, total) {
                Some((start, end)) => (
                    StatusCode::PARTIAL_CONTENT,
                    data.slice(start..=end),
                    Some(format!("bytes {}-{}/{}", start, end, total)),
                ),
                None => {
                    return Ok(Response::builder()
                        .status(StatusCode::RANGE_NOT_SATISFIABLE)
                        .header("content-range", format!("bytes */{}", total))
                        .header("content-length", "0")
                        .body(Full::new(Bytes::new()))
                        .unwrap())
                }
            },
            None => (StatusCode::OK, data.clone(), None),
        };
        let mut builder = Response::builder()
            .status(status)
            .header("content-length", body.len().to_string())
            .header("etag", etag)
            .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .header("content-type", "application/octet-stream")
            .header("accept-ranges", "bytes");
        if let Some(cr) = content_range {
            builder = builder.header("content-range", cr);
        }
        if part_number.is_some() {
            builder = builder.header("x-amz-mp-parts-count", "2");
        }
        let body = if method == Method::HEAD {
            Bytes::new()
        } else {
            body
        };
        Ok(builder.body(Full::new(body)).unwrap())
    }

    /// Serve on an ephemeral loopback port until the returned sender is dropped.
    pub async fn spawn(self) -> (SocketAddr, oneshot::Sender<()>) {
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

/// What the rig builds.
#[derive(Clone)]
pub struct RigOptions {
    /// Run the journal consolidation cycle every 100 ms and fold each seeded fill into its
    /// `.meta`. Off, a fill stays a pending journal `Add`.
    pub consolidate: bool,
    /// Compress cached ranges.
    pub compression: bool,
    /// How long a cached range stays fresh.
    pub get_ttl: Duration,
    /// Enable the RAM cache tier.
    pub ram_cache: bool,
}

impl Default for RigOptions {
    fn default() -> Self {
        Self {
            consolidate: true,
            compression: true,
            get_ttl: Duration::from_secs(3600),
            ram_cache: false,
        }
    }
}

/// A proxy on loopback in front of a [`MockOrigin`], with the cache the proxy fills.
pub struct Rig {
    pub dir: TempDir,
    pub mock: MockOrigin,
    pub mock_host: String,
    pub proxy: SocketAddr,
    pub disk: Arc<tokio::sync::RwLock<DiskCacheManager>>,
    pub cache_manager: Arc<CacheManager>,
    pub range_handler: Arc<RangeHandler>,
    consolidate: bool,
    _mock_shutdown: oneshot::Sender<()>,
    _proxy_shutdown: oneshot::Sender<()>,
}

impl Rig {
    pub async fn new(options: RigOptions) -> Rig {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mock = MockOrigin::default();
        let (mock_addr, mock_shutdown) = mock.clone().spawn().await;
        let mock_host = format!("127.0.0.1:{}", mock_addr.port());
        let dir = TempDir::new().unwrap();
        let mut config = Config::default();
        config.cache.cache_dir = dir.path().to_path_buf();
        config.cache.ram_cache_enabled = options.ram_cache;
        config.cache.write_cache_enabled = true;
        config.cache.read_cache_enabled = true;
        config.cache.download_coordination.enabled = false;
        config.cache.get_ttl = options.get_ttl;
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
            options.compression,
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
        let disk = Arc::new(tokio::sync::RwLock::new(
            cache_manager.create_configured_disk_cache_manager(),
        ));
        cache_manager.initialize().await.expect("cache init");
        disk.write().await.initialize().await.expect("disk init");
        let range_handler = Arc::new(RangeHandler::new(
            Arc::clone(&cache_manager),
            Arc::clone(&disk),
        ));
        if options.consolidate {
            if let Some(consolidator) = cache_manager.get_journal_consolidator().await {
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(Duration::from_millis(100));
                    loop {
                        interval.tick().await;
                        let _ = consolidator.run_consolidation_cycle().await;
                    }
                });
            }
        }
        let inflight_tracker = Arc::new(InFlightTracker::new());
        let request_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_requests));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let proxy = listener.local_addr().unwrap();
        let (proxy_shutdown, mut rx) = oneshot::channel::<()>();
        let serve_cache_manager = Arc::clone(&cache_manager);
        let serve_range_handler = Arc::clone(&range_handler);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        let Ok((stream, peer)) = accept else { break };
                        let config = Arc::clone(&config);
                        let cache_manager = Arc::clone(&serve_cache_manager);
                        let range_handler = Arc::clone(&serve_range_handler);
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
                                        request_semaphore, None, None, inflight_tracker, None,
                                        None, None, inflight_ledger,
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
        Rig {
            dir,
            mock,
            mock_host,
            proxy,
            disk,
            cache_manager,
            range_handler,
            consolidate: options.consolidate,
            _mock_shutdown: mock_shutdown,
            _proxy_shutdown: proxy_shutdown,
        }
    }

    /// One request through the proxy, bounded by [`STEP`].
    pub async fn send(
        &self,
        method: &str,
        path_and_query: &str,
        headers: &[(&str, String)],
    ) -> (StatusCode, Bytes) {
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
                .header("content-length", "0");
            for (k, v) in headers {
                builder = builder.header(*k, v.as_str());
            }
            let resp = sender
                .send_request(builder.body(Full::new(Bytes::new())).unwrap())
                .await
                .expect("proxy request");
            let status = resp.status();
            let body = resp.into_body().collect().await.expect("body").to_bytes();
            (status, body)
        };
        tokio::time::timeout(STEP, fut)
            .await
            .unwrap_or_else(|_| panic!("{} {} timed out", method, path_and_query))
    }

    /// A signed GET with the given extra headers.
    pub async fn get(&self, raw_path: &str, extra: &[(&str, String)]) -> (StatusCode, Bytes) {
        let mut headers = vec![("authorization", AUTH.to_string())];
        headers.extend(extra.iter().map(|(k, v)| (*k, v.clone())));
        self.send("GET", raw_path, &headers).await
    }

    /// The cache key the proxy files `raw_path` under.
    pub fn cache_key(raw_path: &str) -> String {
        CacheManager::generate_cache_key(raw_path, None)
    }

    /// Make `raw_path` hold `start..=end` in the cache: a ranged GET that misses, forwarded to
    /// the origin, then wait until the range fill the request started is listed, and with
    /// consolidation on, folded into the key's `.meta`. Returns the bytes the client received.
    pub async fn seed_range(&self, raw_path: &str, start: u64, end: u64) -> Bytes {
        let (status, body) = self
            .get(raw_path, &[("range", format!("bytes={}-{}", start, end))])
            .await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT, "seeding {raw_path}");
        assert_eq!(body.len() as u64, end - start + 1, "seeding {raw_path}");
        self.wait_listed(raw_path, start, end).await;
        body
    }

    /// Wait until the key's `.meta` file exists and lists any range, for a rig whose ranges are
    /// already stale when they land (so a lookup that skips stale ranges cannot see them). The
    /// file, not the fill-visibility record that stands in for it until the fold: a lookup
    /// before the fold takes the journal fallback, which carries no expiry.
    pub async fn wait_meta(&self, raw_path: &str) {
        let cache_key = Self::cache_key(raw_path);
        let until = tokio::time::Instant::now() + STEP;
        loop {
            let on_disk = self
                .disk
                .read()
                .await
                .get_new_metadata_file_path(&cache_key)
                .exists();
            let listed = on_disk
                && self
                    .cache_manager
                    .get_metadata_from_disk(&cache_key)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|m| !m.ranges.is_empty());
            if listed {
                return;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "no .meta for {raw_path} within {STEP:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait until a lookup finds exactly `start..=end` of `raw_path` in the cache (a pending
    /// journal `Add` counts), and with consolidation on until the `.meta` lists it.
    pub async fn wait_listed(&self, raw_path: &str, start: u64, end: u64) {
        let cache_key = Self::cache_key(raw_path);
        let until = tokio::time::Instant::now() + STEP;
        loop {
            let found = self
                .disk
                .read()
                .await
                .find_cached_ranges(&cache_key, start, end, None, RangeLookupPurpose::FreshServe)
                .await
                .map(|o| o.ranges.iter().any(|r| r.start == start && r.end == end))
                .unwrap_or(false);
            let folded = !self.consolidate
                || (self
                    .disk
                    .read()
                    .await
                    .get_new_metadata_file_path(&cache_key)
                    .exists()
                    && self
                        .cache_manager
                        .get_metadata_from_disk(&cache_key)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|m| {
                            m.ranges.iter().any(|r| r.start == start && r.end == end)
                        }));
            if found && folded {
                return;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "the fill of {raw_path} {start}-{end} was not listed within {STEP:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
