//! Range listings written before 3.0.0 at offset 0, short of the object's end, driven end to
//! end.
//!
//! Before 3.0.0, an open-ended `Range: bytes=N-` or a suffix `Range: bytes=-N` on a key with
//! no cached length was forwarded with the client's header intact and the `206` body was
//! cached as the extent `0..=(len - 1)`: the bytes at offset `N` named as the object's first
//! bytes, under the object's true ETag and length. 3.0.0 does not serve a listing of that
//! shape that records no file identity (`RangeSpec::is_unverified_prefix`), removes it, and
//! lists the refill in its place.
//!
//! Every fixture plants an extent through the real store path and then removes the file
//! identity the 3.0.0 writer recorded, which is what a pre-3.0.0 listing looks like. The
//! object bytes are position-dependent, so a window read from the wrong offset cannot equal
//! the window asked for.
//!
//! The shapes are the ones a stored-header detector missed: a refill at the shifted offset
//! a part GET, a document whose headers came from another fill, page
//! widening's journal fallback, and a document a pre-3.0.0 HEAD stripped. Two vacuity
//! traps are guarded in the fixtures: every planting test asserts the planted document
//! records no identity and holds an extent at 0 shorter than the object before it sends the
//! request under test, and the controls include the adjacent shape the rule serves (a
//! whole-object extent, one byte longer than the shortest refused one).
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
use s3_proxy::cache_types::{ObjectMetadata, RangeLookupPurpose};
use s3_proxy::config::{Config, UpstreamOverrideConfig, UpstreamScheme};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::s3_client::S3Client;
use s3_proxy::S3ClientApi;

const BUCKET: &str = "prefix-bucket";
const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000"; // gitleaks:allow
/// Bound on one request.
const STEP: Duration = Duration::from_secs(20);
/// The object both reconstructed examples were taken from.
const TOTAL: usize = 4 * 1024 * 1024;
const TTL: Duration = Duration::from_secs(3600);
/// Part 1 of the mock's two-part object.
const PART: usize = TOTAL / 2;

// ── Object bytes ────────────────────────────────────────────────────────────

/// Position-dependent object bytes. `* 3` is injective modulo 256, so no two offsets in one
/// period carry the same byte and a 100-byte window read from the wrong offset cannot equal
/// the window the client asked for.
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
    /// Request path -> how many GET/HEAD requests reached the mock for it.
    hits: HashMap<String, usize>,
}

#[derive(Clone, Default)]
struct MockS3 {
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
        let part_one = req
            .uri()
            .query()
            .is_some_and(|q| q.split('&').any(|p| p == "partNumber=1"));
        let range = if part_one {
            Some(format!("bytes=0-{}", PART - 1))
        } else {
            req.headers()
                .get("range")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        };
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
            let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code><Message>gone</Message></Error>";
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("content-type", "application/xml")
                .header("content-length", xml.len().to_string())
                .body(Full::new(Bytes::from_static(xml.as_bytes())))
                .unwrap());
        };

        let total = data.len();
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
            .header("etag", etag_of(&key))
            .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .header("content-type", "application/octet-stream")
            .header("accept-ranges", "bytes");
        if let Some(cr) = content_range {
            builder = builder.header("content-range", cr);
        }
        if part_one {
            builder = builder.header("x-amz-mp-parts-count", "2");
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

/// The object's true ETag, which a shifted entry stores correctly — which is why no
/// freshness or version gate catches one.
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

/// `consolidate` runs the fold on a tight loop, which is what turns a fill's journal `Add`
/// into a `.meta`. The journal-arm test wants it OFF: an unfolded `Add` with no `.meta` is
/// exactly the state that arm serves from, and it is reachable after an upgrade because the
/// fold is a background cycle the listeners do not wait for.
async fn harness(consolidate: bool) -> Harness {
    harness_with(consolidate, None).await
}

/// `full_object_check_threshold`, when set, below the object's size makes the ranged path
/// skip the whole-object check, so the read-path rule is the only defence.
async fn harness_with(consolidate: bool, full_object_check_threshold: Option<u64>) -> Harness {
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
    config.cache.get_ttl = TTL;
    config.cache.head_ttl = TTL;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    if let Some(threshold) = full_object_check_threshold {
        config.cache.full_object_check_threshold = threshold;
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
    if consolidate {
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
    async fn send(
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

    /// A ranged GET, and whether it reached the mock (a miss).
    async fn ranged_probe(&self, key: &str, spec: &str) -> (bool, StatusCode, Bytes) {
        let raw = format!("/{}/{}", BUCKET, key);
        let before = self.mock.hits(&raw);
        let (status, body) = self
            .send(
                "GET",
                &raw,
                &[
                    ("authorization", AUTH.to_string()),
                    ("range", spec.to_string()),
                ],
            )
            .await;
        (self.mock.hits(&raw) > before, status, body)
    }

    async fn head(&self, key: &str) -> StatusCode {
        self.send(
            "HEAD",
            &format!("/{}/{}", BUCKET, key),
            &[("authorization", AUTH.to_string())],
        )
        .await
        .0
    }

    /// Plant one extent through the real store path, with the `ObjectMetadata` the
    /// production extractor builds from the `206` the fill arrived in: `content_length` is
    /// the total read out of `Content-Range`, the header map retains the header, and
    /// `cumulative_size` is what the streaming range-fill arm sets — the total, for a
    /// correct fill and a shifted one alike.
    ///
    /// `(header_start, header_end)` describe what S3 answered; `extent_start` is the offset
    /// the listing claims. A shifted entry is `extent_start == 0` with `header_start > 0`.
    async fn plant(
        &self,
        key: &str,
        header_start: usize,
        header_end: usize,
        extent_start: u64,
        object: &Bytes,
    ) {
        let cache_key = format!("{}/{}", BUCKET, key);
        let body = object.slice(header_start..=header_end);
        let om = ObjectMetadata {
            etag: etag_of(key),
            last_modified: String::from("Wed, 01 Jan 2025 00:00:00 GMT"),
            content_length: object.len() as u64,
            cumulative_size: object.len() as u64,
            content_type: Some(String::from("application/octet-stream")),
            response_headers: HashMap::from([
                (
                    String::from("content-range"),
                    format!("bytes {}-{}/{}", header_start, header_end, object.len()),
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
                &cache_key,
                extent_start,
                extent_start + body.len() as u64 - 1,
                &body,
                om,
                TTL,
                false,
            )
            .await
            .expect("plant the extent");
    }

    /// The planted key's `.meta` as JSON, once one exists.
    async fn meta_doc(&self, key: &str, deadline: Duration) -> Option<serde_json::Value> {
        let want = format!("{}/{}", BUCKET, key);
        let root = self.dir.path().join("metadata");
        let until = tokio::time::Instant::now() + deadline;
        loop {
            let found = walkdir::WalkDir::new(&root)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|x| x == "meta"))
                .filter_map(|e| std::fs::read_to_string(e.path()).ok())
                .filter_map(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .find(|v| {
                    v.get("cache_key")
                        .and_then(|k| k.as_str())
                        .is_some_and(|k| k == want)
                });
            if found.is_some() {
                return found;
            }
            if tokio::time::Instant::now() >= until {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The path of the key's `.meta`, once one exists.
    async fn meta_path(&self, key: &str) -> std::path::PathBuf {
        let want = format!("{}/{}", BUCKET, key);
        let until = tokio::time::Instant::now() + STEP;
        loop {
            let found = walkdir::WalkDir::new(self.dir.path().join("metadata"))
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
                });
            if let Some(found) = found {
                return found.path().to_path_buf();
            }
            assert!(
                tokio::time::Instant::now() < until,
                "no .meta was written for {} within {:?}",
                key,
                STEP
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Rewrite the key's `.meta` with `edit`, then drop this proxy's RAM copy of it.
    async fn rewrite_meta(&self, key: &str, edit: impl Fn(&mut serde_json::Value)) {
        let path = self.meta_path(key).await;
        let mut doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        edit(&mut doc);
        std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
        self.cache_manager
            .invalidate_metadata_cache(&format!("{}/{}", BUCKET, key))
            .await;
    }

    /// Make every listing of the key look as a release before 3.0.0 wrote it: no
    /// `file_identity`.
    async fn make_pre_3_0_0(&self, key: &str) {
        self.rewrite_meta(key, |doc| {
            for range in doc["ranges"].as_array_mut().expect("ranges") {
                range.as_object_mut().unwrap().remove("file_identity");
            }
        })
        .await;
    }

    /// What a pre-3.0.0 HEAD of the key left: no `content-range` among the stored headers.
    async fn strip_content_range(&self, key: &str) {
        self.rewrite_meta(key, |doc| {
            doc["object_metadata"]["response_headers"]
                .as_object_mut()
                .expect("response_headers")
                .remove("content-range");
        })
        .await;
    }

    /// Remove the file identity from every pending journal `Add` of the key. Returns how many
    /// lines were rewritten.
    fn make_journal_pre_3_0_0(&self, key: &str) -> usize {
        let want = format!("{}/{}", BUCKET, key);
        let mut rewritten = 0;
        let journals = self.dir.path().join("metadata").join("_journals");
        for entry in std::fs::read_dir(&journals)
            .expect("journals dir")
            .flatten()
        {
            let path = entry.path();
            if path.extension().is_none_or(|x| x != "journal") {
                continue;
            }
            let content = std::fs::read_to_string(&path).unwrap();
            let mut out = String::new();
            for line in content.lines() {
                let mut value: serde_json::Value = match serde_json::from_str(line) {
                    Ok(v) => v,
                    Err(_) => {
                        out.push_str(line);
                        out.push('\n');
                        continue;
                    }
                };
                if value.get("cache_key").and_then(|k| k.as_str()) == Some(want.as_str())
                    && value.get("operation").and_then(|o| o.as_str()) == Some("Add")
                {
                    if let Some(spec) = value.get_mut("range_spec").and_then(|s| s.as_object_mut())
                    {
                        if spec.remove("file_identity").is_some() {
                            rewritten += 1;
                        }
                    }
                }
                out.push_str(&serde_json::to_string(&value).unwrap());
                out.push('\n');
            }
            std::fs::write(&path, out).unwrap();
        }
        rewritten
    }
}

// ── Shared steps ────────────────────────────────────────────────────────────

/// `Range: bytes=500-` on a 4 MiB object: the listing names `0..=(TOTAL - 501)` and its file
/// holds the object's bytes `500..`.
const SHIFT: usize = 500;

fn shifted_end() -> u64 {
    (TOTAL - SHIFT - 1) as u64
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

fn content_length(doc: &serde_json::Value) -> u64 {
    doc.pointer("/object_metadata/content_length")
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
}

/// Assert the planted `.meta` is the shape the rule branches on before the request under
/// test is sent: it lists `want` exactly, NONE of them records a file identity (a 3.0.0 writer
/// always records one, so a fixture that kept it plants a shape the rule must serve), and the
/// first listed extent starts at 0 and stops short of `content_length` (one byte less and it
/// would be the whole object, which the rule serves).
async fn assert_unverified_prefix_planted(h: &Harness, key: &str, want: &[(u64, u64)]) {
    let doc = h.meta_doc(key, STEP).await.expect("planted .meta");
    let got = extents(&doc);
    let shape: Vec<(u64, u64)> = got.iter().map(|(s, e, _)| (*s, *e)).collect();
    assert_eq!(shape, want, "fixture: the planted extents");
    assert!(
        got.iter().all(|(_, _, identity)| !identity),
        "fixture: a pre-3.0.0 listing records no file identity, got {:?}",
        got
    );
    let len = content_length(&doc);
    assert_eq!(len, TOTAL as u64, "fixture: the document's length");
    assert!(
        want[0].0 == 0 && want[0].1 + 1 < len,
        "fixture: the extent must start at 0 and stop short of the object's end"
    );
}

/// Wait until the key's `.meta` satisfies `pred`, or panic naming `what`.
async fn wait_for_doc(
    h: &Harness,
    key: &str,
    what: &str,
    pred: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let until = tokio::time::Instant::now() + STEP;
    loop {
        if let Some(doc) = h.meta_doc(key, Duration::from_millis(1)).await {
            if pred(&doc) {
                return doc;
            }
        }
        if tokio::time::Instant::now() >= until {
            panic!(
                "{}: {:?}",
                what,
                h.meta_doc(key, Duration::from_millis(1))
                    .await
                    .map(|d| extents(&d))
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ── A refill at the shifted offset ──────────────────────────────────────

/// A refill of `bytes=500-` lists an extent at 500 beside the shifted one and leaves the
/// stored headers alone. With `full_object_check_threshold` below the object's size, the
/// ranged path does not consult the whole-object check, so the read-path rule is the only
/// defence.
#[tokio::test]
async fn f1_a_refill_at_the_shifted_offset_leaves_the_rule_armed() {
    let h = harness_with(true, Some(1024 * 1024)).await;
    let key = "f1/large.bin";
    let object = h.mock.put(key, TOTAL);
    h.plant(key, SHIFT, TOTAL - 1, 0, &object).await;
    h.make_pre_3_0_0(key).await;
    assert_unverified_prefix_planted(&h, key, &[(0, shifted_end())]).await;

    let (reached, status, body) = h
        .ranged_probe(key, &format!("bytes={}-{}", SHIFT, TOTAL - 1))
        .await;
    assert!(reached, "the refill at the shifted offset is a miss");
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, object.slice(SHIFT..TOTAL));
    wait_for_doc(&h, key, "the refill at 500 is listed", |d| {
        extents(d).iter().any(|(s, _, _)| *s == SHIFT as u64)
    })
    .await;

    let (_, status, body) = h.ranged_probe(key, "bytes=0-99").await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        body,
        object.slice(0..100),
        "the client must receive the object's own first 100 bytes"
    );
}

// ── A part GET on the key ───────────────────────────────────────────────

/// A `?partNumber=1` GET resolves through `lookup_part` and stores the part over the
/// document, rewriting its headers. Neither may serve the shifted extent.
#[tokio::test]
async fn f2_a_part_get_neither_serves_nor_launders_the_entry() {
    let h = harness(true).await;
    let key = "f2/parts.bin";
    let object = h.mock.put(key, TOTAL);
    h.plant(key, SHIFT, TOTAL - 1, 0, &object).await;
    h.make_pre_3_0_0(key).await;
    assert_unverified_prefix_planted(&h, key, &[(0, shifted_end())]).await;

    let (status, body) = h
        .send(
            "GET",
            &format!("/{}/{}?partNumber=1", BUCKET, key),
            &[("authorization", AUTH.to_string())],
        )
        .await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "the part GET");
    assert_eq!(
        body,
        object.slice(0..PART),
        "the part GET must return the object's first part"
    );

    let (_, status, body) = h.ranged_probe(key, "bytes=0-99").await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        body,
        object.slice(0..100),
        "after a part GET the client must receive the object's own first 100 bytes"
    );
}

// ── The document's headers came from another fill ───────────────────────

/// A legitimate `bytes=0-99` fill creates the document, so its stored `content-range` is
/// `bytes 0-99/...`, and the shifted fill folds in beside it. No stored header says anything
/// about the shifted extent.
#[tokio::test]
async fn f3_a_shifted_extent_beside_a_legitimate_one_is_not_served() {
    let h = harness(true).await;
    let key = "f3/mixed.bin";
    let object = h.mock.put(key, TOTAL);
    h.plant(key, 0, 99, 0, &object).await;
    wait_for_doc(&h, key, "the first fill's document", |d| {
        extents(d).len() == 1
    })
    .await;
    h.plant(key, SHIFT, TOTAL - 1, 0, &object).await;
    wait_for_doc(&h, key, "the shifted fill folded in", |d| {
        extents(d).len() == 2
    })
    .await;
    h.make_pre_3_0_0(key).await;
    assert_unverified_prefix_planted(&h, key, &[(0, 99), (0, shifted_end())]).await;
    let doc = h.meta_doc(key, STEP).await.unwrap();
    assert_eq!(
        doc.pointer("/object_metadata/response_headers/content-range")
            .and_then(|v| v.as_str()),
        Some(format!("bytes 0-99/{}", TOTAL).as_str()),
        "fixture: the stored header belongs to the legitimate fill"
    );

    // Not inside `0..99`, inside the shifted extent.
    let (_, status, body) = h.ranged_probe(key, "bytes=0-199").await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        body,
        object.slice(0..200),
        "the client must receive the object's own first 200 bytes"
    );
}

// ── Page widening's journal fallback ────────────────────────────────────

/// Page widening reads pending journal fills through
/// `find_pending_journal_ranges_with_etags` when the `.meta` lookup is empty. An unfolded
/// pre-3.0.0 `Add` carries the object's true ETag, so the ETag filter admits it; the rule must
/// not. Asked directly, because that function is the predicate the page path branches on.
#[tokio::test]
async fn f4_an_unfolded_pre_3_0_0_add_is_not_page_coverage() {
    let h = harness(false).await;
    let key = "f4/journal.bin";
    let object = h.mock.put(key, TOTAL);
    h.plant(key, SHIFT, TOTAL - 1, 0, &object).await;
    assert!(
        h.meta_doc(key, Duration::from_secs(2)).await.is_none(),
        "fixture: with no fold the journal is the only record of the fill"
    );
    let stripped = h.make_journal_pre_3_0_0(key);
    assert_eq!(
        stripped, 1,
        "fixture: one pending Add, its identity removed"
    );

    let cache_key = format!("{}/{}", BUCKET, key);
    let disk = h.disk.read().await;
    let pending = disk
        .find_pending_journal_ranges_with_etags(&cache_key, 0, 1024 * 1024 - 1)
        .await
        .expect("journal lookup");
    assert!(
        pending.is_empty(),
        "an unfolded pre-3.0.0 Add at offset 0 must not be page coverage, got {:?}",
        pending
    );
    let overlap = disk
        .find_cached_ranges(&cache_key, 0, 99, None, RangeLookupPurpose::FreshServe)
        .await
        .expect("lookup");
    assert!(
        overlap.ranges.is_empty(),
        "nor coverage for find_cached_ranges, got {:?}",
        overlap.ranges
    );
    drop(disk);

    let (_, status, body) = h.ranged_probe(key, "bytes=0-99").await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, object.slice(0..100));
}

// ── HEAD-stripped ───────────────────────────────────────────────────────────

/// A release before 3.0.0 that answered a HEAD for the key removed `content-range` from the
/// stored headers, leaving a document identical to a legitimate HEADed `bytes=0-99` entry.
/// `Range: bytes=-100`, so the extent `0..99` holds the object's last 100 bytes.
#[tokio::test]
async fn a_head_stripped_entry_is_refetched() {
    let h = harness(true).await;
    let key = "h/suffix.bin";
    let object = h.mock.put(key, TOTAL);
    h.plant(key, TOTAL - 100, TOTAL - 1, 0, &object).await;
    h.make_pre_3_0_0(key).await;
    h.strip_content_range(key).await;
    assert_unverified_prefix_planted(&h, key, &[(0, 99)]).await;
    assert_eq!(h.head(key).await, StatusCode::OK);

    let (_, status, body) = h.ranged_probe(key, "bytes=0-99").await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        body,
        object.slice(0..100),
        "the client must receive the object's own first 100 bytes"
    );
}

// ── Convergence ─────────────────────────────────────────────────────────────

/// The refill of the SAME extent is what a listing left in place would block: a fill of an
/// extent already listed with its file present is not listed again. After the first read the
/// key must be an ordinary hit, with a listing that records its file identity.
#[tokio::test]
async fn the_first_read_heals_and_the_next_reads_hit() {
    let h = harness(true).await;
    let key = "v/converge.bin";
    let object = h.mock.put(key, TOTAL);
    h.plant(key, TOTAL - 100, TOTAL - 1, 0, &object).await;
    h.make_pre_3_0_0(key).await;
    assert_unverified_prefix_planted(&h, key, &[(0, 99)]).await;

    let (reached, _, body) = h.ranged_probe(key, "bytes=0-99").await;
    assert!(reached, "the first read is fetched from S3");
    assert_eq!(body, object.slice(0..100));
    wait_for_doc(
        &h,
        key,
        "the refill is listed with its file identity",
        |d| extents(d) == vec![(0, 99, true)],
    )
    .await;

    for read in 2..=3 {
        let (reached, status, body) = h.ranged_probe(key, "bytes=0-99").await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(body, object.slice(0..100));
        assert!(!reached, "read {} must be served from cache", read);
    }
}

// ── Controls ────────────────────────────────────────────────────────────────

/// Shapes the rule must serve, each from cache with no request to S3.
#[tokio::test]
async fn controls_are_served_from_cache() {
    let h = harness(true).await;

    // A 3.0.0-written first chunk: at 0, shorter than the object, WITH a file identity.
    let a = h.mock.put("c/with-identity.bin", TOTAL);
    h.plant("c/with-identity.bin", 0, 99, 0, &a).await;
    let doc = h.meta_doc("c/with-identity.bin", STEP).await.unwrap();
    assert_eq!(
        extents(&doc),
        vec![(0, 99, true)],
        "fixture: a 3.0.0 writer records an identity"
    );
    let (reached, _, body) = h.ranged_probe("c/with-identity.bin", "bytes=0-99").await;
    assert!(!reached, "a 3.0.0 first chunk must be a cache hit");
    assert_eq!(body, a.slice(0..100));

    // A whole-object pre-3.0.0 extent: `end + 1 == content_length`.
    let b = h.mock.put("c/whole.bin", TOTAL);
    h.plant("c/whole.bin", 0, TOTAL - 1, 0, &b).await;
    h.make_pre_3_0_0("c/whole.bin").await;
    let doc = h.meta_doc("c/whole.bin", STEP).await.unwrap();
    assert_eq!(extents(&doc), vec![(0, (TOTAL - 1) as u64, false)]);
    let (reached, _, body) = h.ranged_probe("c/whole.bin", "bytes=0-99").await;
    assert!(
        !reached,
        "a whole-object pre-3.0.0 entry must be a cache hit"
    );
    assert_eq!(body, b.slice(0..100));

    // A pre-3.0.0 extent that does not start at 0.
    let c = h.mock.put("c/mid.bin", TOTAL);
    h.plant("c/mid.bin", 500, 599, 500, &c).await;
    h.make_pre_3_0_0("c/mid.bin").await;
    let doc = h.meta_doc("c/mid.bin", STEP).await.unwrap();
    assert_eq!(extents(&doc), vec![(500, 599, false)]);
    let (reached, _, body) = h.ranged_probe("c/mid.bin", "bytes=500-599").await;
    assert!(!reached, "a pre-3.0.0 extent above 0 must be a cache hit");
    assert_eq!(body, c.slice(500..600));

    // A 3.0.0 cold `bytes=0-99` fill through the proxy.
    let d = h.mock.put("c/cold.bin", TOTAL);
    let (reached, _, body) = h.ranged_probe("c/cold.bin", "bytes=0-99").await;
    assert!(reached, "the cold read is a miss");
    assert_eq!(body, d.slice(0..100));
    wait_for_doc(&h, "c/cold.bin", "the cold fill is listed", |doc| {
        extents(doc) == vec![(0, 99, true)]
    })
    .await;
    let (reached, _, body) = h.ranged_probe("c/cold.bin", "bytes=0-99").await;
    assert!(!reached, "a 3.0.0 cold bytes=0-99 fill must be a cache hit");
    assert_eq!(body, d.slice(0..100));
}
