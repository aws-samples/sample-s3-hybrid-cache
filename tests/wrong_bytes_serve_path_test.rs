//! Three serve-path defects that hand a client wrong or stale bytes, driven end to end.
//!
//! Every test sends real requests through `HttpProxy::handle_request` to a loopback mock
//! S3 reached over a plaintext upstream override, so each assertion reads what a second
//! client receives rather than an internal figure:
//!
//! 1. a versioned read carrying `Cache-Control: no-cache` filling the *unversioned* key,
//!    so a later plain GET is served the named version's bytes;
//! 2. an open-ended `Range` on a key with no cached metadata stored as the whole object,
//!    so a later `bytes=0-99` is served the bytes at the object's offset `N`;
//! 3. a presigned or anonymous CompleteMultipartUpload leaving the superseded object
//!    cached after the proxy itself acknowledged the overwrite.
//!
//! Two vacuity traps are named in the fixtures, because the realistic case never reaches
//! the code under test and a declined store has several other causes:
//!
//! * "no `.meta` was written" is true of a request the volume-stall breaker, the origin
//!   guard, the unshardable-key term or the danger-band publication guard also declined,
//!   so test 1 carries a positive control in the same run: the same arm, the same headers
//!   and no `versionId` must write the `.meta`. Without that control the assertion passes
//!   on a fixture that never stored anything.
//! * a key whose `.meta` is already on disk never reaches test 2's arm at all (the range
//!   parses against a known length), and an `Authorization`-signed completion never
//!   reaches test 3's branch. Both appear as controls that pass on either tree, so a
//!   fixture built from the realistic case cannot be mistaken for the defect's absence.
//!
//! The object bytes are position-dependent, so a prefix read from the wrong offset cannot
//! match the prefix the client asked for.

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
use s3_proxy::config::{Config, UpstreamOverrideConfig, UpstreamScheme};
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::s3_client::S3Client;
use s3_proxy::S3ClientApi;

const BUCKET: &str = "scn1-bucket";
const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000"; // gitleaks:allow
/// Bound on one request.
const STEP: Duration = Duration::from_secs(20);
/// How long a store that is going to happen has to become visible on disk. Only a
/// negative assertion waits the whole way out.
const SETTLE: Duration = Duration::from_secs(5);

// ── Object bytes ────────────────────────────────────────────────────────────

/// Position-dependent and generation-dependent object bytes. `* 3` is injective modulo
/// 256, so no two offsets inside one period carry the same byte and a 100-byte window
/// read from the wrong offset cannot equal the window the client asked for.
fn object_bytes(generation: u8, len: usize) -> Bytes {
    Bytes::from(
        (0..len)
            .map(|i| ((i % 251) as u8).wrapping_mul(3).wrapping_add(generation))
            .collect::<Vec<u8>>(),
    )
}

// ── Mock S3 ─────────────────────────────────────────────────────────────────

#[derive(Default)]
struct MockState {
    /// Object key -> its versions in write order, newest last.
    versions: HashMap<String, Vec<(String, Bytes)>>,
    /// Request path -> how many GET/HEAD requests reached the mock for it.
    gets: HashMap<String, usize>,
}

#[derive(Clone, Default)]
struct MockS3 {
    state: Arc<Mutex<MockState>>,
}

/// `bytes=A-B`, `bytes=A-` and `bytes=-N` against a known length. `None` means the mock
/// answers the whole object (no range) or `416` (unsatisfiable).
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
    /// Write a new version of `key` and return its version id.
    fn push_len(&self, key: &str, len: usize) -> String {
        let mut st = self.state.lock().unwrap();
        let versions = st.versions.entry(key.to_string()).or_default();
        let generation = versions.len() as u8 + 1;
        let id = format!("v{}", generation);
        versions.push((id.clone(), object_bytes(generation, len)));
        id
    }

    fn push(&self, key: &str) -> String {
        self.push_len(key, 4096)
    }

    /// The bytes the mock holds for one version of `key`.
    fn bytes(&self, key: &str, version_id: &str) -> Bytes {
        self.state
            .lock()
            .unwrap()
            .versions
            .get(key)
            .expect("key")
            .iter()
            .find(|(id, _)| id == version_id)
            .map(|(_, data)| data.clone())
            .expect("version")
    }

    fn gets(&self, raw_path: &str) -> usize {
        *self.state.lock().unwrap().gets.get(raw_path).unwrap_or(&0)
    }

    async fn handle(self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let query = req.uri().query().unwrap_or("").to_string();
        let range = req
            .headers()
            .get("range")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let _body = req.into_body().collect().await?.to_bytes();
        let key = path
            .strip_prefix(&format!("/{}/", BUCKET))
            .unwrap_or("")
            .to_string();

        // CompleteMultipartUpload: a new version of the object is published.
        if method == Method::POST && query.contains("uploadId") && !query.contains("partNumber") {
            let id = self.push(&key);
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult \
                 xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket>\
                 <Key>{}</Key><ETag>&quot;etag-{}&quot;</ETag></CompleteMultipartUploadResult>",
                BUCKET, key, id
            );
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .header("content-length", xml.len().to_string())
                .body(Full::new(Bytes::from(xml)))
                .unwrap());
        }

        if method == Method::GET || method == Method::HEAD {
            let chosen = {
                let mut st = self.state.lock().unwrap();
                *st.gets.entry(path.clone()).or_insert(0) += 1;
                let wanted = query.split('&').find_map(|pair| {
                    pair.strip_prefix("versionId=")
                        .map(|v| v.trim().to_string())
                });
                st.versions.get(&key).and_then(|versions| match wanted {
                    Some(id) => versions.iter().find(|(v, _)| *v == id).cloned(),
                    None => versions.last().cloned(),
                })
            };
            let Some((version_id, data)) = chosen else {
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
                .header("etag", format!("\"etag-{}\"", version_id))
                .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                .header("content-type", "application/octet-stream")
                .header("x-amz-version-id", version_id.as_str())
                .header("accept-ranges", "bytes");
            if let Some(cr) = content_range {
                builder = builder.header("content-range", cr);
            }
            let body = if method == Method::HEAD {
                Bytes::new()
            } else {
                body
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
    _dir: TempDir,
    _mock_shutdown: oneshot::Sender<()>,
    _proxy_shutdown: oneshot::Sender<()>,
    mock: MockS3,
    mock_host: String,
    proxy: SocketAddr,
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
        _dir: dir,
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
        headers: &[(&str, String)],
        body: Bytes,
    ) -> (StatusCode, Bytes, HashMap<String, String>) {
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
            for (k, v) in headers {
                builder = builder.header(*k, v.as_str());
            }
            let resp = sender
                .send_request(builder.body(Full::new(body)).unwrap())
                .await
                .expect("proxy request");
            let status = resp.status();
            let response_headers: HashMap<String, String> = resp
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();
            let body = resp.into_body().collect().await.expect("body").to_bytes();
            (status, body, response_headers)
        };
        tokio::time::timeout(STEP, fut)
            .await
            .unwrap_or_else(|_| panic!("{} {} timed out", method, path_and_query))
    }

    async fn get(&self, key: &str) -> (StatusCode, Bytes) {
        let (status, body, _) = self
            .send(
                "GET",
                &format!("/{}/{}", BUCKET, key),
                &[("authorization", AUTH.to_string())],
                Bytes::new(),
            )
            .await;
        (status, body)
    }

    async fn ranged(&self, key: &str, spec: &str) -> (StatusCode, Bytes) {
        let (status, body, _) = self
            .send(
                "GET",
                &format!("/{}/{}", BUCKET, key),
                &[
                    ("authorization", AUTH.to_string()),
                    ("range", spec.to_string()),
                ],
                Bytes::new(),
            )
            .await;
        (status, body)
    }

    /// GET until a repeat is answered without reaching the mock, then wait for the key's
    /// `.meta`. A hit alone is not a stable state: two fills of one key can overlap, and
    /// the repeat that hits may be served from a fill-visibility record that is gone
    /// before consolidation writes the `.meta`.
    async fn cache(&self, key: &str) -> Bytes {
        let raw = format!("/{}/{}", BUCKET, key);
        let (status, first) = self.get(key).await;
        assert_eq!(status, StatusCode::OK, "seed GET {}", raw);
        for _ in 0..100 {
            let before = self.mock.gets(&raw);
            let (status, body) = self.get(key).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body, first);
            if self.mock.gets(&raw) == before {
                assert!(
                    self.meta_within(key, STEP).await,
                    "{} was a cache hit but no .meta was written within {:?}",
                    raw,
                    STEP
                );
                return first;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("{} never became a cache hit", raw);
    }

    /// Whether a `.meta` naming `key` is on disk within `deadline`.
    async fn meta_within(&self, key: &str, deadline: Duration) -> bool {
        let want = format!("{}/{}", BUCKET, key);
        let root = self._dir.path().join("metadata");
        let until = tokio::time::Instant::now() + deadline;
        loop {
            let found = walkdir::WalkDir::new(&root)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|x| x == "meta"))
                .filter_map(|e| std::fs::read_to_string(e.path()).ok())
                .filter_map(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .any(|v| {
                    v.get("cache_key")
                        .and_then(|k| k.as_str())
                        .is_some_and(|k| k == want)
                });
            if found {
                return true;
            }
            if tokio::time::Instant::now() >= until {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Whether a plain GET of `key` reached the mock (a miss), and what it returned.
    async fn probe(&self, key: &str) -> (bool, StatusCode, Bytes) {
        let raw = format!("/{}/{}", BUCKET, key);
        let before = self.mock.gets(&raw);
        let (status, body) = self.get(key).await;
        (self.mock.gets(&raw) > before, status, body)
    }

    /// Whether a ranged GET of `key` reached the mock (a miss), and what it returned.
    async fn ranged_probe(&self, key: &str, spec: &str) -> (bool, StatusCode, Bytes) {
        let raw = format!("/{}/{}", BUCKET, key);
        let before = self.mock.gets(&raw);
        let (status, body) = self.ranged(key, spec).await;
        (self.mock.gets(&raw) > before, status, body)
    }
}

fn complete_mpu_body() -> Bytes {
    Bytes::from_static(
        b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUpload \
          xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Part><PartNumber>1</PartNumber>\
          <ETag>\"part-1\"</ETag></Part></CompleteMultipartUpload>",
    )
}

// ── 1. A versioned read must not fill the unversioned key ───────────────────

#[tokio::test]
async fn versioned_no_cache_get_does_not_fill_the_unversioned_key() {
    let h = harness().await;

    let old = h.mock.push("v/obj.bin");
    let current = h.mock.push("v/obj.bin");
    let old_bytes = h.mock.bytes("v/obj.bin", &old);
    let current_bytes = h.mock.bytes("v/obj.bin", &current);
    assert_ne!(old_bytes, current_bytes, "fixture: the versions differ");
    h.mock.push("v/control.bin");

    // The request under test: a versioned read on the cache-bypass-header arm.
    let (status, body, _) = h
        .send(
            "GET",
            &format!("/{}/v/obj.bin?versionId={}", BUCKET, old),
            &[
                ("authorization", AUTH.to_string()),
                ("cache-control", "no-cache".to_string()),
            ],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body, old_bytes,
        "the versioned read must still return the version it named"
    );

    // POSITIVE CONTROL, same run, same arm, same headers, no `versionId`: this arm does
    // store. Without it, "no .meta for v/obj.bin" is true of a fixture that stored nothing.
    let (status, _, _) = h
        .send(
            "GET",
            &format!("/{}/v/control.bin", BUCKET),
            &[
                ("authorization", AUTH.to_string()),
                ("cache-control", "no-cache".to_string()),
            ],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        h.meta_within("v/control.bin", STEP).await,
        "apparatus: the no-cache arm wrote no .meta for the control key either, so the \
         assertions below prove nothing about the versioned read"
    );

    let leaked = h.meta_within("v/obj.bin", SETTLE).await;
    let (reached_mock, status, body) = h.probe("v/obj.bin").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body, current_bytes,
        "a plain GET after the versioned read was served the named version's bytes"
    );
    assert!(
        reached_mock,
        "a plain GET after the versioned read was served from the cache"
    );
    assert!(
        !leaked,
        "the versioned read wrote a .meta for the unversioned key v/obj.bin"
    );
}

#[tokio::test]
async fn versioned_no_cache_head_does_not_fill_the_unversioned_key() {
    let h = harness().await;

    let old = h.mock.push_len("v/head.bin", 1024);
    let current = h.mock.push_len("v/head.bin", 4096);
    h.mock.push_len("v/head-control.bin", 2048);

    let (status, _, _) = h
        .send(
            "HEAD",
            &format!("/{}/v/head.bin?versionId={}", BUCKET, old),
            &[
                ("authorization", AUTH.to_string()),
                ("cache-control", "no-cache".to_string()),
            ],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    // POSITIVE CONTROL: the same arm's HEAD store, without `versionId`.
    let (status, _, _) = h
        .send(
            "HEAD",
            &format!("/{}/v/head-control.bin", BUCKET),
            &[
                ("authorization", AUTH.to_string()),
                ("cache-control", "no-cache".to_string()),
            ],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        h.meta_within("v/head-control.bin", STEP).await,
        "apparatus: the no-cache arm wrote no HEAD .meta for the control key"
    );

    let leaked = h.meta_within("v/head.bin", SETTLE).await;
    let (status, _, headers) = h
        .send(
            "HEAD",
            &format!("/{}/v/head.bin", BUCKET),
            &[("authorization", AUTH.to_string())],
            Bytes::new(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("content-length").map(String::as_str),
        Some("4096"),
        "a plain HEAD after the versioned HEAD reported the named version's length \
         (current version {})",
        current
    );
    assert!(
        !leaked,
        "the versioned HEAD wrote a .meta for the unversioned key v/head.bin"
    );
}

// ── 2. An open-ended Range on a cold key must not be stored as the object ───

#[tokio::test]
async fn cold_open_ended_range_is_not_stored_as_the_whole_object() {
    let h = harness().await;

    // CONTROL FIRST, and the vacuity trap named: a key whose `.meta` is already on disk
    // parses its range against a known length and never reaches the arm under test. This
    // control passes on both trees; it is here so a warm fixture cannot be mistaken for
    // the defect's absence.
    let warm_id = h.mock.push_len("r/warm.bin", 4096);
    let warm = h.mock.bytes("r/warm.bin", &warm_id);
    h.cache("r/warm.bin").await;
    let (status, body) = h.ranged("r/warm.bin", "bytes=500-").await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        body,
        warm.slice(500..),
        "control: a warm key's open-ended range"
    );
    let (_, status, body) = h.ranged_probe("r/warm.bin", "bytes=0-99").await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        body,
        warm.slice(0..100),
        "control: a warm key's later prefix read"
    );

    // The request under test: an open-ended range on a key with no cached metadata.
    let cold_id = h.mock.push_len("r/cold.bin", 4096);
    let cold = h.mock.bytes("r/cold.bin", &cold_id);
    let (status, body) = h.ranged("r/cold.bin", "bytes=500-").await;
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "the open-ended range's own response is unchanged: S3's status"
    );
    assert_eq!(
        body,
        cold.slice(500..),
        "the open-ended range's own response is unchanged: S3's bytes"
    );

    let stored = h.meta_within("r/cold.bin", SETTLE).await;
    let (_, status, body) = h.ranged_probe("r/cold.bin", "bytes=0-99").await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        body,
        cold.slice(0..100),
        "a later bytes=0-99 was served the bytes at the object's offset 500"
    );
    assert!(
        !stored,
        "the partial response to bytes=500- was stored for the cold key r/cold.bin"
    );

    // The suffix form reaches the same arm for the same reason (no length to resolve it
    // against), so it is pinned too.
    let suffix_id = h.mock.push_len("r/suffix.bin", 4096);
    let suffix = h.mock.bytes("r/suffix.bin", &suffix_id);
    let (status, body) = h.ranged("r/suffix.bin", "bytes=-100").await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        body,
        suffix.slice(3996..),
        "the suffix range's own response is unchanged"
    );
    let stored = h.meta_within("r/suffix.bin", SETTLE).await;
    let (_, status, body) = h.ranged_probe("r/suffix.bin", "bytes=0-99").await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        body,
        suffix.slice(0..100),
        "a later bytes=0-99 was served the bytes at the object's tail"
    );
    assert!(
        !stored,
        "the partial response to bytes=-100 was stored for the cold key r/suffix.bin"
    );
}

// ── 3. An acknowledged completion must not leave the old object cached ──────

#[tokio::test]
async fn anonymous_complete_multipart_upload_invalidates_the_object() {
    let h = harness().await;
    let completion_query = "uploadId=scn1-upload&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260101T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=0000"; // gitleaks:allow

    // CONTROL FIRST, and the vacuity trap named: an `Authorization`-signed completion
    // never reaches the branch under test, and already invalidates. It passes on both
    // trees, so a signed fixture cannot be mistaken for the defect's absence.
    h.mock.push("m/signed.bin");
    let signed_before = h.cache("m/signed.bin").await;
    let (status, _, _) = h
        .send(
            "POST",
            &format!("/{}/m/signed.bin?{}", BUCKET, completion_query),
            &[
                ("authorization", AUTH.to_string()),
                ("content-type", "application/xml".to_string()),
            ],
            complete_mpu_body(),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "control: the signed completion");
    let signed_after = h.mock.bytes("m/signed.bin", "v2");
    assert_ne!(signed_after, signed_before);
    let (reached_mock, status, body) = h.probe("m/signed.bin").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body, signed_after,
        "control: a signed completion already invalidates the object"
    );
    assert!(reached_mock, "control: the GET after a signed completion");

    // The request under test: the same completion with no `Authorization` header, as a
    // presigned URL or an anonymous caller sends it.
    h.mock.push("m/obj.bin");
    let before = h.cache("m/obj.bin").await;
    let (status, _, _) = h
        .send(
            "POST",
            &format!("/{}/m/obj.bin?{}", BUCKET, completion_query),
            &[("content-type", "application/xml".to_string())],
            complete_mpu_body(),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the completion must be forwarded and acknowledged"
    );
    let after = h.mock.bytes("m/obj.bin", "v2");
    assert_ne!(after, before, "fixture: the completion published new bytes");

    let (reached_mock, status, body) = h.probe("m/obj.bin").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body, after,
        "the superseded object was served after the proxy acknowledged the completion"
    );
    assert!(
        reached_mock,
        "the GET after the acknowledged completion was served from the cache"
    );
}
