//! DeleteObjects and browser form POST uploads invalidate the proxy's cached copies.
//!
//! Every test drives real requests through `HttpProxy::handle_request` (and so through
//! `handle_other_request`) to a loopback mock S3 reached over a plaintext upstream
//! override. The mock keeps per-key object state, answers DeleteObjects with a
//! configurable result document, and bumps an object's generation when a form POST
//! names it, so a GET after the write can tell a cached (stale) answer from a fresh one:
//!
//! * a cached answer never reaches the mock, so the mock's GET count for that request
//!   path does not move, and the bytes are the pre-write generation;
//! * a fresh answer reaches the mock and returns `404` (deleted) or the new generation.
//!
//! A GET is only treated as cached once a repeat of it has been served without the mock
//! seeing it, so the "before" state is observed rather than assumed.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
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

// ── Mock S3 ─────────────────────────────────────────────────────────────────

/// How the mock answers a DeleteObjects request.
#[derive(Clone, Default)]
struct DeleteBehaviour {
    /// Keys the mock refuses with a per-key `<Error>` (and so does not delete).
    refuse: HashSet<String>,
    /// (key, VersionId) entries the mock refuses; the key's other entries are deleted.
    refuse_versions: HashSet<(String, String)>,
    /// Delay between deleting and answering, so a client can disconnect meanwhile.
    respond_after: Option<Duration>,
}

#[derive(Default)]
struct MockState {
    /// Decoded object key -> generation. Absent means the object does not exist.
    objects: HashMap<String, u32>,
    /// Decoded object key -> body written by a PUT, served instead of the generated one.
    contents: HashMap<String, Bytes>,
    /// Raw request path (as the proxy sent it) -> GET count.
    gets: HashMap<String, usize>,
    delete: DeleteBehaviour,
    delete_requests: usize,
    post_requests: usize,
}

#[derive(Clone, Default)]
struct MockS3 {
    state: Arc<Mutex<MockState>>,
}

/// A request path's object key as Amazon S3 reads it: `%XX` is a byte, `+` a space and
/// a raw non-ASCII byte a `?`.
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
        out.push(match bytes[i] {
            b'+' => b' ',
            b if b > 0x7F => b'?',
            b => b,
        });
        i += 1;
    }
    String::from_utf8(out).expect("utf8 key")
}

/// An object key as Amazon S3 spells it in a form POST's `Location` header (measured):
/// a space as `+`, `/` and `+` as `%XX`, letters, digits and `-._` raw.
fn s3_form_encode(key: &str) -> String {
    key.bytes()
        .map(|b| match b {
            b' ' => "+".to_string(),
            b if b.is_ascii_alphanumeric() || b"-._".contains(&b) => (b as char).to_string(),
            b => format!("%{:02X}", b),
        })
        .collect()
}

/// `(Key, VersionId)` for each `<Object>` in a DeleteObjects request body.
fn mock_request_objects(xml: &str) -> Vec<(String, Option<String>)> {
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Object>") {
        let after = &rest[start + 8..];
        let end = after.find("</Object>").expect("closing Object");
        let object = &after[..end];
        let version = object.find("<VersionId>").map(|v| {
            let v = &object[v + 11..];
            v[..v.find("</VersionId>").unwrap()].to_string()
        });
        for key in mock_request_keys(object) {
            out.push((key, version.clone()));
        }
        rest = &after[end + 9..];
    }
    out
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// The `<Key>` values in a DeleteObjects request body. The mock's own minimal reader,
/// independent of the proxy's.
fn mock_request_keys(xml: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Key>") {
        let after = &rest[start + 5..];
        if let Some(cdata) = after.strip_prefix("<![CDATA[") {
            let end = cdata.find("]]>").expect("closing CDATA");
            keys.push(cdata[..end].to_string());
            let close = cdata[end..].find("</Key>").expect("closing Key");
            rest = &cdata[end + close + 6..];
            continue;
        }
        let end = after.find("</Key>").expect("closing Key");
        let raw = &after[..end];
        keys.push(
            raw.replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&apos;", "'")
                .replace("&amp;", "&"),
        );
        rest = &after[end + 6..];
    }
    keys
}

/// Form field `key` and the file part's filename, from a multipart body.
fn mock_form_key(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let key_start = text.find("name=\"key\"\r\n\r\n").expect("key field") + 14;
    let key_end = key_start + text[key_start..].find("\r\n").unwrap();
    let key = text[key_start..key_end].to_string();
    let fname_start = text.find("filename=\"").expect("filename") + 10;
    let fname_end = fname_start + text[fname_start..].find('"').unwrap();
    key.replace("${filename}", &text[fname_start..fname_end])
}

impl MockS3 {
    fn put(&self, key: &str) {
        self.state
            .lock()
            .unwrap()
            .objects
            .insert(key.to_string(), 0);
    }

    fn gets(&self, raw_path: &str) -> usize {
        *self.state.lock().unwrap().gets.get(raw_path).unwrap_or(&0)
    }

    fn set_delete(&self, behaviour: DeleteBehaviour) {
        self.state.lock().unwrap().delete = behaviour;
    }

    async fn handle(self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let host = req
            .headers()
            .get("host")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("")
            .to_string();
        let path = req.uri().path().to_string();
        let query = req.uri().query().unwrap_or("").to_string();
        let body = req.into_body().collect().await?.to_bytes();
        let bucket_root = format!("/{}", BUCKET);

        if method == hyper::Method::POST && path == bucket_root && query == "delete" {
            let xml = String::from_utf8_lossy(&body).to_string();
            let quiet = xml.contains("<Quiet>true</Quiet>");
            let (out, respond_after) = {
                let mut st = self.state.lock().unwrap();
                st.delete_requests += 1;
                let mut out = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
            );
                for (key, version) in mock_request_objects(&xml) {
                    let version_refused = version.as_ref().is_some_and(|v| {
                        st.delete
                            .refuse_versions
                            .contains(&(key.clone(), v.clone()))
                    });
                    if st.delete.refuse.contains(&key) || version_refused {
                        out.push_str(&format!(
                        "<Error><Key>{}</Key>{}<Code>AccessDenied</Code><Message>Access Denied</Message></Error>",
                        xml_escape(&key),
                        version
                            .as_ref()
                            .map(|v| format!("<VersionId>{}</VersionId>", v))
                            .unwrap_or_default()
                    ));
                    } else {
                        st.objects.remove(&key);
                        if !quiet {
                            out.push_str(&format!(
                                "<Deleted><Key>{}</Key></Deleted>",
                                xml_escape(&key)
                            ));
                        }
                    }
                }
                out.push_str("</DeleteResult>");
                (out, st.delete.respond_after)
            };
            if let Some(delay) = respond_after {
                tokio::time::sleep(delay).await;
            }
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .header("content-length", out.len().to_string())
                .body(Full::new(Bytes::from(out)))
                .unwrap());
        }

        if method == hyper::Method::POST && path == bucket_root && query.is_empty() {
            let key = mock_form_key(&body);
            let location = format!("http://{}/{}/{}", host, BUCKET, s3_form_encode(&key));
            let mut st = self.state.lock().unwrap();
            st.post_requests += 1;
            st.contents.remove(&key);
            *st.objects.entry(key).or_insert(0) += 1;
            return Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header("location", location)
                .body(Full::new(Bytes::new()))
                .unwrap());
        }

        if method == hyper::Method::PUT && query.is_empty() {
            let key = percent_decode(
                path.strip_prefix(&format!("{}/", bucket_root))
                    .unwrap_or(""),
            );
            let mut st = self.state.lock().unwrap();
            let generation = {
                let g = st.objects.entry(key.clone()).or_insert(0);
                *g += 1;
                *g
            };
            st.contents.insert(key, body);
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("etag", format!("\"etag-{}\"", generation))
                .header("content-length", "0")
                .body(Full::new(Bytes::new()))
                .unwrap());
        }

        if method == hyper::Method::GET || method == hyper::Method::HEAD {
            let mut st = self.state.lock().unwrap();
            *st.gets.entry(path.clone()).or_insert(0) += 1;
            let key = percent_decode(
                path.strip_prefix(&format!("{}/", bucket_root))
                    .unwrap_or(""),
            );
            let content = st.contents.get(&key).cloned();
            return Ok(match st.objects.get(&key) {
                Some(generation) => {
                    let data = content.unwrap_or_else(|| {
                        Bytes::from(format!("{}-generation-{}", key, generation))
                    });
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-length", data.len().to_string())
                        .header("etag", format!("\"etag-{}\"", generation))
                        .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                        .header("content-type", "application/octet-stream")
                        .body(Full::new(data))
                        .unwrap()
                }
                None => {
                    let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code><Message>gone</Message></Error>";
                    Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .header("content-type", "application/xml")
                        .header("content-length", xml.len().to_string())
                        .body(Full::new(Bytes::from_static(xml.as_bytes())))
                        .unwrap()
                }
            });
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
                .header("content-length", body.len().to_string());
            for (k, v) in headers {
                builder = builder.header(*k, v.as_str());
            }
            let resp = sender
                .send_request(builder.body(Full::new(body)).unwrap())
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

    async fn get(&self, encoded_path: &str) -> (StatusCode, Bytes) {
        self.send(
            "GET",
            &format!("/{}/{}", BUCKET, encoded_path),
            &[("authorization", AUTH.to_string())],
            Bytes::new(),
        )
        .await
    }

    /// GET until a repeat is answered without reaching the mock, then wait for the key's
    /// `.meta`: the entry is cached and consolidated.
    ///
    /// A hit alone is not a stable state. When the seed GET and the first repeat both miss,
    /// two fills of the key run. The repeat that hits can be served from a fill-visibility
    /// record while the other fill is still committing, and that record can be gone before
    /// consolidation writes the `.meta`, so a later GET misses.
    async fn cache(&self, encoded_path: &str) -> Bytes {
        let raw = format!("/{}/{}", BUCKET, encoded_path);
        let (status, first) = self.get(encoded_path).await;
        assert_eq!(status, StatusCode::OK, "seed GET {}", raw);
        for _ in 0..100 {
            let before = self.mock.gets(&raw);
            let (status, body) = self.get(encoded_path).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body, first);
            if self.mock.gets(&raw) == before {
                self.wait_for_meta(encoded_path).await;
                return first;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("{} never became a cache hit", raw);
    }

    /// Wait, bounded, for a `.meta` on disk whose `cache_key` names `encoded_path`.
    async fn wait_for_meta(&self, encoded_path: &str) {
        let want = percent_decode(&format!("{}/{}", BUCKET, encoded_path));
        let root = self._dir.path().join("metadata");
        let deadline = tokio::time::Instant::now() + STEP;
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
                        .is_some_and(|k| percent_decode(k) == want)
                });
            if found {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "/{}/{} was a cache hit but no .meta was written within {:?}",
                BUCKET,
                encoded_path,
                STEP
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn delete_objects(&self, keys: &[&str], quiet: bool) -> StatusCode {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        if quiet {
            xml.push_str("<Quiet>true</Quiet>");
        }
        for key in keys {
            xml.push_str(&format!("<Object><Key>{}</Key></Object>", xml_escape(key)));
        }
        xml.push_str("</Delete>");
        let (status, _) = self
            .send(
                "POST",
                &format!("/{}?delete", BUCKET),
                &[
                    ("authorization", AUTH.to_string()),
                    ("content-type", "application/xml".to_string()),
                ],
                Bytes::from(xml),
            )
            .await;
        status
    }

    async fn form_post(&self, key_field: &str, filename: &str, content: &str) -> StatusCode {
        let boundary = "----scn1boundary7MA4YWxkTrZu0gW";
        let mut body = String::new();
        for (name, value) in [
            ("key", key_field),
            ("acl", "private"),
            (
                "policy",
                "eyJleHBpcmF0aW9uIjoiMjAzMC0wMS0wMVQwMDowMDowMFoifQ==",
            ),
            ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
            ("x-amz-signature", "0000"),
        ] {
            body.push_str(&format!(
                "--{}\r\nContent-Disposition: form-data; name=\"{}\"\r\n\r\n{}\r\n",
                boundary, name, value
            ));
        }
        body.push_str(&format!(
            "--{}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{}\"\r\nContent-Type: application/octet-stream\r\n\r\n{}\r\n--{}--\r\n",
            boundary, filename, content, boundary
        ));
        let (status, _) = self
            .send(
                "POST",
                &format!("/{}", BUCKET),
                &[(
                    "content-type",
                    format!("multipart/form-data; boundary={}", boundary),
                )],
                Bytes::from(body),
            )
            .await;
        status
    }

    async fn delete_objects_xml(&self, xml: &str) -> StatusCode {
        self.delete_objects_xml_full(xml).await.0
    }

    async fn delete_objects_xml_full(&self, xml: &str) -> (StatusCode, Bytes) {
        self.send(
            "POST",
            &format!("/{}?delete", BUCKET),
            &[
                ("authorization", AUTH.to_string()),
                ("content-type", "application/xml".to_string()),
            ],
            Bytes::from(xml.to_string()),
        )
        .await
    }

    fn delete_requests(&self) -> usize {
        self.mock.state.lock().unwrap().delete_requests
    }

    /// Whether a GET of `encoded_path` reached the mock (a miss) and what it returned.
    async fn probe(&self, encoded_path: &str) -> (bool, StatusCode, Bytes) {
        let raw = format!("/{}/{}", BUCKET, encoded_path);
        let before = self.mock.gets(&raw);
        let (status, body) = self.get(encoded_path).await;
        (self.mock.gets(&raw) > before, status, body)
    }
}

// ── Red side 1: DeleteObjects, non-quiet and quiet ──────────────────────────

async fn delete_objects_invalidates(quiet: bool) {
    let h = harness().await;
    h.mock.put("docs/report.txt");
    h.mock.put("docs/other.txt");
    h.cache("docs/report.txt").await;
    h.cache("docs/other.txt").await;

    let status = h
        .delete_objects(&["docs/report.txt", "docs/other.txt"], quiet)
        .await;
    assert_eq!(status, StatusCode::OK);

    for path in ["docs/report.txt", "docs/other.txt"] {
        let (reached, status, body) = h.probe(path).await;
        assert!(
            reached && status == StatusCode::NOT_FOUND,
            "quiet={}: GET {} after DeleteObjects was served from cache: status={} body={:?}",
            quiet,
            path,
            status,
            String::from_utf8_lossy(&body)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_non_quiet_invalidates_every_deleted_key() {
    delete_objects_invalidates(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_quiet_invalidates_every_deleted_key() {
    delete_objects_invalidates(true).await;
}

// ── Red side 2: keys whose request paths encode differently ────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_invalidates_every_request_encoding_of_a_key() {
    let h = harness().await;
    let key = "dir/a b+c!(1)ü%.txt";
    h.mock.put(key);
    // Three spellings real clients send for the same object: everything outside the
    // unreserved set encoded (AWS SDKs and CLI), space as `+` with sub-delims left
    // literal (S3 reads a literal `+` as a space), and lowercase hex.
    let encodings = [
        "dir/a%20b%2Bc%21%281%29%C3%BC%25.txt",
        "dir/a+b%2Bc!(1)%C3%BC%25.txt",
        "dir/a%20b%2bc%21%281%29%c3%bc%25.txt",
    ];
    for encoded in encodings {
        h.cache(encoded).await;
    }

    assert_eq!(h.delete_objects(&[key], false).await, StatusCode::OK);

    for encoded in encodings {
        let (reached, status, body) = h.probe(encoded).await;
        assert!(
            reached && status == StatusCode::NOT_FOUND,
            "GET {} after DeleteObjects of {:?} was served from cache: status={} body={:?}",
            encoded,
            key,
            status,
            String::from_utf8_lossy(&body)
        );
    }
}

// ── Red side 3: form POST overwrite ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn form_post_overwrite_invalidates_the_named_key() {
    let h = harness().await;
    h.mock.put("uploads/plain.txt");
    let old = h.cache("uploads/plain.txt").await;

    assert_eq!(
        h.form_post("uploads/plain.txt", "local.txt", "new-bytes")
            .await,
        StatusCode::NO_CONTENT
    );

    let (reached, status, body) = h.probe("uploads/plain.txt").await;
    assert!(
        reached && status == StatusCode::OK && body != old,
        "GET after form POST overwrite was served from cache: status={} body={:?}",
        status,
        String::from_utf8_lossy(&body)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn form_post_with_filename_variable_invalidates_the_resolved_key() {
    let h = harness().await;
    h.mock.put("uploads/photo 1.jpg");
    let old = h.cache("uploads/photo%201.jpg").await;

    assert_eq!(
        h.form_post("uploads/${filename}", "photo 1.jpg", "new-bytes")
            .await,
        StatusCode::NO_CONTENT
    );

    let (reached, status, body) = h.probe("uploads/photo%201.jpg").await;
    assert!(
        reached && status == StatusCode::OK && body != old,
        "GET after a ${{filename}} form POST was served from cache: status={} body={:?}",
        status,
        String::from_utf8_lossy(&body)
    );
}

// ── Red side 4: a per-key <Error> keeps that key cached ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_keeps_a_refused_key_and_invalidates_the_rest() {
    for quiet in [false, true] {
        let h = harness().await;
        h.mock.put("keep/refused.txt");
        h.mock.put("drop/deleted.txt");
        let kept = h.cache("keep/refused.txt").await;
        h.cache("drop/deleted.txt").await;
        h.mock.set_delete(DeleteBehaviour {
            refuse: ["keep/refused.txt".to_string()].into_iter().collect(),
            ..Default::default()
        });

        assert_eq!(
            h.delete_objects(&["keep/refused.txt", "drop/deleted.txt"], quiet)
                .await,
            StatusCode::OK
        );

        let (reached, status, body) = h.probe("drop/deleted.txt").await;
        assert!(
            reached && status == StatusCode::NOT_FOUND,
            "quiet={}: the deleted key was served from cache: status={} body={:?}",
            quiet,
            status,
            String::from_utf8_lossy(&body)
        );
        let (reached, status, body) = h.probe("keep/refused.txt").await;
        assert!(
            !reached && status == StatusCode::OK && body == kept,
            "quiet={}: the refused key must stay cached: reached={} status={}",
            quiet,
            reached,
            status
        );
    }
}

// ── red side 1: a write through one spelling reaches another spelling's entry ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_through_one_spelling_replaces_the_object_cached_under_another() {
    let h = harness().await;
    h.mock.put("sp/a b+c.txt");
    // Cached through the space-as-`+` spelling a non-SDK client sends.
    let old = h.cache("sp/a+b%2Bc.txt").await;

    let (status, _) = h
        .send(
            "PUT",
            &format!("/{}/sp/a%20b%2Bc.txt", BUCKET),
            &[],
            Bytes::from_static(b"new-bytes"),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (_, status, body) = h.probe("sp/a+b%2Bc.txt").await;
    assert!(
        status == StatusCode::OK && body != old,
        "GET sp/a+b%2Bc.txt after a PUT through sp/a%20b%2Bc.txt was served the old object: status={} body={:?}",
        status,
        String::from_utf8_lossy(&body)
    );
}

// ── red side 2: spellings of different objects never share an entry ────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_s3_reads_as_different_objects_stay_different() {
    let h = harness().await;
    h.mock.put("nc/a+b");
    h.mock.put("nc/a b");
    let plus = h.cache("nc/a%2Bb").await;
    let space = h.cache("nc/a%20b").await;
    assert_ne!(plus, space);
    for (path, want) in [
        ("nc/a%2Bb", &plus),
        ("nc/a%2bb", &plus),
        ("nc/a%20b", &space),
        ("nc/a+b", &space),
    ] {
        let (_, status, body) = h.probe(path).await;
        assert!(
            status == StatusCode::OK && body == *want,
            "GET {} returned another object's bytes: status={} body={:?} want={:?}",
            path,
            status,
            String::from_utf8_lossy(&body),
            String::from_utf8_lossy(want)
        );
    }
}

// ── red side 3: an unreadable DeleteObjects request ───────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_unreadable_request_invalidates_what_it_names_or_is_refused() {
    let h = harness().await;
    h.mock.put("un/commented.txt");
    h.cache("un/commented.txt").await;

    let status = h
        .delete_objects_xml(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Delete><Quiet>true</Quiet><!-- client note --><Object><Key>un/commented.txt</Key></Object></Delete>",
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (reached, status, body) = h.probe("un/commented.txt").await;
    assert!(
        reached && status == StatusCode::NOT_FOUND,
        "quiet DeleteObjects with a comment: the deleted key was served from cache: status={} body={:?}",
        status,
        String::from_utf8_lossy(&body)
    );

    let before = h.delete_requests();
    let (status, body) = h
        .delete_objects_xml_full(
            // The key is hidden in an entity: no `Key` element is visible to the proxy.
            "<!DOCTYPE d [<!ENTITY k \"<Key>secret</Key>\">]><Delete><Quiet>true</Quiet><Object>&k;</Object></Delete>",
        )
        .await;
    assert!(
        status == StatusCode::BAD_REQUEST && h.delete_requests() == before,
        "a DeleteObjects body naming no readable key was forwarded: status={} mock_requests={}",
        status,
        h.delete_requests() - before
    );
    assert!(
        String::from_utf8_lossy(&body).contains("MalformedXML"),
        "refusal is not MalformedXML: {:?}",
        String::from_utf8_lossy(&body)
    );
}

// ── red side 4: a versioned <Error> does not exempt the key ───

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_version_error_does_not_exempt_the_deleted_version() {
    let h = harness().await;
    h.mock.put("ver/k.txt");
    h.cache("ver/k.txt").await;
    h.mock.set_delete(DeleteBehaviour {
        refuse_versions: [("ver/k.txt".to_string(), "v-locked".to_string())]
            .into_iter()
            .collect(),
        ..Default::default()
    });

    let status = h
        .delete_objects_xml(
            "<Delete><Quiet>true</Quiet><Object><Key>ver/k.txt</Key><VersionId>v-current</VersionId></Object><Object><Key>ver/k.txt</Key><VersionId>v-locked</VersionId></Object></Delete>",
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (reached, status, body) = h.probe("ver/k.txt").await;
    assert!(
        reached && status == StatusCode::NOT_FOUND,
        "a refused version exempted the key's deleted version: served from cache: status={} body={:?}",
        status,
        String::from_utf8_lossy(&body)
    );
}

// ── red side 5: a client disconnect does not cancel the work ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_invalidates_after_the_client_disconnects() {
    use tokio::io::AsyncWriteExt;

    let h = harness().await;
    h.mock.put("dc/a.txt");
    h.mock.put("dc/b.txt");
    h.cache("dc/a.txt").await;
    h.cache("dc/b.txt").await;
    h.mock.set_delete(DeleteBehaviour {
        respond_after: Some(Duration::from_millis(500)),
        ..Default::default()
    });

    let xml = "<Delete><Quiet>true</Quiet><Object><Key>dc/a.txt</Key></Object><Object><Key>dc/b.txt</Key></Object></Delete>";
    let request = format!(
        "POST /{}?delete HTTP/1.1\r\nhost: {}\r\nauthorization: {}\r\ncontent-type: application/xml\r\ncontent-length: {}\r\n\r\n{}",
        BUCKET,
        h.mock_host,
        AUTH,
        xml.len(),
        xml
    );
    let before = h.delete_requests();
    let mut stream = TcpStream::connect(h.proxy).await.expect("connect");
    stream.write_all(request.as_bytes()).await.expect("send");
    tokio::time::timeout(STEP, async {
        while h.delete_requests() == before {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the DeleteObjects never reached the mock");
    // S3 has deleted; the client goes away before the answer.
    drop(stream);
    tokio::time::sleep(Duration::from_millis(1500)).await;

    for path in ["dc/a.txt", "dc/b.txt"] {
        let (reached, status, body) = h.probe(path).await;
        assert!(
            reached && status == StatusCode::NOT_FOUND,
            "GET {} after a DeleteObjects whose client disconnected was served from cache: status={} body={:?}",
            path,
            status,
            String::from_utf8_lossy(&body)
        );
    }
}

// ── red side 6: a form POST whose key field cannot be read ────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn form_post_with_unreadable_fields_invalidates_the_object_s3_names() {
    let h = harness().await;
    h.mock.put("big/form.txt");
    let old = h.cache("big/form.txt").await;

    // A field ahead of `key` larger than the proxy reads (1 MiB): the key is unreadable.
    let boundary = "----scn2boundary";
    let pad = "p".repeat((1 << 20) + 4096);
    let body = format!(
        "--{b}\r\nContent-Disposition: form-data; name=\"x-amz-meta-pad\"\r\n\r\n{pad}\r\n\
         --{b}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nbig/form.txt\r\n\
         --{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"f.txt\"\r\nContent-Type: text/plain\r\n\r\nnew-bytes\r\n--{b}--\r\n",
        b = boundary,
        pad = pad
    );
    let (status, _) = h
        .send(
            "POST",
            &format!("/{}", BUCKET),
            &[(
                "content-type",
                format!("multipart/form-data; boundary={}", boundary),
            )],
            Bytes::from(body),
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (reached, status, body) = h.probe("big/form.txt").await;
    assert!(
        reached && status == StatusCode::OK && body != old,
        "GET after a form POST with unreadable fields was served from cache: status={} body={:?}",
        status,
        String::from_utf8_lossy(&body)
    );
}

// ── A versioned <Error> exempts no key whose other version was deleted ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_commented_body_version_error_does_not_exempt_the_deleted_version() {
    let h = harness().await;
    h.mock.put("vl/k.txt");
    h.cache("vl/k.txt").await;
    h.mock.set_delete(DeleteBehaviour {
        refuse_versions: [("vl/k.txt".to_string(), "v-locked".to_string())]
            .into_iter()
            .collect(),
        ..Default::default()
    });

    // The comment sits between elements: the one reader skips it and reads both entries.
    let status = h
        .delete_objects_xml(
            "<Delete><Quiet>true</Quiet><!-- note --><Object><Key>vl/k.txt</Key><VersionId>v-current</VersionId></Object><Object><Key>vl/k.txt</Key><VersionId>v-locked</VersionId></Object></Delete>",
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (reached, status, body) = h.probe("vl/k.txt").await;
    assert!(
        reached && status == StatusCode::NOT_FOUND,
        "DeleteObjects: a refused version exempted the key's deleted version: served from cache: status={} body={:?}",
        status,
        String::from_utf8_lossy(&body)
    );
}

// ── A CDATA key is refused ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_cdata_key_is_refused_not_misread() {
    let h = harness().await;
    h.mock.put("cd/a");
    h.mock.put("cd/a</Key>b");
    let a = h.cache("cd/a").await;
    let ab = h.cache("cd/a%3C%2FKey%3Eb").await;

    let before = h.delete_requests();
    let (status, refusal) = h
        .delete_objects_xml_full(
            "<Delete><Quiet>true</Quiet><!--x--><Object><Key><![CDATA[cd/a</Key>b]]></Key></Object></Delete>",
        )
        .await;
    assert!(
        status == StatusCode::BAD_REQUEST && h.delete_requests() == before,
        "a DeleteObjects body with a CDATA key was forwarded: status={} mock_requests={}",
        status,
        h.delete_requests() - before
    );
    assert!(
        String::from_utf8_lossy(&refusal).contains("MalformedXML"),
        "refusal is not MalformedXML: {:?}",
        String::from_utf8_lossy(&refusal)
    );
    // Nothing was deleted, so both copies stay served from cache.
    for (path, want) in [("cd/a", &a), ("cd/a%3C%2FKey%3Eb", &ab)] {
        let (reached, status, body) = h.probe(path).await;
        assert!(
            !reached && status == StatusCode::OK && body == *want,
            "GET {} after a refused DeleteObjects: reached={} status={}",
            path,
            reached,
            status
        );
    }
}

// ── a processing instruction inside a key is refused ──
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_pi_key_is_refused_not_misread() {
    let h = harness().await;
    h.mock.put("pi/ab");
    let ab = h.cache("pi/ab").await;
    let before = h.delete_requests();
    let (status, refusal) = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        h.delete_objects_xml_full(
            "<Delete><Quiet>true</Quiet><!--x--><Object><Key>pi/a<?p?>b</Key></Object></Delete>",
        ),
    )
    .await
    .expect("DeleteObjects timed out");
    assert!(
        status == StatusCode::BAD_REQUEST && h.delete_requests() == before,
        "a DeleteObjects body with a processing instruction in a key was forwarded: status={} mock_requests={}",
        status,
        h.delete_requests() - before
    );
    assert!(
        String::from_utf8_lossy(&refusal).contains("MalformedXML"),
        "refusal is not MalformedXML: {:?}",
        String::from_utf8_lossy(&refusal)
    );
    let (reached, status, body) = h.probe("pi/ab").await;
    assert!(
        !reached && status == StatusCode::OK && body == ab,
        "GET pi/ab after a refused DeleteObjects: reached={} status={}",
        reached,
        status
    );
}
// ── a body the reader cannot finish is refused, never read as a partial key list ──
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_unreadable_body_is_refused_not_partial() {
    let h = harness().await;
    h.mock.put("pq/z");
    h.mock.put("pq/ab");
    let z = h.cache("pq/z").await;
    let ab = h.cache("pq/ab").await;
    let before = h.delete_requests();
    // The PI's quote never closes, so the reader cannot tell where `pq/ab` begins;
    // the keys found so far are only `pq/z`.
    let (status, refusal) = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        h.delete_objects_xml_full(
            "<Delete><Quiet>true</Quiet><!--x--><Object><Key>pq/z</Key></Object><Object><?p \"?><Key>pq/ab</Key></Object></Delete>",
        ),
    )
    .await
    .expect("DeleteObjects timed out");
    assert!(
        status == StatusCode::BAD_REQUEST && h.delete_requests() == before,
        "a DeleteObjects body the reader could not finish was forwarded: status={} mock_requests={}",
        status,
        h.delete_requests() - before
    );
    assert!(
        String::from_utf8_lossy(&refusal).contains("MalformedXML"),
        "refusal is not MalformedXML: {:?}",
        String::from_utf8_lossy(&refusal)
    );
    for (path, want) in [("pq/z", &z), ("pq/ab", &ab)] {
        let (reached, status, body) = h.probe(path).await;
        assert!(
            !reached && status == StatusCode::OK && body == *want,
            "GET {} after a refused DeleteObjects: reached={} status={}",
            path,
            reached,
            status
        );
    }
}

// ── processing instructions other than a leading declaration are refused ──
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_two_processing_instructions_are_refused() {
    let h = harness().await;
    h.mock.put("pj/z");
    h.mock.put("pj/ab");
    h.mock.put("pj/cd");
    let z = h.cache("pj/z").await;
    let ab = h.cache("pj/ab").await;
    let cd = h.cache("pj/cd").await;
    let before = h.delete_requests();
    // Quotes in the two instructions pair with each other and would hide `pj/ab`.
    let (status, refusal) = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        h.delete_objects_xml_full(
            "<Delete><Quiet>true</Quiet><!--x--><Object><Key>pj/z</Key></Object><Object><?p \"?><Key>pj/ab</Key></Object><Object><?q \"?><Key>pj/cd</Key></Object></Delete>",
        ),
    )
    .await
    .expect("DeleteObjects timed out");
    assert!(
        status == StatusCode::BAD_REQUEST && h.delete_requests() == before,
        "a DeleteObjects body with processing instructions was forwarded: status={} mock_requests={}",
        status,
        h.delete_requests() - before
    );
    assert!(
        String::from_utf8_lossy(&refusal).contains("MalformedXML"),
        "refusal is not MalformedXML: {:?}",
        String::from_utf8_lossy(&refusal)
    );
    for (path, want) in [("pj/z", &z), ("pj/ab", &ab), ("pj/cd", &cd)] {
        let (reached, status, body) = h.probe(path).await;
        assert!(
            !reached && status == StatusCode::OK && body == *want,
            "GET {} after a refused DeleteObjects: reached={} status={}",
            path,
            reached,
            status
        );
    }
}
// ── the one strict reader refuses a DOCTYPE literal that hides keys ──
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_doctype_literal_hiding_keys_is_refused() {
    let h = harness().await;
    h.mock.put("w3/ab");
    h.mock.put("w3/cd");
    let ab = h.cache("w3/ab").await;
    let cd = h.cache("w3/cd").await;
    let before = h.delete_requests();
    // The review's body: a `<!--` inside the DOCTYPE literal, two PIs whose quotes pair.
    let (status, refusal) = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        h.delete_objects_xml_full(
            "<!DOCTYPE Delete SYSTEM \"<!--\"><Delete><Object><?p '?><Key>w3/ab</Key></Object><Object><?q '?><Key>w3/cd</Key></Object><!-- x -->\"></Delete>",
        ),
    )
    .await
    .expect("DeleteObjects timed out");
    assert!(
        status == StatusCode::BAD_REQUEST && h.delete_requests() == before,
        "a DOCTYPE-literal DeleteObjects body was forwarded: status={} mock_requests={}",
        status,
        h.delete_requests() - before
    );
    assert!(
        String::from_utf8_lossy(&refusal).contains("MalformedXML"),
        "refusal is not MalformedXML: {:?}",
        String::from_utf8_lossy(&refusal)
    );
    for (path, want) in [("w3/ab", &ab), ("w3/cd", &cd)] {
        let (reached, status, body) = h.probe(path).await;
        assert!(
            !reached && status == StatusCode::OK && body == *want,
            "GET {} after a refused DeleteObjects: reached={} status={}",
            path,
            reached,
            status
        );
    }
}

// ── regression pins for the one strict reader ────────────────────────
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_declaration_and_comments_are_forwarded_and_invalidated() {
    let h = harness().await;
    for k in ["w3p/a", "w3p/b", "w3p/c"] {
        h.mock.put(k);
        h.cache(k).await;
    }
    let status = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        h.delete_objects_xml(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!-- lead --><Delete><Quiet>true</Quiet><Object><Key>w3p/a</Key></Object><!-- between --><Object><Key>w3p/b</Key></Object><Object><Key>w3p/c</Key></Object></Delete>",
        ),
    )
    .await
    .expect("DeleteObjects timed out");
    assert_eq!(status, StatusCode::OK);
    for k in ["w3p/a", "w3p/b", "w3p/c"] {
        let (reached, status, body) = h.probe(k).await;
        assert!(
            reached && status == StatusCode::NOT_FOUND,
            "{} served from cache after a forwarded DeleteObjects: status={} body={:?}",
            k,
            status,
            String::from_utf8_lossy(&body)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_bodies_the_reader_cannot_read_exactly_are_refused() {
    let h = harness().await;
    h.mock.put("w3r/k");
    let k = h.cache("w3r/k").await;
    let bodies = [
        (
            "DOCTYPE",
            "<!DOCTYPE d><Delete><Object><Key>w3r/k</Key></Object></Delete>",
        ),
        (
            "second PI",
            "<?xml version=\"1.0\"?><?p?><Delete><Object><Key>w3r/k</Key></Object></Delete>",
        ),
        (
            "CDATA",
            "<Delete><Object><Key><![CDATA[w3r/k]]></Key></Object></Delete>",
        ),
        (
            "unknown entity",
            "<Delete><Object><Key>w3r/&custom;</Key></Object></Delete>",
        ),
        (
            "double hyphen in comment",
            "<Delete><!-- a -- b --><Object><Key>w3r/k</Key></Object></Delete>",
        ),
        (
            "unterminated comment",
            "<Delete><Object><Key>w3r/k</Key></Object><!-- never closed</Delete>",
        ),
        (
            "comment inside a key",
            "<Delete><Object><Key>w3r/<!--x-->k</Key></Object></Delete>",
        ),
    ];
    for (what, body) in bodies {
        let before = h.delete_requests();
        let (status, refusal) = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            h.delete_objects_xml_full(body),
        )
        .await
        .expect("DeleteObjects timed out");
        assert!(
            status == StatusCode::BAD_REQUEST && h.delete_requests() == before,
            "{}: forwarded: status={} mock_requests={}",
            what,
            status,
            h.delete_requests() - before
        );
        assert!(
            String::from_utf8_lossy(&refusal).contains("MalformedXML"),
            "{}: refusal is not MalformedXML: {:?}",
            what,
            String::from_utf8_lossy(&refusal)
        );
    }
    let (reached, status, body) = h.probe("w3r/k").await;
    assert!(
        !reached && status == StatusCode::OK && body == k,
        "GET w3r/k after refused DeleteObjects: reached={} status={}",
        reached,
        status
    );
}

// ── The Location fallback reads S3's form-encoded key ──────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn form_post_with_unreadable_fields_reads_the_location_s3_writes() {
    let h = harness().await;
    for key in ["fl/a+b.txt", "fl/a b.txt"] {
        h.mock.put(key);
    }
    let plus = h.cache("fl/a%2Bb.txt").await;
    let space = h.cache("fl/a%20b.txt").await;

    // A field ahead of `key` larger than the proxy reads: the key is unreadable, and
    // the mock answers `Location:.../scn1-bucket/fl%2Fa%2Bb.txt` as S3 does.
    let boundary = "----scn3boundary";
    let pad = "p".repeat((1 << 20) + 4096);
    let body = format!(
        "--{b}\r\nContent-Disposition: form-data; name=\"x-amz-meta-pad\"\r\n\r\n{pad}\r\n\
         --{b}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nfl/a+b.txt\r\n\
         --{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"f.txt\"\r\nContent-Type: text/plain\r\n\r\nnew-bytes\r\n--{b}--\r\n",
        b = boundary,
        pad = pad
    );
    let (status, _) = h
        .send(
            "POST",
            &format!("/{}", BUCKET),
            &[(
                "content-type",
                format!("multipart/form-data; boundary={}", boundary),
            )],
            Bytes::from(body),
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (reached, status, body) = h.probe("fl/a%2Bb.txt").await;
    assert!(
        reached && status == StatusCode::OK && body != plus,
        "GET fl/a%2Bb.txt after a form POST of `fl/a+b.txt` with unreadable fields was served from cache: status={} body={:?}",
        status,
        String::from_utf8_lossy(&body)
    );
    let (reached, status, body) = h.probe("fl/a%20b.txt").await;
    assert!(
        !reached && status == StatusCode::OK && body == space,
        "the form POST of `fl/a+b.txt` invalidated `fl/a b.txt`: reached={} status={}",
        reached,
        status
    );
}

// ── A declaration that is not version 1.0 in UTF-8 is refused ──────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_objects_non_utf8_declaration_is_refused_not_misread() {
    let h = harness().await;
    h.mock.put("fx/caf\u{e9}");
    let cached = h.cache("fx/caf%C3%A9").await;
    let before = h.delete_requests();
    // The declaration says ISO-8859-1; the proxy would read the key as UTF-8.
    let (status, refusal) = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        h.delete_objects_xml_full(
            "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><Delete><Quiet>true</Quiet><Object><Key>fx/caf\u{e9}</Key></Object></Delete>",
        ),
    )
    .await
    .expect("DeleteObjects timed out");
    assert!(
        status == StatusCode::BAD_REQUEST && h.delete_requests() == before,
        "a DeleteObjects body declaring ISO-8859-1 was forwarded: status={} mock_requests={}",
        status,
        h.delete_requests() - before
    );
    assert!(
        String::from_utf8_lossy(&refusal).contains("MalformedXML"),
        "refusal is not MalformedXML: {:?}",
        String::from_utf8_lossy(&refusal)
    );
    let (reached, status, body) = h.probe("fx/caf%C3%A9").await;
    assert!(
        !reached && status == StatusCode::OK && body == cached,
        "GET fx/caf%C3%A9 after a refused DeleteObjects: reached={} status={}",
        reached,
        status
    );
}
