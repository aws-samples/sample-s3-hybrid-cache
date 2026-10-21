//! The proxy's internal headers never reach S3 on a write.
//!
//! The proxy marks preconditions it adds, and a Range it synthesizes, with three internal
//! headers (`x-proxy-injected-if-match`, `x-proxy-injected-if-unmodified-since`,
//! `x-proxy-synthesized-full-object-range`). A client never sends them. The GET/HEAD path and
//! the request-context builders strip them, but the write handlers forward the client's own
//! header map without passing through either, so a client-sent one reached S3 on every write
//! method. Each test here sends all three on one write method through the real
//! `HttpProxy::handle_request` to a loopback mock S3 over a plaintext upstream override, and
//! asserts the mock saw none of them, while an ordinary client header (`x-client-marker`)
//! still arrives, so the assertion cannot pass on a request that never reached the mock.
#![cfg(unix)]

use std::collections::HashMap;
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

const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000"; // gitleaks:allow

const SENTINELS: [&str; 3] = [
    "x-proxy-injected-if-match",
    "x-proxy-injected-if-unmodified-since",
    "x-proxy-synthesized-full-object-range",
];

const STEP: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
struct Seen {
    method: hyper::Method,
    target: String,
    headers: HashMap<String, String>,
}

#[derive(Clone, Default)]
struct MockS3 {
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl MockS3 {
    async fn handle(self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let target = req.uri().to_string();
        let query = req.uri().query().unwrap_or("").to_string();
        let key = req.uri().path().trim_start_matches('/').to_string();
        let headers: HashMap<String, String> = req
            .headers()
            .iter()
            .filter_map(|(k, v)| {
                v.to_str()
                    .ok()
                    .map(|v| (k.as_str().to_string(), v.to_string()))
            })
            .collect();
        let _ = req.into_body().collect().await?;
        self.seen.lock().unwrap().push(Seen {
            method: method.clone(),
            target,
            headers,
        });
        let has = |p: &str| {
            query
                .split('&')
                .any(|q| q == p || q.starts_with(&format!("{p}=")))
        };
        let xml = |body: String| {
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .header("content-length", body.len().to_string())
                .body(Full::new(Bytes::from(body)))
                .unwrap()
        };
        if method == hyper::Method::POST && has("uploads") {
            return Ok(xml(format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult><Key>{key}</Key><UploadId>scn1-upload</UploadId></InitiateMultipartUploadResult>"
            )));
        }
        if method == hyper::Method::POST && has("uploadId") {
            return Ok(xml(format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult><Key>{key}</Key><ETag>\"scn1-mpu-2\"</ETag></CompleteMultipartUploadResult>"
            )));
        }
        if method == hyper::Method::POST && has("delete") {
            return Ok(xml(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult><Deleted><Key>scn1/a</Key></Deleted></DeleteResult>".to_string(),
            ));
        }
        let status = match method {
            hyper::Method::PUT => StatusCode::OK,
            _ => StatusCode::NO_CONTENT,
        };
        Ok(Response::builder()
            .status(status)
            .header("etag", "\"scn1-etag\"")
            .header("content-length", "0")
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
    /// Send one write through the proxy carrying all three internal headers and an ordinary
    /// client header, then assert the mock received the request with the client header and
    /// none of the three.
    async fn assert_write_strips(
        &self,
        what: &str,
        method: &str,
        target: &str,
        signed: bool,
        extra: &[(&str, &str)],
        body: &'static [u8],
    ) {
        let fut = async {
            let stream = TcpStream::connect(self.proxy).await.expect("connect");
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .expect("handshake");
            tokio::spawn(async move {
                let _ = conn.await;
            });
            let mut req = Request::builder()
                .method(method)
                .uri(target)
                .header("host", self.mock_host.as_str())
                .header("x-client-marker", "scn1")
                .header("content-length", body.len().to_string());
            if signed {
                req = req.header("authorization", AUTH);
            }
            for name in SENTINELS {
                req = req.header(name, "1");
            }
            for (name, value) in extra {
                req = req.header(*name, *value);
            }
            let req = req.body(Full::new(Bytes::from_static(body))).unwrap();
            let resp = sender.send_request(req).await.expect("proxy request");
            let status = resp.status();
            let _ = resp.into_body().collect().await;
            status
        };
        let status = tokio::time::timeout(STEP, fut)
            .await
            .unwrap_or_else(|_| panic!("{what}: timed out"));
        let seen = self.mock.seen.lock().unwrap().clone();
        let forwarded: Vec<&Seen> = seen
            .iter()
            .filter(|s| {
                s.method.as_str() == method
                    && s.headers.get("x-client-marker").map(String::as_str) == Some("scn1")
            })
            .collect();
        assert!(
            !forwarded.is_empty(),
            "{what}: the write never reached the mock (proxy answered {status}); mock saw {seen:?}"
        );
        for request in forwarded {
            for name in SENTINELS {
                assert!(
                    !request.headers.contains_key(name),
                    "{what}: the proxy's internal header {name} reached S3 on {} {}: {:?}",
                    request.method,
                    request.target,
                    request.headers
                );
            }
        }
    }
}

#[tokio::test]
async fn signed_put_forwards_no_internal_header() {
    let h = harness().await;
    h.assert_write_strips("signed PUT", "PUT", "/scn1/signed-put", true, &[], b"hello")
        .await;
}

#[tokio::test]
async fn unsigned_put_forwards_no_internal_header() {
    let h = harness().await;
    h.assert_write_strips(
        "unsigned PUT",
        "PUT",
        "/scn1/unsigned-put",
        false,
        &[],
        b"hello",
    )
    .await;
}

#[tokio::test]
async fn upload_part_forwards_no_internal_header() {
    let h = harness().await;
    h.assert_write_strips(
        "UploadPart",
        "PUT",
        "/scn1/mpu?partNumber=1&uploadId=scn1-upload",
        true,
        &[],
        b"part-one",
    )
    .await;
}

#[tokio::test]
async fn create_multipart_upload_forwards_no_internal_header() {
    let h = harness().await;
    h.assert_write_strips(
        "CreateMultipartUpload",
        "POST",
        "/scn1/mpu?uploads",
        true,
        &[],
        b"",
    )
    .await;
}

#[tokio::test]
async fn complete_multipart_upload_forwards_no_internal_header() {
    let h = harness().await;
    h.assert_write_strips(
        "CompleteMultipartUpload",
        "POST",
        "/scn1/mpu?uploadId=scn1-upload",
        true,
        &[("content-type", "application/xml")],
        b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"scn1-etag\"</ETag></Part></CompleteMultipartUpload>",
    )
    .await;
}

#[tokio::test]
async fn delete_object_forwards_no_internal_header() {
    let h = harness().await;
    h.assert_write_strips("DELETE", "DELETE", "/scn1/deleted", true, &[], b"")
        .await;
}

#[tokio::test]
async fn delete_objects_forwards_no_internal_header() {
    let h = harness().await;
    h.assert_write_strips(
        "DeleteObjects",
        "POST",
        "/scn1?delete",
        true,
        &[("content-type", "application/xml")],
        b"<Delete><Object><Key>a</Key></Object></Delete>",
    )
    .await;
}

#[tokio::test]
async fn form_post_forwards_no_internal_header() {
    let h = harness().await;
    h.assert_write_strips(
        "form POST",
        "POST",
        "/scn1",
        false,
        &[("content-type", "multipart/form-data; boundary=scn1")],
        b"--scn1\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nform-key\r\n--scn1\r\nContent-Disposition: form-data; name=\"file\"; filename=\"f\"\r\n\r\nbody\r\n--scn1--\r\n",
    )
    .await;
}
