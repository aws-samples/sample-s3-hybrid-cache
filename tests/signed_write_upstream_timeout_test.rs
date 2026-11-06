//! A signed write whose upstream stops answering ends within
//! `connection_pool.upstream_write_idle_timeout` and releases what the attempt held:
//! the in-flight ledger reservation, the request permit and any write-cache staging.
//! Also pins whether hyper drops a service future when its client disconnects.
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, StatusCode};
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

const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000"; // gitleaks:allow
const IDLE: Duration = Duration::from_millis(500);
const MAX_REQUESTS: usize = 8;
const TEST_BOUND: Duration = Duration::from_secs(30);

/// An origin that reads each request's head and `Content-Length` body. It answers
/// `200` when the request line names `answered`, and otherwise never answers.
async fn origin() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut seen = Vec::new();
                let mut byte = [0u8; 1];
                while !seen.ends_with(b"\r\n\r\n") {
                    if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                        return;
                    }
                    seen.push(byte[0]);
                }
                let head = String::from_utf8_lossy(&seen).to_ascii_lowercase();
                let len = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                let mut body = vec![0u8; len];
                let _ = stream.read_exact(&mut body).await;
                if head.lines().next().is_some_and(|l| l.contains("answered")) {
                    let _ = stream
                        .write_all(b"HTTP/1.1 200 OK\r\nETag: \"0123456789abcdef0123456789abcdef\"\r\nContent-Length: 0\r\n\r\n")
                        .await;
                }
                let _held = stream;
                std::future::pending::<()>().await;
            });
        }
    });
    addr
}

struct Proxy {
    _shutdown: oneshot::Sender<()>,
    addr: SocketAddr,
    dir: TempDir,
    origin_host: String,
    semaphore: Arc<Semaphore>,
    ledger: Arc<InflightLedger>,
}

async fn proxy() -> Proxy {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let origin = origin().await;
    let origin_host = format!("127.0.0.1:{}", origin.port());
    let dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.cache.cache_dir = dir.path().to_path_buf();
    config.cache.ram_cache_enabled = false;
    config.cache.write_cache_enabled = true;
    config.cache.read_cache_enabled = true;
    config.cache.download_coordination.enabled = false;
    config.connection_pool.upstream_write_idle_timeout = IDLE;
    config.connection_pool.upstream_overrides.insert(
        origin_host.clone(),
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
    let ledger = Arc::new(InflightLedger::new(64 * 1024 * 1024));
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
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let request_semaphore = Arc::new(Semaphore::new(MAX_REQUESTS));
    let semaphore = Arc::clone(&request_semaphore);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let addr = listener.local_addr().unwrap();
    let (shutdown, mut rx) = oneshot::channel::<()>();
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
    Proxy {
        _shutdown: shutdown,
        addr,
        dir,
        origin_host,
        semaphore,
        ledger,
    }
}

/// Send one signed request through the proxy and return its status once the body is read.
async fn send(p: &Proxy, method: &str, path_and_query: &str, body: Vec<u8>) -> StatusCode {
    let fut = async {
        let stream = TcpStream::connect(p.addr).await.expect("connect");
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .expect("handshake");
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let req = Request::builder()
            .method(method)
            .uri(path_and_query)
            .header("host", p.origin_host.as_str())
            .header("authorization", AUTH)
            .header("x-amz-date", "20260101T000000Z")
            .header("content-length", body.len().to_string())
            .body(Full::new(Bytes::from(body)))
            .unwrap();
        let resp = sender.send_request(req).await.expect("proxy request");
        let status = resp.status();
        let _ = resp.into_body().collect().await;
        status
    };
    tokio::time::timeout(TEST_BOUND, fut)
        .await
        .expect("the proxy must answer within the test bound")
}

async fn eventually(mut condition: impl FnMut() -> bool) -> bool {
    for _ in 0..150 {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    condition()
}

fn files_ending(root: &Path, suffix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.to_string_lossy().ends_with(suffix) {
                out.push(path.to_string_lossy().into_owned());
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timed_out_complete_releases_its_reservation_and_permit() {
    let p = proxy().await;
    let body = b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"0123456789abcdef0123456789abcdef\"</ETag></Part></CompleteMultipartUpload>".to_vec();
    let body_len = body.len() as u64;
    let status = send(&p, "POST", "/bucket/completed-key?uploadId=upload-1", body).await;
    assert_eq!(
        status,
        StatusCode::GATEWAY_TIMEOUT,
        "a stalled completion is answered 504"
    );
    assert!(
        p.ledger.peak_reserved_bytes() >= body_len,
        "precondition: the completion body was reserved (peak {})",
        p.ledger.peak_reserved_bytes()
    );
    assert!(
        eventually(|| p.ledger.reserved_bytes() == 0).await,
        "the attempt's reservation is released: {} bytes still reserved",
        p.ledger.reserved_bytes()
    );
    assert!(
        eventually(|| p.semaphore.available_permits() == MAX_REQUESTS).await,
        "the request permit is released: {} of {} available",
        p.semaphore.available_permits(),
        MAX_REQUESTS
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timed_out_put_stages_nothing_and_an_answered_put_does() {
    let p = proxy().await;
    let status = send(&p, "PUT", "/bucket/stalled-key", vec![0x5A; 64 * 1024]).await;
    assert_eq!(
        status,
        StatusCode::GATEWAY_TIMEOUT,
        "a stalled PUT is answered 504"
    );
    assert!(
        eventually(|| p.semaphore.available_permits() == MAX_REQUESTS).await,
        "the request permit is released"
    );
    assert_eq!(p.ledger.reserved_bytes(), 0, "nothing stays reserved");
    // Let any background cache work for the failed PUT finish before looking.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let staged: Vec<String> = files_ending(p.dir.path(), ".bin")
        .into_iter()
        .chain(files_ending(p.dir.path(), ".meta"))
        .filter(|f| f.contains("stalled-key"))
        .collect();
    assert!(staged.is_empty(), "a timed-out PUT staged {staged:?}");
    // Control: the same PUT answered 200 is cached, so the scan above can see a staged write.
    let status = send(&p, "PUT", "/bucket/answered-key", vec![0x5A; 64 * 1024]).await;
    assert_eq!(status, StatusCode::OK, "control PUT");
    assert!(
        eventually(|| !files_ending(p.dir.path(), ".bin").is_empty()).await,
        "control: an answered PUT leaves a range file"
    );
}

/// Does hyper drop the service future when the client closes its connection
/// while the service is still running? The proxy's listeners serve with
/// `http1::Builder::new()` defaults, as here.
#[tokio::test]
async fn hyper_drops_the_service_future_when_the_client_disconnects() {
    struct Guard(Arc<AtomicBool>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let flag = Arc::clone(&dropped);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let service = service_fn(move |_req: Request<Incoming>| {
            let guard = Guard(Arc::clone(&flag));
            async move {
                let _guard = guard;
                std::future::pending::<()>().await;
                Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(Bytes::new())))
            }
        });
        let _ = http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .await;
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client
        .write_all(b"PUT /bucket/key HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !dropped.load(Ordering::SeqCst),
        "control: the service runs while the client is connected"
    );
    drop(client);
    assert!(
        eventually(|| dropped.load(Ordering::SeqCst)).await,
        "hyper kept the service future after the client disconnected"
    );
}
