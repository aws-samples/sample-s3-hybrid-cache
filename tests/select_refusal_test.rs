//! SelectObjectContent is refused with `501 NotImplemented` before any handler runs: the
//! upstream sees no connection, nothing is reserved on the in-flight ledger and nothing is
//! cached. Other POST subresources (`?uploads`, `?delete`) still reach their handlers.
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
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
const TEST_BOUND: Duration = Duration::from_secs(30);

/// An upstream that counts every connection it accepts and answers each with an empty 200.
async fn origin(connections: Arc<AtomicUsize>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            connections.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let _ = stream.read(&mut buf).await;
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
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
    connections: Arc<AtomicUsize>,
    ledger: Arc<InflightLedger>,
}

async fn proxy() -> Proxy {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let connections = Arc::new(AtomicUsize::new(0));
    let origin = origin(Arc::clone(&connections)).await;
    let origin_host = format!("127.0.0.1:{}", origin.port());
    let dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.cache.cache_dir = dir.path().to_path_buf();
    config.cache.ram_cache_enabled = false;
    config.cache.write_cache_enabled = true;
    config.cache.read_cache_enabled = true;
    config.cache.download_coordination.enabled = false;
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
    let request_semaphore = Arc::new(Semaphore::new(8));
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
        connections,
        ledger,
    }
}

/// Send raw request text and return the whole response (the request asks for close).
async fn raw(p: &Proxy, request: String) -> String {
    let fut = async {
        let mut stream = TcpStream::connect(p.addr).await.expect("connect");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut out = Vec::new();
        let _ = stream.read_to_end(&mut out).await;
        String::from_utf8_lossy(&out).into_owned()
    };
    tokio::time::timeout(TEST_BOUND, fut)
        .await
        .expect("the proxy must answer within the test bound")
}

const SELECT_BODY: &str =
    "<SelectObjectContentRequest><Expression>SELECT 1</Expression></SelectObjectContentRequest>";

fn signed_post(p: &Proxy, target: &str) -> String {
    signed_post_with(p, target, SELECT_BODY)
}

fn signed_post_with(p: &Proxy, target: &str, body: &str) -> String {
    format!(
        "POST {target} HTTP/1.1\r\nHost: {}\r\nAuthorization: {AUTH}\r\nx-amz-date: 20260101T000000Z\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        p.origin_host,
        body.len()
    )
}

/// Every file under the cache directory (the ranges and metadata trees).
fn cached_files(p: &Proxy) -> Vec<String> {
    let mut found = Vec::new();
    let mut stack = vec![p.dir.path().to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("bin") | Some("meta")
            ) {
                found.push(path.display().to_string());
            }
        }
    }
    found
}

fn assert_refused(p: &Proxy, response: &str, what: &str) {
    assert!(
        response.starts_with("HTTP/1.1 501"),
        "{what}: want 501, got {:?}",
        response.lines().next()
    );
    assert!(
        response.contains("<Code>NotImplemented</Code>"),
        "{what}: want an S3-style NotImplemented body, got {response:?}"
    );
    assert_eq!(
        p.connections.load(Ordering::SeqCst),
        0,
        "{what}: the upstream must see no connection"
    );
    assert_eq!(
        p.ledger.reserved_bytes(),
        0,
        "{what}: nothing may stay reserved on the ledger"
    );
    assert_eq!(
        p.ledger.granted_total(),
        0,
        "{what}: nothing may be reserved on the ledger at all"
    );
    assert!(
        cached_files(p).is_empty(),
        "{what}: nothing may be cached: {:?}",
        cached_files(p)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_select_is_refused_without_reaching_the_upstream() {
    let p = proxy().await;
    let r = raw(
        &p,
        signed_post(&p, "/bucket/fixture-object?select&select-type=2"),
    )
    .await;
    assert_refused(&p, &r, "signed Select");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_presigned_select_is_refused_without_reaching_the_upstream() {
    let p = proxy().await;
    let target = "/bucket/fixture-object?select&select-type=2&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260101T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=0000"; // gitleaks:allow
    let request = format!(
        "POST {target} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{SELECT_BODY}",
        p.origin_host,
        SELECT_BODY.len()
    );
    let r = raw(&p, request).await;
    assert_refused(&p, &r, "presigned Select");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_absolute_uri_select_is_refused_without_reaching_the_upstream() {
    let p = proxy().await;
    let target = format!(
        "http://{}/bucket/fixture-object?select&select-type=2",
        p.origin_host
    );
    let r = raw(&p, signed_post(&p, &target)).await;
    assert_refused(&p, &r, "absolute-URI Select");
}

/// Controls: the neighbouring POST subresources are still forwarded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn other_post_subresources_still_reach_the_upstream() {
    let delete = "<Delete><Object><Key>fixture-object</Key></Object></Delete>";
    for (target, body, what) in [
        (
            "/bucket/fixture-object?uploads",
            "",
            "CreateMultipartUpload",
        ),
        ("/bucket?delete", delete, "DeleteObjects"),
    ] {
        let p = proxy().await;
        let r = raw(&p, signed_post_with(&p, target, body)).await;
        assert!(
            !r.starts_with("HTTP/1.1 501"),
            "{what} must not be refused as Select: {:?}",
            r.lines().next()
        );
        assert!(
            p.connections.load(Ordering::SeqCst) >= 1,
            "{what} must reach the upstream; the proxy answered {r:?}"
        );
    }
}
