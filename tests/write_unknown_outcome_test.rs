//! A signed write whose request reached the upstream in full, and whose answer never
//! arrived (or was refused for size), may have been executed by S3. The proxy then
//! drops the key's cached copy before it answers, so the next GET goes to the origin
//! instead of serving the version the write may have replaced.
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
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
use tokio::sync::{oneshot, Notify, Semaphore};

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
const TEST_BOUND: Duration = Duration::from_secs(30);
const KEY: &str = "/bucket/fixture-object";
/// The body of the `n`th version the origin serves; every version has one length.
fn version(n: usize) -> Vec<u8> {
    format!("fixture object, origin version {n:03}").into_bytes()
}
/// Larger than the 8 MiB buffered-response bound for a signed write.
const OVERSIZED_RESPONSE: usize = 9 * 1024 * 1024;

/// An origin that reads each request's head and `Content-Length` body. A request
/// carrying `x-fixture-stall` is never answered; one carrying `x-fixture-oversized` is
/// answered `200` with a body over the write-response bound; the `n`th GET is answered
/// with [`version`] `n`; any other request is answered `200` with no body.
async fn origin(reads: Arc<AtomicUsize>) -> (SocketAddr, Arc<Notify>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let close = Arc::new(Notify::new());
    serve_origin(listener, reads, Arc::clone(&close));
    (addr, close)
}

/// Serve `listener` as [`origin`] does. A notification on `close` drops the listener, so
/// a later connect to its address is refused; connections already accepted stay open.
fn serve_origin(listener: TcpListener, reads: Arc<AtomicUsize>, close: Arc<Notify>) {
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok(accepted) => accepted,
                    Err(_) => break,
                },
                _ = close.notified() => break,
            };
            let reads = Arc::clone(&reads);
            let close = Arc::clone(&close);
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
                let method = head.split(' ').next().unwrap_or("").to_string();
                if head.contains("x-fixture-stall:") {
                    if head.contains("x-fixture-refuse-after:") {
                        // Never answer, and refuse every later connect.
                        close.notify_one();
                    }
                    let _held = stream;
                    std::future::pending::<()>().await;
                } else if head.contains("x-fixture-close:") {
                    // Read the whole request, then close without answering.
                    drop(stream);
                } else if head.contains("x-fixture-cut-chunked:") {
                    // A chunked 200 that ends a chunk and closes: no zero-size chunk.
                    let _ = stream
                        .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nHello\r\n")
                        .await;
                } else if head.contains("x-fixture-oversized:") {
                    let head =
                        format!("HTTP/1.1 200 OK\r\nContent-Length: {OVERSIZED_RESPONSE}\r\n\r\n");
                    let _ = stream.write_all(head.as_bytes()).await;
                    let _ = stream.write_all(&vec![b' '; OVERSIZED_RESPONSE]).await;
                    let _held = stream;
                    std::future::pending::<()>().await;
                } else if method == "get" {
                    let n = reads.fetch_add(1, Ordering::SeqCst) + 1;
                    let body = version(n);
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nETag: \"{n:032x}\"\r\nLast-Modified: Wed, 01 Jan 2026 00:00:00 GMT\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(head.as_bytes()).await;
                    let _ = stream.write_all(&body).await;
                } else {
                    let _ = stream
                        .write_all(b"HTTP/1.1 200 OK\r\nETag: \"0123456789abcdef0123456789abcdef\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await;
                }
            });
        }
    });
}

struct Proxy {
    _shutdown: oneshot::Sender<()>,
    addr: SocketAddr,
    _dir: TempDir,
    origin_host: String,
    origin_reads: Arc<AtomicUsize>,
    origin_addr: SocketAddr,
    close_origin: Arc<Notify>,
}

/// The proxy with the write cache on or off. On, a signed PUT of a small body takes
/// `SignedPutHandler`'s cached arm (`CacheDecision::Cache`); off, it takes
/// `forward_signed_write_uncached`.
async fn proxy_with(write_cache: bool) -> Proxy {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let origin_reads = Arc::new(AtomicUsize::new(0));
    let (origin, close_origin) = origin(Arc::clone(&origin_reads)).await;
    let origin_host = format!("127.0.0.1:{}", origin.port());
    let dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.cache.cache_dir = dir.path().to_path_buf();
    config.cache.ram_cache_enabled = false;
    config.cache.write_cache_enabled = write_cache;
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
            .with_request_timeout(Duration::from_secs(2))
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
        _dir: dir,
        origin_host,
        origin_reads,
        origin_addr: origin,
        close_origin,
    }
}

/// Send one signed request through the proxy; return its status, body and ETag.
async fn send(
    p: &Proxy,
    method: &str,
    path_and_query: &str,
    extra: &[(&str, &str)],
    body: Vec<u8>,
) -> (StatusCode, Bytes, String) {
    let fut = async {
        let stream = TcpStream::connect(p.addr).await.expect("connect");
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .expect("handshake");
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let presigned = extra.iter().any(|(name, _)| *name == "x-fixture-presigned");
        let mut req = Request::builder()
            .method(method)
            .uri(path_and_query)
            .header("host", p.origin_host.as_str())
            .header("x-amz-date", "20260101T000000Z")
            .header("content-length", body.len().to_string());
        if !presigned {
            req = req.header("authorization", AUTH);
        }
        for (name, value) in extra {
            req = req.header(*name, *value);
        }
        let req = req.body(Full::new(Bytes::from(body))).unwrap();
        let resp = sender.send_request(req).await.expect("proxy request");
        let status = resp.status();
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .map(|c| c.to_bytes())
            .unwrap_or_default();
        (status, bytes, etag)
    };
    tokio::time::timeout(TEST_BOUND, fut)
        .await
        .expect("the proxy must answer within the test bound")
}

/// `path` with a presigned query appended: the signature is in the URL, so the request
/// carries no `Authorization` header (mark it with `x-fixture-presigned`). `extra` is
/// further query parameters, joined with `&`.
fn presigned(path: &str, extra: &str) -> String {
    let now = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    let sep = if extra.is_empty() { "" } else { "&" };
    format!(
        "{path}?{extra}{sep}X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date={now}&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=0000" // gitleaks:allow
    )
}

/// Fill the cache for `KEY` through GET misses until a GET is served from the cache
/// without reaching the origin. Returns the proxy and the cached version.
async fn cached_proxy() -> (Proxy, usize) {
    cached_proxy_with(true).await
}

async fn cached_proxy_with(write_cache: bool) -> (Proxy, usize) {
    let p = proxy_with(write_cache).await;
    for _ in 0..150 {
        let before = p.origin_reads.load(Ordering::SeqCst);
        let (status, body, etag) = send(&p, "GET", KEY, &[], Vec::new()).await;
        assert_eq!(status, StatusCode::OK, "precondition: a GET of the fixture");
        if before > 0 && p.origin_reads.load(Ordering::SeqCst) == before {
            // Under load a second GET can miss while the first fill is still landing, so
            // the version cached may be any one the origin sent. Its bytes and its ETag
            // must name the same one.
            let Some(n) = (1..=before).find(|n| body.to_vec() == version(*n)) else {
                panic!(
                    "precondition: the cache served bytes of no version the origin sent: {:?}",
                    String::from_utf8_lossy(&body)
                );
            };
            assert_eq!(
                etag,
                format!("\"{n:032x}\""),
                "the cached bytes of version {n} must carry that version's ETag"
            );
            return (p, before);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("precondition: no GET was served from the cache");
}

/// After a write whose outcome is unknown, the next GET goes to the origin.
async fn assert_next_get_reaches_the_origin(p: &Proxy, cached: usize, write: &str) {
    let (status, body, _) = send(p, "GET", KEY, &[], Vec::new()).await;
    let reads = p.origin_reads.load(Ordering::SeqCst);
    assert!(
        reads == cached + 1 && body.to_vec() == version(cached + 1),
        "after a {write} whose outcome is unknown the GET must reach the origin: \
         origin reads {reads}, status {status}, body {:?}",
        String::from_utf8_lossy(&body)
    );
}

/// Stop the origin listening: a connect to it is refused until [`reopen_origin`].
async fn close_origin(p: &Proxy) {
    p.close_origin.notify_one();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        TcpStream::connect(p.origin_addr).await.is_err(),
        "precondition: the origin refuses connections"
    );
}

/// Listen again on the origin's address and serve as before.
async fn reopen_origin(p: &Proxy) {
    let listener = TcpListener::bind(p.origin_addr)
        .await
        .expect("rebind origin");
    serve_origin(
        listener,
        Arc::clone(&p.origin_reads),
        Arc::new(Notify::new()),
    );
}

/// This PUT reaches `SignedPutHandler`'s cached arm (`handle_with_caching`): the write
/// cache is on and the body is small.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_put_with_no_answer_invalidates_the_cached_copy() {
    let (p, cached) = cached_proxy().await;
    let stall = [("x-fixture-stall", "1")];
    let (status, _, _) = send(&p, "PUT", KEY, &stall, b"replacement".to_vec()).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_next_get_reaches_the_origin(&p, cached, "PUT").await;
}

/// With the write cache off the PUT takes the uncached signed arm, which invalidates
/// through its own call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_uncached_put_with_no_answer_invalidates_the_cached_copy() {
    let (p, cached) = cached_proxy_with(false).await;
    let stall = [("x-fixture-stall", "1")];
    let (status, _, _) = send(&p, "PUT", KEY, &stall, b"replacement".to_vec()).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_next_get_reaches_the_origin(&p, cached, "PUT with the write cache off").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_put_whose_answer_is_over_the_bound_invalidates_the_cached_copy() {
    let (p, cached) = cached_proxy().await;
    let big = [("x-fixture-oversized", "1")];
    let (status, _, _) = send(&p, "PUT", KEY, &big, b"replacement".to_vec()).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_next_get_reaches_the_origin(&p, cached, "PUT answered over the bound").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_complete_with_no_answer_invalidates_the_cached_copy() {
    let (p, cached) = cached_proxy().await;
    let stall = [("x-fixture-stall", "1")];
    let body = b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"0123456789abcdef0123456789abcdef\"</ETag></Part></CompleteMultipartUpload>".to_vec();
    let uri = format!("{KEY}?uploadId=upload-1");
    let (status, _, _) = send(&p, "POST", &uri, &stall, body).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_next_get_reaches_the_origin(&p, cached, "CompleteMultipartUpload").await;
}

/// A chunked 200 cut at a chunk boundary is a failure the client is told about, and the
/// request was sent in full, so the key's cached copy is dropped as for any unknown outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_complete_answered_by_a_cut_chunked_body_is_a_502_and_invalidates_the_cached_copy() {
    let (p, cached) = cached_proxy().await;
    let cut = [("x-fixture-cut-chunked", "1")];
    let body = b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"0123456789abcdef0123456789abcdef\"</ETag></Part></CompleteMultipartUpload>".to_vec();
    let uri = format!("{KEY}?uploadId=upload-1");
    let (status, _, _) = send(&p, "POST", &uri, &cut, body).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_next_get_reaches_the_origin(
        &p,
        cached,
        "CompleteMultipartUpload answered by a cut body",
    )
    .await;
}

/// A CompleteMultipartUpload signed in the query reaches a different branch from the
/// `Authorization`-signed one, and must drop the cached copy on an unknown outcome too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_presigned_complete_with_no_answer_invalidates_the_cached_copy() {
    let (p, cached) = cached_proxy().await;
    let headers = [("x-fixture-stall", "1"), ("x-fixture-presigned", "1")];
    let body = b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"0123456789abcdef0123456789abcdef\"</ETag></Part></CompleteMultipartUpload>".to_vec();
    let uri = presigned(KEY, "uploadId=upload-1");
    let (status, _, _) = send(&p, "POST", &uri, &headers, body).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_next_get_reaches_the_origin(&p, cached, "presigned CompleteMultipartUpload").await;
}

/// A DELETE is idempotent, so a presigned one that timed out drops the cached copy: the
/// delete may have happened, and the cost of being wrong is one cache miss.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_presigned_delete_with_no_answer_invalidates_the_cached_copy() {
    let (p, cached) = cached_proxy().await;
    let headers = [("x-fixture-stall", "1"), ("x-fixture-presigned", "1")];
    let uri = presigned(KEY, "");
    let (status, _, _) = send(&p, "DELETE", &uri, &headers, Vec::new()).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_next_get_reaches_the_origin(&p, cached, "presigned DELETE").await;
}

/// The same holds when the connection closes after the request was read instead of
/// timing out: S3 may have executed the delete.
/// The first attempt is sent and never answered; every retry then fails to connect. The
/// delete may still have happened, so the cached copy goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_presigned_delete_sent_once_and_then_refused_invalidates_the_cached_copy() {
    let (p, cached) = cached_proxy().await;
    let headers = [
        ("x-fixture-stall", "1"),
        ("x-fixture-refuse-after", "1"),
        ("x-fixture-presigned", "1"),
    ];
    let uri = presigned(KEY, "");
    let (status, _, _) = send(&p, "DELETE", &uri, &headers, Vec::new()).await;
    assert!(status.is_server_error(), "the delete fails: {status}");
    reopen_origin(&p).await;
    assert_next_get_reaches_the_origin(&p, cached, "presigned DELETE sent then refused").await;
}

/// Control: no attempt reaches the origin, so nothing was deleted and the cached copy
/// stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_presigned_delete_refused_on_every_attempt_keeps_the_cached_copy() {
    let (p, cached) = cached_proxy().await;
    close_origin(&p).await;
    let headers = [("x-fixture-presigned", "1")];
    let uri = presigned(KEY, "");
    let (status, _, _) = send(&p, "DELETE", &uri, &headers, Vec::new()).await;
    assert!(status.is_server_error(), "the delete fails: {status}");
    reopen_origin(&p).await;
    let (status, body, _) = send(&p, "GET", KEY, &[], Vec::new()).await;
    let reads = p.origin_reads.load(Ordering::SeqCst);
    assert!(
        status == StatusCode::OK
            && reads == cached
            && (1..=cached).any(|n| body.to_vec() == version(n)),
        "nothing was sent, so the GET must be served from the cache: origin reads {reads} \
         (cached {cached}), status {status}, body {:?}",
        String::from_utf8_lossy(&body)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_presigned_delete_whose_connection_closes_invalidates_the_cached_copy() {
    let (p, cached) = cached_proxy().await;
    let headers = [("x-fixture-close", "1"), ("x-fixture-presigned", "1")];
    let uri = presigned(KEY, "");
    let (status, _, _) = send(&p, "DELETE", &uri, &headers, Vec::new()).await;
    assert!(
        status.is_server_error(),
        "the closed connection is an error: {status}"
    );
    assert_next_get_reaches_the_origin(&p, cached, "presigned DELETE").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copy_with_no_answer_invalidates_the_cached_copy() {
    let (p, cached) = cached_proxy().await;
    let headers = [
        ("x-fixture-stall", "1"),
        ("x-amz-copy-source", "/bucket/fixture-source"),
    ];
    let (status, _, _) = send(&p, "PUT", KEY, &headers, Vec::new()).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_next_get_reaches_the_origin(&p, cached, "CopyObject").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_delete_with_no_answer_invalidates_the_cached_copy() {
    let (p, cached) = cached_proxy().await;
    let stall = [("x-fixture-stall", "1")];
    let (status, _, _) = send(&p, "DELETE", KEY, &stall, Vec::new()).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_next_get_reaches_the_origin(&p, cached, "signed DELETE").await;
}

/// Already covered before this change: DeleteObjects invalidates every requested key
/// when its forward fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delete_objects_with_no_answer_invalidates_the_cached_copy() {
    let (p, cached) = cached_proxy().await;
    let stall = [("x-fixture-stall", "1")];
    let body = b"<Delete><Object><Key>fixture-object</Key></Object></Delete>".to_vec();
    let (status, _, _) = send(&p, "POST", "/bucket?delete", &stall, body).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_next_get_reaches_the_origin(&p, cached, "DeleteObjects").await;
}
