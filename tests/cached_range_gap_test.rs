//! Regression coverage for a range request spanning incomplete cached extents.
//!
//! The integration cases drive `HttpProxy::handle_request` through a loopback
//! HTTP/1 connection. They verify the production expiry → conditional 304 path,
//! rather than only exercising the range assembler in isolation.
//!
//! **Validates: Requirements 1.1, 2.1, 2.2, 2.3, 3.1, 3.2, 4.1, 4.2, 4.3,
//! 6.1, 6.2, 6.3**

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Semaphore};

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use s3_proxy::S3ClientApi;

use common::{StubResponse, StubS3Client};

const BODY: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
const ETAG: &str = "\"cached-range-gap-etag\"";
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

fn test_config(cache_dir: std::path::PathBuf) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.get_ttl = Duration::ZERO;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    Arc::new(config)
}

async fn make_cache_infra(
    config: &Arc<Config>,
) -> (
    Arc<CacheManager>,
    Arc<tokio::sync::RwLock<DiskCacheManager>>,
    Arc<RangeHandler>,
) {
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        config.cache.cache_dir.clone(),
        false,
        0,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::LRU,
        1024,
        false,
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
    // CacheManager initialization requires its configured disk-cache manager to
    // install the journal components. The regression fixture writes through a
    // direct DiskCacheManager instead, so each synthetic range is immediately
    // visible without waiting for background consolidation.
    let _configured_disk_cache = cache_manager.create_configured_disk_cache_manager();
    cache_manager.initialize().await.expect("cache init");

    let disk_cache = Arc::new(tokio::sync::RwLock::new(DiskCacheManager::new(
        config.cache.cache_dir.clone(),
        false,
        0,
        config.cache.write_cache_enabled,
        config.cache.compression_batch_size,
    )));
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
    (cache_manager, disk_cache, range_handler)
}

fn object_metadata() -> ObjectMetadata {
    let mut response_headers = HashMap::new();
    response_headers.insert("etag".to_string(), ETAG.to_string());
    response_headers.insert("last-modified".to_string(), LAST_MODIFIED.to_string());
    response_headers.insert("content-length".to_string(), BODY.len().to_string());
    ObjectMetadata {
        etag: ETAG.to_string(),
        last_modified: LAST_MODIFIED.to_string(),
        content_length: BODY.len() as u64,
        content_type: Some("application/octet-stream".to_string()),
        response_headers,
        upload_state: UploadState::Complete,
        cumulative_size: BODY.len() as u64,
        compression_algorithm: CompressionAlgorithm::None,
        ..ObjectMetadata::default()
    }
}

async fn cache_ranges(
    disk_cache: &Arc<tokio::sync::RwLock<DiskCacheManager>>,
    cache_key: &str,
    ranges: &[(usize, usize)],
) {
    for &(start, end) in ranges {
        disk_cache
            .write()
            .await
            .store_range(
                cache_key,
                start as u64,
                end as u64,
                &BODY[start..=end],
                object_metadata(),
                Duration::from_secs(3600),
                false,
            )
            .await
            .expect("cache range");
    }
}

struct ProxyServer {
    addr: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
}

async fn spawn_proxy_server(
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
    s3_client: Arc<dyn S3ClientApi + Send + Sync>,
) -> ProxyServer {
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
    ProxyServer { addr, shutdown_tx }
}

async fn proxy_range_get(addr: SocketAddr, path: &str, range: &str) -> (StatusCode, Bytes, String) {
    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .build_http::<Full<Bytes>>();
    let request = Request::builder()
        .method("GET")
        .uri(format!("http://{}{}", addr, path))
        .header("range", range)
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=sig",
        )
        .body(Full::new(Bytes::new()))
        .expect("request");
    let response = client.request(request).await.expect("proxy request");
    let status = response.status();
    let cache_header = response
        .headers()
        .get("x-cache")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    (status, body, cache_header)
}

/// Like [`proxy_range_get`] but surfaces `retry-after` instead of `x-cache`,
/// because a Shed_Response is identified by that header.
async fn proxy_range_get_retry_after(
    addr: SocketAddr,
    path: &str,
    range: &str,
) -> (StatusCode, Bytes, Option<String>) {
    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .build_http::<Full<Bytes>>();
    let request = Request::builder()
        .method("GET")
        .uri(format!("http://{}{}", addr, path))
        .header("range", range)
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=sig",
        )
        .body(Full::new(Bytes::new()))
        .expect("request");
    let response = client.request(request).await.expect("proxy request");
    let status = response.status();
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    (status, body, retry_after)
}

fn range_stub() -> StubS3Client {
    StubS3Client::new()
        .with_response_for_etag(
            ETAG,
            StubResponse::not_modified()
                .with_header("etag", ETAG)
                .with_header("last-modified", LAST_MODIFIED),
        )
        .with_response_for_range(
            "bytes=0-29",
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::copy_from_slice(&BODY[..30]))
                .with_header("content-range", format!("bytes 0-29/{}", BODY.len())),
        )
        .with_response_for_range(
            "bytes=0-9",
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::copy_from_slice(&BODY[0..10]))
                .with_header("content-range", format!("bytes 0-9/{}", BODY.len())),
        )
        .with_response_for_range(
            "bytes=10-19",
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::copy_from_slice(&BODY[10..20]))
                .with_header("content-range", format!("bytes 10-19/{}", BODY.len())),
        )
        .with_response_for_range(
            "bytes=20-29",
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::copy_from_slice(&BODY[20..30]))
                .with_header("content-range", format!("bytes 20-29/{}", BODY.len())),
        )
}

#[tokio::test]
async fn merge_range_segments_rejects_holey_cached_extents() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = test_config(temp_dir.path().to_path_buf());
    let (cache_manager, disk_cache, range_handler) = make_cache_infra(&config).await;
    let cache_key = "bucket/holey-unit.bin";
    cache_ranges(&disk_cache, cache_key, &[(0, 9), (20, 29)]).await;
    let overlap = range_handler
        .find_cached_ranges(
            cache_key,
            &RangeSpec { start: 0, end: 29 },
            Some(ETAG),
            None,
            s3_proxy::cache_types::RangeLookupPurpose::FreshServe,
        )
        .await
        .expect("overlap");

    let error = range_handler
        .merge_range_segments(
            cache_key,
            &RangeSpec { start: 0, end: 29 },
            &overlap.cached_ranges,
            &[],
        )
        .await
        .expect_err("a hole must fail the strict assembler");
    assert!(error.to_string().contains("Gap detected"));
    assert_eq!(cache_manager.get_statistics().incomplete_range_fallbacks, 0);
}

#[tokio::test]
async fn merge_ranges_with_fallback_refetches_holey_cached_extents() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = test_config(temp_dir.path().to_path_buf());
    let (cache_manager, disk_cache, range_handler) = make_cache_infra(&config).await;
    let cache_key = "bucket/holey-fallback.bin";
    cache_ranges(&disk_cache, cache_key, &[(0, 9), (20, 29)]).await;
    let request = RangeSpec { start: 0, end: 29 };
    let overlap = range_handler
        .find_cached_ranges(
            cache_key,
            &request,
            Some(ETAG),
            None,
            s3_proxy::cache_types::RangeLookupPurpose::FreshServe,
        )
        .await
        .expect("overlap");
    let stub: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(
        StubS3Client::new().with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::copy_from_slice(&BODY[..30])),
        ),
    );

    let merged = range_handler
        .merge_ranges_with_fallback(
            cache_key,
            &request,
            &overlap.cached_ranges,
            &[],
            &stub,
            "bucket.s3.amazonaws.com",
            &"https://bucket.s3.amazonaws.com/object"
                .parse()
                .expect("uri"),
            &HashMap::new(),
            // Direct call: no enclosing caller holds a reservation, so the
            // fallback fetch reserves for itself.
            None,
        )
        .await
        .expect("fallback result");
    assert_eq!(merged.data, Bytes::copy_from_slice(&BODY[..30]));
    assert_eq!(merged.bytes_from_s3, 30);
    assert!(
        cache_manager.get_statistics().incomplete_range_fallbacks >= 1,
        "an incomplete extent must record at least one safe fallback"
    );
}

#[tokio::test]
async fn expired_holey_ranges_revalidated_with_304_return_exact_bytes() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = test_config(temp_dir.path().to_path_buf());
    let (cache_manager, disk_cache, range_handler) = make_cache_infra(&config).await;
    let path = "/bucket/expired-holey.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);
    cache_ranges(&disk_cache, &cache_key, &[(0, 9), (20, 29)]).await;
    let stub = range_stub();
    let server = spawn_proxy_server(
        config,
        Arc::clone(&cache_manager),
        range_handler,
        stub.clone().into_trait_object(),
    )
    .await;

    let (status, body, x_cache) = proxy_range_get(server.addr, path, "bytes=0-29").await;
    let _ = server.shutdown_tx.send(());
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "response body: {body:?}, captured requests: {:?}",
        stub.captured()
    );
    assert_eq!(body, Bytes::copy_from_slice(&BODY[..30]));
    assert_ne!(
        x_cache, "HIT",
        "a response using upstream bytes is not a clean hit"
    );
    assert!(stub
        .captured()
        .iter()
        .any(|request| request.if_none_match() == Some(ETAG)));
    assert!(
        cache_manager.get_statistics().incomplete_range_fallbacks >= 1,
        "an incomplete extent must record at least one safe fallback"
    );
}

#[tokio::test]
async fn partial_eviction_hole_returns_exact_bytes() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = test_config(temp_dir.path().to_path_buf());
    let (cache_manager, disk_cache, range_handler) = make_cache_infra(&config).await;
    let path = "/bucket/evicted-middle.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);
    cache_ranges(&disk_cache, &cache_key, &[(0, 9), (10, 19), (20, 29)]).await;
    disk_cache
        .read()
        .await
        .delete_ranges(&cache_key, &[(10, 19)])
        .await
        .expect("partial eviction");
    let stub = range_stub();
    let server = spawn_proxy_server(
        config,
        Arc::clone(&cache_manager),
        range_handler,
        stub.into_trait_object(),
    )
    .await;

    let (status, body, _) = proxy_range_get(server.addr, path, "bytes=0-29").await;
    let _ = server.shutdown_tx.send(());
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "response body: {body:?}"
    );
    assert_eq!(body, Bytes::copy_from_slice(&BODY[..30]));
    assert!(
        cache_manager.get_statistics().incomplete_range_fallbacks >= 1,
        "an incomplete extent must record at least one safe fallback"
    );
}

#[tokio::test]
async fn single_partially_covering_extent_returns_exact_bytes() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = test_config(temp_dir.path().to_path_buf());
    let (cache_manager, disk_cache, range_handler) = make_cache_infra(&config).await;
    let path = "/bucket/single-partial.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);
    cache_ranges(&disk_cache, &cache_key, &[(10, 19)]).await;
    let stub = range_stub();
    let server = spawn_proxy_server(
        config,
        Arc::clone(&cache_manager),
        range_handler,
        stub.into_trait_object(),
    )
    .await;

    let (status, body, _) = proxy_range_get(server.addr, path, "bytes=0-29").await;
    let _ = server.shutdown_tx.send(());
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "response body: {body:?}"
    );
    assert_eq!(body, Bytes::copy_from_slice(&BODY[..30]));
    assert!(
        cache_manager.get_statistics().incomplete_range_fallbacks >= 1,
        "an incomplete extent must record at least one safe fallback"
    );
}

// ---------------------------------------------------------------------------
// A repair fetch refused by the in-flight memory ledger must shed, and the shed
// must be TRANSIENT.
//
// When cached extents no longer cover a requested range, the merge degrades to
// a complete S3 refetch, and that refetch's bytes are accounted against the
// in-flight memory ledger. Two properties, and the second is the one that was
// broken:
//
// 1. A refusal surfaces as the Shed_Response (503 SlowDown + Retry-After) that
//    every other ledger rejection produces — never a 500 or a 502, which a
//    client reads as permanent and does not retry.
// 2. Retrying it eventually succeeds. The repair fetch used to take a SECOND
//    reservation for the same bytes the request's own buffered-serve
//    reservation already covered, so under a ceiling that fits one claim of the
//    range the pair could never fit — the request's own claim was what stood in
//    the way, and no amount of waiting freed it. A 503 for a condition that
//    never clears is worse than the 500 it replaced.
//
// **Validates: Requirements IMA 2.1, 2.2, 10.9, 10.10**
// ---------------------------------------------------------------------------

/// Ceiling that fits exactly ONE claim of the 30-byte request and no more.
/// Measured, not assumed: at 29 the buffered-serve reservation itself is refused
/// (`peak_reserved_bytes` stays 0, and admission sheds before any repair); at 60
/// two independent claims of the range fit concurrently. Only the 30..=59 band
/// admits one claim of the range while refusing a second, which is the band that
/// can tell a single honest claim apart from a double count.
const LEDGER_CEILING: u64 = 30;

/// Cache three adjacent extents, then delete one range file directly from disk
/// while leaving metadata claiming full coverage.
///
/// This detail is load-bearing. `DiskCacheManager::delete_ranges` also updates
/// metadata, which drops `can_serve_from_cache` to false and routes the request
/// down the plain S3-fetch path instead — a path whose ledger rejection was
/// already mapped correctly, so the case would pass without touching the code
/// under test. Removing only the file keeps the request classified as a cache
/// hit, which is the sole entry into the merge/recovery arms being fixed here.
async fn cache_ranges_then_orphan_metadata(
    disk_cache: &Arc<tokio::sync::RwLock<DiskCacheManager>>,
    cache_dir: &std::path::Path,
    cache_key: &str,
) {
    cache_ranges(disk_cache, cache_key, &[(0, 9), (10, 19), (20, 29)]).await;

    let mut removed = 0;
    let mut stack = vec![cache_dir.join("ranges")];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .map(|name| name.to_string_lossy().contains("_10-19"))
                .unwrap_or(false)
            {
                std::fs::remove_file(&path).expect("remove range file");
                removed += 1;
            }
        }
    }
    assert_eq!(
        removed, 1,
        "the fixture must orphan exactly one range file, or it is not \
         reproducing an incomplete cached extent"
    );
}

/// Config for the ledger cases: a fresh TTL keeps the request on the cache-hit
/// route. With `get_ttl = 0` the entry revalidates first and the repair happens
/// on a different path.
fn ledger_test_config(cache_dir: std::path::PathBuf) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.get_ttl = Duration::from_secs(3600);
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    Arc::new(config)
}

/// A repair fetch must claim the bytes the request's own buffered-serve
/// reservation already covers, not reserve them a second time.
///
/// The ceiling fits exactly one claim of the 30-byte range, so the arithmetic
/// decides the outcome on its own: one honest claim is admitted and the request
/// completes, two claims of the same bytes cannot fit and the request sheds.
/// Before the fix this returned 503 with a peak of 30 and one rejection.
#[tokio::test]
async fn repair_fetch_reuses_serve_reservation_instead_of_double_counting() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = ledger_test_config(temp_dir.path().to_path_buf());

    let (cache_manager, disk_cache, range_handler) = make_cache_infra(&config).await;
    let path = "/bucket/ledger-single-claim.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);
    cache_ranges_then_orphan_metadata(&disk_cache, temp_dir.path(), &cache_key).await;

    let ledger = Arc::new(s3_proxy::inflight_ledger::InflightLedger::new(
        LEDGER_CEILING,
    ));
    let stub = range_stub().with_inflight_ledger(Arc::clone(&ledger));
    let server = spawn_proxy_server(
        config,
        Arc::clone(&cache_manager),
        range_handler,
        stub.into_trait_object(),
    )
    .await;

    let (status, body, _) = proxy_range_get(server.addr, path, "bytes=0-29").await;
    let _ = server.shutdown_tx.send(());

    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "a repair fetch of bytes the request already reserved for must fit \
         under a ceiling sized for one claim of them; body: {body:?}"
    );
    assert_eq!(body, Bytes::copy_from_slice(&BODY[..30]));
    assert_eq!(
        ledger.peak_reserved_bytes(),
        LEDGER_CEILING,
        "the same 30 bytes must peak at 30, not 60 — a higher peak means the \
         repair fetch took a second claim for memory already accounted for"
    );
    assert_eq!(
        ledger.rejected_total(),
        0,
        "nothing should have been refused: one claim of the range fits"
    );
    assert_eq!(
        ledger.reserved_bytes(),
        0,
        "every reservation must be released once the request completes"
    );
}

/// A ledger refusal on the repair path must be transient: refused while
/// capacity is genuinely occupied by something else, then admitted byte-exact
/// once that capacity frees.
///
/// The occupied capacity is an explicit reservation held outside the request
/// rather than a second concurrent request, so the contention is deterministic
/// and needs no timing. It is the same shape as the fleet suite's T40g (leg A
/// holds, leg B sheds, leg A releases, leg B retries byte-exact).
///
/// Before the fix the retry failed with a second 503: with capacity free, the
/// request's own serve reservation plus the repair fetch's duplicate claim for
/// the same bytes still exceeded a ceiling that fits one claim, so waiting
/// could never help.
#[tokio::test]
async fn ledger_refused_repair_fetch_sheds_transiently_then_succeeds() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = ledger_test_config(temp_dir.path().to_path_buf());

    let (cache_manager, disk_cache, range_handler) = make_cache_infra(&config).await;
    let path = "/bucket/ledger-transient-repair.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);
    cache_ranges_then_orphan_metadata(&disk_cache, temp_dir.path(), &cache_key).await;

    let ledger = Arc::new(s3_proxy::inflight_ledger::InflightLedger::new(
        LEDGER_CEILING,
    ));
    let stub = range_stub().with_inflight_ledger(Arc::clone(&ledger));
    let server = spawn_proxy_server(
        config,
        Arc::clone(&cache_manager),
        range_handler,
        stub.into_trait_object(),
    )
    .await;

    // Occupy one byte of the ceiling. The request needs the whole of it, so
    // while this is held nothing it does can be admitted.
    let occupant = ledger
        .try_reserve(1)
        .expect("one byte must fit under the ceiling");

    let (shed_status, shed_body, retry_after) =
        proxy_range_get_retry_after(server.addr, path, "bytes=0-29").await;
    assert_eq!(
        shed_status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a request that cannot fit under the ceiling must shed, not return \
         500 or 502; body: {shed_body:?}"
    );
    assert_eq!(
        retry_after.as_deref(),
        Some("5"),
        "a Shed_Response must carry Retry-After so the client retries"
    );
    assert!(
        String::from_utf8_lossy(&shed_body).contains("SlowDown"),
        "the shed body must carry the SlowDown error code, got: {shed_body:?}"
    );
    assert!(ledger.rejected_total() >= 1);

    // Free the capacity and retry. This is the assertion the double-count broke:
    // the shed must have been about capacity, not about the request's own claim.
    drop(occupant);
    assert_eq!(ledger.reserved_bytes(), 0, "capacity must be free again");

    let (retry_status, retry_body, _) = proxy_range_get(server.addr, path, "bytes=0-29").await;
    let _ = server.shutdown_tx.send(());

    assert_eq!(
        retry_status,
        StatusCode::PARTIAL_CONTENT,
        "the retry must succeed once capacity frees, or the shed was permanent \
         rather than transient; body: {retry_body:?}"
    );
    assert_eq!(
        retry_body,
        Bytes::copy_from_slice(&BODY[..30]),
        "the retry must return the requested bytes exactly"
    );
    assert_eq!(
        ledger.peak_reserved_bytes(),
        LEDGER_CEILING,
        "the repair must have claimed the serve reservation's bytes rather \
         than duplicating them"
    );
    assert_eq!(
        ledger.reserved_bytes(),
        0,
        "every reservation must be released once the request completes"
    );
}

/// The single-partially-covering-extent recovery arm takes its `Err` from the
/// same call, so this pins the error variant that arm matches on.
///
/// That arm cannot be driven end-to-end from this harness: reaching it requires
/// a request classified as fully served from cache whose lone cached extent
/// nevertheless does not contain the requested range, and the metadata API
/// cannot produce that pair — coverage is what makes the request a cache hit in
/// the first place. It is a defensive branch. What is testable, and what its
/// correctness depends on, is that a ledger-refused repair fetch surfaces as
/// `InflightCeilingExceeded` rather than some generic error, which is the
/// variant both arms pattern-match.
#[tokio::test]
async fn ledger_refused_repair_fetch_surfaces_ceiling_exceeded() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = test_config(temp_dir.path().to_path_buf());
    let (_cache_manager, disk_cache, range_handler) = make_cache_infra(&config).await;
    let cache_key = "bucket/ledger-variant.bin";
    cache_ranges(&disk_cache, cache_key, &[(0, 9), (20, 29)]).await;
    let request = RangeSpec { start: 0, end: 29 };
    let overlap = range_handler
        .find_cached_ranges(
            cache_key,
            &request,
            Some(ETAG),
            None,
            s3_proxy::cache_types::RangeLookupPurpose::FreshServe,
        )
        .await
        .expect("overlap");

    // A ceiling of 1 refuses the 30-byte repair fetch outright.
    let ledger = Arc::new(s3_proxy::inflight_ledger::InflightLedger::new(1));
    let stub: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(
        StubS3Client::new()
            .with_default(
                StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                    .with_body(Bytes::copy_from_slice(&BODY[..30])),
            )
            .with_inflight_ledger(Arc::clone(&ledger)),
    );

    let error = range_handler
        .merge_ranges_with_fallback(
            cache_key,
            &request,
            &overlap.cached_ranges,
            &[],
            &stub,
            "bucket.s3.amazonaws.com",
            &"https://bucket.s3.amazonaws.com/object"
                .parse()
                .expect("uri"),
            &HashMap::new(),
            // Direct call: no enclosing caller holds a reservation, so the
            // fallback fetch reserves for itself.
            None,
        )
        .await
        .expect_err("a refused repair fetch must fail rather than serve short");

    assert!(
        matches!(error, s3_proxy::ProxyError::InflightCeilingExceeded { .. }),
        "the repair fetch must surface the ceiling variant both recovery arms \
         match on, got: {error:?}"
    );
    assert_eq!(ledger.rejected_total(), 1);
    assert_eq!(ledger.reserved_bytes(), 0);
}
