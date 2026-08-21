//! Request-path coverage for the part-scoped-HEAD cache-poisoning fix.
//!
//! Spec: `.kiro/specs/head-partnumber-cache-poisoning/`
//!
//! Why these cannot be `#[cfg(test)]` unit tests: the behaviour under test is a
//! **routing** decision inside the private `handle_get_head_request` — whether a
//! `HEAD ?partNumber=N` reaches the cache at all — which needs a request, a proxy
//! and an upstream. The storage half of the fix (what gets written into a `.meta`)
//! is covered by `part_scoped_head_storage_tests` in `src/cache.rs`, which needs
//! only a `CacheManager` over a `TempDir`.
//!
//! The defect: one `HEAD ?partNumber=N` used to poison the whole-object cache
//! entry, after which every later HEAD and GET of that object returned the wrong
//! answer with HTTP 200 and a success status. Measured on the fleet at 2.5.0:
//! 5,242,880 bytes returned for a 52,428,800-byte object, deterministically,
//! 10/10, with no load and no race. Three ingredients, the last of which landed at
//! v0.5.0 (2026-01-02), so every release from then to 2.5.0 is affected — not a
//! 2.5.0 regression.
//!
//! **Stub routing note.** `StubS3Client` matches on `If-None-Match`, then `Range`,
//! then `authorization`, then a default — it has no method- or query-based
//! routing. So a part-scoped request is disambiguated here by sending a distinct
//! `authorization` value and registering the part-scoped response against it.
//! That is the mechanism these tests use to make S3 answer a part HEAD with a
//! part's `Content-Length` and a `Content-Range`, as the real S3 does.

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Request, StatusCode};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::Semaphore;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::S3ClientApi;

use common::{StubResponse, StubS3Client};

/// The real object: 50 MiB in ten 5 MiB parts, matching the measured fixture.
const OBJECT_LEN: u64 = 52_428_800;
const PART_LEN: u64 = 5_242_880;

/// A plain SigV4-shaped header. Routing is by value, so it only needs to differ
/// between the two request shapes, not to be valid.
const AUTH_PLAIN: &str = "AWS4-HMAC-SHA256 Credential=AKIA-PLAIN/20260101/us-west-2/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=sig";
const AUTH_PART: &str = "AWS4-HMAC-SHA256 Credential=AKIA-PART/20260101/us-west-2/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=sig";

fn test_config(cache_dir: std::path::PathBuf) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    // Every miss must be an independently observable S3 fetch, and nothing may be
    // promoted in the background, or "was this served from cache" is not decidable
    // from the captured trace.
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    config.cache.read_cache_enabled = true;
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
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::LRU,
        1024,
        false, // compression disabled — stored bytes stay 1:1 with the body
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

    (cache_manager, disk_cache_manager, range_handler)
}

struct ProxyServer {
    addr: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
}

/// Serve the real `HttpProxy::handle_request` on a loopback port, so requests go
/// through the genuine dispatch into `handle_get_head_request`.
async fn spawn_proxy_server(
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
    s3_client: Arc<dyn S3ClientApi + Send + Sync>,
    inflight_tracker: Arc<InFlightTracker>,
) -> ProxyServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let request_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_requests));

    tokio::spawn(async move {
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let (stream, peer) = match accept {
                        Ok(v) => v,
                        Err(_) => break,
                    };
                    let io = TokioIo::new(stream);
                    let config = Arc::clone(&config);
                    let cache_manager = Arc::clone(&cache_manager);
                    let range_handler = Arc::clone(&range_handler);
                    let s3_client = Arc::clone(&s3_client);
                    let inflight_tracker = Arc::clone(&inflight_tracker);
                    let request_semaphore = Arc::clone(&request_semaphore);

                    tokio::spawn(async move {
                        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                            let config = Arc::clone(&config);
                            let cache_manager = Arc::clone(&cache_manager);
                            let range_handler = Arc::clone(&range_handler);
                            let s3_client = Arc::clone(&s3_client);
                            let inflight_tracker = Arc::clone(&inflight_tracker);
                            let request_semaphore = Arc::clone(&request_semaphore);
                            let inflight_ledger = s3_client.get_inflight_ledger();
                            async move {
                                HttpProxy::handle_request(
                                    req,
                                    peer,
                                    config,
                                    cache_manager,
                                    s3_client,
                                    range_handler,
                                    request_semaphore,
                                    None,
                                    None,
                                    inflight_tracker,
                                    None,
                                    None,
                                    None,
                                    inflight_ledger,
                                )
                                .await
                            }
                        });
                        if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                            eprintln!("proxy connection error: {}", e);
                        }
                    });
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    ProxyServer { addr, shutdown_tx }
}

/// HEAD through the proxy, returning status AND headers — the served
/// `content-length` is the value under test in several of these.
async fn proxy_head(addr: SocketAddr, path: &str, auth: &str) -> (StatusCode, HeaderMap) {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let req = Request::builder()
        .method("HEAD")
        .uri(format!("http://{}{}", addr, path))
        .header("authorization", auth)
        .body(Full::new(Bytes::new()))
        .expect("build HEAD request");

    let resp = client.request(req).await.expect("proxy HEAD failed");
    let status = resp.status();
    let headers = resp.headers().clone();
    let _ = resp.into_body().collect().await.unwrap().to_bytes();
    (status, headers)
}

fn served_content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
}

/// A whole-object HEAD response, as S3 answers a plain `HeadObject`.
fn whole_object_head() -> StubResponse {
    StubResponse::with_status(StatusCode::OK)
        .with_header("content-length", OBJECT_LEN.to_string())
        .with_header("etag", "\"whole-object-etag-10\"")
        .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
        .with_header("content-type", "application/octet-stream")
}

/// A PART-scoped HEAD response, as S3 answers `HeadObject` with `partNumber=1`:
/// the PART's `Content-Length`, a `Content-Range` naming the whole-object total,
/// and the parts count.
fn part_scoped_head() -> StubResponse {
    StubResponse::with_status(StatusCode::OK)
        .with_header("content-length", PART_LEN.to_string())
        .with_header(
            "content-range",
            format!("bytes 0-{}/{}", PART_LEN - 1, OBJECT_LEN),
        )
        .with_header("x-amz-mp-parts-count", "10")
        .with_header("etag", "\"whole-object-etag-10\"")
        .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
}

fn stub() -> StubS3Client {
    StubS3Client::new()
        .with_default(whole_object_head())
        .with_response_for_authorization(AUTH_PART, part_scoped_head())
}

fn find_meta_files_for(cache_dir: &std::path::Path, fragment: &str) -> Vec<std::path::PathBuf> {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    walk(&p, out);
                } else {
                    out.push(p);
                }
            }
        }
    }
    let mut all = Vec::new();
    walk(&cache_dir.join("metadata"), &mut all);
    all.into_iter()
        .filter(|p| {
            p.extension().map(|e| e == "meta").unwrap_or(false)
                && p.to_string_lossy().contains(fragment)
        })
        .collect()
}

fn part_scoped_requests(s: &StubS3Client, fragment: &str) -> usize {
    s.captured()
        .iter()
        .filter(|r| r.uri.contains(fragment) && r.uri.contains("partNumber=1"))
        .count()
}

/// Write a `.meta` straight to disk, bypassing every write-side guard.
///
/// Task 13 asks for the poisoned entry to be planted this way rather than by
/// running an old binary: it is deterministic, needs no pre-fix build, and it is
/// the only way to construct the second case at all, since S3 will not emit a
/// `Content-Range` with a `*` total on demand.
fn plant_meta(
    cache_manager: &CacheManager,
    cache_key: &str,
    content_length: u64,
    response_headers: HashMap<String, String>,
) {
    let now = SystemTime::now();
    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata {
            etag: "\"whole-object-etag-10\"".to_string(),
            last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            response_headers,
            ..Default::default()
        },
        ranges: Vec::new(),
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        head_expires_at: Some(now + Duration::from_secs(3600)),
        head_last_accessed: Some(now),
        head_access_count: 1,
        head_cached_at: Some(now),
    };
    let path = cache_manager.get_new_metadata_file_path(cache_key);
    std::fs::create_dir_all(path.parent().unwrap()).expect("create metadata dir");
    std::fs::write(&path, serde_json::to_string_pretty(&metadata).unwrap()).expect("plant .meta");
}

// ===========================================================================
// Change 1 — a part-scoped HEAD neither writes nor reads the whole-object entry
// ===========================================================================

/// A part-scoped HEAD must not create a cache entry for the object.
///
/// This is the poisoning itself. Before the fix a `.meta` appeared under the
/// WHOLE-OBJECT key holding the PART's `content-length`, because
/// `is_get_object_part` refuses to classify a HEAD and `generate_cache_key` drops
/// the query string.
#[tokio::test(flavor = "multi_thread")]
async fn part_scoped_head_does_not_touch_the_whole_object_entry() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = test_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let s = stub();
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s.clone().into_trait_object(),
        Arc::new(InFlightTracker::new()),
    )
    .await;

    let (status, headers) = proxy_head(
        server.addr,
        "/mybucket/mpu/poison-me.bin?partNumber=1",
        AUTH_PART,
    )
    .await;
    assert!(status.is_success(), "part-scoped HEAD should succeed");

    // Forwarded to S3, and the client gets the PART's length, which is the
    // correct answer to the question it asked.
    assert_eq!(
        part_scoped_requests(&s, "poison-me"),
        1,
        "the part-scoped HEAD must reach S3"
    );
    assert_eq!(
        served_content_length(&headers),
        Some(PART_LEN),
        "a part-scoped HEAD must report the PART's length"
    );

    let metas = find_meta_files_for(temp_dir.path(), "poison-me");
    assert!(
        metas.is_empty(),
        "a part-scoped HEAD must not create a whole-object cache entry. A .meta \
         appeared at {:?} — that entry holds a partial response's headers under \
         the whole-object key, and replaying them is what truncated later reads \
         to {} of {} bytes with HTTP 200.",
        metas,
        PART_LEN,
        OBJECT_LEN
    );

    let _ = server.shutdown_tx.send(());
}

/// The second, converse defect: with a clean whole-object entry already cached, a
/// part-scoped HEAD used to be ANSWERED from it — returning the object's length
/// and no `PartsCount`, which is simply the wrong answer to the part query.
#[tokio::test(flavor = "multi_thread")]
async fn part_scoped_head_is_not_answered_from_the_whole_object_entry() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = test_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let s = stub();
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s.clone().into_trait_object(),
        Arc::new(InFlightTracker::new()),
    )
    .await;

    let path = "/mybucket/mpu/clean-first.bin";
    // A plain HEAD first, creating a clean entry.
    let (status, headers) = proxy_head(server.addr, path, AUTH_PLAIN).await;
    assert!(status.is_success());
    assert_eq!(served_content_length(&headers), Some(OBJECT_LEN));
    assert!(
        !find_meta_files_for(temp_dir.path(), "clean-first").is_empty(),
        "pre-condition: the plain HEAD should have cached an entry"
    );

    // Now the part-scoped query for the same key.
    let (status, headers) =
        proxy_head(server.addr, &format!("{}?partNumber=1", path), AUTH_PART).await;
    assert!(status.is_success());
    assert_eq!(
        part_scoped_requests(&s, "clean-first"),
        1,
        "the part-scoped HEAD must reach S3 rather than being answered from the \
         whole-object entry — being answered from it is the second measured \
         defect, where the part query gets the object's length"
    );
    assert_eq!(
        served_content_length(&headers),
        Some(PART_LEN),
        "the part query must be answered with the PART's length"
    );
    assert_eq!(
        headers
            .get("x-amz-mp-parts-count")
            .and_then(|v| v.to_str().ok()),
        Some("10"),
        "the part-scoped response must carry its parts count, which the \
         cache-served answer could not supply"
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// Requirement 5.1 — no operator action on upgrade
//
// Both cases below plant a poisoned `.meta` by hand and prove the guarantee
// rather than reasoning about it. The second is the one change 2b alone does not
// cover, and it is why the poisoned-entry detector exists.
// ===========================================================================

/// Case 1, the OBSERVED shape: `object_metadata.content_length` is correct
/// (52,428,800, because `Content-Range`'s total parsed) while the stored
/// `content-length` header is the part's (5,242,880).
///
/// Serving the length from the object metadata heals this on the first request
/// after upgrade, with no cache flush and no config change. This is the
/// self-healing test and it is load-bearing for Requirement 5.1.
///
/// Note the planted entry deliberately carries NO `content-range`, so the
/// poisoned-entry detector does not fire and the entry really is served from
/// cache — which is what makes this a test of change 2b specifically rather than
/// of the detector.
#[tokio::test(flavor = "multi_thread")]
async fn head_serve_takes_content_length_from_object_metadata() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = test_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let s = stub();
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s.clone().into_trait_object(),
        Arc::new(InFlightTracker::new()),
    )
    .await;

    let cache_key = "mybucket/legacy/disagreeing-header.bin";
    plant_meta(
        &cache_manager,
        cache_key,
        OBJECT_LEN, // correct object length
        HashMap::from([
            // ...but the stored header is the PART's length.
            ("content-length".to_string(), PART_LEN.to_string()),
            (
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            ),
            ("etag".to_string(), "\"whole-object-etag-10\"".to_string()),
        ]),
    );

    let before = s.captured().len();
    let (status, headers) = proxy_head(
        server.addr,
        "/mybucket/legacy/disagreeing-header.bin",
        AUTH_PLAIN,
    )
    .await;
    assert!(status.is_success());
    assert_eq!(
        s.captured().len(),
        before,
        "pre-condition: this entry must be served FROM CACHE, or the test is \
         measuring a fresh S3 response instead of the healing"
    );
    assert_eq!(
        served_content_length(&headers),
        Some(OBJECT_LEN),
        "a HEAD served from a legacy entry must report the OBJECT's length from \
         object metadata, not the disagreeing stored header. Reporting {} here is \
         the truncation a client cannot detect.",
        PART_LEN
    );
    assert!(
        !headers.contains_key("content-range"),
        "a cached content-range must never be replayed onto a whole-object response"
    );

    let _ = server.shutdown_tx.send(());
}

/// Case 2, the LATENT shape, and the reason change 3 exists:
/// `object_metadata.content_length` is **also** wrong.
///
/// A poisoned entry's `content_length` is correct only because
/// `parse_content_range_total_size` found a numeric total. A `Content-Range` whose
/// total is `*` yields no total, leaving `content_length` holding the PART's
/// length — a genuinely poisoned value that serving from object metadata would
/// faithfully serve. S3 does not send `*` for a part HEAD today, so this is
/// latent rather than observed, but "no operator action on upgrade" cannot be a
/// guarantee if it rests on that staying true.
///
/// The stored `content-range` is the fingerprint no correct whole-object entry
/// carries, so the entry is detected, treated as a miss, revalidated against S3
/// and rewritten clean.
#[tokio::test(flavor = "multi_thread")]
async fn poisoned_entry_with_wrong_object_length_is_detected_and_repaired() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = test_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let s = stub();
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s.clone().into_trait_object(),
        Arc::new(InFlightTracker::new()),
    )
    .await;

    let cache_key = "mybucket/legacy/star-total.bin";
    plant_meta(
        &cache_manager,
        cache_key,
        PART_LEN, // the part's length, stored as the OBJECT's — genuinely poisoned
        HashMap::from([
            ("content-length".to_string(), PART_LEN.to_string()),
            // `*` total: this is what leaves content_length holding the part's length.
            ("content-range".to_string(), "bytes 0-5242879/*".to_string()),
            ("x-amz-mp-parts-count".to_string(), "10".to_string()),
            ("etag".to_string(), "\"whole-object-etag-10\"".to_string()),
        ]),
    );

    let before = s.captured().len();
    let (status, headers) =
        proxy_head(server.addr, "/mybucket/legacy/star-total.bin", AUTH_PLAIN).await;
    assert!(status.is_success());

    assert!(
        s.captured().len() > before,
        "an entry carrying the poisoning fingerprint must be treated as a MISS and \
         revalidated against S3. Serving it from cache would faithfully return the \
         part's length as the object's, which change 2b alone cannot fix because \
         the stored object length is itself wrong."
    );
    assert_eq!(
        served_content_length(&headers),
        Some(OBJECT_LEN),
        "after repair the client must see the object's true length"
    );

    // The rewritten entry is clean, so the next read is a normal cache hit rather
    // than a permanent revalidation loop.
    let path = cache_manager.get_new_metadata_file_path(cache_key);
    let repaired: NewCacheMetadata =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        repaired.object_metadata.content_length, OBJECT_LEN,
        "the repaired entry must hold the object's true length"
    );
    for banned in ["content-range", "x-amz-mp-parts-count", "content-length"] {
        assert!(
            !repaired
                .object_metadata
                .response_headers
                .keys()
                .any(|k| k.eq_ignore_ascii_case(banned)),
            "the repaired entry must not carry '{}', or it is detected again on \
             every subsequent read and never converges. Stored: {:?}",
            banned,
            repaired.object_metadata.response_headers
        );
    }

    let after_repair = s.captured().len();
    let (status, headers) =
        proxy_head(server.addr, "/mybucket/legacy/star-total.bin", AUTH_PLAIN).await;
    assert!(status.is_success());
    assert_eq!(
        s.captured().len(),
        after_repair,
        "the repair must CONVERGE: the second read is a cache hit, not another \
         revalidation. A repair that never stops firing is a cache-hit-rate \
         regression with no obvious cause."
    );
    assert_eq!(served_content_length(&headers), Some(OBJECT_LEN));

    let _ = server.shutdown_tx.send(());
}

/// An entry legitimately populated by a part GET must NOT be flagged as poisoned
/// (task 10a).
///
/// This is the interaction that would otherwise be a comment rather than
/// coverage. `store_part_as_range` writes under the whole-object key from a part
/// GET response, which carries exactly the headers the detector looks for. That
/// path is CORRECT for length — its `content_length` comes from `Content-Range`'s
/// total, which is the whole object — so flagging it would not be a correctness
/// bug, but it would revalidate a healthy entry on every read: a cache-hit-rate
/// regression with no obvious cause. The false positive is removed at the source
/// by stripping the headers, rather than by special-casing the detector.
///
/// It lives here rather than in the `src/cache.rs` test module because
/// `store_part_as_range` reaches the disk cache and needs the shared-storage
/// wiring that only `new_with_shared_storage` provides.
///
/// **The base entry is planted ALREADY CARRYING the fingerprint headers, and that
/// is deliberate rather than convenient.** A part store on a brand-new key writes
/// its metadata through the journal, so the `.meta` does not exist until
/// consolidation runs — polling for it made this test flaky-by-construction, and
/// waiting on the background consolidation interval would make it slow and still
/// timing-dependent. Planting the base entry removes that dependency, and
/// planting it POISONED makes the test strictly stronger: it now fails if the part
/// store either introduces the fingerprint or merely PRESERVES one an earlier
/// release left behind. A test that planted a clean base entry could pass without
/// the part store's metadata ever reaching disk, which would be decoration.
#[tokio::test(flavor = "multi_thread")]
async fn part_get_populated_entry_is_not_flagged_as_poisoned() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = test_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, _range_handler) = make_cache_infra(&config).await;
    let cache_key = "mybucket/parts/part-get-populated.bin";

    // A base entry as a pre-fix release would have left it.
    plant_meta(
        &cache_manager,
        cache_key,
        4096,
        HashMap::from([
            ("content-length".to_string(), "1024".to_string()),
            ("content-range".to_string(), "bytes 0-1023/4096".to_string()),
            ("x-amz-mp-parts-count".to_string(), "4".to_string()),
            (
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            ),
        ]),
    );

    let part_headers = HashMap::from([
        ("content-length".to_string(), "1024".to_string()),
        ("content-range".to_string(), "bytes 0-1023/4096".to_string()),
        ("x-amz-mp-parts-count".to_string(), "4".to_string()),
        ("etag".to_string(), "\"part-etag\"".to_string()),
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
    ]);
    cache_manager
        .store_part_as_range(
            cache_key,
            1,
            "bytes 0-1023/4096",
            &part_headers,
            &[7u8; 1024],
        )
        .await
        .expect("store_part_as_range should succeed");

    let metadata = cache_manager
        .get_metadata_from_disk(cache_key)
        .await
        .expect("read metadata")
        .expect("the planted base entry must be readable");

    assert!(
        !CacheManager::is_part_scoped_entry(&metadata.object_metadata),
        "a part-GET-populated entry must not carry the poisoning fingerprint — \
         neither introduced by the part store nor left over from a pre-fix release \
         — or the detector revalidates a healthy entry on every read forever. \
         Stored headers: {:?}",
        metadata.object_metadata.response_headers
    );
    assert_eq!(
        metadata.object_metadata.content_length, 4096,
        "the part-GET path takes the whole-object length from Content-Range's \
         total, and stripping the stored headers must not disturb that"
    );
    assert_eq!(
        metadata.object_metadata.parts_count,
        Some(4),
        "the part count survives as a typed field, so stripping the header loses \
         no information"
    );
}
